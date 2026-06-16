"""
MQTT buffer - offline message buffering for guaranteed delivery.

Wraps an MQTT client so that messages published while the broker is
unreachable are buffered in memory and flushed automatically once the
connection is back. Expired messages (older than ``max_age_seconds``) are
dropped on flush.

    from agent_resilience import ResilientMQTTPublisher

    publisher = ResilientMQTTPublisher(mqtt_client)
    publisher.start()
    publisher.publish_critical("agents/heartbeat", {"id": "worker-1", "ok": True})

The wrapped client is expected to expose ``is_connected`` and an inner
``_client.publish(topic, payload, qos, retain)`` (paho-mqtt style). Adapt the
two call sites if your client differs.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from collections import deque
from typing import Dict, Optional

logger = logging.getLogger(__name__)

__all__ = ["MQTTMessageBuffer", "ResilientMQTTPublisher", "create_resilient_publisher"]


class MQTTMessageBuffer:
    """Thread-safe bounded buffer for offline message queuing."""

    def __init__(self, max_size: int = 1000, max_age_seconds: int = 3600):
        self._queue: deque = deque(maxlen=max_size)
        self._lock = threading.Lock()
        self.max_age = max_age_seconds

    def add(self, topic: str, payload: Dict, qos: int = 1, retain: bool = False) -> None:
        """Buffer a message for later delivery."""
        with self._lock:
            self._queue.append(
                {"topic": topic, "payload": payload, "qos": qos, "retain": retain, "timestamp": time.time()}
            )

    def get_pending(self) -> list:
        """Drain pending messages, dropping any that have expired."""
        with self._lock:
            now = time.time()
            valid = []
            while self._queue:
                msg = self._queue.popleft()
                if now - msg["timestamp"] < self.max_age:
                    valid.append(msg)
            return valid

    def size(self) -> int:
        """Return the number of buffered messages."""
        with self._lock:
            return len(self._queue)


class ResilientMQTTPublisher:
    """MQTT publisher with offline buffering for critical messages."""

    def __init__(self, mqtt_client, buffer_size: int = 1000):
        self.client = mqtt_client
        self.buffer = MQTTMessageBuffer(max_size=buffer_size)
        self._flush_thread: Optional[threading.Thread] = None
        self._running = False

    def start(self) -> None:
        """Start the background flush thread."""
        self._running = True
        self._flush_thread = threading.Thread(target=self._flush_loop, daemon=True, name="mqtt_buffer_flush")
        self._flush_thread.start()

    def stop(self) -> None:
        """Stop the background flush thread."""
        self._running = False

    def publish_critical(self, topic: str, payload: Dict, retain: bool = False) -> bool:
        """Publish now if connected, otherwise buffer. Returns True if sent immediately."""
        if self.client and getattr(self.client, "is_connected", False):
            try:
                self.client._client.publish(topic, json.dumps(payload, default=str), qos=1, retain=retain)
                return True
            except (json.JSONDecodeError, ValueError) as e:
                logger.warning("publish failed, buffering: %s", e)
        self.buffer.add(topic, payload, qos=1, retain=retain)
        return False

    def _flush_loop(self) -> None:
        """Background loop that flushes buffered messages when connected."""
        while self._running:
            try:
                if self.client and getattr(self.client, "is_connected", False):
                    pending = self.buffer.get_pending()
                    if pending:
                        logger.info("flushing %d buffered messages", len(pending))
                        for msg in pending:
                            try:
                                payload = json.dumps(msg["payload"], default=str)
                                self.client._client.publish(
                                    msg["topic"], payload, qos=msg["qos"], retain=msg["retain"]
                                )
                                time.sleep(0.01)  # small gap between messages
                            except (json.JSONDecodeError, ValueError) as e:
                                logger.error("flush error: %s", e)
                                self.buffer.add(msg["topic"], msg["payload"], msg["qos"], msg["retain"])
                time.sleep(5)
            except (json.JSONDecodeError, ValueError) as e:
                logger.error("flush loop error: %s", e)
                time.sleep(10)


def create_resilient_publisher(mqtt_client, buffer_size: int = 1000) -> Optional[ResilientMQTTPublisher]:
    """Create and start a resilient publisher. Returns None if no client is given."""
    if not mqtt_client:
        return None
    publisher = ResilientMQTTPublisher(mqtt_client, buffer_size)
    publisher.start()
    return publisher
