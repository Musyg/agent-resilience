"""Unit tests for offline MQTT buffering and immediate publication."""

import json

import agent_resilience.mqtt_buffer as mqtt_buffer_module
from agent_resilience import MQTTMessageBuffer, ResilientMQTTPublisher


class FakeMQTTClient:
    def __init__(self, connected: bool, error: Exception | None = None):
        self.is_connected = connected
        self._client = self
        self.error = error
        self.published: list[tuple[str, str, int, bool]] = []

    def publish(self, topic: str, payload: str, qos: int, retain: bool) -> None:
        if self.error:
            raise self.error
        self.published.append((topic, payload, qos, retain))


def test_buffer_is_bounded_and_discards_oldest():
    buffer = MQTTMessageBuffer(max_size=2)

    buffer.add("topic/1", {"n": 1})
    buffer.add("topic/2", {"n": 2})
    buffer.add("topic/3", {"n": 3})

    pending = buffer.get_pending()
    assert [message["topic"] for message in pending] == ["topic/2", "topic/3"]
    assert buffer.size() == 0


def test_expired_messages_are_dropped(monkeypatch):
    now = [100.0]
    monkeypatch.setattr(mqtt_buffer_module.time, "time", lambda: now[0])
    buffer = MQTTMessageBuffer(max_age_seconds=10)

    buffer.add("expired", {"n": 1})
    now[0] = 111.0

    assert buffer.get_pending() == []


def test_offline_publish_is_buffered():
    client = FakeMQTTClient(connected=False)
    publisher = ResilientMQTTPublisher(client)

    assert not publisher.publish_critical("agents/heartbeat", {"ok": True})
    assert publisher.buffer.size() == 1
    assert client.published == []


def test_connected_publish_is_immediate():
    client = FakeMQTTClient(connected=True)
    publisher = ResilientMQTTPublisher(client)

    assert publisher.publish_critical("agents/heartbeat", {"ok": True}, retain=True)

    assert publisher.buffer.size() == 0
    topic, payload, qos, retain = client.published[0]
    assert topic == "agents/heartbeat"
    assert json.loads(payload) == {"ok": True}
    assert qos == 1
    assert retain is True


def test_publish_value_error_falls_back_to_buffer():
    client = FakeMQTTClient(connected=True, error=ValueError("broker rejected message"))
    publisher = ResilientMQTTPublisher(client)

    assert not publisher.publish_critical("agents/heartbeat", {"ok": False})
    assert publisher.buffer.size() == 1
