import pytest

from ccgram.session_protocol.events import EventHub, ResyncRequiredError


def test_resume_up_to_date_empty_hub_and_future_cursor():
    hub = EventHub()
    assert hub.replay(hub.epoch, 0) == (True, [])
    assert hub.replay(hub.epoch, 1) == (False, [])
    assert hub.replay("old", 0) == (False, [])
    assert hub.replay(hub.epoch, -1) == (False, [])


def test_ring_eviction_and_ephemeral_events():
    hub = EventHub(capacity=2)
    first = hub.publish("session.updated", "s", {"bindingVersion": 1})
    tick = hub.publish(
        "status.tick", "s", {"state": "idle", "detail": ""}, durable=False
    )
    second = hub.publish("session.updated", "s", {"bindingVersion": 2})
    third = hub.publish("session.updated", "s", {"bindingVersion": 3})
    assert first.seq == 1 and second.seq == 2 and third.seq == 3
    assert "seq" not in tick.body()
    assert hub.replay(hub.epoch, 0) == (False, [])
    assert hub.replay(hub.epoch, 1) == (True, [second, third])


def test_age_eviction_at_ten_minutes():
    now = [0.0]
    hub = EventHub(clock=lambda: now[0])
    hub.publish("message.upsert", "s", {})
    now[0] = 600
    assert hub.replay(hub.epoch, 0) == (False, [])
    assert hub.replay(hub.epoch, 1) == (True, [])


async def test_slow_subscriber_requires_resync_without_blocking_another():
    hub = EventHub(queue_capacity=1)
    slow = hub.subscribe()
    fast = hub.subscribe(["wanted"])
    hub.publish("message.upsert", "wanted", {"text": "first"})
    assert (await fast.get()).data == {"text": "first"}
    hub.publish("message.upsert", "wanted", {"text": "second"})
    with pytest.raises(ResyncRequiredError):
        await slow.get()
    assert (await fast.get()).data == {"text": "second"}
    hub.unsubscribe(slow)
    hub.unsubscribe(fast)


def test_published_payload_is_immutable_snapshot():
    hub = EventHub()
    data = {"items": [{"text": "before"}]}
    event = hub.publish("test", "s", data)
    data["items"][0]["text"] = "after"
    assert event.data["items"][0]["text"] == "before"
