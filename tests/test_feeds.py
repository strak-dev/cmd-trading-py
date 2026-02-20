"""
tests/test_feeds.py

Tests for the feeds layer: store, bus, and finnhub adapter.
Run with: pytest tests/test_feeds.py -v
"""

import asyncio
import json
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock

from cmd_slack.feeds.store import Tick, TickHistory, SymbolStore
from cmd_slack.feeds.bus import Bus, Event, EventType
from cmd_slack.feeds.finnhub import FinnhubFeed, MockFeed, _normalize_trade
from cmd_slack.core.engine import Engine, TickResult
from cmd_slack.core.sequence import SequenceState
from cmd_slack.format.schema import SetupConfig, Step, Condition, ConditionType


# ============================================================
# Helpers
# ============================================================

def make_tick(
    symbol="AAPL",
    price=100.0,
    volume=10_000.0,
    source="test",
) -> Tick:
    return Tick.now(symbol=symbol, price=price, volume=volume, source=source)


def make_condition(ctype: ConditionType, target: float) -> Condition:
    return Condition(condition=ctype, target=target)


def make_step(id: str, ctype: ConditionType, target: float) -> Step:
    return Step(id=id, condition=ctype, target=target)


def make_config(steps, symbol="AAPL", name="test_setup", invalidation=None):
    return SetupConfig(symbol=symbol, name=name, steps=steps, invalidation=invalidation)


def simple_config(symbol="AAPL") -> SetupConfig:
    """Single-step setup: price above 100."""
    return make_config(
        steps=[make_step("break", ConditionType.PRICE_ABOVE, 100.0)],
        symbol=symbol,
    )


def three_step_config(symbol="AAPL") -> SetupConfig:
    return make_config(
        steps=[
            make_step("break",    ConditionType.PRICE_ABOVE, 100.0),
            make_step("extend",   ConditionType.PRICE_ABOVE, 105.0),
            make_step("pullback", ConditionType.PRICE_BELOW, 103.0),
        ],
        symbol=symbol,
    )


def make_engine_and_bus(config: SetupConfig | None = None):
    engine = Engine()
    store = SymbolStore()
    bus = Bus(engine=engine, store=store)
    if config:
        engine.activate(config)
        store.subscribe(config.symbol)
    return engine, store, bus


# ============================================================
# Tick
# ============================================================

class TestTick:
    def test_now_factory(self):
        tick = Tick.now("AAPL", 150.25, volume=5000, source="finnhub")
        assert tick.symbol == "AAPL"
        assert tick.price == 150.25
        assert tick.volume == 5000
        assert tick.source == "finnhub"
        assert tick.timestamp.tzinfo is not None

    def test_frozen(self):
        tick = make_tick()
        with pytest.raises(Exception):
            tick.price = 999.0  # frozen dataclass

    def test_default_volume(self):
        tick = Tick.now("AAPL", 100.0)
        assert tick.volume == 0.0


# ============================================================
# TickHistory
# ============================================================

class TestTickHistory:
    def test_append_and_len(self):
        h = TickHistory("AAPL")
        h.append(make_tick(price=100.0))
        h.append(make_tick(price=101.0))
        assert len(h) == 2

    def test_latest_single(self):
        h = TickHistory("AAPL")
        h.append(make_tick(price=100.0))
        h.append(make_tick(price=101.0))
        latest = h.latest(1)
        assert len(latest) == 1
        assert latest[0].price == 101.0

    def test_latest_multiple(self):
        h = TickHistory("AAPL")
        for p in [100.0, 101.0, 102.0, 103.0]:
            h.append(make_tick(price=p))
        last3 = h.latest(3)
        assert [t.price for t in last3] == [101.0, 102.0, 103.0]

    def test_maxlen_evicts_oldest(self):
        h = TickHistory("AAPL", maxlen=3)
        for p in [1.0, 2.0, 3.0, 4.0]:
            h.append(make_tick(price=p))
        assert len(h) == 3
        assert h.all()[0].price == 2.0  # 1.0 evicted

    def test_last_price_empty(self):
        h = TickHistory("AAPL")
        assert h.last_price() is None

    def test_last_price(self):
        h = TickHistory("AAPL")
        h.append(make_tick(price=99.0))
        h.append(make_tick(price=101.5))
        assert h.last_price() == 101.5

    def test_price_range(self):
        h = TickHistory("AAPL")
        for p in [100.0, 95.0, 110.0, 105.0]:
            h.append(make_tick(price=p))
        lo, hi = h.price_range(4)
        assert lo == 95.0
        assert hi == 110.0

    def test_price_range_empty(self):
        h = TickHistory("AAPL")
        assert h.price_range() == (0.0, 0.0)


# ============================================================
# SymbolStore
# ============================================================

class TestSymbolStore:
    def test_subscribe_new(self):
        store = SymbolStore()
        is_new = store.subscribe("AAPL")
        assert is_new is True
        assert store.is_subscribed("AAPL")

    def test_subscribe_duplicate(self):
        store = SymbolStore()
        store.subscribe("AAPL")
        is_new = store.subscribe("AAPL")
        assert is_new is False

    def test_unsubscribe_existing(self):
        store = SymbolStore()
        store.subscribe("AAPL")
        removed = store.unsubscribe("AAPL")
        assert removed is True
        assert not store.is_subscribed("AAPL")

    def test_unsubscribe_nonexistent(self):
        store = SymbolStore()
        removed = store.unsubscribe("NVDA")
        assert removed is False

    def test_record_tick_updates_history(self):
        store = SymbolStore()
        store.subscribe("AAPL")
        store.record_tick(make_tick("AAPL", price=150.0))
        assert store.last_price("AAPL") == 150.0

    def test_record_tick_ignored_for_unsubscribed(self):
        store = SymbolStore()
        # not subscribed — no history entry created
        store.record_tick(make_tick("AAPL", price=150.0))
        assert store.last_price("AAPL") is None

    def test_last_price_no_ticks(self):
        store = SymbolStore()
        store.subscribe("AAPL")
        assert store.last_price("AAPL") is None

    def test_subscribed_symbols(self):
        store = SymbolStore()
        store.subscribe("AAPL")
        store.subscribe("NVDA")
        assert store.subscribed_symbols() == {"AAPL", "NVDA"}

    def test_len(self):
        store = SymbolStore()
        store.subscribe("AAPL")
        store.subscribe("NVDA")
        assert len(store) == 2

    def test_history_returns_none_for_unknown(self):
        store = SymbolStore()
        assert store.history("AAPL") is None

    def test_history_after_subscribe(self):
        store = SymbolStore()
        store.subscribe("AAPL")
        assert store.history("AAPL") is not None

    def test_unsubscribe_preserves_history(self):
        # History is kept after unsubscribe (for context window, LLM use)
        store = SymbolStore()
        store.subscribe("AAPL")
        store.record_tick(make_tick("AAPL", price=150.0))
        store.unsubscribe("AAPL")
        assert store.history("AAPL") is not None
        assert store.last_price("AAPL") == 150.0


# ============================================================
# Bus
# ============================================================

class TestBus:
    @pytest.mark.asyncio
    async def test_publish_records_tick_in_store(self):
        _, store, bus = make_engine_and_bus()
        store.subscribe("AAPL")
        await bus.publish(make_tick("AAPL", price=99.0))
        assert store.last_price("AAPL") == 99.0

    @pytest.mark.asyncio
    async def test_tick_received_handler_called(self):
        _, store, bus = make_engine_and_bus()
        store.subscribe("AAPL")
        received = []

        async def handler(event: Event):
            received.append(event)

        bus.on(EventType.TICK_RECEIVED, handler)
        await bus.publish(make_tick("AAPL", price=99.0))
        assert len(received) == 1
        assert received[0].type == EventType.TICK_RECEIVED
        assert received[0].payload["tick"].price == 99.0

    @pytest.mark.asyncio
    async def test_step_advanced_event_fires(self):
        engine, store, bus = make_engine_and_bus(three_step_config())
        advanced = []

        async def handler(event: Event):
            advanced.append(event)

        bus.on(EventType.STEP_ADVANCED, handler)
        await bus.publish(make_tick("AAPL", price=101.0))  # clears step 0
        assert len(advanced) == 1
        assert advanced[0].payload["progress"] == "1/3"

    @pytest.mark.asyncio
    async def test_setup_completed_event_fires(self):
        engine, store, bus = make_engine_and_bus(simple_config())
        completed = []

        async def handler(event: Event):
            completed.append(event)

        bus.on(EventType.SETUP_COMPLETED, handler)
        await bus.publish(make_tick("AAPL", price=101.0))
        assert len(completed) == 1
        seq: SequenceState = completed[0].payload["sequence"]
        assert seq.complete is True
        assert seq.symbol == "AAPL"

    @pytest.mark.asyncio
    async def test_setup_completed_includes_step_prices(self):
        engine, store, bus = make_engine_and_bus(three_step_config())
        completed = []

        async def handler(event: Event):
            completed.append(event)

        bus.on(EventType.SETUP_COMPLETED, handler)
        await bus.publish(make_tick("AAPL", price=101.0))
        await bus.publish(make_tick("AAPL", price=106.0))
        await bus.publish(make_tick("AAPL", price=102.0))

        assert len(completed) == 1
        assert completed[0].payload["step_prices"] == [101.0, 106.0, 102.0]

    @pytest.mark.asyncio
    async def test_setup_invalidated_event_fires(self):
        inv = make_condition(ConditionType.PRICE_BELOW, 95.0)
        config = make_config(
            steps=[make_step("break", ConditionType.PRICE_ABOVE, 100.0)],
            invalidation=inv,
        )
        engine, store, bus = make_engine_and_bus(config)
        invalidated = []

        async def handler(event: Event):
            invalidated.append(event)

        bus.on(EventType.SETUP_INVALIDATED, handler)
        await bus.publish(make_tick("AAPL", price=94.0))
        assert len(invalidated) == 1

    @pytest.mark.asyncio
    async def test_no_event_on_no_change(self):
        engine, store, bus = make_engine_and_bus(simple_config())
        events = []

        for et in EventType:
            async def handler(event: Event, _et=et):
                if event.type != EventType.TICK_RECEIVED:
                    events.append(event)
            bus.on(et, handler)

        await bus.publish(make_tick("AAPL", price=99.0))  # below threshold, no step change
        assert len(events) == 0

    @pytest.mark.asyncio
    async def test_multiple_handlers_same_event(self):
        _, store, bus = make_engine_and_bus()
        store.subscribe("AAPL")
        calls = []

        async def h1(event): calls.append("h1")
        async def h2(event): calls.append("h2")

        bus.on(EventType.TICK_RECEIVED, h1)
        bus.on(EventType.TICK_RECEIVED, h2)
        await bus.publish(make_tick("AAPL"))
        assert calls == ["h1", "h2"]

    @pytest.mark.asyncio
    async def test_off_removes_handler(self):
        _, store, bus = make_engine_and_bus()
        store.subscribe("AAPL")
        calls = []

        async def handler(event): calls.append(event)

        bus.on(EventType.TICK_RECEIVED, handler)
        bus.off(EventType.TICK_RECEIVED, handler)
        await bus.publish(make_tick("AAPL"))
        assert calls == []

    @pytest.mark.asyncio
    async def test_failing_handler_does_not_break_others(self):
        _, store, bus = make_engine_and_bus()
        store.subscribe("AAPL")
        results = []

        async def bad_handler(event):
            raise RuntimeError("handler exploded")

        async def good_handler(event):
            results.append("ok")

        bus.on(EventType.TICK_RECEIVED, bad_handler)
        bus.on(EventType.TICK_RECEIVED, good_handler)

        # Should not raise, good_handler still runs
        await bus.publish(make_tick("AAPL"))
        assert results == ["ok"]

    @pytest.mark.asyncio
    async def test_emit_system_event(self):
        _, store, bus = make_engine_and_bus()
        system_events = []

        async def handler(event): system_events.append(event)

        bus.on(EventType.FEED_CONNECTED, handler)
        await bus.emit_system(EventType.FEED_CONNECTED, host="finnhub")
        assert len(system_events) == 1
        assert system_events[0].payload["host"] == "finnhub"

    @pytest.mark.asyncio
    async def test_publish_returns_tick_result(self):
        engine, store, bus = make_engine_and_bus(simple_config())
        result = await bus.publish(make_tick("AAPL", price=101.0))
        assert isinstance(result, TickResult)
        assert len(result.completed) == 1

    @pytest.mark.asyncio
    async def test_multiple_symbols_independent_events(self):
        engine = Engine()
        store = SymbolStore()
        bus = Bus(engine=engine, store=store)

        engine.activate(simple_config("AAPL"))
        engine.activate(simple_config("NVDA"))
        store.subscribe("AAPL")
        store.subscribe("NVDA")

        completed = []
        async def handler(event): completed.append(event.payload["sequence"].symbol)
        bus.on(EventType.SETUP_COMPLETED, handler)

        await bus.publish(make_tick("AAPL", price=101.0))
        await bus.publish(make_tick("NVDA", price=101.0))

        assert sorted(completed) == ["AAPL", "NVDA"]


# ============================================================
# Finnhub normalization
# ============================================================

class TestNormalizeTrade:
    def test_valid_trade(self):
        raw = {"s": "AAPL", "p": 150.25, "v": 100, "t": 1_700_000_000_000}
        tick = _normalize_trade(raw)
        assert tick is not None
        assert tick.symbol == "AAPL"
        assert tick.price == 150.25
        assert tick.volume == 100.0
        assert tick.source == "finnhub"

    def test_missing_symbol(self):
        raw = {"p": 150.25, "v": 100, "t": 1_700_000_000_000}
        assert _normalize_trade(raw) is None

    def test_zero_price_rejected(self):
        raw = {"s": "AAPL", "p": 0.0, "v": 100, "t": 1_700_000_000_000}
        assert _normalize_trade(raw) is None

    def test_negative_price_rejected(self):
        raw = {"s": "AAPL", "p": -1.0, "v": 100, "t": 1_700_000_000_000}
        assert _normalize_trade(raw) is None

    def test_missing_volume_defaults_to_zero(self):
        raw = {"s": "AAPL", "p": 150.0, "t": 1_700_000_000_000}
        tick = _normalize_trade(raw)
        assert tick is not None
        assert tick.volume == 0.0

    def test_malformed_price(self):
        raw = {"s": "AAPL", "p": "bad", "v": 100, "t": 1_700_000_000_000}
        assert _normalize_trade(raw) is None

    def test_timestamp_conversion(self):
        # 1_700_000_000_000 ms = 1_700_000_000 seconds
        raw = {"s": "AAPL", "p": 150.0, "v": 100, "t": 1_700_000_000_000}
        tick = _normalize_trade(raw)
        assert tick is not None
        expected = datetime.fromtimestamp(1_700_000_000, tz=timezone.utc)
        assert tick.timestamp == expected


# ============================================================
# FinnhubFeed — message handling (no live socket)
# ============================================================

class TestFinnhubFeedMessageHandling:
    """
    Tests the message handling logic in isolation — we test _handle_message
    directly without spinning up a real WebSocket connection.
    """

    def _make_feed(self, config=None) -> tuple[FinnhubFeed, Bus, SymbolStore]:
        engine = Engine()
        store = SymbolStore()
        bus = Bus(engine=engine, store=store)
        if config:
            engine.activate(config)
            store.subscribe(config.symbol)
        feed = FinnhubFeed(api_key="test_key", bus=bus, store=store)
        # Inject a mock websocket so subscribe/unsubscribe don't error
        feed._ws = AsyncMock()
        feed._ws.closed = False
        return feed, bus, store

    @pytest.mark.asyncio
    async def test_handle_trade_message_publishes_tick(self):
        feed, bus, store = self._make_feed()
        store.subscribe("AAPL")
        received = []

        async def handler(event): received.append(event)
        bus.on(EventType.TICK_RECEIVED, handler)

        msg = json.dumps({
            "type": "trade",
            "data": [{"s": "AAPL", "p": 150.25, "v": 100, "t": 1_700_000_000_000}]
        })
        await feed._handle_message(msg)

        assert len(received) == 1
        assert received[0].payload["tick"].price == 150.25

    @pytest.mark.asyncio
    async def test_handle_trade_bundles_multiple_ticks(self):
        feed, bus, store = self._make_feed()
        store.subscribe("AAPL")
        received = []

        async def handler(event): received.append(event)
        bus.on(EventType.TICK_RECEIVED, handler)

        msg = json.dumps({
            "type": "trade",
            "data": [
                {"s": "AAPL", "p": 150.00, "v": 100, "t": 1_700_000_000_000},
                {"s": "AAPL", "p": 150.25, "v": 200, "t": 1_700_000_000_001},
            ]
        })
        await feed._handle_message(msg)
        assert len(received) == 2

    @pytest.mark.asyncio
    async def test_unsubscribed_symbol_ignored(self):
        feed, bus, store = self._make_feed()
        # NVDA not subscribed
        received = []

        async def handler(event): received.append(event)
        bus.on(EventType.TICK_RECEIVED, handler)

        msg = json.dumps({
            "type": "trade",
            "data": [{"s": "NVDA", "p": 500.0, "v": 100, "t": 1_700_000_000_000}]
        })
        await feed._handle_message(msg)
        assert len(received) == 0

    @pytest.mark.asyncio
    async def test_ping_sends_pong(self):
        feed, bus, store = self._make_feed()
        await feed._handle_message(json.dumps({"type": "ping"}))
        feed._ws.send.assert_called_once_with(json.dumps({"type": "pong"}))

    @pytest.mark.asyncio
    async def test_bad_json_does_not_raise(self):
        feed, bus, store = self._make_feed()
        await feed._handle_message("not json at all {{{")  # should not raise

    @pytest.mark.asyncio
    async def test_error_message_logged(self):
        feed, bus, store = self._make_feed()
        msg = json.dumps({"type": "error", "msg": "invalid token"})
        # Should not raise
        await feed._handle_message(msg)

    @pytest.mark.asyncio
    async def test_subscribe_queued_when_not_connected(self):
        engine = Engine()
        store = SymbolStore()
        bus = Bus(engine=engine, store=store)
        feed = FinnhubFeed(api_key="key", bus=bus, store=store)
        # _ws is None — not connected

        feed.subscribe("AAPL")
        assert "AAPL" in feed._pending_subscribes
        assert store.is_subscribed("AAPL")

    @pytest.mark.asyncio
    async def test_subscribe_sent_immediately_when_connected(self):
        feed, bus, store = self._make_feed()
        # Give it a moment for the task to run
        feed.subscribe("AAPL")
        await asyncio.sleep(0)  # yield to event loop so task fires
        feed._ws.send.assert_called()
        sent = json.loads(feed._ws.send.call_args[0][0])
        assert sent["type"] == "subscribe"
        assert sent["symbol"] == "AAPL"

    @pytest.mark.asyncio
    async def test_unsubscribe_queued_when_not_connected(self):
        engine = Engine()
        store = SymbolStore()
        bus = Bus(engine=engine, store=store)
        feed = FinnhubFeed(api_key="key", bus=bus, store=store)
        store.subscribe("AAPL")  # must be subscribed first to unsubscribe
        feed.unsubscribe("AAPL")
        assert "AAPL" in feed._pending_unsubscribes


# ============================================================
# MockFeed
# ============================================================

class TestMockFeed:
    @pytest.mark.asyncio
    async def test_mock_feed_publishes_all_ticks(self):
        engine, store, bus = make_engine_and_bus()
        store.subscribe("AAPL")
        received = []

        async def handler(event): received.append(event.payload["tick"])
        bus.on(EventType.TICK_RECEIVED, handler)

        ticks = [("AAPL", 99.0), ("AAPL", 101.0), ("AAPL", 102.0)]
        feed = MockFeed(ticks, bus=bus, store=store, delay=0)
        await feed.run()

        assert len(received) == 3
        assert [t.price for t in received] == [99.0, 101.0, 102.0]

    @pytest.mark.asyncio
    async def test_mock_feed_emits_connect_disconnect(self):
        engine, store, bus = make_engine_and_bus()
        store.subscribe("AAPL")
        system_events = []

        async def handler(event): system_events.append(event.type)
        bus.on(EventType.FEED_CONNECTED, handler)
        bus.on(EventType.FEED_DISCONNECTED, handler)

        feed = MockFeed([], bus=bus, store=store, delay=0)
        await feed.run()

        assert EventType.FEED_CONNECTED in system_events
        assert EventType.FEED_DISCONNECTED in system_events

    @pytest.mark.asyncio
    async def test_mock_feed_triggers_setup_completion(self):
        engine, store, bus = make_engine_and_bus(three_step_config())
        completed = []

        async def handler(event): completed.append(event)
        bus.on(EventType.SETUP_COMPLETED, handler)

        ticks = [
            ("AAPL", 99.0),   # miss
            ("AAPL", 101.0),  # step 1
            ("AAPL", 106.0),  # step 2
            ("AAPL", 102.0),  # step 3 → complete
        ]
        feed = MockFeed(ticks, bus=bus, store=store, delay=0)
        await feed.run()

        assert len(completed) == 1
        seq: SequenceState = completed[0].payload["sequence"]
        assert seq.name == "test_setup"
        assert seq.symbol == "AAPL"
        assert seq.step_prices == [101.0, 106.0, 102.0]

    @pytest.mark.asyncio
    async def test_mock_feed_step_advancement_events(self):
        engine, store, bus = make_engine_and_bus(three_step_config())
        advanced = []

        async def handler(event): advanced.append(event.payload["progress"])
        bus.on(EventType.STEP_ADVANCED, handler)

        ticks = [("AAPL", 101.0), ("AAPL", 106.0)]
        feed = MockFeed(ticks, bus=bus, store=store, delay=0)
        await feed.run()

        assert advanced == ["1/3", "2/3"]

    @pytest.mark.asyncio
    async def test_mock_feed_multi_symbol(self):
        engine = Engine()
        store = SymbolStore()
        bus = Bus(engine=engine, store=store)

        engine.activate(simple_config("AAPL"))
        engine.activate(simple_config("NVDA"))
        store.subscribe("AAPL")
        store.subscribe("NVDA")

        completed = []
        async def handler(event): completed.append(event.payload["sequence"].symbol)
        bus.on(EventType.SETUP_COMPLETED, handler)

        ticks = [("AAPL", 101.0), ("NVDA", 101.0)]
        feed = MockFeed(ticks, bus=bus, store=store, delay=0)
        await feed.run()

        assert sorted(completed) == ["AAPL", "NVDA"]

    @pytest.mark.asyncio
    async def test_mock_feed_tick_has_correct_source(self):
        engine, store, bus = make_engine_and_bus()
        store.subscribe("AAPL")
        received = []

        async def handler(event): received.append(event.payload["tick"])
        bus.on(EventType.TICK_RECEIVED, handler)

        feed = MockFeed([("AAPL", 100.0)], bus=bus, store=store, delay=0)
        await feed.run()

        assert received[0].source == "mock"


# ============================================================
# Integration — full pipeline
# ============================================================

class TestFullPipeline:
    """
    End-to-end: YAML-like config → engine → bus → mock feed → signal.
    Tests the full stack without touching real I/O.
    """

    @pytest.mark.asyncio
    async def test_bull_flag_full_sequence(self):
        config = three_step_config("AAPL")
        engine, store, bus = make_engine_and_bus(config)

        signals = []
        async def on_complete(event): signals.append(event)
        bus.on(EventType.SETUP_COMPLETED, on_complete)

        ticks = [
            ("AAPL", 99.0),   # no change
            ("AAPL", 101.0),  # break above 100
            ("AAPL", 104.0),  # still below 105
            ("AAPL", 106.0),  # extend above 105
            ("AAPL", 102.5),  # pullback below 103 → SIGNAL
        ]
        feed = MockFeed(ticks, bus=bus, store=store, delay=0)
        await feed.run()

        assert len(signals) == 1
        assert signals[0].payload["step_prices"] == [101.0, 106.0, 102.5]
        assert signals[0].payload["duration_seconds"] >= 0

    @pytest.mark.asyncio
    async def test_invalidation_before_completion(self):
        inv = make_condition(ConditionType.PRICE_BELOW, 95.0)
        config = make_config(
            steps=[
                make_step("break",  ConditionType.PRICE_ABOVE, 100.0),
                make_step("extend", ConditionType.PRICE_ABOVE, 105.0),
            ],
            invalidation=inv,
        )
        engine, store, bus = make_engine_and_bus(config)

        completed = []
        invalidated = []
        async def on_complete(event): completed.append(event)
        async def on_invalid(event): invalidated.append(event)
        bus.on(EventType.SETUP_COMPLETED, on_complete)
        bus.on(EventType.SETUP_INVALIDATED, on_invalid)

        ticks = [
            ("AAPL", 101.0),  # step 1 done
            ("AAPL", 94.0),   # blows through invalidation
            ("AAPL", 106.0),  # too late — already invalidated
        ]
        feed = MockFeed(ticks, bus=bus, store=store, delay=0)
        await feed.run()

        assert len(completed) == 0
        assert len(invalidated) == 1

    @pytest.mark.asyncio
    async def test_engine_evicts_after_completion(self):
        config = simple_config()
        engine, store, bus = make_engine_and_bus(config)

        assert len(engine) == 1
        feed = MockFeed([("AAPL", 101.0)], bus=bus, store=store, delay=0)
        await feed.run()
        assert len(engine) == 0  # evicted from active store

    @pytest.mark.asyncio
    async def test_tick_history_populated_during_run(self):
        engine, store, bus = make_engine_and_bus()
        store.subscribe("AAPL")

        ticks = [("AAPL", 100.0), ("AAPL", 101.0), ("AAPL", 102.0)]
        feed = MockFeed(ticks, bus=bus, store=store, delay=0)
        await feed.run()

        history = store.history("AAPL")
        assert history is not None
        assert len(history) == 3
        assert history.last_price() == 102.0