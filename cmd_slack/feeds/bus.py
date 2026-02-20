"""
feeds/bus.py

Internal event bus. Wires together:
  Feed adapter (finnhub.py)  →  Bus  →  Engine (tick processing)
                                    →  Store (tick history)
                                    →  Handlers (alerts, logging, UI)

This is the BEAM process mailbox equivalent — instead of Elixir's message
passing between processes, we have synchronous handler dispatch in a single
asyncio event loop. Simple, fast, and easy to reason about.

Usage:
    bus = Bus(engine=engine, store=store)

    # Register handlers for events you care about
    bus.on(EventType.SETUP_COMPLETED, alert_handler)
    bus.on(EventType.STEP_ADVANCED, log_handler)

    # Feed adapter calls this on every tick
    await bus.publish(tick)
"""

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum, auto
from typing import Callable, Awaitable

from cmd_slack.core.engine import Engine, TickResult
from cmd_slack.core.sequence import SequenceState
from cmd_slack.feeds.store import SymbolStore, Tick

logger = logging.getLogger(__name__)


class EventType(Enum):
    TICK_RECEIVED    = auto()  # every tick, before engine processes it
    STEP_ADVANCED    = auto()  # a setup moved to the next step
    SETUP_COMPLETED  = auto()  # a setup ran all steps — this is your signal
    SETUP_INVALIDATED = auto() # a setup was killed by its invalidation condition
    SYMBOL_SUBSCRIBED = auto() # a new symbol was added to the watch list
    SYMBOL_REMOVED   = auto()  # a symbol was removed from the watch list
    FEED_CONNECTED   = auto()  # websocket feed came online
    FEED_DISCONNECTED = auto() # websocket feed dropped


@dataclass
class Event:
    type: EventType
    timestamp: datetime
    payload: dict  # flexible — different keys per event type

    @classmethod
    def now(cls, event_type: EventType, **kwargs) -> "Event":
        return cls(
            type=event_type,
            timestamp=datetime.now(timezone.utc),
            payload=kwargs,
        )


# Handler signature: async def handler(event: Event) -> None
Handler = Callable[[Event], Awaitable[None]]


class Bus:
    """
    Central event bus. Single instance per application.

    Tick flow:
      1. Feed adapter calls await bus.publish(tick)
      2. Bus fires TICK_RECEIVED to any handlers that care
      3. Bus calls store.record_tick(tick) to update history
      4. Bus calls engine.push_tick() to run setup evaluation
      5. Bus fires STEP_ADVANCED / SETUP_COMPLETED / SETUP_INVALIDATED
         based on TickResult

    All handlers are called sequentially in the order they were registered.
    For a human trader tool this is fine — we're not doing microsecond routing.
    If you add heavy handlers (LLM calls, HTTP requests), make them async
    and await them here, or offload to a background task.
    """

    def __init__(self, engine: Engine, store: SymbolStore):
        self.engine = engine
        self.store = store
        self._handlers: dict[EventType, list[Handler]] = {et: [] for et in EventType}

    def on(self, event_type: EventType, handler: Handler) -> None:
        """Register an async handler for an event type."""
        self._handlers[event_type].append(handler)

    def off(self, event_type: EventType, handler: Handler) -> None:
        """Remove a previously registered handler."""
        try:
            self._handlers[event_type].remove(handler)
        except ValueError:
            pass

    async def publish(self, tick: Tick) -> TickResult:
        """
        Main entry point. Called by feed adapters on every price update.
        Returns the TickResult so callers can inspect changes if needed.
        """
        # 1. Record tick in history
        self.store.record_tick(tick)

        # 2. Notify TICK_RECEIVED handlers
        await self._emit(EventType.TICK_RECEIVED, tick=tick)

        # 3. Run engine — this is where setup evaluation happens
        result = self.engine.push_tick(tick.symbol, tick.price, tick.volume)

        # 4. Emit granular events for what changed
        for seq in result.advanced:
            await self._emit(
                EventType.STEP_ADVANCED,
                sequence=seq,
                tick=tick,
                step=seq.current_step,
                progress=seq.progress,
            )

        for seq in result.completed:
            await self._emit(
                EventType.SETUP_COMPLETED,
                sequence=seq,
                tick=tick,
                step_prices=seq.step_prices,
                duration_seconds=_duration(seq),
            )

        for seq in result.invalidated:
            await self._emit(
                EventType.SETUP_INVALIDATED,
                sequence=seq,
                tick=tick,
            )

        return result

    async def emit_system(self, event_type: EventType, **kwargs) -> None:
        """Emit non-tick system events (connect, disconnect, subscribe)."""
        await self._emit(event_type, **kwargs)

    async def _emit(self, event_type: EventType, **kwargs) -> None:
        handlers = self._handlers.get(event_type, [])
        if not handlers:
            return
        event = Event.now(event_type, **kwargs)
        for handler in handlers:
            try:
                await handler(event)
            except Exception as e:
                logger.error(
                    "handler error on %s: %s",
                    event_type.name,
                    e,
                    exc_info=True,
                )
                # Don't let one bad handler kill the others


# --- Built-in handlers ---
# These are ready-to-use handlers you can pass to bus.on()

async def log_all(event: Event) -> None:
    """Log every event. Useful for development."""
    logger.info("[%s] %s", event.type.name, event.payload)


async def log_signals(event: Event) -> None:
    """Log only completed setups and invalidations."""
    if event.type in (EventType.SETUP_COMPLETED, EventType.SETUP_INVALIDATED):
        seq: SequenceState = event.payload["sequence"]
        tick: Tick = event.payload["tick"]
        action = "SIGNAL" if event.type == EventType.SETUP_COMPLETED else "INVALIDATED"
        logger.info(
            "[%s] %s | %s @ %.2f | steps: %s",
            action,
            seq.name,
            seq.symbol,
            tick.price,
            seq.step_prices,
        )


def make_print_handler(
    events: set[EventType] | None = None
) -> Handler:
    """
    Factory for a simple print handler.
    Pass a set of EventTypes to filter, or None for all.
    """
    async def handler(event: Event) -> None:
        if events is None or event.type in events:
            if event.type == EventType.SETUP_COMPLETED:
                seq: SequenceState = event.payload["sequence"]
                tick: Tick = event.payload["tick"]
                print(
                    f"✅ SIGNAL  {seq.symbol:<6} {seq.name:<25} "
                    f"@ {tick.price:.2f}  steps: {seq.step_prices}"
                )
            elif event.type == EventType.SETUP_INVALIDATED:
                seq = event.payload["sequence"]
                tick = event.payload["tick"]
                print(f"❌ INVALID {seq.symbol:<6} {seq.name:<25} @ {tick.price:.2f}")
            elif event.type == EventType.STEP_ADVANCED:
                seq = event.payload["sequence"]
                tick = event.payload["tick"]
                print(
                    f"→  STEP    {seq.symbol:<6} {seq.name:<25} "
                    f"step {event.payload['progress']} @ {tick.price:.2f}"
                )
    return handler


# --- Helpers ---

def _duration(seq: SequenceState) -> float:
    """Seconds from setup start to completion."""
    if seq.completed_at is None:
        return 0.0
    return (seq.completed_at - seq.started_at).total_seconds()