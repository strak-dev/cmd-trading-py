"""
feeds/store.py

Two responsibilities:
  1. Symbol subscription registry — what symbols is the feed currently watching
  2. Tick history — a rolling window of recent ticks per symbol for context

This is intentionally separate from core/engine.py which owns setup state.
The store owns market data. The engine owns setup progression.

The bus wires them together.
"""

from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional


@dataclass(frozen=True)
class Tick:
    """
    Normalized tick — one price update from any feed source.
    Feed adapters (finnhub.py etc.) are responsible for normalizing
    into this shape before publishing to the bus.
    """
    symbol: str
    price: float
    volume: float
    timestamp: datetime
    source: str  # "finnhub", "alpaca", "mock", etc.

    @classmethod
    def now(cls, symbol: str, price: float, volume: float = 0.0, source: str = "unknown") -> "Tick":
        return cls(
            symbol=symbol,
            price=price,
            volume=volume,
            timestamp=datetime.now(timezone.utc),
            source=source,
        )


class TickHistory:
    """
    Rolling window of recent ticks for a single symbol.
    Used to provide context to the LLM layer and for debugging.
    Max size is configurable — default keeps last 500 ticks (~1 trading minute at busy pace).
    """

    def __init__(self, symbol: str, maxlen: int = 500):
        self.symbol = symbol
        self._ticks: deque[Tick] = deque(maxlen=maxlen)

    def append(self, tick: Tick) -> None:
        self._ticks.append(tick)

    def latest(self, n: int = 1) -> list[Tick]:
        ticks = list(self._ticks)
        return ticks[-n:]

    def all(self) -> list[Tick]:
        return list(self._ticks)

    def last_price(self) -> Optional[float]:
        if not self._ticks:
            return None
        return self._ticks[-1].price

    def price_range(self, n: int = 20) -> tuple[float, float]:
        """High/low over last n ticks."""
        recent = self.latest(n)
        if not recent:
            return (0.0, 0.0)
        prices = [t.price for t in recent]
        return (min(prices), max(prices))

    def __len__(self) -> int:
        return len(self._ticks)


class SymbolStore:
    """
    Tracks which symbols are currently subscribed and maintains
    a rolling tick history for each.

    The feed adapter calls subscribe/unsubscribe to tell the store
    what it should be watching. The bus calls record_tick on every
    incoming tick to keep history fresh.
    """

    def __init__(self, tick_history_maxlen: int = 500):
        self._subscribed: set[str] = set()
        self._history: dict[str, TickHistory] = {}
        self._tick_history_maxlen = tick_history_maxlen

    def subscribe(self, symbol: str) -> bool:
        """
        Mark a symbol as subscribed.
        Returns True if this is a new subscription (caller should notify feed adapter).
        """
        if symbol in self._subscribed:
            return False
        self._subscribed.add(symbol)
        self._history[symbol] = TickHistory(symbol, self._tick_history_maxlen)
        return True

    def unsubscribe(self, symbol: str) -> bool:
        """
        Remove a symbol subscription.
        Returns True if it existed (caller should notify feed adapter).
        """
        if symbol not in self._subscribed:
            return False
        self._subscribed.discard(symbol)
        # Keep history around for a bit — don't purge immediately
        # The feed adapter handles the actual unsubscribe message
        return True

    def record_tick(self, tick: Tick) -> None:
        """Store an incoming tick in the symbol's history."""
        if tick.symbol in self._history:
            self._history[tick.symbol].append(tick)

    def history(self, symbol: str) -> Optional[TickHistory]:
        return self._history.get(symbol)

    def last_price(self, symbol: str) -> Optional[float]:
        h = self._history.get(symbol)
        return h.last_price() if h else None

    def subscribed_symbols(self) -> set[str]:
        return set(self._subscribed)

    def is_subscribed(self, symbol: str) -> bool:
        return symbol in self._subscribed

    def __len__(self) -> int:
        return len(self._subscribed)

    def __repr__(self) -> str:
        return f"SymbolStore(subscribed={sorted(self._subscribed)})"