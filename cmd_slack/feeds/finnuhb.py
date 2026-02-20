"""
feeds/finnhub.py

Finnhub WebSocket feed adapter.

Connects to Finnhub's real-time trade stream, normalizes incoming messages
into Tick objects, and publishes them to the Bus.

Finnhub docs: https://finnhub.io/docs/api/websocket-trades
Message format: {"type":"trade","data":[{"s":"AAPL","p":150.25,"v":100,"t":1234567890000}]}

Usage:
    feed = FinnhubFeed(api_key="your_key", bus=bus, store=store)
    await feed.start()            # connects and begins streaming
    feed.subscribe("AAPL")        # add a symbol (can call before or after start)
    feed.unsubscribe("AAPL")      # remove a symbol
    await feed.stop()             # clean shutdown
"""

import asyncio
import json
import logging
from datetime import datetime, timezone

import websockets
from websockets.exceptions import ConnectionClosed

from cmd_slack.feeds.bus import Bus, EventType
from cmd_slack.feeds.store import SymbolStore, Tick

logger = logging.getLogger(__name__)

FINNHUB_WSS = "wss://ws.finnhub.io"

# How long to wait before reconnecting after a drop
RECONNECT_DELAY_SECONDS = 3

# Finnhub sends a ping every 30s — we send one back to stay alive
PING_INTERVAL_SECONDS = 25


class FinnhubFeed:
    """
    Manages the Finnhub WebSocket connection lifecycle.

    Designed to run as a long-lived asyncio task. Handles:
      - Initial connection and authentication
      - Subscribing/unsubscribing symbols on the fly
      - Automatic reconnection with backoff
      - Normalization of Finnhub trade messages into Tick objects
      - Publishing to Bus
    """

    def __init__(self, api_key: str, bus: Bus, store: SymbolStore):
        self._api_key = api_key
        self._bus = bus
        self._store = store
        self._ws = None
        self._running = False
        self._pending_subscribes: set[str] = set()
        self._pending_unsubscribes: set[str] = set()
        self._reconnect_delay = RECONNECT_DELAY_SECONDS

    # --- Public API ---

    async def start(self) -> None:
        """
        Start the feed. Runs the connection loop in the background.
        Call this once — it spawns an asyncio task that manages reconnection.
        """
        self._running = True
        asyncio.create_task(self._connection_loop(), name="finnhub_feed")
        logger.info("FinnhubFeed started")

    async def stop(self) -> None:
        """Graceful shutdown."""
        self._running = False
        if self._ws:
            await self._ws.close()
        logger.info("FinnhubFeed stopped")

    def subscribe(self, symbol: str) -> None:
        """
        Subscribe to a symbol. Safe to call before the connection is established —
        pending subscribes are flushed once connected.
        """
        is_new = self._store.subscribe(symbol)
        if not is_new:
            return  # already watching this symbol

        if self._ws and not self._ws.closed:
            # Connected — send immediately via a fire-and-forget task
            asyncio.create_task(self._send_subscribe(symbol))
        else:
            # Not connected yet — queue it, will be sent on connect
            self._pending_subscribes.add(symbol)

        logger.info("subscribed to %s", symbol)

    def unsubscribe(self, symbol: str) -> None:
        """Remove a symbol from the feed."""
        removed = self._store.unsubscribe(symbol)
        if not removed:
            return

        if self._ws and not self._ws.closed:
            asyncio.create_task(self._send_unsubscribe(symbol))
        else:
            self._pending_unsubscribes.add(symbol)

        logger.info("unsubscribed from %s", symbol)

    @property
    def connected(self) -> bool:
        return self._ws is not None and not self._ws.closed

    # --- Connection lifecycle ---

    async def _connection_loop(self) -> None:
        """
        Outer loop — reconnects automatically if the connection drops.
        Backs off exponentially up to 60 seconds between attempts.
        """
        attempt = 0
        while self._running:
            try:
                attempt += 1
                logger.info("connecting to Finnhub (attempt %d)", attempt)
                await self._connect_and_run()
                # Clean exit — reset backoff
                attempt = 0
                self._reconnect_delay = RECONNECT_DELAY_SECONDS
            except ConnectionClosed as e:
                logger.warning("connection closed: %s", e)
            except Exception as e:
                logger.error("feed error: %s", e, exc_info=True)
            finally:
                await self._bus.emit_system(EventType.FEED_DISCONNECTED)

            if self._running:
                logger.info("reconnecting in %ds", self._reconnect_delay)
                await asyncio.sleep(self._reconnect_delay)
                # Exponential backoff capped at 60s
                self._reconnect_delay = min(self._reconnect_delay * 2, 60)

    async def _connect_and_run(self) -> None:
        """
        Single connection session — connect, subscribe pending symbols,
        then pump messages until the connection drops.
        """
        url = f"{FINNHUB_WSS}?token={self._api_key}"

        async with websockets.connect(
            url,
            ping_interval=PING_INTERVAL_SECONDS,
            ping_timeout=10,
            max_size=2**20,  # 1MB message limit
        ) as ws:
            self._ws = ws
            logger.info("connected to Finnhub")
            await self._bus.emit_system(EventType.FEED_CONNECTED)

            # Flush any symbols that were subscribed before we connected
            await self._flush_pending()

            # Re-subscribe to all currently watched symbols
            # (handles the reconnect case where we lost the session)
            for symbol in self._store.subscribed_symbols():
                await self._send_subscribe(symbol)

            # Message pump
            async for raw in ws:
                if not self._running:
                    break
                await self._handle_message(raw)

    async def _flush_pending(self) -> None:
        for symbol in list(self._pending_subscribes):
            await self._send_subscribe(symbol)
        self._pending_subscribes.clear()

        for symbol in list(self._pending_unsubscribes):
            await self._send_unsubscribe(symbol)
        self._pending_unsubscribes.clear()

    # --- Message handling ---

    async def _handle_message(self, raw: str) -> None:
        """
        Parse a raw Finnhub message and publish ticks to the bus.

        Finnhub bundles multiple trades in a single message:
        {
          "type": "trade",
          "data": [
            {"s": "AAPL", "p": 150.25, "v": 100, "t": 1234567890000},
            ...
          ]
        }
        """
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("bad JSON from Finnhub: %s", raw[:100])
            return

        msg_type = msg.get("type")

        if msg_type == "trade":
            for trade in msg.get("data", []):
                tick = _normalize_trade(trade)
                if tick is None:
                    continue
                if not self._store.is_subscribed(tick.symbol):
                    continue  # Finnhub sometimes sends data for symbols we didn't ask for
                await self._bus.publish(tick)

        elif msg_type == "ping":
            await self._ws.send(json.dumps({"type": "pong"}))

        elif msg_type == "error":
            logger.error("Finnhub error message: %s", msg.get("msg"))

        else:
            logger.debug("unhandled message type: %s", msg_type)

    # --- Send helpers ---

    async def _send_subscribe(self, symbol: str) -> None:
        if not self._ws or self._ws.closed:
            return
        payload = json.dumps({"type": "subscribe", "symbol": symbol})
        await self._ws.send(payload)
        logger.debug("sent subscribe: %s", symbol)

    async def _send_unsubscribe(self, symbol: str) -> None:
        if not self._ws or self._ws.closed:
            return
        payload = json.dumps({"type": "unsubscribe", "symbol": symbol})
        await self._ws.send(payload)
        logger.debug("sent unsubscribe: %s", symbol)


# --- Normalization ---

def _normalize_trade(trade: dict) -> Tick | None:
    """
    Convert a raw Finnhub trade dict into a normalized Tick.
    Returns None if the trade is malformed.

    Finnhub fields:
      s = symbol
      p = price
      v = volume
      t = timestamp (milliseconds since epoch)
    """
    try:
        symbol = trade["s"]
        price = float(trade["p"])
        volume = float(trade.get("v", 0.0))
        ts_ms = trade.get("t", 0)
        timestamp = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)

        if price <= 0:
            return None

        return Tick(
            symbol=symbol,
            price=price,
            volume=volume,
            timestamp=timestamp,
            source="finnhub",
        )
    except (KeyError, ValueError, TypeError) as e:
        logger.warning("could not normalize trade %s: %s", trade, e)
        return None


# --- Mock feed for local development ---

class MockFeed:
    """
    Replays a list of (symbol, price) tuples through the bus.
    Use this for testing the full pipeline without a live connection.

    Usage:
        ticks = [("AAPL", 99.0), ("AAPL", 101.0), ("AAPL", 106.0)]
        feed = MockFeed(ticks, bus=bus, store=store, delay=0.1)
        await feed.run()
    """

    def __init__(
        self,
        ticks: list[tuple[str, float]],
        bus: Bus,
        store: SymbolStore,
        delay: float = 0.05,  # seconds between ticks
        volume: float = 10_000.0,
    ):
        self._ticks = ticks
        self._bus = bus
        self._store = store
        self._delay = delay
        self._volume = volume

    async def run(self) -> None:
        await self._bus.emit_system(EventType.FEED_CONNECTED)
        for symbol, price in self._ticks:
            tick = Tick.now(symbol=symbol, price=price, volume=self._volume, source="mock")
            await self._bus.publish(tick)
            if self._delay > 0:
                await asyncio.sleep(self._delay)
        await self._bus.emit_system(EventType.FEED_DISCONNECTED)