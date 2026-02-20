"""
main.py

Wires the full stack together end to end.
Run with: python main.py

Uses MockFeed by default so you can test without a Finnhub key.
Swap in FinnhubFeed when you're ready for live data.
"""

import asyncio
import logging
import os
from dotenv import load_dotenv

from cmd_slack.core.engine import Engine
from cmd_slack.feeds.store import SymbolStore
from cmd_slack.feeds.bus import Bus, EventType, make_print_handler
from cmd_slack.feeds.finnhub import MockFeed, FinnhubFeed
from cmd_slack.format.parser import load_all_setups

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
load_dotenv()


async def main():
    # --- Build the stack ---
    engine = Engine()
    store = SymbolStore()
    bus = Bus(engine=engine, store=store)

    # --- Register output handlers ---
    bus.on(
        EventType.SETUP_COMPLETED,
        make_print_handler({EventType.SETUP_COMPLETED, EventType.SETUP_INVALIDATED, EventType.STEP_ADVANCED})
    )
    bus.on(EventType.STEP_ADVANCED, make_print_handler({EventType.STEP_ADVANCED}))
    bus.on(EventType.SETUP_INVALIDATED, make_print_handler({EventType.SETUP_INVALIDATED}))

    # --- Load setup configs from YAML ---
    setups = load_all_setups("cmd_slack/format/examples")
    for config in setups:
        engine.register_template(config)

    # --- Subscribe symbols and activate setups ---
    store.subscribe("AAPL")
    engine.activate_template("bull_flag_entry", "AAPL")

    print(f"\nEngine ready: {engine}")
    print(f"Store: {store}\n")

    # --- Run with mock feed ---
    mock_ticks = [
        ("AAPL", 99.0),
        ("AAPL", 101.0),   # break above 100
        ("AAPL", 104.0),
        ("AAPL", 106.0),   # extend above 105
        ("AAPL", 102.5),   # pullback below 103 → SIGNAL
    ]

    feed = MockFeed(mock_ticks, bus=bus, store=store, delay=0.2)
    await feed.run()

    # --- Swap for live feed ---
    # api_key = os.getenv("FINNHUB_API_KEY")
    # feed = FinnhubFeed(api_key=api_key, bus=bus, store=store)
    # feed.subscribe("AAPL")
    # await feed.start()
    # await asyncio.sleep(3600)  # run for an hour


if __name__ == "__main__":
    asyncio.run(main())