"""
core/engine.py

Orchestrates tick processing across all active setup sequences.
This is your ETS-equivalent state store plus the routing logic.

Architecture:
  - One Engine instance per process
  - Keyed by (symbol, setup_name) — same symbol can run multiple setups simultaneously
  - push_tick() is the hot path — called on every price update from the feed
  - Results (completions, invalidations) are returned from push_tick for the caller to act on
"""

from dataclasses import dataclass, field
from copy import deepcopy
from typing import Optional
from cmd_slack.format.schema import SetupConfig
from cmd_slack.core.sequence import SequenceState, from_config


@dataclass
class TickResult:
    """Returned by push_tick — describes what changed on this tick."""
    symbol: str
    price: float
    volume: float
    completed: list[SequenceState] = field(default_factory=list)
    invalidated: list[SequenceState] = field(default_factory=list)
    advanced: list[SequenceState] = field(default_factory=list)  # step progressed but not done

    @property
    def any_change(self) -> bool:
        return bool(self.completed or self.invalidated or self.advanced)


class Engine:
    """
    Central orchestrator. Holds all active SequenceState instances
    and routes incoming ticks to the right ones.

    Usage:
        engine = Engine()
        engine.activate(setup_config)          # load a setup for a symbol
        result = engine.push_tick("AAPL", 101.5)
        for seq in result.completed:
            print(f"SIGNAL: {seq.name} completed on {seq.symbol}")
    """

    def __init__(self):
        # (symbol, setup_name) -> SequenceState
        self._store: dict[tuple[str, str], SequenceState] = {}

        # Template store — configs keyed by name so we can re-instantiate
        self._templates: dict[str, SetupConfig] = {}

    # --- Setup management ---

    def register_template(self, config: SetupConfig) -> None:
        """
        Register a setup config as a reusable template.
        Use activate_template() to instantiate it for a specific symbol.
        """
        self._templates[config.name] = config

    def activate(self, config: SetupConfig) -> SequenceState:
        """
        Instantiate and activate a setup for the symbol in config.
        If the same (symbol, setup_name) is already active, it is replaced.
        """
        state = from_config(config)
        key = (config.symbol, config.name)
        self._store[key] = state
        return state

    def activate_template(self, setup_name: str, symbol: str) -> SequenceState:
        """
        Instantiate a registered template for a specific symbol.
        Deep-copies the config so symbols don't share state.
        """
        if setup_name not in self._templates:
            raise KeyError(f"no template registered for '{setup_name}'")
        config = deepcopy(self._templates[setup_name])
        config.symbol = symbol
        return self.activate(config)

    def deactivate(self, symbol: str, setup_name: str) -> bool:
        """Remove a specific setup. Returns True if it existed."""
        return self._store.pop((symbol, setup_name), None) is not None

    def deactivate_symbol(self, symbol: str) -> int:
        """Remove all setups for a symbol. Returns count removed."""
        keys = [k for k in self._store if k[0] == symbol]
        for k in keys:
            del self._store[k]
        return len(keys)

    def reset(self, symbol: str, setup_name: str) -> Optional[SequenceState]:
        """
        Reset a setup back to step 0 without deactivating it.
        Useful when a trader wants to re-watch the same setup after completion.
        """
        key = (symbol, setup_name)
        if key not in self._store:
            return None
        old = self._store[key]
        fresh = from_config(old.config)
        self._store[key] = fresh
        return fresh

    # --- Tick processing ---

    def push_tick(self, symbol: str, price: float, volume: float = 0.0) -> TickResult:
        """
        Push a price tick to all active setups watching this symbol.
        Returns a TickResult describing everything that changed.

        This is called on every tick from the feed. Keep it tight.
        """
        result = TickResult(symbol=symbol, price=price, volume=volume)

        for (sym, _name), state in self._store.items():
            if sym != symbol:
                continue
            if not state.active:
                continue

            prev_step = state.current_step
            changed = state.check_price(price, volume)

            if not changed:
                continue

            if state.invalidated:
                result.invalidated.append(state)
            elif state.complete:
                result.completed.append(state)
            elif state.current_step > prev_step:
                result.advanced.append(state)

        # Evict completed and invalidated states
        # Keep them in result for the caller, remove from active store
        for state in result.completed + result.invalidated:
            self._store.pop((state.symbol, state.name), None)

        return result

    # --- Inspection ---

    def active_for(self, symbol: str) -> list[SequenceState]:
        return [s for (sym, _), s in self._store.items() if sym == symbol]

    def all_active(self) -> list[SequenceState]:
        return list(self._store.values())

    def get(self, symbol: str, setup_name: str) -> Optional[SequenceState]:
        return self._store.get((symbol, setup_name))

    def symbol_count(self) -> dict[str, int]:
        """How many active setups per symbol — useful for monitoring."""
        counts: dict[str, int] = {}
        for (sym, _) in self._store:
            counts[sym] = counts.get(sym, 0) + 1
        return counts

    def __len__(self) -> int:
        return len(self._store)

    def __repr__(self) -> str:
        return f"Engine(active_setups={len(self)}, symbols={list(self.symbol_count().keys())})"