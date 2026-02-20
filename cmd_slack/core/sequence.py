"""
core/sequence.py

Stateful step progression for a single setup instance.
One SequenceState exists per (symbol, setup_name) pair.

Mirrors Elixir's CmdSlack.Sequence module — check_price/2 is the hot path.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional
from cmd_slack.format.schema import SetupConfig, Step, Condition
from cmd_slack.core.evaluator import evaluate, update_crossed_at


@dataclass
class SequenceState:
    # Config — set once on creation, never mutated
    config: SetupConfig

    # Progression state
    current_step: int = 0
    complete: bool = False
    invalidated: bool = False

    # Hold timer — tracks when price first entered valid zone for current step
    # Reset to None each time a step advances (mirrors Elixir's crossed_at: nil)
    crossed_at: Optional[datetime] = None

    # Metadata
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: Optional[datetime] = None
    invalidated_at: Optional[datetime] = None

    # Price at each completed step — useful for pct_move and alert messages
    step_prices: list[float] = field(default_factory=list)

    @property
    def symbol(self) -> str:
        return self.config.symbol

    @property
    def name(self) -> str:
        return self.config.name

    @property
    def active(self) -> bool:
        return not self.complete and not self.invalidated

    @property
    def current_step_config(self) -> Optional[Step]:
        steps = self.config.steps
        if self.current_step < len(steps):
            return steps[self.current_step]
        return None

    @property
    def progress(self) -> str:
        total = len(self.config.steps)
        return f"{self.current_step}/{total}"

    def check_price(self, price: float, volume: float = 0.0) -> bool:
        """
        Process a new tick against current step.

        Returns True if state changed (step advanced or sequence completed/invalidated).
        Mutates self in place — no copy needed since we're not in an immutable world.

        Mirrors Elixir's check_price/2 including the hold timer logic.
        """
        if not self.active:
            return False

        # Check invalidation first — if invalidation condition is met, kill the setup
        if self._check_invalidation(price, volume):
            self.invalidated = True
            self.invalidated_at = datetime.now(timezone.utc)
            return True

        step = self.current_step_config
        if step is None:
            return False

        if evaluate(step, price, volume, self.crossed_at):
            # Step satisfied — advance
            self.step_prices.append(price)
            self.current_step += 1
            self.crossed_at = None  # Reset hold timer for next step

            if self.current_step >= len(self.config.steps):
                self.complete = True
                self.completed_at = datetime.now(timezone.utc)
            return True
        else:
            # Step not yet satisfied — update hold timer if applicable
            new_crossed_at = update_crossed_at(step, price, self.crossed_at)
            if new_crossed_at != self.crossed_at:
                self.crossed_at = new_crossed_at
                return True  # Timer started or reset — caller may want to log this

        return False

    def _check_invalidation(self, price: float, volume: float) -> bool:
        inv = self.config.invalidation
        if inv is None:
            return False
        return evaluate(inv, price, volume, crossed_at=None)

    def summary(self) -> dict:
        return {
            "symbol": self.symbol,
            "name": self.name,
            "progress": self.progress,
            "complete": self.complete,
            "invalidated": self.invalidated,
            "current_step": self.current_step,
            "step_prices": self.step_prices,
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
        }


def from_config(config: SetupConfig) -> SequenceState:
    """Instantiate a fresh SequenceState from a loaded SetupConfig."""
    return SequenceState(config=config)