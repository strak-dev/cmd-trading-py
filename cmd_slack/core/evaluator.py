"""
core/evaluator.py

Pure, stateless condition evaluation functions.
No I/O, no side effects — given a price/volume and a condition, return True/False.
These are the atoms that sequence.py chains together.
"""

from datetime import datetime, timezone
from typing import Optional
from cmd_slack.format.schema import Condition, ConditionType


def evaluate(
    condition: Condition,
    price: float,
    volume: float = 0.0,
    crossed_at: Optional[datetime] = None,
) -> bool:
    """
    Evaluate a condition against current market data.

    Returns True if the condition is satisfied, False otherwise.
    For hold-based conditions, crossed_at tracks when price first entered
    the valid zone — the condition only returns True once hold_seconds have elapsed.
    """
    match condition.condition:
        case ConditionType.PRICE_ABOVE:
            return _price_above(price, condition.target, condition.hold_seconds, crossed_at)

        case ConditionType.PRICE_BELOW:
            return _price_below(price, condition.target, condition.hold_seconds, crossed_at)

        case ConditionType.PRICE_HOLDS:
            # price_holds requires both a target and hold_seconds in the config
            if condition.hold_seconds is None:
                raise ValueError("price_holds condition requires hold_seconds")
            return _price_above(price, condition.target, condition.hold_seconds, crossed_at)

        case ConditionType.VOLUME_ABOVE:
            return volume > condition.target

        case ConditionType.PCT_MOVE:
            # Requires a reference price — handled at the sequence level
            # where prior step prices are available
            raise NotImplementedError(
                "pct_move must be evaluated at the sequence level with a reference price"
            )

        case ConditionType.TIME_EXPIRED:
            if crossed_at is None:
                return False
            return _seconds_elapsed(crossed_at) >= condition.target

        case ConditionType.INVALIDATED:
            # Invalidation is checked separately by the engine, not inline
            # but we expose it here for completeness
            return False

        case _:
            raise ValueError(f"unknown condition type: {condition.condition}")


def update_crossed_at(
    condition: Condition,
    price: float,
    current_crossed_at: Optional[datetime],
) -> Optional[datetime]:
    """
    Update the timer for hold-based conditions.

    - If price enters the valid zone for the first time: start the timer
    - If price leaves the valid zone: reset the timer to None
    - If price stays in the zone: keep existing timer unchanged
    - For non-hold conditions: always return None (no timer needed)

    This mirrors Elixir's update_timer/3 exactly.
    """
    if condition.hold_seconds is None:
        return None

    match condition.condition:
        case ConditionType.PRICE_ABOVE | ConditionType.PRICE_HOLDS:
            if price > condition.target:
                # In zone — start timer if not already started
                return current_crossed_at if current_crossed_at is not None else _now()
            else:
                # Left zone — reset timer
                return None

        case ConditionType.PRICE_BELOW:
            if price < condition.target:
                return current_crossed_at if current_crossed_at is not None else _now()
            else:
                return None

        case _:
            return None


# --- private helpers ---

def _price_above(
    price: float,
    target: float,
    hold_seconds: Optional[float],
    crossed_at: Optional[datetime],
) -> bool:
    if price <= target:
        return False
    if hold_seconds is None:
        return True
    # Price is above target but we need it to hold
    if crossed_at is None:
        return False  # Just entered, timer not started yet
    return _seconds_elapsed(crossed_at) >= hold_seconds


def _price_below(
    price: float,
    target: float,
    hold_seconds: Optional[float],
    crossed_at: Optional[datetime],
) -> bool:
    if price >= target:
        return False
    if hold_seconds is None:
        return True
    if crossed_at is None:
        return False
    return _seconds_elapsed(crossed_at) >= hold_seconds


def _seconds_elapsed(since: datetime) -> float:
    return (_now() - since).total_seconds()


def _now() -> datetime:
    return datetime.now(timezone.utc)