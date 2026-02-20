"""
tests/test_core.py

Tests for evaluator, sequence, and engine.
Run with: pytest tests/test_core.py -v
"""

from datetime import datetime, timezone, timedelta

from cmd_slack.format.schema import (
    SetupConfig, Step, Condition, ConditionType
)
from cmd_slack.core.evaluator import evaluate, update_crossed_at
from cmd_slack.core.sequence import from_config
from cmd_slack.core.engine import Engine


# --- Helpers ---

def make_condition(
    ctype: ConditionType,
    target: float,
    hold_seconds: float = None
) -> Condition:
    return Condition(condition=ctype, target=target, hold_seconds=hold_seconds)


def make_step(id: str, ctype: ConditionType, target: float, hold_seconds=None) -> Step:
    return Step(id=id, condition=ctype, target=target, hold_seconds=hold_seconds)


def make_config(steps: list[Step], symbol="AAPL", name="test_setup", invalidation=None):
    return SetupConfig(symbol=symbol, name=name, steps=steps, invalidation=invalidation)


def past(seconds: float) -> datetime:
    return datetime.now(timezone.utc) - timedelta(seconds=seconds)


# --- Evaluator tests ---

class TestEvaluator:
    def test_price_above(self):
        c = make_condition(ConditionType.PRICE_ABOVE, 100.0)
        assert evaluate(c, 101.0) is True
        assert evaluate(c, 100.0) is False
        assert evaluate(c, 99.0) is False

    def test_price_below(self):
        c = make_condition(ConditionType.PRICE_BELOW, 100.0)
        assert evaluate(c, 99.0) is True
        assert evaluate(c, 100.0) is False
        assert evaluate(c, 101.0) is False

    def test_volume_above(self):
        c = make_condition(ConditionType.VOLUME_ABOVE, 1_000_000)
        assert evaluate(c, 50.0, volume=2_000_000) is True
        assert evaluate(c, 50.0, volume=500_000) is False

    def test_price_above_with_hold_not_started(self):
        c = make_condition(ConditionType.PRICE_ABOVE, 100.0, hold_seconds=5)
        # Price is above but no timer started yet
        assert evaluate(c, 101.0, crossed_at=None) is False

    def test_price_above_with_hold_not_elapsed(self):
        c = make_condition(ConditionType.PRICE_ABOVE, 100.0, hold_seconds=10)
        assert evaluate(c, 101.0, crossed_at=past(3)) is False

    def test_price_above_with_hold_elapsed(self):
        c = make_condition(ConditionType.PRICE_ABOVE, 100.0, hold_seconds=5)
        assert evaluate(c, 101.0, crossed_at=past(6)) is True

    def test_price_below_with_hold_elapsed(self):
        c = make_condition(ConditionType.PRICE_BELOW, 100.0, hold_seconds=5)
        assert evaluate(c, 99.0, crossed_at=past(6)) is True

    def test_price_not_in_zone_with_hold(self):
        c = make_condition(ConditionType.PRICE_ABOVE, 100.0, hold_seconds=5)
        # Price below target — fails regardless of timer
        assert evaluate(c, 99.0, crossed_at=past(10)) is False


class TestUpdateCrossedAt:
    def test_starts_timer_when_price_enters_zone(self):
        c = make_condition(ConditionType.PRICE_ABOVE, 100.0, hold_seconds=5)
        result = update_crossed_at(c, 101.0, current_crossed_at=None)
        assert result is not None

    def test_keeps_timer_when_already_in_zone(self):
        c = make_condition(ConditionType.PRICE_ABOVE, 100.0, hold_seconds=5)
        existing = past(3)
        result = update_crossed_at(c, 101.0, current_crossed_at=existing)
        assert result == existing  # unchanged

    def test_resets_timer_when_price_leaves_zone(self):
        c = make_condition(ConditionType.PRICE_ABOVE, 100.0, hold_seconds=5)
        result = update_crossed_at(c, 99.0, current_crossed_at=past(3))
        assert result is None

    def test_no_timer_for_non_hold_condition(self):
        c = make_condition(ConditionType.PRICE_ABOVE, 100.0)  # no hold_seconds
        result = update_crossed_at(c, 101.0, current_crossed_at=None)
        assert result is None


# --- Sequence tests ---

class TestSequenceState:
    def _bull_flag_config(self):
        return make_config([
            make_step("break", ConditionType.PRICE_ABOVE, 100.0),
            make_step("extend", ConditionType.PRICE_ABOVE, 105.0),
            make_step("pullback", ConditionType.PRICE_BELOW, 103.0),
        ])

    def test_initial_state(self):
        state = from_config(self._bull_flag_config())
        assert state.current_step == 0
        assert state.complete is False
        assert state.active is True

    def test_step_does_not_advance_on_miss(self):
        state = from_config(self._bull_flag_config())
        changed = state.check_price(99.0)
        assert changed is False
        assert state.current_step == 0

    def test_step_advances_on_hit(self):
        state = from_config(self._bull_flag_config())
        changed = state.check_price(101.0)
        assert changed is True
        assert state.current_step == 1

    def test_full_sequence_completes(self):
        state = from_config(self._bull_flag_config())
        state.check_price(101.0)  # break
        state.check_price(106.0)  # extend
        assert not state.complete
        state.check_price(102.0)  # pullback
        assert state.complete
        assert state.active is False
        assert len(state.step_prices) == 3

    def test_crossed_at_reset_on_step_advance(self):
        state = from_config(self._bull_flag_config())
        state.crossed_at = past(10)
        state.check_price(101.0)
        assert state.crossed_at is None  # reset after advancing

    def test_invalidation_kills_setup(self):
        inv = make_condition(ConditionType.PRICE_BELOW, 95.0)
        config = make_config(
            steps=[make_step("break", ConditionType.PRICE_ABOVE, 100.0)],
            invalidation=inv
        )
        state = from_config(config)
        state.check_price(101.0)  # first step done
        changed = state.check_price(94.0)  # blows through invalidation
        assert changed is True
        assert state.invalidated is True
        assert state.active is False

    def test_no_change_after_complete(self):
        state = from_config(make_config([
            make_step("only", ConditionType.PRICE_ABOVE, 100.0)
        ]))
        state.check_price(101.0)
        assert state.complete
        changed = state.check_price(102.0)
        assert changed is False


# --- Engine tests ---

class TestEngine:
    def _config(self, symbol="AAPL", name="bull_flag"):
        return make_config([
            make_step("break", ConditionType.PRICE_ABOVE, 100.0),
            make_step("extend", ConditionType.PRICE_ABOVE, 105.0),
            make_step("pullback", ConditionType.PRICE_BELOW, 103.0),
        ], symbol=symbol, name=name)

    def test_activate_and_count(self):
        engine = Engine()
        engine.activate(self._config())
        assert len(engine) == 1

    def test_push_tick_no_change(self):
        engine = Engine()
        engine.activate(self._config())
        result = engine.push_tick("AAPL", 99.0)
        assert not result.any_change

    def test_push_tick_advances_step(self):
        engine = Engine()
        engine.activate(self._config())
        result = engine.push_tick("AAPL", 101.0)
        assert len(result.advanced) == 1
        assert result.advanced[0].current_step == 1

    def test_push_tick_completion_evicts_from_store(self):
        engine = Engine()
        engine.activate(self._config())
        engine.push_tick("AAPL", 101.0)
        engine.push_tick("AAPL", 106.0)
        result = engine.push_tick("AAPL", 102.0)
        assert len(result.completed) == 1
        assert len(engine) == 0  # evicted

    def test_irrelevant_symbol_ignored(self):
        engine = Engine()
        engine.activate(self._config(symbol="AAPL"))
        result = engine.push_tick("NVDA", 200.0)
        assert not result.any_change
        assert engine.get("AAPL", "bull_flag").current_step == 0

    def test_multiple_symbols_independent(self):
        engine = Engine()
        engine.activate(self._config(symbol="AAPL"))
        engine.activate(self._config(symbol="NVDA"))

        engine.push_tick("AAPL", 101.0)
        assert engine.get("AAPL", "bull_flag").current_step == 1
        assert engine.get("NVDA", "bull_flag").current_step == 0

    def test_template_instantiation(self):
        engine = Engine()
        engine.register_template(self._config(symbol="TEMPLATE"))
        engine.activate_template("bull_flag", "AAPL")
        engine.activate_template("bull_flag", "NVDA")
        assert len(engine) == 2
        assert engine.get("AAPL", "bull_flag") is not None
        assert engine.get("NVDA", "bull_flag") is not None

    def test_reset(self):
        engine = Engine()
        engine.activate(self._config())
        engine.push_tick("AAPL", 101.0)
        assert engine.get("AAPL", "bull_flag").current_step == 1
        engine.reset("AAPL", "bull_flag")
        assert engine.get("AAPL", "bull_flag").current_step == 0

    def test_invalidation_evicts(self):
        inv = make_condition(ConditionType.PRICE_BELOW, 95.0)
        config = make_config(
            steps=[make_step("break", ConditionType.PRICE_ABOVE, 100.0)],
            invalidation=inv,
            symbol="AAPL",
            name="bull_flag"
        )
        engine = Engine()
        engine.activate(config)
        result = engine.push_tick("AAPL", 94.0)
        assert len(result.invalidated) == 1
        assert len(engine) == 0