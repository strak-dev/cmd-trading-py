from pydantic import BaseModel, field_validator
from enum import Enum
from typing import Optional

class ConditionType(str, Enum):
    PRICE_ABOVE = "price_above"
    PRICE_BELOW = "price_below"
    PRICE_HOLDS = "price_holds"
    VOLUME_ABOVE = "volume_above"
    PCT_MOVE = "pct_move"
    TIME_EXPIRED = "time_expired"
    INVALIDATED = "invalidated"

class Condition(str, Enum):
    condition: ConditionType
    target: float
    hold_time: Optional[int] = None
    description: Optional[str] = None

class Setup(BaseModel):
    id: str
    title: str
    steps: list[Condition]
    targets: list[int]

class SetupConfig(BaseModel): 
    name: str
    date: str
    description: Optional[str] = None
    setups: list[Setup]

    @field_validator("setups")
    @classmethod
    def must_have_setups(cls, v):
        if len(v) == 0:
            raise ValueError("setup must have at least one setup")
        return v
