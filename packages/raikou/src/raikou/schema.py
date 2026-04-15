from __future__ import annotations

from pydantic import BaseModel


class Schema(BaseModel):
    """Row schema base for raikou frames (Pydantic v2)."""

    model_config = {"extra": "forbid"}
