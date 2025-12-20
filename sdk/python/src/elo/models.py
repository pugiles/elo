from typing import Dict, List

from pydantic import BaseModel, ConfigDict, Field


class CreateNode(BaseModel):
    id: str


class EdgeView(BaseModel):
    to: str
    data: Dict[str, str] = Field(default_factory=dict)


class NodeView(BaseModel):
    id: str
    data: Dict[str, str] = Field(default_factory=dict)
    edges: List[EdgeView] = Field(default_factory=list)


class Recommendation(BaseModel):
    id: str
    score: float
    data: Dict[str, str]


class EdgeListResult(BaseModel):
    from_: str = Field(alias="from")
    to: str
    data: Dict[str, str] = Field(default_factory=dict)

    model_config = ConfigDict(populate_by_name=True)
