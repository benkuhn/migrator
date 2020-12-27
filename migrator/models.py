from __future__ import annotations

import abc
from pydantic import BaseModel, root_validator
from dataclasses import dataclass, field
from typing import List, Union, Optional

class Migration(BaseModel):
    revision: int
    message: str
    pre_deploy: List[StepWrapper] = []
    post_deploy: List[StepWrapper] = []

class AbstractStep(abc.ABC):
    pass

class StepWrapper(BaseModel):
    run_ddl: Optional[DDLStep] = None

    @property
    def step(self) -> AbstractStep:
        result = self.run_ddl
        assert result is not None
        return result

class DDLStep(AbstractStep, BaseModel):
    up: str
    down: str

class OtherStep(AbstractStep, BaseModel):
    up: str
    down: str

AnyStep = Union[DDLStep, OtherStep]

for s in BaseModel.__subclasses__(): # type: ignore
    s.update_forward_refs() # type: ignore
