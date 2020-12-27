from typing import Any, NoReturn
import os
import pytest

from migrator.commands import Context, UserInterface, text, up

class FakeExit(Exception):
    pass

class FakeUserInterface(UserInterface):
    def __init__(self) -> None:
        self.outputs = []
        self.responses = {}

    def respond_to(self, message: str, response: str) -> None:
        self.responses.setdefault(message, []).append(response)

    def respond_yes_no(self, message: str, response: str) -> None:
        self.respond_to(f"{message} {text.PROMPT_YES_NO}", response)
        
    def print(self, *args: Any, **kwargs: Any) -> None:
        self.outputs.append((args, kwargs))

    def input(self, prompt: str) -> str:
        return self.responses[prompt].pop()

    def exit(self, status: int) -> NoReturn:
        raise FakeExit(status)
    
@pytest.fixture
def ctx() -> Context:
    return Context(
        "test/migrator.yml",
        os.environ["DATABASE_URL"],
        FakeUserInterface()
    )

def test_init(ctx: Context) -> None:
    ctx.ui.respond_yes_no(text.ASK_TO_INITIALIZE_DB, "y")
    up.up(ctx)
