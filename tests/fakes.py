import hashlib
import os
import tempfile
from typing import Any, NoReturn, TextIO

from migrator.commands import UserInterface, text


class FakeExit(Exception):
    pass


class FakeUserInterface(UserInterface):
    def __init__(self) -> None:
        self.outputs = []
        self.responses = {}
        self.tmpdir = tempfile.TemporaryDirectory()

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

    def open(self, filename: str, mode: str) -> TextIO:
        newname = hashlib.md5(filename.encode('utf-8')).hexdigest()
        return open(os.path.join(self.tmpdir.name, newname), mode)

    def close(self) -> None:
        self.tmpdir.cleanup()
