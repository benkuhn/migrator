from __future__ import annotations

import abc
from dataclasses import dataclass

from typing import Any, NoReturn, Optional

from .. import models, db
from . import text

@dataclass(frozen=True)
class Context:
    config_path: str
    database_url: str
    ui: UserInterface

    def repo(self) -> models.Repo:
        return models.Repo.parse(self.config_path)

    def db(self) -> db.Database:
        return db.Database(self.database_url)

class UserInterface(abc.ABC):
    def ask_yes_no(self, message: str) -> bool:
        result = self.input(f"{message} {text.PROMPT_YES_NO}")
        while True:
            if result in "Yy":
                return True
            elif result in "Nn":
                return False
            result = self.input(f"Invalid input. {text.PROMPT_YES_NO}")

    def die(self, msg: str) -> NoReturn:
        self.print(msg)
        self.exit(1)

    @abc.abstractmethod
    def print(self, *args: object, sep: Optional[str] = ' ', end: Optional[str] = '\n') -> None:
        pass

    @abc.abstractmethod
    def input(self, prompt: str) -> str:
        pass

    @abc.abstractmethod
    def exit(self, status: int) -> NoReturn:
        pass

class ConsoleUserInterface(UserInterface):
    def print(self, *args: object, sep: Optional[str] = ' ', end: Optional[str] = '\n') -> None:
        print(args, sep=sep, end=end)

    def input(self, prompt: str) -> str:
        return input(prompt)

    def exit(self, status: int) -> NoReturn:
        exit(status)
