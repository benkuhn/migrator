from __future__ import annotations

import abc
from dataclasses import dataclass

from typing import Any, NoReturn, Optional, TextIO, cast

from .. import models, db
from . import text

@dataclass
class Context:
    config_path: str
    database_url: str
    ui: UserInterface
    _db: Optional[db.Database] = None
    _repo: Optional[models.Repo] = None

    def repo(self) -> models.Repo:
        if self._repo is None:
            self._repo = models.Repo.parse(self.config_path)
        return self._repo

    def db(self) -> db.Database:
        if self._db is None:
            self._db = db.Database(self.database_url)
        return self._db

    def close(self) -> None:
        if self._db is not None:
            self._db.close()
            self._db = None
        self.ui.close()

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

    @abc.abstractmethod
    def open(self, filename: str, mode: str) -> TextIO:
        pass

    def close(self) -> None:
        pass

class ConsoleUserInterface(UserInterface):
    def print(self, *args: object, sep: Optional[str] = ' ', end: Optional[str] = '\n') -> None:
        print(args, sep=sep, end=end)

    def input(self, prompt: str) -> str:
        return input(prompt)

    def exit(self, status: int) -> NoReturn:
        exit(status)

    def open(self, filename: str, mode: str) -> TextIO:
        return cast(TextIO, open(filename, mode))
