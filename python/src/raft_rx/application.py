from __future__ import annotations

from typing import Protocol

from .messages import Command


class RaftApplication(Protocol):
    def apply(self, command: Command) -> None: ...
    def reload(self) -> None: ...
