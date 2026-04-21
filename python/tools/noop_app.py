from __future__ import annotations

from raft_rx import Command


class NoopApp:
    def apply(self, command: Command) -> None:
        del command

    def reload(self) -> None:
        return
