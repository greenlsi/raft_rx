# Copyright 2026 Jose M. Moya <jm.moya@upm.es>
# SPDX-License-Identifier: MIT

from __future__ import annotations

from raft_rx import Command


class NoopApp:
    def apply(self, command: Command) -> None:
        del command

    def reload(self) -> None:
        return

    def snapshot(self) -> object:
        return {}

    def restore_snapshot(self, snapshot: object) -> None:
        del snapshot
