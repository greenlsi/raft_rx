# Copyright 2026 Jose M. Moya <jm.moya@upm.es>
# SPDX-License-Identifier: GPL-3.0-only

from __future__ import annotations

from typing import Protocol

from .messages import Command


class RaftApplication(Protocol):
    def apply(self, command: Command) -> None: ...
    def reload(self) -> None: ...
    def snapshot(self) -> object: ...
    def restore_snapshot(self, snapshot: object) -> None: ...
