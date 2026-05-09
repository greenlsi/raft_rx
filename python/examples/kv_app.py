# Copyright 2026 Jose M. Moya <jm.moya@upm.es>
# SPDX-License-Identifier: GPL-3.0-or-later

from __future__ import annotations

import json
from pathlib import Path

from raft_rx import Command


class KVApp:
    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.data: dict[str, str] = {}
        self.reload()

    def reload(self) -> None:
        if self.path.exists():
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            self.data = {str(k): str(v) for k, v in raw.items()}
        else:
            self.data = {}

    def save(self) -> None:
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps(self.data, indent=2, sort_keys=True), encoding="utf-8")
        tmp.replace(self.path)

    def apply(self, command: Command) -> None:
        if command.op == "set":
            if command.value is None:
                raise ValueError("set requires value")
            self.data[command.key] = command.value
        elif command.op == "delete":
            self.data.pop(command.key, None)
        else:
            raise ValueError(f"unsupported op: {command.op}")
        self.save()

    def get(self, key: str) -> str | None:
        return self.data.get(key)

    def snapshot(self) -> object:
        return dict(self.data)

    def restore_snapshot(self, snapshot: object) -> None:
        self.data = {str(k): str(v) for k, v in dict(snapshot).items()}
        self.save()
