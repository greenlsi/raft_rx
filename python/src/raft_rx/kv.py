from __future__ import annotations

import json
from pathlib import Path

from .messages import Command


class KVStore:
    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.data: dict[str, str] = {}
        self.load()

    def load(self) -> None:
        if self.path.exists():
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            self.data = {str(k): str(v) for k, v in raw.items()}

    def save(self) -> None:
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps(self.data, indent=2, sort_keys=True), encoding="utf-8")
        tmp.replace(self.path)

    def apply(self, command: Command) -> None:
        if command.op == "set":
            if command.value is None:
                raise ValueError("set requires value")
            self.data[command.key] = command.value
            return
        if command.op == "delete":
            self.data.pop(command.key, None)
            return
        raise ValueError(f"unsupported op: {command.op}")

    def get(self, key: str) -> str | None:
        return self.data.get(key)


class KVStateMachine:
    def __init__(self, store: KVStore) -> None:
        self.store = store

    def apply(self, command: Command) -> None:
        self.store.apply(command)
        self.store.save()

    def get(self, key: str) -> str | None:
        return self.store.get(key)
