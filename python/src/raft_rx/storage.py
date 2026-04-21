from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from .messages import LogEntry


@dataclass(slots=True)
class PersistentState:
    current_term: int
    voted_for: str | None
    log: list[LogEntry]


class JsonFileStorage:
    def __init__(self, root: Path | str, node_id: str) -> None:
        self.root = Path(root)
        self.node_id = node_id
        self.node_dir = self.root / node_id
        self.meta_path = self.node_dir / "meta.json"
        self.log_path = self.node_dir / "log.json"
        self.node_dir.mkdir(parents=True, exist_ok=True)

    def load(self) -> PersistentState:
        if not self.meta_path.exists():
            return PersistentState(current_term=0, voted_for=None, log=[])
        meta = json.loads(self.meta_path.read_text(encoding="utf-8"))
        log_data = []
        if self.log_path.exists():
            log_data = json.loads(self.log_path.read_text(encoding="utf-8"))
        return PersistentState(
            current_term=int(meta.get("current_term", 0)),
            voted_for=meta.get("voted_for"),
            log=[LogEntry.from_dict(item) for item in log_data],
        )

    def save(self, current_term: int, voted_for: str | None, log: list[LogEntry]) -> None:
        self._atomic_write_json(
            self.meta_path,
            {"current_term": current_term, "voted_for": voted_for},
        )
        self._atomic_write_json(self.log_path, [entry.to_dict() for entry in log])

    def _atomic_write_json(self, path: Path, data: object) -> None:
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")
        tmp.replace(path)
