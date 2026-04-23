from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from .messages import LogEntry


@dataclass(slots=True)
class PersistentState:
    current_term: int
    voted_for: str | None
    old_members: list[str]
    new_members: list[str] | None
    configuration_index: int
    log: list[LogEntry]
    commit_index: int
    compaction_threshold: int
    snapshot_last_included_index: int
    snapshot_last_included_term: int
    snapshot: object | None


class JsonFileStorage:
    def __init__(self, root: Path | str, node_id: str) -> None:
        self.root = Path(root)
        self.node_id = node_id
        self.node_dir = self.root / node_id
        self.meta_path = self.node_dir / "meta.json"
        self.log_path = self.node_dir / "log.json"
        self.snapshot_path = self.node_dir / "snapshot.json"
        self.node_dir.mkdir(parents=True, exist_ok=True)

    def load(self) -> PersistentState:
        if not self.meta_path.exists():
            return PersistentState(
                current_term=0,
                voted_for=None,
                old_members=[],
                new_members=None,
                configuration_index=0,
                log=[],
                commit_index=0,
                compaction_threshold=32,
                snapshot_last_included_index=0,
                snapshot_last_included_term=0,
                snapshot=None,
            )
        meta = json.loads(self.meta_path.read_text(encoding="utf-8"))
        log_data = []
        if self.log_path.exists():
            log_data = json.loads(self.log_path.read_text(encoding="utf-8"))
        snapshot = None
        if self.snapshot_path.exists():
            snapshot = json.loads(self.snapshot_path.read_text(encoding="utf-8"))
        return PersistentState(
            current_term=int(meta.get("current_term", 0)),
            voted_for=meta.get("voted_for"),
            old_members=[str(member) for member in meta.get("old_members", meta.get("members", []))],
            new_members=(
                [str(member) for member in meta.get("new_members", [])]
                if meta.get("new_members") is not None
                else None
            ),
            configuration_index=int(meta.get("configuration_index", 0)),
            log=[LogEntry.from_dict(item) for item in log_data],
            commit_index=int(meta.get("commit_index", 0)),
            compaction_threshold=int(meta.get("compaction_threshold", 32)),
            snapshot_last_included_index=int(meta.get("snapshot_last_included_index", 0)),
            snapshot_last_included_term=int(meta.get("snapshot_last_included_term", 0)),
            snapshot=snapshot,
        )

    def save(
        self,
        current_term: int,
        voted_for: str | None,
        old_members: list[str],
        new_members: list[str] | None,
        configuration_index: int,
        log: list[LogEntry],
        commit_index: int,
        compaction_threshold: int,
        snapshot_last_included_index: int,
        snapshot_last_included_term: int,
    ) -> None:
        self._atomic_write_json(
            self.meta_path,
            {
                "current_term": current_term,
                "voted_for": voted_for,
                "old_members": old_members,
                "new_members": new_members,
                "configuration_index": configuration_index,
                "commit_index": commit_index,
                "compaction_threshold": compaction_threshold,
                "snapshot_last_included_index": snapshot_last_included_index,
                "snapshot_last_included_term": snapshot_last_included_term,
            },
        )
        self._atomic_write_json(self.log_path, [entry.to_dict() for entry in log])

    def save_snapshot(
        self,
        *,
        last_included_index: int,
        last_included_term: int,
        snapshot: object,
    ) -> None:
        self._atomic_write_json(
            self.snapshot_path,
            {
                "last_included_index": last_included_index,
                "last_included_term": last_included_term,
                "application": snapshot,
            },
        )

    def _atomic_write_json(self, path: Path, data: object) -> None:
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")
        tmp.replace(path)
