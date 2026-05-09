# Copyright 2026 Jose M. Moya <jm.moya@upm.es>
# SPDX-License-Identifier: GPL-3.0-or-later

from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import Any


class MessageKind(StrEnum):
    REQUEST_VOTE = "request_vote"
    REQUEST_VOTE_RESPONSE = "request_vote_response"
    APPEND_ENTRIES = "append_entries"
    APPEND_ENTRIES_RESPONSE = "append_entries_response"
    CLIENT_COMMAND = "client_command"


@dataclass(slots=True)
class Command:
    op: str
    key: str
    value: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Command":
        return cls(op=str(data["op"]), key=str(data["key"]), value=data.get("value"))


@dataclass(slots=True)
class LogEntry:
    index: int
    term: int
    command: Command
    leader_id: str = ""  # node that created this entry

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["command"] = self.command.to_dict()
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "LogEntry":
        return cls(
            index=int(data["index"]),
            term=int(data["term"]),
            command=Command.from_dict(dict(data["command"])),
            leader_id=str(data.get("leader_id", "")),
        )


@dataclass(slots=True)
class Message:
    kind: MessageKind
    term: int
    source: str
    target: str
    payload: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind.value,
            "term": self.term,
            "source": self.source,
            "target": self.target,
            "payload": self.payload,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Message":
        return cls(
            kind=MessageKind(data["kind"]),
            term=int(data["term"]),
            source=str(data["source"]),
            target=str(data["target"]),
            payload=dict(data["payload"]),
        )
