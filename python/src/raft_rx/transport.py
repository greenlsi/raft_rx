# Copyright 2026 Jose M. Moya <jm.moya@upm.es>
# SPDX-License-Identifier: GPL-3.0-or-later

from __future__ import annotations

from collections import defaultdict, deque
from collections.abc import Iterable

from .messages import Command, Message


class MemoryTransport:
    def __init__(self) -> None:
        self._messages: dict[str, deque[Message]] = defaultdict(deque)
        self._client_commands: dict[str, deque[Command]] = defaultdict(deque)
        self._enabled: dict[str, bool] = defaultdict(lambda: True)

    def set_enabled(self, node_id: str, enabled: bool) -> None:
        self._enabled[node_id] = enabled
        if not enabled:
            self._messages[node_id].clear()
            self._client_commands[node_id].clear()

    def send(self, message: Message) -> None:
        if not self._enabled[message.target]:
            return
        self._messages[message.target].append(message)

    def send_many(self, messages: Iterable[Message]) -> None:
        for message in messages:
            self.send(message)

    def recv_for(self, node_id: str) -> list[Message]:
        if not self._enabled[node_id]:
            return []
        queue = self._messages[node_id]
        items = list(queue)
        queue.clear()
        return items

    def submit_client_command(self, node_id: str, command: Command) -> None:
        if not self._enabled[node_id]:
            raise RuntimeError(f"node {node_id} is not running")
        self._client_commands[node_id].append(command)

    def recv_client_commands(self, node_id: str) -> list[Command]:
        if not self._enabled[node_id]:
            return []
        queue = self._client_commands[node_id]
        items = list(queue)
        queue.clear()
        return items
