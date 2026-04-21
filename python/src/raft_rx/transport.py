from __future__ import annotations

from collections import defaultdict, deque
from collections.abc import Iterable

from .messages import Command, Message


class MemoryTransport:
    def __init__(self) -> None:
        self._messages: dict[str, deque[Message]] = defaultdict(deque)
        self._client_commands: dict[str, deque[Command]] = defaultdict(deque)

    def send(self, message: Message) -> None:
        self._messages[message.target].append(message)

    def send_many(self, messages: Iterable[Message]) -> None:
        for message in messages:
            self.send(message)

    def recv_for(self, node_id: str) -> list[Message]:
        queue = self._messages[node_id]
        items = list(queue)
        queue.clear()
        return items

    def submit_client_command(self, node_id: str, command: Command) -> None:
        self._client_commands[node_id].append(command)

    def recv_client_commands(self, node_id: str) -> list[Command]:
        queue = self._client_commands[node_id]
        items = list(queue)
        queue.clear()
        return items
