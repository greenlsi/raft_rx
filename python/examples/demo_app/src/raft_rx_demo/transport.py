# Copyright 2026 Jose M. Moya <jm.moya@upm.es>
# SPDX-License-Identifier: GPL-3.0-or-later

"""HTTP transport and REST client helpers for the demo node."""
from __future__ import annotations

import json
import threading
import urllib.error
import urllib.request
from collections import deque

from raft_rx.messages import Command, Message


class HttpTransport:
    def __init__(self, *, timeout_s: float = 1.0) -> None:
        self.timeout_s = timeout_s
        self._messages: deque[Message] = deque()
        self._client_commands: deque[Command] = deque()
        self._enabled = True
        self._lock = threading.RLock()

    def set_enabled(self, node_id: str, enabled: bool) -> None:
        del node_id
        with self._lock:
            self._enabled = enabled
            if not enabled:
                self._messages.clear()
                self._client_commands.clear()

    def send(self, message: Message) -> None:
        with self._lock:
            enabled = self._enabled
        if not enabled:
            return
        try:
            http_json(
                "POST",
                f"http://{message.target}/raft/message",
                payload=message.to_dict(),
                timeout_s=self.timeout_s,
            )
        except OSError:
            return

    def send_many(self, messages: list[Message]) -> None:
        for message in messages:
            self.send(message)

    def recv_for(self, node_id: str) -> list[Message]:
        del node_id
        with self._lock:
            if not self._enabled:
                return []
            items = list(self._messages)
            self._messages.clear()
            return items

    def submit_client_command(self, node_id: str, command: Command) -> None:
        del node_id
        with self._lock:
            if not self._enabled:
                raise RuntimeError("node is not running")
            self._client_commands.append(command)

    def recv_client_commands(self, node_id: str) -> list[Command]:
        del node_id
        with self._lock:
            if not self._enabled:
                return []
            items = list(self._client_commands)
            self._client_commands.clear()
            return items

    def enqueue_message(self, message: Message) -> None:
        with self._lock:
            if not self._enabled:
                return
            self._messages.append(message)


class HttpError(RuntimeError):
    def __init__(self, status: int, body: dict[str, object]) -> None:
        super().__init__(body.get("error", f"http {status}"))
        self.status = status
        self.body = body


def http_json(
    method: str,
    url: str,
    *,
    payload: dict[str, object] | None = None,
    timeout_s: float = 2.0,
) -> dict[str, object]:
    data = None
    headers = {"Content-Type": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(url, method=method, data=data, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=timeout_s) as response:
            raw = response.read()
            return json.loads(raw.decode("utf-8")) if raw else {}
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        body = json.loads(raw.decode("utf-8")) if raw else {}
        raise HttpError(exc.code, body) from exc
