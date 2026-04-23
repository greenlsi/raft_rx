from __future__ import annotations

import argparse
import cmd
import json
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import deque
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from rxnet import fsm

from .clock import SystemClock
from .messages import Command, Message
from .node import NodeConfig, RaftNode, Role
from .storage import JsonFileStorage


class DemoKVApp:
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
            _http_json(
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


class DemoNode:
    def __init__(
        self,
        *,
        bind: str,
        data_dir: Path,
        join: str | None,
        tick_ms: int = 25,
    ) -> None:
        self.bind = bind
        self.host, port_text = bind.rsplit(":", 1)
        self.port = int(port_text)
        self.data_dir = data_dir
        self.join_target = join
        self.tick_ms = tick_ms
        self.lock = threading.RLock()
        self.clock = SystemClock()
        self.transport = HttpTransport()
        self.runtime = fsm.Runtime()
        self.application = DemoKVApp(data_dir / "kv.json")
        self.node = RaftNode(
            config=NodeConfig(
                node_id=bind,
                peers=[],
                election_timeout_ms=400 + (self.port % 200),
                initial_members=[] if join else [bind],
            ),
            clock=self.clock,
            transport=self.transport,
            storage=JsonFileStorage(data_dir / "storage", bind),
            application=self.application,
        )
        self.runtime.add_machine(self.node.machine)
        self.runtime.add_machine(self.node.compaction_machine)
        self.runtime.add_machine(self.node.membership_machine)
        self.runtime.build()
        self._httpd = ThreadingHTTPServer((self.host, self.port), self._make_handler())
        self._tick_stop = threading.Event()
        self._tick_thread = threading.Thread(target=self._tick_loop, daemon=True, name="raft-rx-demo-tick")
        self._server_thread = threading.Thread(target=self._httpd.serve_forever, daemon=True, name="raft-rx-demo-http")
        self.command_timeout_s = 5.0
        self.startup_timeout_s = 3.0
        self.join_timeout_s = 20.0

    def start(self) -> None:
        self._server_thread.start()
        self._tick_thread.start()
        if self.join_target is not None:
            self.join_cluster(self.join_target)
        else:
            self._wait_for_leader(self.startup_timeout_s)

    def close(self) -> None:
        self._tick_stop.set()
        self._httpd.shutdown()
        self._httpd.server_close()
        self._server_thread.join(timeout=1.0)
        self._tick_thread.join(timeout=1.0)

    def status(self) -> dict[str, Any]:
        with self.lock:
            summary = self.node.summary()
            summary["address"] = self.bind
            summary["peers"] = list(self.node.peers)
            summary["display_role"] = self._display_role()
            return summary

    def cluster_config(self) -> dict[str, Any]:
        with self.lock:
            return {
                "node_id": self.bind,
                "display_role": self._display_role(),
                "running": self.node.running,
                "term": self.node.current_term,
                "leader_id": self.node.leader_id,
                "membership_mode": self.node.config_state.mode(),
                "configuration_index": self.node.config_state.index,
                "members": list(self.node.config_state.all_members()),
                "old_members": list(self.node.config_state.old_members),
                "new_members": list(self.node.config_state.new_members) if self.node.config_state.new_members is not None else None,
            }

    def log_entries(self, limit: int | None = None) -> dict[str, Any]:
        with self.lock:
            entries = list(self.node.log)
            if limit is not None and limit >= 0:
                entries = entries[-limit:]
            return {
                "node_id": self.bind,
                "display_role": self._display_role(),
                "snapshot_index": self.node.snapshot_last_included_index,
                "commit_index": self.node.commit_index,
                "last_applied": self.node.last_applied,
                "entries": [
                    {
                        "index": entry.index,
                        "term": entry.term,
                        "committed": entry.index <= self.node.commit_index,
                        "applied": entry.index <= self.node.last_applied,
                        "command": entry.command.to_dict(),
                    }
                    for entry in entries
                ],
            }

    def get(self, key: str) -> str | None:
        with self.lock:
            return self.application.get(key)

    def set_key(self, key: str, value: str) -> dict[str, Any]:
        return self._submit_or_forward(Command(op="set", key=key, value=value), "/kv/set", {"key": key, "value": value})

    def delete_key(self, key: str) -> dict[str, Any]:
        return self._submit_or_forward(Command(op="delete", key=key), "/kv/delete", {"key": key})

    def add_node(self, node_id: str) -> dict[str, Any]:
        local_leader = False
        with self.lock:
            if self.node.role == Role.LEADER:
                local_leader = True
                members = list(self.node.config_state.all_members())
                if node_id not in members:
                    self.node.request_membership_change([*members, node_id])
            else:
                leader = self.node.leader_id
        if local_leader:
            committed = self._wait_for_membership(
                lambda config: config["membership_mode"] == "stable" and node_id in config["old_members"]
            )
            with self.lock:
                members = list(self.node.config_state.all_members())
            return {
                "ok": committed,
                "committed": committed,
                "leader": self.bind,
                "members": members,
            }
        return self._forward_to_leader("/cluster/add-node", {"node": node_id}, leader)

    def remove_node(self, node_id: str) -> dict[str, Any]:
        local_leader = False
        with self.lock:
            if self.node.role == Role.LEADER:
                local_leader = True
                members = list(self.node.config_state.all_members())
                self.node.request_membership_change([member for member in members if member != node_id])
            else:
                leader = self.node.leader_id
        if local_leader:
            committed = self._wait_for_membership(
                lambda config: config["membership_mode"] == "stable" and node_id not in config["old_members"]
            )
            with self.lock:
                members = list(self.node.config_state.all_members())
            return {
                "ok": committed,
                "committed": committed,
                "leader": self.bind,
                "members": members,
            }
        return self._forward_to_leader("/cluster/remove-node", {"node": node_id}, leader)

    def join_cluster(self, target: str) -> dict[str, Any]:
        response = self._request_with_leader_follow(target, "/cluster/join", {"node": self.bind})
        if not self._wait_for_local_stable_membership(self.bind, timeout_s=self.join_timeout_s):
            raise RuntimeError("join was accepted by the leader but this node did not apply the final stable configuration")
        return response

    def stop_node(self) -> dict[str, Any]:
        with self.lock:
            self.node.stop()
            return {"ok": True, "running": self.node.running}

    def start_node(self) -> dict[str, Any]:
        with self.lock:
            self.node.start()
            return {"ok": True, "running": self.node.running}

    def _submit_or_forward(self, command: Command, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        local_leader = False
        with self.lock:
            if self.node.role == Role.LEADER:
                local_leader = True
                self.node.submit_command(command)
            else:
                leader = self.node.leader_id
        if local_leader:
            committed = self._wait_for_command(command)
            return {"ok": committed, "committed": committed, "leader": self.bind}
        if leader is None and self._wait_for_leader(self.command_timeout_s):
            with self.lock:
                if self.node.role == Role.LEADER:
                    self.node.submit_command(command)
                    committed = self._wait_for_command(command)
                    return {"ok": committed, "committed": committed, "leader": self.bind}
                leader = self.node.leader_id
        return self._forward_to_leader(path, payload, leader)

    def _forward_to_leader(self, path: str, payload: dict[str, Any], leader: str | None) -> dict[str, Any]:
        if leader is None:
            raise RuntimeError("no leader known")
        return _http_json("POST", f"http://{leader}{path}", payload=payload, timeout_s=self.command_timeout_s + 1.0)

    def _request_with_leader_follow(self, target: str, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        current = target
        for _ in range(max(4, int(self.startup_timeout_s / max(self.tick_ms / 1000.0, 0.01)))):
            try:
                return _http_json(
                    "POST",
                    f"http://{current}{path}",
                    payload=payload,
                    timeout_s=self.command_timeout_s + 1.0,
                )
            except HttpError as exc:
                if exc.status != HTTPStatus.CONFLICT:
                    raise
                leader = exc.body.get("leader")
                if leader and leader != current:
                    current = str(leader)
                time.sleep(max(self.tick_ms / 1000.0, 0.01))
        raise RuntimeError("could not reach leader")

    def _tick_loop(self) -> None:
        while not self._tick_stop.wait(self.tick_ms / 1000.0):
            with self.lock:
                self.runtime.tick()

    def _display_role(self) -> str:
        if not self.node.running:
            return self.node.role.name
        if not self.node._is_voting_member():
            return "LEARNER"
        return self.node.role.name

    def _wait_for_command(self, command: Command) -> bool:
        deadline = time.monotonic() + self.command_timeout_s
        while time.monotonic() < deadline:
            with self.lock:
                committed = self.node.last_committed_command()
                if committed == command:
                    return True
                if command.op == "set" and self.application.get(command.key) == command.value:
                    return True
                if command.op == "delete" and self.application.get(command.key) is None:
                    return True
            time.sleep(max(self.tick_ms / 1000.0, 0.01))
        return False

    def _wait_for_leader(self, timeout_s: float) -> bool:
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            with self.lock:
                if self.node.role == Role.LEADER or self.node.leader_id is not None:
                    return True
            time.sleep(max(self.tick_ms / 1000.0, 0.01))
        return False

    def _wait_for_membership(self, predicate: Any) -> bool:
        deadline = time.monotonic() + self.command_timeout_s
        while time.monotonic() < deadline:
            config = self.cluster_config()
            if predicate(config):
                return True
            time.sleep(max(self.tick_ms / 1000.0, 0.01))
        return False

    def _wait_for_local_stable_membership(self, node_id: str, *, timeout_s: float) -> bool:
        deadline = time.monotonic() + timeout_s
        last_progress: tuple[str, int, int, bool] | None = None
        while time.monotonic() < deadline:
            with self.lock:
                mode = self.node.config_state.mode()
                in_members = node_id in self.node.config_state.old_members
                cfg_index = self.node.config_state.index
                applied = self.node.last_applied
                if (
                    mode == "stable"
                    and in_members
                    and applied >= cfg_index
                ):
                    return True
            progress = (mode, cfg_index, applied, in_members)
            if last_progress is None or progress != last_progress:
                deadline = time.monotonic() + timeout_s
                last_progress = progress
            time.sleep(max(self.tick_ms / 1000.0, 0.01))
        return False

    def _make_handler(self) -> type[BaseHTTPRequestHandler]:
        service = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                try:
                    if self.path == "/status":
                        self._respond(HTTPStatus.OK, service.status())
                        return
                    if self.path == "/cluster/config":
                        self._respond(HTTPStatus.OK, service.cluster_config())
                        return
                    if self.path.startswith("/log"):
                        parsed = urllib.parse.urlparse(self.path)
                        params = urllib.parse.parse_qs(parsed.query)
                        limit = None
                        if "limit" in params and params["limit"]:
                            limit = int(params["limit"][0])
                        self._respond(HTTPStatus.OK, service.log_entries(limit=limit))
                        return
                    if self.path.startswith("/kv/"):
                        key = urllib.parse.unquote(self.path.removeprefix("/kv/"))
                        self._respond(HTTPStatus.OK, {"key": key, "value": service.get(key)})
                        return
                    self._respond(HTTPStatus.NOT_FOUND, {"error": "not_found"})
                except Exception as exc:  # pragma: no cover - demo path
                    self._respond(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": str(exc)})

            def do_POST(self) -> None:
                try:
                    body = self._read_json()
                    if self.path == "/raft/message":
                        service.transport.enqueue_message(Message.from_dict(body))
                        self._respond(HTTPStatus.ACCEPTED, {"ok": True})
                        return
                    if self.path == "/kv/set":
                        self._respond(HTTPStatus.OK, service.set_key(str(body["key"]), str(body["value"])))
                        return
                    if self.path == "/kv/delete":
                        self._respond(HTTPStatus.OK, service.delete_key(str(body["key"])))
                        return
                    if self.path == "/cluster/add-node":
                        self._respond(HTTPStatus.OK, service.add_node(str(body["node"])))
                        return
                    if self.path == "/cluster/remove-node":
                        self._respond(HTTPStatus.OK, service.remove_node(str(body["node"])))
                        return
                    if self.path == "/cluster/join":
                        node_id = str(body["node"])
                        local_leader = False
                        with service.lock:
                            if service.node.role == Role.LEADER:
                                local_leader = True
                                members = list(service.node.config_state.all_members())
                                if node_id not in members:
                                    service.node.request_membership_change([*members, node_id])
                            else:
                                leader = service.node.leader_id
                        if local_leader:
                            committed = service._wait_for_membership(
                                lambda config: config["membership_mode"] == "stable" and node_id in config["old_members"]
                            )
                            self._respond(
                                HTTPStatus.OK,
                                {
                                    "ok": committed,
                                    "committed": committed,
                                    "leader": service.bind,
                                    "members": list(service.node.config_state.all_members()),
                                },
                            )
                            return
                        self._respond(HTTPStatus.CONFLICT, {"error": "not_leader", "leader": leader})
                        return
                    if self.path == "/node/stop":
                        self._respond(HTTPStatus.OK, service.stop_node())
                        return
                    if self.path == "/node/start":
                        self._respond(HTTPStatus.OK, service.start_node())
                        return
                    self._respond(HTTPStatus.NOT_FOUND, {"error": "not_found"})
                except HttpError as exc:
                    self._respond(exc.status, exc.body)
                except Exception as exc:  # pragma: no cover - demo path
                    self._respond(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": str(exc)})

            def log_message(self, format: str, *args: object) -> None:
                del format, args

            def _read_json(self) -> dict[str, Any]:
                size = int(self.headers.get("Content-Length", "0"))
                raw = self.rfile.read(size) if size else b"{}"
                return json.loads(raw.decode("utf-8"))

            def _respond(self, status: HTTPStatus | int, payload: dict[str, Any]) -> None:
                body = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
                self.send_response(int(status))
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        return Handler


class HttpError(RuntimeError):
    def __init__(self, status: int, body: dict[str, Any]) -> None:
        super().__init__(body.get("error", f"http {status}"))
        self.status = status
        self.body = body


def _http_json(method: str, url: str, *, payload: dict[str, Any] | None = None, timeout_s: float = 2.0) -> dict[str, Any]:
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


class DemoShell(cmd.Cmd):
    intro = "raft-rx demo shell. Type 'help' to list commands."
    prompt = "demo> "

    def __init__(self, service: DemoNode) -> None:
        super().__init__()
        self.service = service
        self._refresh_prompt()

    def preloop(self) -> None:
        self._refresh_prompt()

    def postcmd(self, stop: bool, line: str) -> bool:
        del line
        self._refresh_prompt()
        return stop

    def _refresh_prompt(self) -> None:
        try:
            status = self.service.status()
            leader = status["leader_id"] or "-"
            self.prompt = (
                f"[{status['display_role']} {status['membership_mode']} term={status['term']} leader={leader} "
                f"log={status['log_len']} commit={status['commit_index']}] demo> "
            )
        except Exception:
            self.prompt = "[unknown] demo> "

    def do_status(self, arg: str) -> None:
        del arg
        status = self.service.status()
        last = status.get("last_committed_command")
        print(
            f"node={status['node_id']} running={status['running']} role={status['display_role']} "
            f"term={status['term']} log={status['log_len']} commit={status['commit_index']} "
            f"last_committed={last}"
        )

    def do_members(self, arg: str) -> None:
        del arg
        config = self.service.cluster_config()
        print(
            f"node={config['node_id']} role={config['display_role']} term={config['term']} "
            f"leader={config['leader_id']} mode={config['membership_mode']} "
            f"config_index={config['configuration_index']}"
        )
        print(f"members={config['members']}")
        print(f"old_members={config['old_members']}")
        print(f"new_members={config['new_members']}")

    def do_log(self, arg: str) -> None:
        value = arg.strip()
        limit = None
        if value:
            try:
                limit = int(value)
            except ValueError:
                print("usage: log [LIMIT]")
                return
        payload = self.service.log_entries(limit=limit)
        print(
            f"node={payload['node_id']} role={payload['display_role']} "
            f"snapshot_index={payload['snapshot_index']} commit={payload['commit_index']} "
            f"applied={payload['last_applied']}"
        )
        for item in payload["entries"]:
            status = []
            if item["committed"]:
                status.append("COMMITTED")
            if item["applied"]:
                status.append("APPLIED")
            state = ",".join(status) if status else "PENDING"
            command = item["command"]
            print(
                f"#{item['index']} term={item['term']} state={state} "
                f"op={command['op']} key={command['key']!r} value={command.get('value')!r}"
            )

    def do_set(self, arg: str) -> None:
        parts = arg.split(maxsplit=1)
        if len(parts) != 2:
            print("usage: set KEY VALUE")
            return
        print(self.service.set_key(parts[0], parts[1]))

    def do_get(self, arg: str) -> None:
        key = arg.strip()
        if not key:
            print("usage: get KEY")
            return
        print(self.service.get(key))

    def do_delete(self, arg: str) -> None:
        key = arg.strip()
        if not key:
            print("usage: delete KEY")
            return
        print(self.service.delete_key(key))

    def do_addnode(self, arg: str) -> None:
        node_id = arg.strip()
        if not node_id:
            print("usage: addnode HOST:PORT")
            return
        print(self.service.add_node(node_id))

    def do_rmnode(self, arg: str) -> None:
        node_id = arg.strip()
        if not node_id:
            print("usage: rmnode HOST:PORT")
            return
        print(self.service.remove_node(node_id))

    def do_join(self, arg: str) -> None:
        target = arg.strip()
        if not target:
            print("usage: join HOST:PORT")
            return
        print(self.service.join_cluster(target))

    def do_stop(self, arg: str) -> None:
        del arg
        print(self.service.stop_node())

    def do_start(self, arg: str) -> None:
        del arg
        print(self.service.start_node())

    def do_quit(self, arg: str) -> bool:
        del arg
        return True

    def do_exit(self, arg: str) -> bool:
        return self.do_quit(arg)

    def do_EOF(self, arg: str) -> bool:
        print()
        return self.do_quit(arg)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Raft KV demo node with REST API and local CLI")
    parser.add_argument("--bind", default="127.0.0.1:7400", help="address to bind and advertise as HOST:PORT")
    parser.add_argument("--join", help="existing HOST:PORT to join via REST")
    parser.add_argument("--data-dir", help="directory for persistent state")
    parser.add_argument("--tick-ms", type=int, default=25, help="tick period in milliseconds")
    args = parser.parse_args(argv)

    data_dir = Path(args.data_dir) if args.data_dir else Path("var/demo") / args.bind.replace(":", "_")
    service = DemoNode(bind=args.bind, data_dir=data_dir, join=args.join, tick_ms=args.tick_ms)
    service.start()

    print(f"node={args.bind}")
    print(f"rest=http://{args.bind}")
    print(f"data_dir={data_dir}")
    if args.join:
        print(f"joined_via={args.join}")

    shell = DemoShell(service)
    try:
        shell.cmdloop()
    finally:
        service.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
