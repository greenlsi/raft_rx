# Copyright 2026 Jose M. Moya <jm.moya@upm.es>
# SPDX-License-Identifier: MIT

"""Single-process Raft/KV demo node with HTTP transport and rxnet CLI."""
from __future__ import annotations

import argparse
import json
import threading
import time
import urllib.parse
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, cast

from rxnet import fsm

from raft_rx.clock import SystemClock
from raft_rx.messages import Command, Message
from raft_rx.node import NodeConfig, RaftNode, Role
from raft_rx.storage import JsonFileStorage
from raft_rx_demo.app import DemoKVApp
from raft_rx_demo.cli import CLI_PERIOD_US, DemoCli, create_cli_fsm
from raft_rx_demo.transport import HttpError, HttpTransport, http_json as _http_json

_DEFAULT_TICK_MS = 25


class DemoNode:
    def __init__(
        self,
        *,
        bind: str,
        data_dir: Path,
        join: str | None,
        tick_ms: int = _DEFAULT_TICK_MS,
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
        self.runtime: fsm.Runtime | None = None
        self.application = DemoKVApp(data_dir / "kv.json")
        self.node = RaftNode(
            config=NodeConfig(
                node_id=bind,
                peers=[],
                election_timeout_ms=400 + (self.port % 200),
                initial_members=[] if join else [bind],
            ),
            clock=self.clock,
            transport=cast(Any, self.transport),
            storage=JsonFileStorage(data_dir / "storage", bind),
            application=self.application,
        )
        self._httpd = ThreadingHTTPServer((self.host, self.port), self._make_handler())
        self._server_thread = threading.Thread(
            target=self._httpd.serve_forever, daemon=True, name="raft-rx-demo-http"
        )
        self._exec: Any = None
        self._exec_thread: threading.Thread | None = None
        self.command_timeout_s = 5.0
        self.startup_timeout_s = 3.0
        self.join_timeout_s = 20.0

    def start(self) -> None:
        self.start_http()
        self.runtime = create_runtime(self, cli=None)
        from rxnet.coop import CoopExecutive

        self._exec = CoopExecutive()
        self._exec.add(self.runtime)
        self._exec_thread = threading.Thread(target=self._exec.run, daemon=True, name="raft-rx-exec")
        self._exec_thread.start()
        if self.join_target is not None:
            self.join_cluster(self.join_target)
        else:
            self._wait_for_leader(self.startup_timeout_s)

    def close(self) -> None:
        if self._exec is not None:
            self._exec.stop()
        if self._exec_thread is not None:
            self._exec_thread.join(timeout=1.0)
        self._exec = None
        self._exec_thread = None
        self.stop_http()

    def start_http(self) -> None:
        if not self._server_thread.is_alive():
            self._server_thread.start()

    def stop_http(self) -> None:
        self._httpd.shutdown()
        self._httpd.server_close()
        if self._server_thread.is_alive():
            self._server_thread.join(timeout=1.0)

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

    def queue_client_command(self, command: Command) -> dict[str, Any]:
        with self.lock:
            if self.node.role == Role.LEADER:
                self.node.submit_command(command)
                return {"ok": True, "queued": True, "leader": self.bind}
            leader = self.node.leader_id
        if leader is None:
            return {"ok": False, "queued": False, "error": "no_leader_known"}
        return _http_json(
            "POST",
            f"http://{leader}/raft/client-command",
            payload=command.to_dict(),
            timeout_s=self.command_timeout_s + 1.0,
        )

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

    def queue_add_node(self, node_id: str) -> dict[str, Any]:
        with self.lock:
            members = list(self.node.config_state.all_members())
            if node_id not in members:
                members.append(node_id)
        return self.queue_membership_change(members)

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

    def queue_remove_node(self, node_id: str) -> dict[str, Any]:
        with self.lock:
            members = [member for member in self.node.config_state.all_members() if member != node_id]
        return self.queue_membership_change(members)

    def queue_membership_change(self, members: list[str]) -> dict[str, Any]:
        with self.lock:
            if self.node.role == Role.LEADER:
                self.node.request_membership_change(members)
                return {"ok": True, "queued": True, "leader": self.bind, "members": members}
            leader = self.node.leader_id
        if leader is None:
            return {"ok": False, "queued": False, "error": "no_leader_known"}
        return _http_json(
            "POST",
            f"http://{leader}/cluster/membership-request",
            payload={"members": members},
            timeout_s=self.command_timeout_s + 1.0,
        )

    def join_cluster(self, target: str) -> dict[str, Any]:
        response = self._request_with_leader_follow(target, "/cluster/join", {"node": self.bind})
        if not self._wait_for_local_stable_membership(self.bind, timeout_s=self.join_timeout_s):
            raise RuntimeError("join was accepted by the leader but this node did not apply the final stable configuration")
        return response

    def merge_cluster(self, target: str) -> dict[str, Any]:
        """Merge this cluster with the cluster at target (HOST:PORT)."""
        with self.lock:
            local_members = list(self.node.config_state.all_members())

        response = _http_json(
            "POST",
            f"http://{target}/cluster/merge",
            payload={"members": local_members},
            timeout_s=self.join_timeout_s,
        )
        remote_members = [str(m) for m in response.get("members", [])]

        for member in remote_members:
            if member not in local_members:
                self.add_node(member)

        all_expected = set(local_members) | set(remote_members)
        deadline = time.monotonic() + self.join_timeout_s
        while time.monotonic() < deadline:
            config = self.cluster_config()
            if (config["membership_mode"] == "stable" and
                    all_expected.issubset(set(config["old_members"]))):
                break
            time.sleep(max(self.tick_ms / 1000.0, 0.01))

        with self.lock:
            merged = list(self.node.config_state.all_members())
        return {
            "ok": True,
            "local_members": local_members,
            "remote_members": remote_members,
            "merged_members": merged,
        }

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
                    if self.path == "/raft/client-command":
                        with service.lock:
                            if service.node.role == Role.LEADER:
                                command = Command.from_dict(body)
                                service.node.submit_command(command)
                                self._respond(
                                    HTTPStatus.ACCEPTED,
                                    {"ok": True, "queued": True, "leader": service.bind},
                                )
                                return
                            leader = service.node.leader_id
                        self._respond(HTTPStatus.CONFLICT, {"error": "not_leader", "leader": leader})
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
                    if self.path == "/cluster/membership-request":
                        members = [str(member) for member in body.get("members", [])]
                        with service.lock:
                            if service.node.role == Role.LEADER:
                                service.node.request_membership_change(members)
                                self._respond(
                                    HTTPStatus.ACCEPTED,
                                    {"ok": True, "queued": True, "leader": service.bind, "members": members},
                                )
                                return
                            leader = service.node.leader_id
                        self._respond(HTTPStatus.CONFLICT, {"error": "not_leader", "leader": leader})
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
                    if self.path == "/cluster/merge":
                        remote_members = [str(m) for m in body.get("members", [])]
                        with service.lock:
                            local_members = list(service.node.config_state.all_members())
                        for member in remote_members:
                            if member not in local_members:
                                service.add_node(member)
                        self._respond(HTTPStatus.OK, {"members": local_members})
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


def create_runtime(service: DemoNode, cli: DemoCli | None, *, tick_ms: int | None = None) -> fsm.Runtime:
    period_us = (tick_ms if tick_ms is not None else service.tick_ms) * 1000
    runtime = fsm.Runtime()
    service.node.add_to_runtime(runtime, period_us)
    if cli is not None:
        runtime.add_machine(create_cli_fsm(cli), CLI_PERIOD_US)
    runtime.build()
    return runtime


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Raft KV demo node with REST API and local CLI")
    parser.add_argument("--bind", default="127.0.0.1:7400", help="address to bind and advertise as HOST:PORT")
    parser.add_argument("--join", help="existing HOST:PORT to join via REST")
    parser.add_argument("--data-dir", help="directory for persistent state")
    parser.add_argument("--tick-ms", type=int, default=_DEFAULT_TICK_MS, help="tick period in milliseconds")
    args = parser.parse_args(argv)

    data_dir = Path(args.data_dir) if args.data_dir else Path("var/demo") / args.bind.replace(":", "_")
    service = DemoNode(
        bind=args.bind,
        data_dir=data_dir,
        join=args.join,
        tick_ms=args.tick_ms,
    )
    cli = DemoCli(service)
    runtime = create_runtime(service, cli)
    from rxnet.coop import CoopExecutive

    executive = CoopExecutive()
    cli.executive = executive
    executive.add(runtime)
    service.runtime = runtime
    service._exec = executive
    service.start_http()

    print(f"node={args.bind}")
    print(f"rest=http://{args.bind}")
    print(f"data_dir={data_dir}")
    if args.join:
        print(f"joined_via={args.join}")

        def join_worker() -> None:
            try:
                service.join_cluster(args.join)
            except Exception as exc:  # pragma: no cover - interactive path
                print(f"\njoin failed: {exc}")
                cli.prompt_needed = True

        threading.Thread(target=join_worker, daemon=True, name="raft-rx-join").start()

    try:
        executive.run()
    finally:
        service.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
