# Copyright 2026 Jose M. Moya <jm.moya@upm.es>
# SPDX-License-Identifier: GPL-3.0-only

from __future__ import annotations

from pathlib import Path
import socket
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from raft_rx_demo.demo import DemoCli, DemoNode, _http_json, create_runtime


def free_bind() -> str:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return f"127.0.0.1:{sock.getsockname()[1]}"


def test_runtime_registers_raft_and_cli_as_analyzable_nodes(tmp_path: Path) -> None:
    bind = free_bind()
    service = DemoNode(
        bind=bind,
        data_dir=tmp_path / "n1",
        join=None,
        tick_ms=10,
    )
    cli = DemoCli(service)
    runtime = create_runtime(service, cli)

    names = [machine.name for machine in runtime.machines]
    assert names == [
        bind,
        f"{bind}.compaction",
        f"{bind}.membership",
        "cli",
    ]


def test_demo_single_node_set_commits_and_reads_back(tmp_path: Path) -> None:
    bind = free_bind()
    service = DemoNode(
        bind=bind,
        data_dir=tmp_path / "n1",
        join=None,
        tick_ms=10,
    )
    service.start()
    try:
        status = _http_json("GET", f"http://{bind}/status")
        assert status["node_id"] == bind

        response = _http_json(
            "POST",
            f"http://{bind}/kv/set",
            payload={"key": "alpha", "value": "1"},
        )
        assert response["ok"] is True
        assert response["committed"] is True

        value = _http_json("GET", f"http://{bind}/kv/alpha")
        assert value["value"] == "1"

        after = _http_json("GET", f"http://{bind}/status")
        assert after["commit_index"] >= 1
        assert after["display_role"] == "LEADER"
        assert after["last_committed_command"]["op"] == "set"
        assert after["last_committed_command"]["key"] == "alpha"

        config = _http_json("GET", f"http://{bind}/cluster/config")
        assert config["members"] == [bind]
        assert config["membership_mode"] == "stable"

        log_payload = _http_json("GET", f"http://{bind}/log")
        assert len(log_payload["entries"]) >= 1
        assert log_payload["entries"][-1]["committed"] is True
        assert log_payload["entries"][-1]["command"]["op"] == "set"
    finally:
        service.close()


def test_demo_join_adds_node_to_cluster(tmp_path: Path) -> None:
    leader_bind = free_bind()
    joiner_bind = free_bind()
    leader = DemoNode(
        bind=leader_bind,
        data_dir=tmp_path / "leader",
        join=None,
        tick_ms=10,
    )
    joiner = DemoNode(
        bind=joiner_bind,
        data_dir=tmp_path / "joiner",
        join=leader_bind,
        tick_ms=10,
    )
    leader.start()
    joiner.start()
    try:
        leader_status = _http_json("GET", f"http://{leader_bind}/status")
        joiner_status = _http_json("GET", f"http://{joiner_bind}/status")
        assert joiner_bind in leader_status["members"]
        assert joiner_bind in joiner_status["members"]
        leader_config = _http_json("GET", f"http://{leader_bind}/cluster/config")
        joiner_config = _http_json("GET", f"http://{joiner_bind}/cluster/config")
        assert leader_config["membership_mode"] == "stable"
        assert joiner_config["membership_mode"] == "stable"
        joiner_log = _http_json("GET", f"http://{joiner_bind}/log")
        assert joiner_log["entries"][-1]["command"]["op"] == "cluster.leave_joint"
    finally:
        joiner.close()
        leader.close()


def test_demo_re_elects_after_original_leader_stops(tmp_path: Path) -> None:
    n1_bind = free_bind()
    n2_bind = free_bind()
    n3_bind = free_bind()
    n1 = DemoNode(
        bind=n1_bind,
        data_dir=tmp_path / "n1",
        join=None,
        tick_ms=10,
    )
    n2 = DemoNode(
        bind=n2_bind,
        data_dir=tmp_path / "n2",
        join=n1_bind,
        tick_ms=10,
    )
    n3 = DemoNode(
        bind=n3_bind,
        data_dir=tmp_path / "n3",
        join=n1_bind,
        tick_ms=10,
    )
    n1.start()
    n2.start()
    join_deadline = time.monotonic() + 5.0
    while time.monotonic() < join_deadline:
        s1 = _http_json("GET", f"http://{n1_bind}/status")
        s2 = _http_json("GET", f"http://{n2_bind}/status")
        if (
            s1["membership_mode"] == "stable"
            and s2["membership_mode"] == "stable"
            and n2_bind in s1["members"]
            and n2_bind in s2["members"]
        ):
            break
        time.sleep(0.05)
    n3.start()
    try:
        before = _http_json("GET", f"http://{n1_bind}/status")
        assert before["role"] == "LEADER"

        _http_json("POST", f"http://{n1_bind}/node/stop", payload={})

        deadline = time.monotonic() + 5.0
        elected = None
        while time.monotonic() < deadline:
            s2 = _http_json("GET", f"http://{n2_bind}/status")
            s3 = _http_json("GET", f"http://{n3_bind}/status")
            leaders = [status["node_id"] for status in (s2, s3) if status["running"] and status["role"] == "LEADER"]
            if len(leaders) == 1:
                elected = leaders[0]
                break
            time.sleep(0.05)

        assert elected in {n2_bind, n3_bind}
    finally:
        n3.close()
        n2.close()
        n1.close()
