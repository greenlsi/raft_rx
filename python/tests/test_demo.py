from __future__ import annotations

from pathlib import Path
import time

from raft_rx.demo import DemoNode, _http_json


def test_demo_single_node_set_commits_and_reads_back(tmp_path: Path) -> None:
    service = DemoNode(
        bind="127.0.0.1:7510",
        data_dir=tmp_path / "n1",
        join=None,
        tick_ms=10,
    )
    service.start()
    try:
        status = _http_json("GET", "http://127.0.0.1:7510/status")
        assert status["node_id"] == "127.0.0.1:7510"

        response = _http_json(
            "POST",
            "http://127.0.0.1:7510/kv/set",
            payload={"key": "alpha", "value": "1"},
        )
        assert response["ok"] is True
        assert response["committed"] is True

        value = _http_json("GET", "http://127.0.0.1:7510/kv/alpha")
        assert value["value"] == "1"

        after = _http_json("GET", "http://127.0.0.1:7510/status")
        assert after["commit_index"] >= 1
        assert after["display_role"] == "LEADER"
        assert after["last_committed_command"]["op"] == "set"
        assert after["last_committed_command"]["key"] == "alpha"

        config = _http_json("GET", "http://127.0.0.1:7510/cluster/config")
        assert config["members"] == ["127.0.0.1:7510"]
        assert config["membership_mode"] == "stable"

        log_payload = _http_json("GET", "http://127.0.0.1:7510/log")
        assert len(log_payload["entries"]) >= 1
        assert log_payload["entries"][-1]["committed"] is True
        assert log_payload["entries"][-1]["command"]["op"] == "set"
    finally:
        service.close()


def test_demo_join_adds_node_to_cluster(tmp_path: Path) -> None:
    leader = DemoNode(
        bind="127.0.0.1:7511",
        data_dir=tmp_path / "leader",
        join=None,
        tick_ms=10,
    )
    joiner = DemoNode(
        bind="127.0.0.1:7512",
        data_dir=tmp_path / "joiner",
        join="127.0.0.1:7511",
        tick_ms=10,
    )
    leader.start()
    joiner.start()
    try:
        leader_status = _http_json("GET", "http://127.0.0.1:7511/status")
        joiner_status = _http_json("GET", "http://127.0.0.1:7512/status")
        assert "127.0.0.1:7512" in leader_status["members"]
        assert "127.0.0.1:7512" in joiner_status["members"]
        leader_config = _http_json("GET", "http://127.0.0.1:7511/cluster/config")
        joiner_config = _http_json("GET", "http://127.0.0.1:7512/cluster/config")
        assert leader_config["membership_mode"] == "stable"
        assert joiner_config["membership_mode"] == "stable"
        joiner_log = _http_json("GET", "http://127.0.0.1:7512/log")
        assert joiner_log["entries"][-1]["command"]["op"] == "cluster.leave_joint"
    finally:
        joiner.close()
        leader.close()


def test_demo_re_elects_after_original_leader_stops(tmp_path: Path) -> None:
    n1 = DemoNode(
        bind="127.0.0.1:7521",
        data_dir=tmp_path / "n1",
        join=None,
        tick_ms=10,
    )
    n2 = DemoNode(
        bind="127.0.0.1:7522",
        data_dir=tmp_path / "n2",
        join="127.0.0.1:7521",
        tick_ms=10,
    )
    n3 = DemoNode(
        bind="127.0.0.1:7523",
        data_dir=tmp_path / "n3",
        join="127.0.0.1:7521",
        tick_ms=10,
    )
    n1.start()
    n2.start()
    join_deadline = time.monotonic() + 5.0
    while time.monotonic() < join_deadline:
        s1 = _http_json("GET", "http://127.0.0.1:7521/status")
        s2 = _http_json("GET", "http://127.0.0.1:7522/status")
        if (
            s1["membership_mode"] == "stable"
            and s2["membership_mode"] == "stable"
            and "127.0.0.1:7522" in s1["members"]
            and "127.0.0.1:7522" in s2["members"]
        ):
            break
        time.sleep(0.05)
    n3.start()
    try:
        before = _http_json("GET", "http://127.0.0.1:7521/status")
        assert before["role"] == "LEADER"

        _http_json("POST", "http://127.0.0.1:7521/node/stop", payload={})

        deadline = time.monotonic() + 5.0
        elected = None
        while time.monotonic() < deadline:
            s2 = _http_json("GET", "http://127.0.0.1:7522/status")
            s3 = _http_json("GET", "http://127.0.0.1:7523/status")
            leaders = [status["node_id"] for status in (s2, s3) if status["running"] and status["role"] == "LEADER"]
            if len(leaders) == 1:
                elected = leaders[0]
                break
            time.sleep(0.05)

        assert elected in {"127.0.0.1:7522", "127.0.0.1:7523"}
    finally:
        n3.close()
        n2.close()
        n1.close()
