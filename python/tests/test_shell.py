# Copyright 2026 Jose M. Moya <jm.moya@upm.es>
# SPDX-License-Identifier: GPL-3.0-only

from __future__ import annotations

from pathlib import Path
import time

from raft_rx import Command, ManualClock, NodeConfig, RaftCluster, RaftShell
from raft_rx.cluster import ClusterNodePaths

try:
    from python.examples.kv_app import KVApp
except ImportError:
    from examples.kv_app import KVApp


def make_shell(tmp_path: Path, *, trace_enabled: bool = False) -> RaftShell:
    def application_factory(node_id: str) -> KVApp:
        return KVApp(tmp_path / "kv" / f"{node_id}.json")

    def paths_factory(node_id: str) -> ClusterNodePaths:
        return ClusterNodePaths(
            storage_dir=tmp_path / "storage",
            telemetry_path=tmp_path / "telemetry" / f"{node_id}.jsonl",
        )

    cluster = RaftCluster(
        clock=ManualClock(),
        application_factory=application_factory,
        paths_factory=paths_factory,
        trace_enabled=trace_enabled,
    )
    node_ids = ["n1", "n2", "n3"]
    timeouts = {"n1": 150, "n2": 250, "n3": 350}
    for node_id in node_ids:
        peers = [peer for peer in node_ids if peer != node_id]
        cluster.add_node(
            NodeConfig(node_id=node_id, peers=peers, election_timeout_ms=timeouts[node_id]),
            paths_factory(node_id),
            application=application_factory(node_id),
        )
    cluster.run(40, advance_ms=10)
    return RaftShell(cluster, tmp_path, auto_tick_ms=0)


def test_can_register_custom_shell_command(tmp_path: Path, capsys) -> None:
    shell = make_shell(tmp_path)

    def ping(active_shell: RaftShell, args: list[str]) -> None:
        del active_shell
        print("pong", " ".join(args))

    shell.register_command("ping", ping, help_text="custom ping", usage="ping [ARGS]")
    shell.onecmd("ping a b")
    out = capsys.readouterr().out
    assert "pong a b" in out


def test_help_lists_registered_commands(tmp_path: Path, capsys) -> None:
    shell = make_shell(tmp_path)
    shell.register_command("ping", lambda shell, args: None, help_text="custom ping", usage="ping")
    shell.do_help("")
    out = capsys.readouterr().out
    assert "ping" in out


def test_completion_includes_registered_commands(tmp_path: Path) -> None:
    shell = make_shell(tmp_path)
    shell.register_command("ping", lambda shell, args: None, help_text="custom ping", usage="ping")
    completions = shell.completenames("pi")
    assert "ping" in completions


def test_autotick_advances_manual_clock(tmp_path: Path, capsys) -> None:
    shell = make_shell(tmp_path)
    initial_now = shell.cluster.clock.now_ms()
    shell.onecmd("autotick 20")
    time.sleep(0.07)
    shell.onecmd("autotick 0")
    out = capsys.readouterr().out
    assert "autotick=20ms" in out
    assert "autotick disabled" in out
    assert shell.cluster.clock.now_ms() >= initial_now + 40


def test_default_autotick_is_300(tmp_path: Path) -> None:
    cluster = RaftCluster(clock=ManualClock())
    shell = RaftShell(cluster, tmp_path)
    try:
        assert shell.auto_tick_ms == 300
    finally:
        shell.onecmd("autotick 0")


def test_kv_command_works_with_autotick(tmp_path: Path) -> None:
    shell = make_shell(tmp_path)

    def set_cmd(active_shell: RaftShell, args: list[str]) -> None:
        with active_shell.cluster_locked() as cluster:
            leader = cluster.leader()
            assert leader is not None
            leader.submit_command(Command(op="set", key=args[0], value=args[1]))

    shell.register_command("set", set_cmd, help_text="set key", usage="set KEY VALUE")
    shell.onecmd("autotick 20")
    shell.onecmd("set alpha 1")
    time.sleep(0.20)
    shell.onecmd("autotick 0")
    shell.onecmd("tick 10 10")
    with shell.cluster_locked() as cluster:
        assert cluster.nodes["n1"].application.get("alpha") == "1"
        assert cluster.nodes["n2"].application.get("alpha") == "1"
        assert cluster.nodes["n3"].application.get("alpha") == "1"


def test_default_autotick_does_not_destabilize_leader(tmp_path: Path) -> None:
    shell = make_shell(tmp_path)
    shell.onecmd("autotick 300")
    time.sleep(0.35)
    with shell.cluster_locked() as cluster:
        leader_before = cluster.leader()
        assert leader_before is not None
        term_before = leader_before.current_term
    time.sleep(0.35)
    shell.onecmd("autotick 0")
    with shell.cluster_locked() as cluster:
        leader_after = cluster.leader()
        assert leader_after is not None
        assert leader_after.current_term == term_before


def test_log_command_prints_node_log(tmp_path: Path, capsys) -> None:
    shell = make_shell(tmp_path)

    def set_cmd(active_shell: RaftShell, args: list[str]) -> None:
        with active_shell.cluster_locked() as cluster:
            leader = cluster.leader()
            assert leader is not None
            leader.submit_command(Command(op="set", key=args[0], value=args[1]))

    shell.register_command("set", set_cmd, help_text="set key", usage="set KEY VALUE")
    shell.onecmd("set alpha 1")
    shell.onecmd("tick 40 10")
    shell.onecmd("log n1")
    out = capsys.readouterr().out
    assert "log_len=1" in out
    assert "COMMITTED" in out
    assert "op=set" in out
    assert "key='alpha'" in out


def test_maxlog_command_updates_all_nodes(tmp_path: Path, capsys) -> None:
    shell = make_shell(tmp_path)
    shell.onecmd("maxlog 12")
    shell.onecmd("tick 40 10")
    out = capsys.readouterr().out
    assert "queued maxlog=12" in out
    with shell.cluster_locked() as cluster:
        assert all(node.compaction_threshold == 12 for node in cluster.nodes.values())


def test_members_and_addnode_commands(tmp_path: Path, capsys) -> None:
    shell = make_shell(tmp_path)
    shell.onecmd("addnode n4")
    shell.onecmd("tick 40 10")
    shell.onecmd("members")
    out = capsys.readouterr().out
    assert "requested addnode=n4" in out
    assert '"n4"' in out


def test_trace_command_exports_trace_file(tmp_path: Path, capsys) -> None:
    shell = make_shell(tmp_path, trace_enabled=True)
    trace_path = tmp_path / "trace.bin"
    shell.onecmd(f"trace {trace_path}")
    out = capsys.readouterr().out
    assert str(trace_path) in out
    assert trace_path.exists()
    assert trace_path.stat().st_size > 0


def test_raft_events_register_trace_labels(tmp_path: Path) -> None:
    shell = make_shell(tmp_path, trace_enabled=True)

    def set_cmd(active_shell: RaftShell, args: list[str]) -> None:
        with active_shell.cluster_locked() as cluster:
            leader = cluster.leader()
            assert leader is not None
            leader.submit_command(Command(op="set", key=args[0], value=args[1]))

    shell.register_command("set", set_cmd, help_text="set key", usage="set KEY VALUE")
    shell.onecmd("set alpha 1")
    shell.onecmd("tick 40 10")
    tracer = shell.cluster.tracer
    assert tracer is not None
    assert "raft.election.start" in tracer._labels
    assert "raft.election.win" in tracer._labels
    assert "raft.log.append" in tracer._labels
    assert "raft.commit" in tracer._labels
    assert "raft.apply" in tracer._labels


def test_trace_continues_across_topology_change(tmp_path: Path) -> None:
    shell = make_shell(tmp_path, trace_enabled=True)
    tracer_before = shell.cluster.tracer
    assert tracer_before is not None

    shell.onecmd("addnode n4")
    shell.onecmd("start n4")
    shell.onecmd("tick 120 10")

    tracer_after = shell.cluster.tracer
    assert tracer_after is tracer_before
    assert any(name == "n4" for name in tracer_after._names.values())
    assert "raft.membership.requested" in tracer_after._labels
    assert "raft.membership.joint_append" in tracer_after._labels
    assert "raft.membership.joint_committed" in tracer_after._labels
    assert "raft.membership.catchup_ready" in tracer_after._labels
    assert "raft.membership.stable_committed" in tracer_after._labels
    events, count, _, _ = tracer_after._buf.drain()
    assert count > 0
    assert len(events) > 0
