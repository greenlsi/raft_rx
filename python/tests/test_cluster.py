from __future__ import annotations

from pathlib import Path

from raft_rx import Command, ManualClock, NodeConfig, RaftCluster
from raft_rx.cluster import ClusterNodePaths

try:
    from python.examples.kv_app import KVApp
except ImportError:
    from examples.kv_app import KVApp


def make_cluster(tmp_path: Path) -> RaftCluster:
    cluster = RaftCluster(clock=ManualClock())
    node_ids = ["n1", "n2", "n3"]
    timeouts = {"n1": 150, "n2": 250, "n3": 350}
    for node_id in node_ids:
        peers = [peer for peer in node_ids if peer != node_id]
        cluster.add_node(
            NodeConfig(node_id=node_id, peers=peers, election_timeout_ms=timeouts[node_id]),
            ClusterNodePaths(
                storage_dir=tmp_path / "storage",
                telemetry_path=tmp_path / "telemetry" / f"{node_id}.jsonl",
            ),
            application=KVApp(tmp_path / "kv" / f"{node_id}.json"),
        )
    return cluster


def test_election_and_replication(tmp_path: Path) -> None:
    cluster = make_cluster(tmp_path)
    cluster.run(80, advance_ms=10)

    leader = cluster.leader()
    assert leader is not None
    assert leader.node_id == "n1"

    leader.submit_command(Command(op="set", key="alpha", value="1"))
    cluster.run(80, advance_ms=10)

    for node in cluster.nodes.values():
        assert node.application.get("alpha") == "1"
        assert node.commit_index >= 1


def test_restart_recovers_persisted_state(tmp_path: Path) -> None:
    cluster = make_cluster(tmp_path)
    cluster.run(80, advance_ms=10)
    leader = cluster.leader()
    assert leader is not None

    leader.submit_command(Command(op="set", key="beta", value="2"))
    cluster.run(80, advance_ms=10)

    restarted = make_cluster(tmp_path)
    for node in restarted.nodes.values():
        assert node.application.get("beta") == "2"
        assert len(node.log) >= 1


def test_stop_and_restart_node_recovers_state(tmp_path: Path) -> None:
    cluster = make_cluster(tmp_path)
    cluster.run(80, advance_ms=10)

    leader = cluster.leader()
    assert leader is not None
    leader.submit_command(Command(op="set", key="gamma", value="3"))
    cluster.run(80, advance_ms=10)

    cluster.stop_node("n2")
    assert cluster.nodes["n2"].running is False
    assert cluster.nodes["n2"].role.name == "FOLLOWER"

    cluster.run(30, advance_ms=10)
    cluster.start_node("n2")
    cluster.run(40, advance_ms=10)

    assert cluster.nodes["n2"].application.get("gamma") == "3"
    assert cluster.nodes["n2"].commit_index >= 1
