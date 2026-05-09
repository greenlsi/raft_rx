# Copyright 2026 Jose M. Moya <jm.moya@upm.es>
# SPDX-License-Identifier: GPL-3.0-or-later

from __future__ import annotations

from pathlib import Path

from raft_rx import Command, ManualClock, Message, MessageKind, NodeConfig, RaftCluster
from raft_rx.cluster import ClusterNodePaths
from raft_rx.configuration import ClusterConfiguration

try:
    from python.examples.kv_app import KVApp
except ImportError:
    from examples.kv_app import KVApp


def make_cluster(tmp_path: Path) -> RaftCluster:
    def application_factory(node_id: str) -> KVApp:
        return KVApp(tmp_path / "kv" / f"{node_id}.json")

    def paths_factory(node_id: str) -> ClusterNodePaths:
        return ClusterNodePaths(
            storage_dir=tmp_path / "storage",
            telemetry_path=tmp_path / "telemetry" / f"{node_id}.jsonl",
        )

    cluster = RaftCluster(clock=ManualClock(), application_factory=application_factory, paths_factory=paths_factory)
    node_ids = ["n1", "n2", "n3"]
    timeouts = {"n1": 150, "n2": 250, "n3": 350}
    for node_id in node_ids:
        peers = [peer for peer in node_ids if peer != node_id]
        cluster.add_node(
            NodeConfig(node_id=node_id, peers=peers, election_timeout_ms=timeouts[node_id]),
            paths_factory(node_id),
            application=application_factory(node_id),
        )
    return cluster


def make_dynamic_cluster(tmp_path: Path) -> RaftCluster:
    def application_factory(node_id: str) -> KVApp:
        return KVApp(tmp_path / "kv" / f"{node_id}.json")

    def paths_factory(node_id: str) -> ClusterNodePaths:
        return ClusterNodePaths(
            storage_dir=tmp_path / "storage",
            telemetry_path=tmp_path / "telemetry" / f"{node_id}.jsonl",
        )

    return RaftCluster(clock=ManualClock(), application_factory=application_factory, paths_factory=paths_factory)


def add_dynamic_node(cluster: RaftCluster, tmp_path: Path, node_id: str, initial_members: list[str]) -> None:
    timeouts = {"n1": 150, "n2": 250, "n3": 350}
    cluster.add_node(
        NodeConfig(
            node_id=node_id,
            peers=[],
            election_timeout_ms=timeouts[node_id],
            initial_members=initial_members,
        ),
        ClusterNodePaths(
            storage_dir=tmp_path / "storage",
            telemetry_path=tmp_path / "telemetry" / f"{node_id}.jsonl",
        ),
        application=KVApp(tmp_path / "kv" / f"{node_id}.json"),
    )


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


def test_membership_normalization_matches_c_ordering() -> None:
    assert ClusterConfiguration.normalize_members(["n2", "n1", "n2", "", "n3"]) == ("n1", "n2", "n3")


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


def test_membership_change_replicates(tmp_path: Path) -> None:
    cluster = make_cluster(tmp_path)
    cluster.run(80, advance_ms=10)
    leader = cluster.leader()
    assert leader is not None

    cluster.provision_node("n4", members=["n1", "n2", "n3", "n4"], running=True)
    leader.request_membership_change(["n1", "n2", "n3", "n4"])
    cluster.run(120, advance_ms=10)

    for node in cluster.nodes.values():
        assert "n4" in node.config_state.all_members()


def test_restart_replays_sequential_membership_without_expelled_node(tmp_path: Path) -> None:
    cluster = make_dynamic_cluster(tmp_path)
    add_dynamic_node(cluster, tmp_path, "n1", ["n1"])
    cluster.run(80, advance_ms=10)

    leader = cluster.leader()
    assert leader is not None
    assert leader.node_id == "n1"

    add_dynamic_node(cluster, tmp_path, "n2", [])
    leader.request_membership_change(["n1", "n2"])
    cluster.run(160, advance_ms=10)

    add_dynamic_node(cluster, tmp_path, "n3", [])
    leader.request_membership_change(["n1", "n2", "n3"])
    cluster.run(160, advance_ms=10)

    assert cluster.nodes["n3"].running is True
    assert cluster.nodes["n3"].config_state.old_members == ("n1", "n2", "n3")

    restarted = make_dynamic_cluster(tmp_path)
    for node_id in ("n1", "n2", "n3"):
        add_dynamic_node(restarted, tmp_path, node_id, [])

    restarted.run(1, advance_ms=10)

    assert restarted.nodes["n3"].running is True
    assert restarted.nodes["n3"].config_state.old_members == ("n1", "n2", "n3")


def test_follower_that_becomes_leader_finalizes_committed_joint_config(tmp_path: Path) -> None:
    cluster = make_cluster(tmp_path)
    cluster.run(80, advance_ms=10)
    leader = cluster.leader()
    assert leader is not None
    assert leader.node_id == "n1"

    cluster.provision_node("n4", members=["n1", "n2", "n3", "n4"], running=True)
    leader.request_membership_change(["n1", "n2", "n3", "n4"])

    for _ in range(80):
        cluster.tick(advance_ms=10)
        if (
            leader.config_state.is_joint()
            and cluster.nodes["n2"].config_state.is_joint()
            and cluster.nodes["n3"].config_state.is_joint()
            and cluster.nodes["n4"].config_state.is_joint()
        ):
            break

    assert leader.config_state.is_joint()
    assert cluster.nodes["n2"].config_state.is_joint()
    assert cluster.nodes["n3"].config_state.is_joint()
    assert cluster.nodes["n4"].config_state.is_joint()
    cluster.stop_node(leader.node_id)
    for _ in range(220):
        cluster.tick(advance_ms=10)
        elected = cluster.leader()
        if elected is not None and elected.node_id != "n1":
            break

    elected = cluster.leader()
    assert elected is not None
    assert elected.node_id != "n1"
    cluster.start_node("n1")
    cluster.run(220, advance_ms=10)

    new_leader = cluster.leader()
    assert new_leader is not None
    assert new_leader.node_id != "n1"
    assert new_leader.config_state.is_joint() is False
    assert new_leader.config_state.old_members == ("n1", "n2", "n3", "n4")


def test_leader_steps_down_on_higher_term_vote_request(tmp_path: Path) -> None:
    cluster = make_cluster(tmp_path)
    cluster.run(80, advance_ms=10)

    leader = cluster.leader()
    assert leader is not None
    higher_term = leader.current_term + 5

    cluster.transport.send(
        Message(
            kind=MessageKind.REQUEST_VOTE,
            term=higher_term,
            source="n2",
            target=leader.node_id,
            payload={"last_log_index": leader._last_log_index(), "last_log_term": leader._last_log_term()},
        )
    )
    cluster.run(2, advance_ms=10)

    updated = cluster.nodes[leader.node_id]
    assert updated.role.name == "FOLLOWER"
    assert updated.current_term == higher_term


def test_leader_steps_down_on_higher_term_append_entries_response(tmp_path: Path) -> None:
    cluster = make_cluster(tmp_path)
    cluster.run(80, advance_ms=10)

    leader = cluster.leader()
    assert leader is not None
    higher_term = leader.current_term + 7

    cluster.transport.send(
        Message(
            kind=MessageKind.APPEND_ENTRIES_RESPONSE,
            term=higher_term,
            source="n2",
            target=leader.node_id,
            payload={"success": False, "match_index": 0},
        )
    )
    cluster.run(2, advance_ms=10)

    updated = cluster.nodes[leader.node_id]
    assert updated.role.name == "FOLLOWER"
    assert updated.current_term == higher_term
