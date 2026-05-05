# Copyright 2026 Jose M. Moya <jm.moya@upm.es>
# SPDX-License-Identifier: MIT

"""
demo_coop.py — Python equivalent of examples/demo_app/src/main.c.

Runs a 4-node raft cluster (n1/n2/n3 as initial members, n4 as non-member
joiner) inside a single rxnet CoopExecutive alongside an orchestrator FSM
that drives the demo sequence automatically:

  WAITING_LEADER  → leader elected → submit "color=blue"
  WAITING_COMMIT  → color committed → request membership change to {n1..n4}
  WAITING_MEMBERSHIP → membership stable → print summary and stop
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from rxnet import fsm
from rxnet.coop import CoopExecutive

from raft_rx import Command, NodeConfig, RaftCluster, SystemClock
from raft_rx.cluster import ClusterNodePaths

try:
    from .kv_app import KVApp
except ImportError:
    from kv_app import KVApp  # type: ignore[import]

PERIOD_US = 10_000  # 10 ms tick

# ── Orchestrator states ────────────────────────────────────────────────────

WAITING_LEADER = 0
WAITING_COMMIT = 1
WAITING_MEMBERSHIP = 2


@dataclass
class _Demo:
    cluster: RaftCluster
    apps: dict[str, KVApp]
    ce: CoopExecutive | None = field(default=None, repr=False)


# ── Guards ─────────────────────────────────────────────────────────────────


def _guard_leader_ready(ctx: object, d: _Demo) -> bool:
    return d.cluster.leader() is not None


def _guard_color_committed(ctx: object, d: _Demo) -> bool:
    return d.apps["n1"].get("color") == "blue"


def _guard_membership_stable(ctx: object, d: _Demo) -> bool:
    nodes = d.cluster.nodes
    if len(nodes) < 4:
        return False
    return all(
        n.config_state.mode() == "stable" and len(n.config_state.old_members) == 4
        for n in nodes.values()
    )


# ── Actions ────────────────────────────────────────────────────────────────


def _action_submit_command(ctx: object, d: _Demo) -> None:
    leader = d.cluster.leader()
    assert leader is not None
    leader.submit_command(Command(op="set", key="color", value="blue"))


def _action_request_membership(ctx: object, d: _Demo) -> None:
    leader = d.cluster.leader()
    assert leader is not None
    leader.request_membership_change(["n1", "n2", "n3", "n4"])


def _action_print_and_stop(ctx: object, d: _Demo) -> None:
    for node_id, node in d.cluster.nodes.items():
        print(
            f"{node_id} role={node.role.name} term={node.current_term}"
            f" cfg_members={len(node.config_state.old_members)}"
            f" color={d.apps[node_id].get('color')}"
        )
    assert d.ce is not None
    d.ce.stop()


# ── Main ───────────────────────────────────────────────────────────────────


def main() -> None:
    root = Path("var/python-demo-coop")

    node_ids = ["n1", "n2", "n3", "n4"]
    apps = {nid: KVApp(root / "kv" / f"{nid}.json") for nid in node_ids}

    runtime = fsm.Runtime()
    cluster = RaftCluster(clock=SystemClock(), runtime=runtime)

    for node_id, timeout_ms in [("n1", 150), ("n2", 250), ("n3", 350)]:
        cluster.add_node(
            NodeConfig(
                node_id=node_id,
                peers=[p for p in ["n1", "n2", "n3"] if p != node_id],
                election_timeout_ms=timeout_ms,
                initial_members=["n1", "n2", "n3"],
            ),
            ClusterNodePaths(storage_dir=root / "storage"),
            application=apps[node_id],
            period_us=PERIOD_US,
        )

    # n4 starts as a non-member learner (initial_members=[]) and is admitted
    # to the cluster when the orchestrator requests the membership change.
    cluster.add_node(
        NodeConfig(
            node_id="n4",
            peers=["n1", "n2", "n3"],
            election_timeout_ms=450,
            initial_members=[],
        ),
        ClusterNodePaths(storage_dir=root / "storage"),
        application=apps["n4"],
        period_us=PERIOD_US,
    )

    demo = _Demo(cluster=cluster, apps=apps)

    orchestrator = fsm.Machine(
        name="demo",
        state=WAITING_LEADER,
        transitions=[
            fsm.Transition(WAITING_LEADER, WAITING_COMMIT, _guard_leader_ready, _action_submit_command, "leader_ready"),
            fsm.Transition(WAITING_COMMIT, WAITING_MEMBERSHIP, _guard_color_committed, _action_request_membership, "color_committed"),
            fsm.Transition(WAITING_MEMBERSHIP, WAITING_MEMBERSHIP, _guard_membership_stable, _action_print_and_stop, "membership_stable"),
        ],
        user=demo,
    )
    runtime.add_machine(orchestrator, PERIOD_US)

    ce = CoopExecutive()
    demo.ce = ce
    ce.add(runtime)
    ce.run()


if __name__ == "__main__":
    main()
