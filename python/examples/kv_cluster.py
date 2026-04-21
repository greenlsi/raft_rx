from __future__ import annotations

from pathlib import Path

from raft_rx import Command, ManualClock, NodeConfig, RaftCluster
from raft_rx.cluster import ClusterNodePaths

try:
    from .kv_app import KVApp
except ImportError:
    from kv_app import KVApp


def build_cluster(root: Path) -> RaftCluster:
    cluster = RaftCluster(clock=ManualClock())
    node_ids = ["n1", "n2", "n3"]
    timeouts = {"n1": 150, "n2": 250, "n3": 350}
    for node_id in node_ids:
        peers = [peer for peer in node_ids if peer != node_id]
        cluster.add_node(
            NodeConfig(node_id=node_id, peers=peers, election_timeout_ms=timeouts[node_id]),
            ClusterNodePaths(
                storage_dir=root / "storage",
                telemetry_path=root / "telemetry" / f"{node_id}.jsonl",
            ),
            application=KVApp(root / "kv" / f"{node_id}.json"),
        )
    return cluster


def main() -> None:
    root = Path("var/python-example")
    cluster = build_cluster(root)

    cluster.run(40, advance_ms=10)
    leader = cluster.leader()
    if leader is None:
        raise SystemExit("no leader elected")

    print(f"leader={leader.node_id} term={leader.current_term}")
    leader.submit_command(Command(op="set", key="color", value="blue"))
    leader.submit_command(Command(op="set", key="mode", value="active"))
    cluster.run(30, advance_ms=10)

    for summary in cluster.summaries():
        node = cluster.nodes[str(summary["node_id"])]
        kv = node.application
        print(summary, kv.get("color"), kv.get("mode"))


if __name__ == "__main__":
    main()
