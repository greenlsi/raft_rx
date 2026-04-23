from __future__ import annotations

import sys
from pathlib import Path

from raft_rx import ManualClock, NodeConfig, RaftCluster, RaftShell
from raft_rx.cluster import ClusterNodePaths
from noop_app import NoopApp


def build_cluster(root: Path) -> RaftCluster:
    cluster = RaftCluster(clock=ManualClock(), trace_enabled=True)
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
            application=NoopApp(),
        )
    return cluster


def main(argv: list[str]) -> int:
    root = Path(argv[1]) if len(argv) > 1 else Path("var/python-shell")
    cluster = build_cluster(root)
    cluster.run(40, advance_ms=10)
    RaftShell(cluster, root).cmdloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
