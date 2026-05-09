# Copyright 2026 Jose M. Moya <jm.moya@upm.es>
# SPDX-License-Identifier: GPL-3.0-or-later

from __future__ import annotations

from pathlib import Path

from raft_rx import Command

try:
    from .kv_cluster import build_cluster
except ImportError:
    from kv_cluster import build_cluster


def main() -> None:
    root = Path("var/python-reconfigure-trace")
    cluster = build_cluster(root, trace_enabled=True)

    cluster.run(40, advance_ms=10)
    leader = cluster.leader()
    if leader is None:
        raise SystemExit("no leader elected")

    print(f"leader={leader.node_id} term={leader.current_term}")
    leader.submit_command(Command(op="set", key="color", value="blue"))
    leader.submit_command(Command(op="set", key="mode", value="active"))
    cluster.run(40, advance_ms=10)

    print("requesting membership change: add n4")
    cluster.provision_node("n4", members=["n1", "n2", "n3", "n4"], running=True)
    leader.request_membership_change(["n1", "n2", "n3", "n4"])
    cluster.run(120, advance_ms=10)

    for summary in cluster.summaries():
        node = cluster.nodes[str(summary["node_id"])]
        print(
            summary["node_id"],
            f"role={summary['role']}",
            f"cfg={summary['membership_mode']}",
            f"cfg_idx={summary['configuration_index']}",
            f"log={summary['log_len']}",
            f"commit={summary['commit_index']}",
            f"applied={summary['last_applied']}",
            f"color={node.application.get('color')}",
            f"mode={node.application.get('mode')}",
        )

    trace_path = root / "trace.bin"
    report_path = root / "trace.html"
    cluster.export_trace(trace_path)
    cluster.report_trace(report_path)

    print(f"trace={trace_path}")
    print(f"report={report_path}")
    print("open with: python -m rxnet.tools.trace var/python-reconfigure-trace/trace.bin --report report.html --open")


if __name__ == "__main__":
    main()
