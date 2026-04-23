from __future__ import annotations

import sys
from pathlib import Path

from raft_rx import Command, RaftShell

try:
    from .kv_cluster import build_cluster
except ImportError:
    from kv_cluster import build_cluster


def register_kv_commands(shell: RaftShell) -> None:
    def cmd_set(active_shell: RaftShell, args: list[str]) -> None:
        if len(args) != 2:
            print("usage: set KEY VALUE")
            return
        with active_shell.cluster_locked() as cluster:
            leader = cluster.leader()
            if leader is None:
                print("no leader")
                return
            leader.submit_command(Command(op="set", key=args[0], value=args[1]))
            print(f"queued on {leader.node_id}")

    def cmd_get(active_shell: RaftShell, args: list[str]) -> None:
        if not args:
            print("usage: get KEY [NODE]")
            return
        key = args[0]
        if len(args) > 1:
            node_id = args[1]
        else:
            with active_shell.cluster_locked() as cluster:
                leader = cluster.leader()
                node_id = leader.node_id if leader is not None else cluster.node_ids()[0]
        with active_shell.cluster_locked() as cluster:
            node = cluster.nodes.get(node_id)
            if node is None:
                print(f"unknown node: {node_id}")
                return
            print(node.application.get(key))

    def cmd_delete(active_shell: RaftShell, args: list[str]) -> None:
        if len(args) != 1:
            print("usage: delete KEY")
            return
        with active_shell.cluster_locked() as cluster:
            leader = cluster.leader()
            if leader is None:
                print("no leader")
                return
            leader.submit_command(Command(op="delete", key=args[0]))
            print(f"queued on {leader.node_id}")

    shell.register_command("set", cmd_set, help_text="enqueue set(KEY, VALUE) on the leader", usage="set KEY VALUE")
    shell.register_command("get", cmd_get, help_text="read KEY from a node", usage="get KEY [NODE]")
    shell.register_command("delete", cmd_delete, help_text="enqueue delete(KEY) on the leader", usage="delete KEY")


def main(argv: list[str]) -> int:
    root = Path(argv[1]) if len(argv) > 1 else Path("var/python-kv-shell")
    cluster = build_cluster(root, trace_enabled=True)
    cluster.run(40, advance_ms=10)
    shell = RaftShell(cluster, root)
    register_kv_commands(shell)
    shell.cmdloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
