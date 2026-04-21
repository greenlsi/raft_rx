from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from rxnet import fsm

from .application import RaftApplication
from .clock import Clock, ManualClock
from .messages import Command
from .node import NodeConfig, RaftNode, Role
from .storage import JsonFileStorage
from .telemetry import JsonlTelemetrySink, NullTelemetrySink, TelemetrySink
from .transport import MemoryTransport


@dataclass(slots=True)
class ClusterNodePaths:
    storage_dir: Path
    telemetry_path: Path | None = None


class RaftCluster:
    def __init__(self, clock: Clock | None = None, transport: MemoryTransport | None = None) -> None:
        self.clock = clock or ManualClock()
        self.transport = transport or MemoryTransport()
        self.runtime = fsm.Runtime()
        self.nodes: dict[str, RaftNode] = {}
        self.paths: dict[str, ClusterNodePaths] = {}

    def add_node(
        self,
        config: NodeConfig,
        paths: ClusterNodePaths,
        application: RaftApplication,
    ) -> RaftNode:
        telemetry: TelemetrySink
        if paths.telemetry_path is None:
            telemetry = NullTelemetrySink()
        else:
            telemetry = JsonlTelemetrySink(paths.telemetry_path)
        node = RaftNode(
            config=config,
            clock=self.clock,
            transport=self.transport,
            storage=JsonFileStorage(paths.storage_dir, config.node_id),
            application=application,
            telemetry=telemetry,
        )
        self.nodes[config.node_id] = node
        self.paths[config.node_id] = paths
        self.runtime.add_machine(node.machine)
        self.runtime.build()
        return node

    def tick(self, advance_ms: int = 10) -> None:
        if isinstance(self.clock, ManualClock):
            self.clock.advance(advance_ms)
        self.runtime.tick()

    def run(self, ticks: int, advance_ms: int = 10) -> None:
        for _ in range(ticks):
            self.tick(advance_ms=advance_ms)

    def leader(self) -> RaftNode | None:
        leaders = [node for node in self.nodes.values() if node.running and node.role == Role.LEADER]
        return leaders[0] if leaders else None

    def submit_to_leader(self, command: Command) -> bool:
        leader = self.leader()
        if leader is None:
            return False
        leader.submit_command(command)
        return True

    def summaries(self) -> list[dict[str, object]]:
        return [node.summary() for node in self.nodes.values()]

    def stop_node(self, node_id: str) -> None:
        self.nodes[node_id].stop()

    def start_node(self, node_id: str) -> None:
        self.nodes[node_id].start()

    def restart_node(self, node_id: str) -> None:
        self.stop_node(node_id)
        self.start_node(node_id)

    def node_ids(self) -> list[str]:
        return list(self.nodes.keys())
