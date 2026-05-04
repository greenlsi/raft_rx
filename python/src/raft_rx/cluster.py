from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from rxnet import fsm
from rxnet.trace import Tracer

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
    def __init__(
        self,
        clock: Clock | None = None,
        transport: MemoryTransport | None = None,
        runtime: fsm.Runtime | None = None,
        *,
        application_factory: Callable[[str], RaftApplication] | None = None,
        paths_factory: Callable[[str], ClusterNodePaths] | None = None,
        trace_enabled: bool = False,
        trace_max_events: int = 8192,
        trace_phases: bool = True,
    ) -> None:
        self.clock = clock or ManualClock()
        self.transport = transport or MemoryTransport()
        self.runtime = runtime if runtime is not None else fsm.Runtime()
        self.nodes: dict[str, RaftNode] = {}
        self.paths: dict[str, ClusterNodePaths] = {}
        self.application_factory = application_factory
        self.paths_factory = paths_factory
        self.trace_enabled = trace_enabled
        self.trace_max_events = trace_max_events
        self.trace_phases = trace_phases
        self.tracer = Tracer(max_events=trace_max_events, phases=trace_phases) if trace_enabled else None

    def add_node(
        self,
        config: NodeConfig,
        paths: ClusterNodePaths,
        application: RaftApplication,
        period_us: int = 0,
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
            trace_event=self.emit_trace,
        )
        self.nodes[config.node_id] = node
        self.paths[config.node_id] = paths
        self.runtime.add_machine(node.machine, period_us)
        self.runtime.add_machine(node.compaction_machine, period_us)
        self.runtime.add_machine(node.membership_machine, period_us)
        self._refresh_trace_attachment()
        return node

    def provision_node(
        self,
        node_id: str,
        *,
        members: list[str],
        running: bool = False,
        election_timeout_ms: int = 450,
    ) -> RaftNode:
        if node_id in self.nodes:
            return self.nodes[node_id]
        if self.application_factory is None or self.paths_factory is None:
            raise RuntimeError("cluster cannot provision nodes without factories")
        paths = self.paths_factory(node_id)
        node = self.add_node(
            NodeConfig(
                node_id=node_id,
                peers=[member for member in members if member != node_id],
                election_timeout_ms=election_timeout_ms,
            ),
            paths,
            application=self.application_factory(node_id),
        )
        node.config_state = node.config_state.stable(members)
        node._refresh_membership_state()
        node.persist_dirty = True
        if not running:
            node.stop()
        return node

    def emit_trace(self, label: str, value: int = 0) -> None:
        if self.tracer is None:
            return
        self.tracer.user(label, value)

    def export_trace(self, path: str | Path) -> bool:
        if self.tracer is None:
            return False
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.tracer.export(str(path))
        return True

    def report_trace(self, path: str | Path) -> bool:
        if self.tracer is None:
            return False
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.tracer.report(str(path))
        return True

    def serve_trace(self, host: str = "0.0.0.0", port: int = 7777) -> bool:
        if self.tracer is None:
            return False
        self.tracer.serve(host=host, port=port)
        return True

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

    def _refresh_trace_attachment(self) -> None:
        self.runtime.build()
        if self.tracer is None:
            return
        self.tracer.attach(self.runtime)
