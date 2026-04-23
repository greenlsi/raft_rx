from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import IntEnum
from typing import Any
import zlib

from rxnet import fsm

from .application import RaftApplication
from .clock import Clock
from .configuration import ClusterConfiguration
from .messages import Command, LogEntry, Message, MessageKind
from .storage import JsonFileStorage
from .telemetry import NullTelemetrySink, TelemetrySink
from .transport import MemoryTransport


class Role(IntEnum):
    FOLLOWER = 0
    CANDIDATE = 1
    LEADER = 2


class CompactionState(IntEnum):
    IDLE = 0
    SNAPSHOT_PENDING = 1
    COMPACTING = 2


class MembershipState(IntEnum):
    STABLE = 0
    JOINT_PENDING = 1
    JOINT = 2
    FINALIZING = 3


@dataclass(slots=True)
class NodeConfig:
    node_id: str
    peers: list[str]
    election_timeout_ms: int
    heartbeat_interval_ms: int = 50
    initial_members: list[str] | None = None


@dataclass(frozen=True, slots=True)
class TransitionSpec:
    from_state: int
    to_state: int
    guard_name: str
    action_name: str
    label: str


class RaftNode:
    def __init__(
        self,
        config: NodeConfig,
        clock: Clock,
        transport: MemoryTransport,
        storage: JsonFileStorage,
        application: RaftApplication,
        telemetry: TelemetrySink | None = None,
        trace_event: Callable[[str, int], None] | None = None,
    ) -> None:
        self.config = config
        self.clock = clock
        self.transport = transport
        self.storage = storage
        self.application = application
        self.telemetry = telemetry or NullTelemetrySink()
        self.trace_event = trace_event
        self.node_id = config.node_id
        initial_members = config.initial_members if config.initial_members is not None else [self.node_id, *config.peers]
        self.config_state = ClusterConfiguration.stable(initial_members)
        self.peers = [member for member in self.config_state.all_members() if member != self.node_id]
        self.running = True
        self.current_term = 0
        self.voted_for: str | None = None
        self.log: list[LogEntry] = []
        self.snapshot_last_included_index = 0
        self.snapshot_last_included_term = 0
        self.compaction_threshold = 32
        self.commit_index = 0
        self.last_applied = 0
        self.leader_id: str | None = None
        self.votes_received: set[str] = set()
        self.next_index = {peer: 1 for peer in self.peers}
        self.match_index = {peer: 0 for peer in self.peers}
        self.outbox: list[Message] = []
        self.pending_append_entries: list[Message] = []
        self.pending_vote_requests: list[Message] = []
        self.pending_votes: list[Message] = []
        self.requested_membership_change: tuple[str, ...] | None = None
        self.requested_membership_change_pending = False
        self.pending_configuration_members: tuple[str, ...] | None = None
        self.joint_config_index = 0
        self.final_config_index = 0
        self.compaction_requested = False
        self.compaction_in_progress = False
        self.compaction_done = False
        self.compaction_target_index = 0
        self.compaction_target_term = 0
        self.compaction_snapshot: object | None = None
        self.persist_dirty = False
        self.last_contact_ms = self.clock.now_ms()
        self.election_deadline_ms = self.last_contact_ms + self._next_election_timeout_ms()
        self.heartbeat_deadline_ms = self.last_contact_ms + self.config.heartbeat_interval_ms

        self.machine = fsm.Machine(
            name=self.node_id,
            state=Role.FOLLOWER,
            transitions=self._build_transitions(),
            user=self,
            latch_inputs_cb=lambda ctx, user: user._latch_inputs(),
            dump_outputs_cb=lambda ctx, user: user._dump_outputs(),
            state_names={role: role.name for role in Role},
        )
        self.compaction_machine = fsm.Machine(
            name=f"{self.node_id}.compaction",
            state=CompactionState.IDLE,
            transitions=self._build_compaction_transitions(),
            user=self,
            latch_inputs_cb=lambda ctx, user: user._latch_compaction_inputs(),
            dump_outputs_cb=lambda ctx, user: user._dump_compaction_outputs(),
            state_names={state: state.name for state in CompactionState},
        )
        self.membership_machine = fsm.Machine(
            name=f"{self.node_id}.membership",
            state=MembershipState.STABLE,
            transitions=self._build_membership_transitions(),
            user=self,
            latch_inputs_cb=lambda ctx, user: user._latch_membership_inputs(),
            dump_outputs_cb=lambda ctx, user: user._dump_membership_outputs(),
            state_names={state: state.name for state in MembershipState},
        )

        self._load_from_storage()

    @property
    def role(self) -> Role:
        return Role(self.machine.state)

    def submit_command(self, command: Command) -> None:
        self.transport.submit_client_command(self.node_id, command)

    def request_membership_change(self, members: list[str]) -> None:
        self.requested_membership_change = ClusterConfiguration.normalize_members(members)
        self.requested_membership_change_pending = True
        self._trace("raft.membership.requested", len(self.requested_membership_change))

    def summary(self) -> dict[str, Any]:
        last_committed = self.last_committed_command()
        return {
            "node_id": self.node_id,
            "running": self.running,
            "role": self.role.name,
            "membership_mode": self.config_state.mode(),
            "configuration_index": self.config_state.index,
            "term": self.current_term,
            "leader_id": self.leader_id,
            "log_len": len(self.log),
            "snapshot_index": self.snapshot_last_included_index,
            "compaction_threshold": self.compaction_threshold,
            "members": list(self.config_state.all_members()),
            "old_members": list(self.config_state.old_members),
            "new_members": list(self.config_state.new_members) if self.config_state.new_members is not None else None,
            "commit_index": self.commit_index,
            "last_applied": self.last_applied,
            "last_committed_command": last_committed.to_dict() if last_committed is not None else None,
        }

    def last_committed_command(self) -> Command | None:
        if self.last_applied <= self.snapshot_last_included_index or self.last_applied == 0:
            return None
        return self._entry_at_index(self.last_applied).command

    def stop(self) -> None:
        self.running = False
        self.transport.set_enabled(self.node_id, False)
        self.pending_append_entries.clear()
        self.pending_vote_requests.clear()
        self.pending_votes.clear()
        self.outbox.clear()
        self.votes_received.clear()
        self.leader_id = None
        self.machine.state = Role.FOLLOWER
        self.machine._next_state = Role.FOLLOWER
        self.compaction_machine.state = CompactionState.IDLE
        self.compaction_machine._next_state = CompactionState.IDLE
        self.membership_machine.state = MembershipState.STABLE
        self.membership_machine._next_state = MembershipState.STABLE
        self._emit("lifecycle", {"action": "stop"})

    def start(self) -> None:
        self.transport.set_enabled(self.node_id, True)
        self._load_from_storage()
        self.running = True
        self.machine.state = Role.FOLLOWER
        self.machine._next_state = Role.FOLLOWER
        self.compaction_machine.state = CompactionState.IDLE
        self.compaction_machine._next_state = CompactionState.IDLE
        self.membership_machine.state = MembershipState.JOINT if self.config_state.is_joint() else MembershipState.STABLE
        self.membership_machine._next_state = self.membership_machine.state
        self._emit("lifecycle", {"action": "start"})

    def _latch_inputs(self) -> None:
        if not self.running:
            return
        now = self.clock.now_ms()

        for message in self.transport.recv_for(self.node_id):
            self._emit("recv", {"kind": message.kind.value, "source": message.source, "term": message.term})
            if message.kind == MessageKind.APPEND_ENTRIES:
                self.pending_append_entries.append(message)
            elif message.kind == MessageKind.REQUEST_VOTE:
                self.pending_vote_requests.append(message)
            elif message.kind == MessageKind.REQUEST_VOTE_RESPONSE:
                self.pending_votes.append(message)
            elif message.kind == MessageKind.APPEND_ENTRIES_RESPONSE:
                self._handle_append_entries_response(message)

        if self.role == Role.LEADER:
            for command in self.transport.recv_client_commands(self.node_id):
                self._append_client_command(command)
        else:
            pending = self.transport.recv_client_commands(self.node_id)
            if pending:
                self._emit("client_rejected", {"reason": "not_leader", "count": len(pending)})

    def _dump_outputs(self) -> None:
        if not self.running:
            return
        if self.persist_dirty:
            self.storage.save(
                self.current_term,
                self.voted_for,
                list(self.config_state.old_members),
                list(self.config_state.new_members) if self.config_state.new_members is not None else None,
                self.config_state.index,
                self.log,
                self.commit_index,
                self.compaction_threshold,
                self.snapshot_last_included_index,
                self.snapshot_last_included_term,
            )
            self.persist_dirty = False
            self._emit(
                "persist",
                {
                    "term": self.current_term,
                    "log_len": len(self.log),
                    "snapshot_index": self.snapshot_last_included_index,
                },
            )

        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self._entry_at_index(self.last_applied)
            self._apply_committed_command(entry.command)
            self._trace("raft.apply", entry.index)
            self._emit(
                "apply",
                {
                    "index": entry.index,
                    "term": entry.term,
                    "command": entry.command.to_dict(),
                },
            )

        if self.outbox:
            self.transport.send_many(self.outbox)
            for message in self.outbox:
                self._emit("send", {"kind": message.kind.value, "target": message.target, "term": message.term})
            self.outbox.clear()

    def _handle_term_update(self, message: Message) -> None:
        if message.term > self.current_term:
            self.current_term = message.term
            self.voted_for = None
            self.leader_id = None
            self.persist_dirty = True
            self._step_down_to_follower()

    def _step_down_to_follower(self) -> None:
        now_ms = self.clock.now_ms()
        self.votes_received.clear()
        self.machine.state = Role.FOLLOWER
        self.machine._next_state = Role.FOLLOWER
        self.election_deadline_ms = now_ms + self._next_election_timeout_ms()
        self.last_contact_ms = now_ms

    def _handle_request_vote(self, message: Message, now_ms: int) -> None:
        self._handle_term_update(message)
        granted = False
        if self._is_voting_member() and message.term == self.current_term:
            candidate = str(message.source)
            if self.voted_for in (None, candidate) and self._candidate_is_up_to_date(message.payload):
                self.voted_for = candidate
                self.persist_dirty = True
                self.election_deadline_ms = now_ms + self._next_election_timeout_ms()
                self.last_contact_ms = now_ms
                granted = True
                self._trace("raft.vote.grant", self.current_term)
        self.outbox.append(
            Message(
                kind=MessageKind.REQUEST_VOTE_RESPONSE,
                term=self.current_term,
                source=self.node_id,
                target=message.source,
                payload={"granted": granted},
            )
        )

    def _handle_request_vote_response(self, message: Message) -> None:
        self._handle_term_update(message)
        if not self._is_voting_member() or self.role != Role.CANDIDATE or message.term != self.current_term:
            return
        if bool(message.payload.get("granted")):
            self.votes_received.add(message.source)
            self._trace("raft.vote.collected", len(self.votes_received))

    def _handle_append_entries(self, message: Message, now_ms: int) -> None:
        self._handle_term_update(message)
        success = False
        if message.term == self.current_term:
            self.leader_id = message.source
            self.election_deadline_ms = now_ms + self._next_election_timeout_ms()
            self.last_contact_ms = now_ms
            success = self._append_entries_from_leader(message)
        self._trace("raft.append.ok" if success else "raft.append.reject", self._last_log_index())
        self.outbox.append(
            Message(
                kind=MessageKind.APPEND_ENTRIES_RESPONSE,
                term=self.current_term,
                source=self.node_id,
                target=message.source,
                payload={"success": success, "match_index": self._last_log_index()},
            )
        )

    def _handle_append_entries_response(self, message: Message) -> None:
        self._handle_term_update(message)
        if self.role != Role.LEADER or message.term != self.current_term:
            return
        peer = message.source
        if bool(message.payload.get("success")):
            match_index = int(message.payload.get("match_index", 0))
            self.match_index[peer] = match_index
            self.next_index[peer] = match_index + 1
            self._trace("raft.repl.match_advanced", match_index)
            self._advance_commit_index()
            return
        self.next_index[peer] = max(1, self.next_index[peer] - 1)
        self._trace("raft.repl.backoff", self.next_index[peer])
        self._send_append_entries(peer)

    def _append_entries_from_leader(self, message: Message) -> bool:
        prev_index = int(message.payload["prev_log_index"])
        prev_term = int(message.payload["prev_log_term"])
        if prev_index > self._last_log_index():
            return False
        if prev_index < self.snapshot_last_included_index:
            return False
        if prev_index > 0 and self._term_at_index(prev_index) != prev_term:
            return False

        new_entries = [LogEntry.from_dict(item) for item in message.payload["entries"]]
        for entry in new_entries:
            if entry.index <= self.snapshot_last_included_index:
                continue
            if self._has_log_index(entry.index):
                if self._term_at_index(entry.index) != entry.term:
                    self._truncate_log_suffix_from(entry.index)
                    self.persist_dirty = True
                else:
                    continue
            if entry.index == self._last_log_index() + 1:
                self.log.append(entry)
                self.persist_dirty = True

        leader_commit = int(message.payload["leader_commit"])
        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, self._last_log_index())
            self.persist_dirty = True
        return True

    def _append_client_command(self, command: Command) -> None:
        entry = LogEntry(index=self._last_log_index() + 1, term=self.current_term, command=command)
        self.log.append(entry)
        self.persist_dirty = True
        self.match_index[self.node_id] = entry.index
        self._advance_commit_index()
        self._broadcast_append_entries()
        self._emit("append_local", {"index": entry.index, "command": command.to_dict()})
        self._trace("raft.log.append", entry.index)
        if command.op == "cluster.enter_joint":
            self._trace("raft.membership.joint_append", entry.index)
        elif command.op == "cluster.leave_joint":
            self._trace("raft.membership.stable_append", entry.index)

    def _become_candidate(self) -> None:
        self.current_term += 1
        self.voted_for = self.node_id
        self.votes_received = {self.node_id}
        self.leader_id = None
        self.persist_dirty = True
        now = self.clock.now_ms()
        self.election_deadline_ms = now + self._next_election_timeout_ms()
        last_index = self._last_log_index()
        last_term = self._last_log_term()
        for peer in self.peers:
            self.outbox.append(
                Message(
                    kind=MessageKind.REQUEST_VOTE,
                    term=self.current_term,
                    source=self.node_id,
                    target=peer,
                    payload={
                        "last_log_index": last_index,
                        "last_log_term": last_term,
                    },
                )
            )
        self._emit("role_action", {"action": "start_election", "term": self.current_term})
        self._trace("raft.election.start", self.current_term)

    def _become_leader(self) -> None:
        self.leader_id = self.node_id
        last_index = self._last_log_index()
        self.next_index = {peer: last_index + 1 for peer in self.peers}
        self.match_index = {peer: 0 for peer in self.peers}
        self.match_index[self.node_id] = last_index
        self.heartbeat_deadline_ms = self.clock.now_ms()
        self._broadcast_append_entries()
        self.heartbeat_deadline_ms = self.clock.now_ms() + self.config.heartbeat_interval_ms
        self._emit("role_action", {"action": "become_leader", "term": self.current_term})
        self._trace("raft.election.win", self.current_term)

    def _back_to_follower_due_to_timeout(self) -> None:
        self.votes_received.clear()
        self.leader_id = None
        self.election_deadline_ms = self.clock.now_ms() + self._next_election_timeout_ms()
        self._emit("role_action", {"action": "back_to_follower_due_to_timeout", "term": self.current_term})

    def _send_heartbeat(self) -> None:
        self._broadcast_append_entries()
        self.heartbeat_deadline_ms = self.clock.now_ms() + self.config.heartbeat_interval_ms
        self._emit("role_action", {"action": "send_heartbeat", "term": self.current_term})

    def _handle_next_append_entries(self) -> None:
        if not self.pending_append_entries:
            return
        message = self.pending_append_entries.pop(0)
        self._handle_append_entries(message, self.clock.now_ms())

    def _handle_next_vote_request(self) -> None:
        if not self.pending_vote_requests:
            return
        message = self.pending_vote_requests.pop(0)
        self._handle_request_vote(message, self.clock.now_ms())

    def _handle_next_vote(self) -> None:
        if not self.pending_votes:
            return
        message = self.pending_votes.pop(0)
        if message.kind == MessageKind.REQUEST_VOTE_RESPONSE:
            self._handle_request_vote_response(message)
        elif message.kind == MessageKind.APPEND_ENTRIES_RESPONSE:
            self._handle_append_entries_response(message)

    def _ignore_vote(self) -> None:
        if self.pending_votes:
            message = self.pending_votes.pop(0)
            if message.kind == MessageKind.APPEND_ENTRIES_RESPONSE:
                self._handle_append_entries_response(message)
                return
            self._emit("ignore_vote", {"source": message.source, "kind": message.kind.value})

    def _ignore_vote_request(self) -> None:
        if self.pending_vote_requests:
            self._handle_next_vote_request()

    def _broadcast_append_entries(self) -> None:
        for peer in self.peers:
            self._send_append_entries(peer)

    def _send_append_entries(self, peer: str) -> None:
        next_index = self.next_index.get(peer, self._last_log_index() + 1)
        next_index = max(next_index, self._log_base_index())
        prev_index = next_index - 1
        prev_term = 0 if prev_index == 0 else self._term_at_index(prev_index)
        entries = [entry.to_dict() for entry in self._entries_from(next_index)]
        self._trace("raft.repl.send", len(entries))
        self.outbox.append(
            Message(
                kind=MessageKind.APPEND_ENTRIES,
                term=self.current_term,
                source=self.node_id,
                target=peer,
                payload={
                    "prev_log_index": prev_index,
                    "prev_log_term": prev_term,
                    "entries": entries,
                    "leader_commit": self.commit_index,
                },
            )
        )

    def _advance_commit_index(self) -> None:
        for candidate_index in range(self._last_log_index(), self.commit_index, -1):
            if self._term_at_index(candidate_index) != self.current_term:
                continue
            if self._is_committed_under_current_configuration(candidate_index):
                self.commit_index = candidate_index
                self.persist_dirty = True
                self._trace("raft.commit", candidate_index)
                self._trace(
                    "raft.commit.joint" if self.config_state.is_joint() else "raft.commit.stable",
                    candidate_index,
                )
                break

    def _candidate_is_up_to_date(self, payload: dict[str, Any]) -> bool:
        candidate_term = int(payload["last_log_term"])
        candidate_index = int(payload["last_log_index"])
        my_term = self._last_log_term()
        my_index = self._last_log_index()
        if candidate_term != my_term:
            return candidate_term > my_term
        return candidate_index >= my_index

    def _last_log_index(self) -> int:
        return self.snapshot_last_included_index + len(self.log)

    def _last_log_term(self) -> int:
        if self.log:
            return self.log[-1].term
        return self.snapshot_last_included_term

    def _majority(self, members: tuple[str, ...]) -> int:
        return (len(members) // 2) + 1

    def _is_voting_member(self) -> bool:
        return self.node_id in self.config_state.all_members()

    def _timeout_expired(self) -> bool:
        return self._is_voting_member() and self.clock.now_ms() >= self.election_deadline_ms

    def _has_append_entries(self) -> bool:
        return bool(self.pending_append_entries)

    def _has_vote_request(self) -> bool:
        return bool(self.pending_vote_requests)

    def _has_higher_term_vote_request(self) -> bool:
        return bool(self.pending_vote_requests) and self.pending_vote_requests[0].term > self.current_term

    def _has_vote(self) -> bool:
        return bool(self.pending_votes)

    def _received_majority_votes(self) -> bool:
        return self._has_quorum(self.votes_received, self.config_state.old_members) and (
            self.config_state.new_members is None
            or self._has_quorum(self.votes_received, self.config_state.new_members)
        )

    def _time_for_heartbeat(self) -> bool:
        return self.clock.now_ms() >= self.heartbeat_deadline_ms

    def _compaction_threshold_reached(self) -> bool:
        return self._compactible_index() - self.snapshot_last_included_index >= self._compaction_threshold()

    def _can_start_compaction(self) -> bool:
        return self.compaction_requested and not self.compaction_in_progress

    def _compaction_finished(self) -> bool:
        return self.compaction_done

    def _emit(self, event_type: str, payload: dict[str, Any]) -> None:
        self.telemetry.emit(
            {
                "time_ms": self.clock.now_ms(),
                "node_id": self.node_id,
                "running": self.running,
                "role": self.role.name,
                "term": self.current_term,
                "event": event_type,
                "payload": payload,
            }
        )

    def _trace(self, label: str, value: int = 0) -> None:
        if self.trace_event is None:
            return
        self.trace_event(label, value)

    def _load_from_storage(self) -> None:
        persisted = self.storage.load()
        self.current_term = persisted.current_term
        self.voted_for = persisted.voted_for
        self.log = list(persisted.log)
        self.compaction_threshold = persisted.compaction_threshold
        if persisted.old_members:
            if persisted.new_members is None:
                self.config_state = ClusterConfiguration.stable(
                    persisted.old_members,
                    index=persisted.configuration_index,
                )
            else:
                self.config_state = ClusterConfiguration.joint(
                    persisted.old_members,
                    persisted.new_members,
                    index=persisted.configuration_index,
                )
        else:
            initial_members = (
                self.config.initial_members if self.config.initial_members is not None else [self.node_id, *self.config.peers]
            )
            self.config_state = ClusterConfiguration.stable(initial_members)
        self.snapshot_last_included_index = persisted.snapshot_last_included_index
        self.snapshot_last_included_term = persisted.snapshot_last_included_term
        self.commit_index = min(persisted.commit_index, self._last_log_index())
        self.last_applied = 0
        self.leader_id = None
        self.votes_received.clear()
        self._refresh_membership_state()
        self.outbox.clear()
        self.pending_append_entries.clear()
        self.pending_vote_requests.clear()
        self.pending_votes.clear()
        self.requested_membership_change = None
        self.requested_membership_change_pending = False
        self.pending_configuration_members = None
        self.joint_config_index = 0
        self.final_config_index = 0
        self.compaction_requested = False
        self.compaction_in_progress = False
        self.compaction_done = False
        self.compaction_target_index = 0
        self.compaction_target_term = 0
        self.compaction_snapshot = None
        self.persist_dirty = False
        if persisted.snapshot is not None:
            snapshot_payload = dict(persisted.snapshot).get("application", persisted.snapshot)
            self.application.restore_snapshot(snapshot_payload)
        else:
            self.application.reload()
        self.last_contact_ms = self.clock.now_ms()
        self.election_deadline_ms = self.last_contact_ms + self._next_election_timeout_ms()
        self.heartbeat_deadline_ms = self.last_contact_ms + self.config.heartbeat_interval_ms
        self.membership_machine.state = MembershipState.JOINT if self.config_state.is_joint() else MembershipState.STABLE
        self.membership_machine._next_state = self.membership_machine.state

    def _next_election_timeout_ms(self) -> int:
        base = max(1, self.config.election_timeout_ms)
        window = max(1, base)
        salt = f"{self.node_id}:{self.current_term}:{self.last_contact_ms}".encode("utf-8")
        return base + (zlib.crc32(salt) % window)

    def _build_transitions(self) -> list[fsm.Transition]:
        specs = [
            TransitionSpec(Role.FOLLOWER,  Role.CANDIDATE, "_timeout_expired",         "_become_candidate",                "timeout"),
            TransitionSpec(Role.FOLLOWER,  Role.FOLLOWER,  "_has_append_entries",      "_handle_next_append_entries",      "append_entries"),
            TransitionSpec(Role.FOLLOWER,  Role.FOLLOWER,  "_has_vote_request",        "_handle_next_vote_request",        "vote_request"),
            TransitionSpec(Role.FOLLOWER,  Role.FOLLOWER,  "_has_vote",                "_ignore_vote",                     "ignore_vote"),
            TransitionSpec(Role.CANDIDATE, Role.FOLLOWER,  "_timeout_expired",         "_back_to_follower_due_to_timeout", "candidate_timeout"),
            TransitionSpec(Role.CANDIDATE, Role.LEADER,    "_received_majority_votes", "_become_leader",                   "won_election"),
            TransitionSpec(Role.CANDIDATE, Role.FOLLOWER,  "_has_append_entries",      "_handle_next_append_entries",      "append_entries"),
            TransitionSpec(Role.CANDIDATE, Role.FOLLOWER,  "_has_higher_term_vote_request", "_handle_next_vote_request",   "higher_term_vote_request"),
            TransitionSpec(Role.CANDIDATE, Role.CANDIDATE, "_has_vote_request",        "_handle_next_vote_request",        "vote_request"),
            TransitionSpec(Role.CANDIDATE, Role.CANDIDATE, "_has_vote",                "_handle_next_vote",                "vote"),
            TransitionSpec(Role.LEADER,    Role.FOLLOWER,  "_has_append_entries",      "_handle_next_append_entries",      "append_entries"),
            TransitionSpec(Role.LEADER,    Role.FOLLOWER,  "_has_higher_term_vote_request", "_handle_next_vote_request",   "higher_term_vote_request"),
            TransitionSpec(Role.LEADER,    Role.LEADER,    "_has_vote_request",        "_ignore_vote_request",             "ignore_vote_request"),
            TransitionSpec(Role.LEADER,    Role.LEADER,    "_has_vote",                "_ignore_vote",                     "ignore_vote"),
            TransitionSpec(Role.LEADER,    Role.LEADER,    "_time_for_heartbeat",      "_send_heartbeat",                  "heartbeat"),
        ]
        return [self._make_transition(spec) for spec in specs]

    def _build_compaction_transitions(self) -> list[fsm.Transition]:
        specs = [
            TransitionSpec(CompactionState.IDLE, CompactionState.SNAPSHOT_PENDING, "_compaction_threshold_reached", "_mark_snapshot_pending", "threshold_reached"),
            TransitionSpec(CompactionState.SNAPSHOT_PENDING, CompactionState.COMPACTING, "_can_start_compaction", "_begin_compaction", "begin_compaction"),
            TransitionSpec(CompactionState.COMPACTING, CompactionState.IDLE, "_compaction_finished", "_finish_compaction", "finish_compaction"),
        ]
        return [self._make_transition(spec) for spec in specs]

    def _build_membership_transitions(self) -> list[fsm.Transition]:
        specs = [
            TransitionSpec(MembershipState.STABLE, MembershipState.JOINT_PENDING, "_has_membership_change_request", "_begin_membership_change", "begin_membership_change"),
            TransitionSpec(MembershipState.JOINT_PENDING, MembershipState.JOINT, "_joint_config_committed", "_enter_joint_config", "enter_joint_config"),
            TransitionSpec(MembershipState.JOINT, MembershipState.FINALIZING, "_can_finalize_joint", "_append_final_config", "append_final_config"),
            TransitionSpec(MembershipState.FINALIZING, MembershipState.STABLE, "_final_config_committed", "_leave_joint_config", "leave_joint_config"),
        ]
        return [self._make_transition(spec) for spec in specs]

    def _make_transition(self, spec: TransitionSpec) -> fsm.Transition:
        return fsm.Transition(
            from_state=spec.from_state,
            to_state=spec.to_state,
            guard=lambda ctx, user, name=spec.guard_name: getattr(user, name)(),
            action=lambda ctx, user, name=spec.action_name: getattr(user, name)(),
            label=spec.label,
        )

    def _latch_compaction_inputs(self) -> None:
        return

    def _dump_compaction_outputs(self) -> None:
        return

    def _latch_membership_inputs(self) -> None:
        return

    def _dump_membership_outputs(self) -> None:
        return

    def _mark_snapshot_pending(self) -> None:
        self.compaction_requested = True
        self.compaction_done = False

    def _begin_compaction(self) -> None:
        self.compaction_target_index = self._compactible_index()
        self.compaction_target_term = self._term_at_index(self.compaction_target_index)
        self.compaction_snapshot = self.application.snapshot()
        self.compaction_in_progress = True
        self.compaction_done = True

    def _finish_compaction(self) -> None:
        self.storage.save_snapshot(
            last_included_index=self.compaction_target_index,
            last_included_term=self.compaction_target_term,
            snapshot=self.compaction_snapshot,
        )
        self._truncate_log_prefix_through(self.compaction_target_index)
        self.snapshot_last_included_index = self.compaction_target_index
        self.snapshot_last_included_term = self.compaction_target_term
        self.persist_dirty = True
        self.compaction_requested = False
        self.compaction_in_progress = False
        self.compaction_done = False
        self.compaction_snapshot = None
        self._emit(
            "compaction",
            {
                "snapshot_index": self.snapshot_last_included_index,
                "snapshot_term": self.snapshot_last_included_term,
                "log_len": len(self.log),
            },
        )

    def _log_base_index(self) -> int:
        return self.snapshot_last_included_index + 1

    def _has_log_index(self, index: int) -> bool:
        return self._log_base_index() <= index <= self._last_log_index()

    def _physical_index(self, index: int) -> int:
        return index - self._log_base_index()

    def _entry_at_index(self, index: int) -> LogEntry:
        return self.log[self._physical_index(index)]

    def _term_at_index(self, index: int) -> int:
        if index == 0:
            return 0
        if index == self.snapshot_last_included_index:
            return self.snapshot_last_included_term
        return self._entry_at_index(index).term

    def _entries_from(self, index: int) -> list[LogEntry]:
        if index > self._last_log_index():
            return []
        return self.log[self._physical_index(index) :]

    def _truncate_log_suffix_from(self, index: int) -> None:
        if index <= self.snapshot_last_included_index:
            self.log = []
            return
        self.log = self.log[: self._physical_index(index)]

    def _truncate_log_prefix_through(self, index: int) -> None:
        if index < self._log_base_index():
            return
        if index >= self._last_log_index():
            self.log = []
            return
        self.log = self.log[self._physical_index(index + 1) :]

    def _compactible_index(self) -> int:
        if self.role == Role.LEADER and self.match_index:
            return min(self.last_applied, min(self.match_index.values()))
        return self.last_applied

    def _compaction_threshold(self) -> int:
        return self.compaction_threshold

    def _has_membership_change_request(self) -> bool:
        return (
            self.role == Role.LEADER
            and self.requested_membership_change_pending
            and self.requested_membership_change is not None
            and not self.config_state.is_joint()
            and self.requested_membership_change != self.config_state.old_members
        )

    def _joint_config_committed(self) -> bool:
        return self.config_state.is_joint() and self.config_state.index == self.joint_config_index

    def _can_finalize_joint(self) -> bool:
        return (
            self.role == Role.LEADER
            and self.config_state.is_joint()
            and self.pending_configuration_members is not None
            and self._joint_members_are_caught_up()
        )

    def _final_config_committed(self) -> bool:
        return (
            not self.config_state.is_joint()
            and self.final_config_index > 0
            and self.config_state.index == self.final_config_index
        )

    def _begin_membership_change(self) -> None:
        import json

        self.pending_configuration_members = self.requested_membership_change
        self.joint_config_index = self._last_log_index() + 1
        self.outbox.clear()
        self._append_client_command(
            Command(
                op="cluster.enter_joint",
                key="config",
                value=json.dumps(list(self.pending_configuration_members)),
            )
        )
        self._trace("raft.membership.request", self.joint_config_index)
        if self.pending_configuration_members is not None:
            self._trace("raft.membership.target_size", len(self.pending_configuration_members))

    def _enter_joint_config(self) -> None:
        self._emit(
            "membership",
            {
                "action": "joint_committed",
                "old_members": list(self.config_state.old_members),
                "new_members": list(self.config_state.new_members or ()),
                "index": self.config_state.index,
            },
        )
        self._trace("raft.membership.joint_committed", self.config_state.index)

    def _append_final_config(self) -> None:
        import json

        if self.pending_configuration_members is None:
            return
        self.final_config_index = self._last_log_index() + 1
        self._append_client_command(
            Command(
                op="cluster.leave_joint",
                key="config",
                value=json.dumps(list(self.pending_configuration_members)),
            )
        )
        self._trace("raft.membership.catchup_ready", self._last_log_index())
        self._trace("raft.membership.finalize", self.final_config_index)

    def _leave_joint_config(self) -> None:
        self.requested_membership_change = None
        self.requested_membership_change_pending = False
        self.pending_configuration_members = None
        self.joint_config_index = 0
        self.final_config_index = 0
        self._emit(
            "membership",
            {
                "action": "stable_committed",
                "members": list(self.config_state.old_members),
                "index": self.config_state.index,
            },
        )
        self._trace("raft.membership.stable_committed", self.config_state.index)

    def _joint_members_are_caught_up(self) -> bool:
        if self.config_state.new_members is None:
            return False
        target_index = self._last_log_index()
        for member in self.config_state.new_members:
            if member == self.node_id:
                continue
            if self.match_index.get(member, 0) < target_index:
                return False
        return True

    def _apply_committed_command(self, command: Command) -> None:
        if command.op == "cluster.set_maxlog":
            if command.value is None:
                raise ValueError("cluster.set_maxlog requires value")
            self.compaction_threshold = int(command.value)
            self.persist_dirty = True
            return
        if command.op == "cluster.enter_joint":
            self._apply_enter_joint(command)
            return
        if command.op == "cluster.leave_joint":
            self._apply_leave_joint(command)
            return
        self.application.apply(command)

    def _apply_enter_joint(self, command: Command) -> None:
        target_members = self._decode_members(command.value)
        if target_members is None:
            raise ValueError("cluster.enter_joint requires a target member set")
        self.config_state = ClusterConfiguration.joint(
            self.config_state.old_members,
            target_members,
            index=self.last_applied,
        )
        self.joint_config_index = self.last_applied
        self._refresh_membership_state()
        self.persist_dirty = True
        self._emit(
            "membership",
            {
                "action": "enter_joint",
                "old_members": list(self.config_state.old_members),
                "new_members": list(target_members),
                "index": self.config_state.index,
            },
        )
        self._trace("raft.membership.enter_joint", self.config_state.index)
        self._trace("raft.membership.mode_joint", len(self.config_state.all_members()))

    def _apply_leave_joint(self, command: Command) -> None:
        target_members = self._decode_members(command.value)
        if target_members is None:
            raise ValueError("cluster.leave_joint requires a target member set")
        was_member = self.node_id in self.config_state.all_members()
        self.config_state = ClusterConfiguration.stable(target_members, index=self.last_applied)
        self.final_config_index = self.last_applied
        self._refresh_membership_state()
        self.persist_dirty = True
        if was_member and self.node_id not in self.config_state.all_members():
            self.stop()
        self._emit(
            "membership",
            {
                "action": "leave_joint",
                "members": list(target_members),
                "index": self.config_state.index,
            },
        )
        self._trace("raft.membership.leave_joint", self.config_state.index)
        self._trace("raft.membership.mode_stable", len(self.config_state.old_members))

    def _refresh_membership_state(self) -> None:
        self.peers = [member for member in self.config_state.all_members() if member != self.node_id]
        preserved_next = self.next_index if hasattr(self, "next_index") else {}
        preserved_match = self.match_index if hasattr(self, "match_index") else {}
        # New members should start replication from the log base index to avoid
        # a long one-by-one backoff from the end of a large log.
        self.next_index = {peer: preserved_next.get(peer, self._log_base_index()) for peer in self.peers}
        self.match_index = {peer: preserved_match.get(peer, 0) for peer in self.peers}

    def _decode_members(self, encoded: str | None) -> tuple[str, ...] | None:
        if encoded is None:
            return None
        import json

        raw = json.loads(encoded)
        return ClusterConfiguration.normalize_members([str(member) for member in raw])

    def _has_quorum(self, votes: set[str], members: tuple[str, ...]) -> bool:
        return sum(1 for member in members if member in votes) >= self._majority(members)

    def _replicated_on_members(self, candidate_index: int, members: tuple[str, ...]) -> bool:
        replicated = 0
        for member in members:
            if member == self.node_id:
                if self._last_log_index() >= candidate_index:
                    replicated += 1
            elif self.match_index.get(member, 0) >= candidate_index:
                replicated += 1
        return replicated >= self._majority(members)

    def _is_committed_under_current_configuration(self, candidate_index: int) -> bool:
        if not self._replicated_on_members(candidate_index, self.config_state.old_members):
            return False
        if self.config_state.new_members is None:
            return True
        return self._replicated_on_members(candidate_index, self.config_state.new_members)
