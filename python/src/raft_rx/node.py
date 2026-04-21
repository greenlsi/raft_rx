from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum
from typing import Any

from rxnet import fsm

from .clock import Clock
from .kv import KVStateMachine
from .messages import Command, LogEntry, Message, MessageKind
from .storage import JsonFileStorage
from .telemetry import NullTelemetrySink, TelemetrySink
from .transport import MemoryTransport


class Role(IntEnum):
    FOLLOWER = 0
    CANDIDATE = 1
    LEADER = 2


@dataclass(slots=True)
class NodeConfig:
    node_id: str
    peers: list[str]
    election_timeout_ms: int
    heartbeat_interval_ms: int = 50


class RaftNode:
    def __init__(
        self,
        config: NodeConfig,
        clock: Clock,
        transport: MemoryTransport,
        storage: JsonFileStorage,
        state_machine: KVStateMachine,
        telemetry: TelemetrySink | None = None,
    ) -> None:
        self.config = config
        self.clock = clock
        self.transport = transport
        self.storage = storage
        self.state_machine = state_machine
        self.telemetry = telemetry or NullTelemetrySink()

        persisted = self.storage.load()
        self.node_id = config.node_id
        self.peers = list(config.peers)
        self.current_term = persisted.current_term
        self.voted_for = persisted.voted_for
        self.log = list(persisted.log)
        self.commit_index = 0
        self.last_applied = 0
        self.leader_id: str | None = None
        self.votes_received: set[str] = set()
        self.next_index = {peer: len(self.log) + 1 for peer in self.peers}
        self.match_index = {peer: 0 for peer in self.peers}
        self.outbox: list[Message] = []
        self.pending_append_entries: list[Message] = []
        self.pending_vote_requests: list[Message] = []
        self.pending_votes: list[Message] = []
        self.persist_dirty = False
        self.last_contact_ms = self.clock.now_ms()
        self.election_deadline_ms = self.last_contact_ms + self.config.election_timeout_ms
        self.heartbeat_deadline_ms = self.last_contact_ms + self.config.heartbeat_interval_ms

        self.machine = fsm.Machine(
            name=self.node_id,
            state=Role.FOLLOWER,
            transitions=[
                fsm.Transition(
                    from_state=Role.FOLLOWER,
                    to_state=Role.CANDIDATE,
                    guard=lambda ctx, user: user._timeout_expired(),
                    action=lambda ctx, user: user._become_candidate(),
                    label="timeout",
                ),
                fsm.Transition(
                    from_state=Role.FOLLOWER,
                    to_state=Role.FOLLOWER,
                    guard=lambda ctx, user: user._has_append_entries(),
                    action=lambda ctx, user: user._handle_next_append_entries(),
                    label="append_entries",
                ),
                fsm.Transition(
                    from_state=Role.FOLLOWER,
                    to_state=Role.FOLLOWER,
                    guard=lambda ctx, user: user._has_vote_request(),
                    action=lambda ctx, user: user._handle_next_vote_request(),
                    label="vote_request",
                ),
                fsm.Transition(
                    from_state=Role.FOLLOWER,
                    to_state=Role.FOLLOWER,
                    guard=lambda ctx, user: user._has_vote(),
                    action=lambda ctx, user: user._ignore_vote(),
                    label="ignore_vote",
                ),
                fsm.Transition(
                    from_state=Role.CANDIDATE,
                    to_state=Role.FOLLOWER,
                    guard=lambda ctx, user: user._timeout_expired(),
                    action=lambda ctx, user: user._back_to_follower_due_to_timeout(),
                    label="candidate_timeout",
                ),
                fsm.Transition(
                    from_state=Role.CANDIDATE,
                    to_state=Role.LEADER,
                    guard=lambda ctx, user: user._received_majority_votes(),
                    action=lambda ctx, user: user._become_leader(),
                    label="won-election",
                ),
                fsm.Transition(
                    from_state=Role.CANDIDATE,
                    to_state=Role.FOLLOWER,
                    guard=lambda ctx, user: user._has_append_entries(),
                    action=lambda ctx, user: user._handle_next_append_entries(),
                    label="append_entries",
                ),
                fsm.Transition(
                    from_state=Role.CANDIDATE,
                    to_state=Role.CANDIDATE,
                    guard=lambda ctx, user: user._has_vote_request(),
                    action=lambda ctx, user: user._handle_next_vote_request(),
                    label="vote_request",
                ),
                fsm.Transition(
                    from_state=Role.CANDIDATE,
                    to_state=Role.CANDIDATE,
                    guard=lambda ctx, user: user._has_vote(),
                    action=lambda ctx, user: user._handle_next_vote(),
                    label="vote",
                ),
                fsm.Transition(
                    from_state=Role.LEADER,
                    to_state=Role.FOLLOWER,
                    guard=lambda ctx, user: user._has_append_entries(),
                    action=lambda ctx, user: user._handle_next_append_entries(),
                    label="append_entries",
                ),
                fsm.Transition(
                    from_state=Role.LEADER,
                    to_state=Role.LEADER,
                    guard=lambda ctx, user: user._has_vote_request(),
                    action=lambda ctx, user: user._ignore_vote_request(),
                    label="ignore_vote_request",
                ),
                fsm.Transition(
                    from_state=Role.LEADER,
                    to_state=Role.LEADER,
                    guard=lambda ctx, user: user._has_vote(),
                    action=lambda ctx, user: user._ignore_vote(),
                    label="ignore_vote",
                ),
                fsm.Transition(
                    from_state=Role.LEADER,
                    to_state=Role.LEADER,
                    guard=lambda ctx, user: user._time_for_heartbeat(),
                    action=lambda ctx, user: user._send_heartbeat(),
                    label="heartbeat",
                ),
            ],
            user=self,
            latch_inputs_cb=lambda ctx, user: user._latch_inputs(),
            dump_outputs_cb=lambda ctx, user: user._dump_outputs(),
            state_names={role: role.name for role in Role},
        )

        while self.last_applied < min(self.commit_index, len(self.log)):
            self.last_applied += 1
            self.state_machine.apply(self.log[self.last_applied - 1].command)

    @property
    def role(self) -> Role:
        return Role(self.machine.state)

    def submit_command(self, command: Command) -> None:
        self.transport.submit_client_command(self.node_id, command)

    def get(self, key: str) -> str | None:
        return self.state_machine.get(key)

    def summary(self) -> dict[str, Any]:
        return {
            "node_id": self.node_id,
            "role": self.role.name,
            "term": self.current_term,
            "leader_id": self.leader_id,
            "log_len": len(self.log),
            "commit_index": self.commit_index,
            "last_applied": self.last_applied,
        }

    def _latch_inputs(self) -> None:
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
        if self.persist_dirty:
            self.storage.save(self.current_term, self.voted_for, self.log)
            self.persist_dirty = False
            self._emit("persist", {"term": self.current_term, "log_len": len(self.log)})

        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied - 1]
            self.state_machine.apply(entry.command)
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

    def _handle_request_vote(self, message: Message, now_ms: int) -> None:
        self._handle_term_update(message)
        granted = False
        if message.term == self.current_term:
            candidate = str(message.source)
            if self.voted_for in (None, candidate) and self._candidate_is_up_to_date(message.payload):
                self.voted_for = candidate
                self.persist_dirty = True
                self.election_deadline_ms = now_ms + self.config.election_timeout_ms
                self.last_contact_ms = now_ms
                granted = True
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
        if self.role != Role.CANDIDATE or message.term != self.current_term:
            return
        if bool(message.payload.get("granted")):
            self.votes_received.add(message.source)

    def _handle_append_entries(self, message: Message, now_ms: int) -> None:
        self._handle_term_update(message)
        success = False
        if message.term == self.current_term:
            self.leader_id = message.source
            self.election_deadline_ms = now_ms + self.config.election_timeout_ms
            self.last_contact_ms = now_ms
            success = self._append_entries_from_leader(message)
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
            self._advance_commit_index()
            return
        self.next_index[peer] = max(1, self.next_index[peer] - 1)
        self._send_append_entries(peer)

    def _append_entries_from_leader(self, message: Message) -> bool:
        prev_index = int(message.payload["prev_log_index"])
        prev_term = int(message.payload["prev_log_term"])
        if prev_index > self._last_log_index():
            return False
        if prev_index > 0 and self.log[prev_index - 1].term != prev_term:
            return False

        new_entries = [LogEntry.from_dict(item) for item in message.payload["entries"]]
        for entry in new_entries:
            if entry.index <= len(self.log):
                if self.log[entry.index - 1].term != entry.term:
                    self.log = self.log[: entry.index - 1]
                    self.persist_dirty = True
                else:
                    continue
            if entry.index == len(self.log) + 1:
                self.log.append(entry)
                self.persist_dirty = True

        leader_commit = int(message.payload["leader_commit"])
        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, self._last_log_index())
        return True

    def _append_client_command(self, command: Command) -> None:
        entry = LogEntry(index=len(self.log) + 1, term=self.current_term, command=command)
        self.log.append(entry)
        self.persist_dirty = True
        self.match_index[self.node_id] = entry.index
        self._broadcast_append_entries()
        self._emit("append_local", {"index": entry.index, "command": command.to_dict()})

    def _become_candidate(self) -> None:
        self.current_term += 1
        self.voted_for = self.node_id
        self.votes_received = {self.node_id}
        self.leader_id = None
        self.persist_dirty = True
        now = self.clock.now_ms()
        self.election_deadline_ms = now + self.config.election_timeout_ms
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

    def _back_to_follower_due_to_timeout(self) -> None:
        self.votes_received.clear()
        self.leader_id = None
        self.election_deadline_ms = self.clock.now_ms() + self.config.election_timeout_ms
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
            self._emit("ignore_vote", {"source": message.source, "kind": message.kind.value})

    def _ignore_vote_request(self) -> None:
        if self.pending_vote_requests:
            message = self.pending_vote_requests.pop(0)
            self._emit("ignore_vote_request", {"source": message.source, "term": message.term})

    def _broadcast_append_entries(self) -> None:
        for peer in self.peers:
            self._send_append_entries(peer)

    def _send_append_entries(self, peer: str) -> None:
        next_index = self.next_index.get(peer, self._last_log_index() + 1)
        prev_index = next_index - 1
        prev_term = 0 if prev_index == 0 else self.log[prev_index - 1].term
        entries = [entry.to_dict() for entry in self.log[next_index - 1 :]]
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
            if self.log[candidate_index - 1].term != self.current_term:
                continue
            replicated = 1
            for peer in self.peers:
                if self.match_index.get(peer, 0) >= candidate_index:
                    replicated += 1
            if replicated >= self._majority():
                self.commit_index = candidate_index
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
        return len(self.log)

    def _last_log_term(self) -> int:
        return 0 if not self.log else self.log[-1].term

    def _majority(self) -> int:
        return ((len(self.peers) + 1) // 2) + 1

    def _timeout_expired(self) -> bool:
        return self.clock.now_ms() >= self.election_deadline_ms

    def _has_append_entries(self) -> bool:
        return bool(self.pending_append_entries)

    def _has_vote_request(self) -> bool:
        return bool(self.pending_vote_requests)

    def _has_vote(self) -> bool:
        return bool(self.pending_votes)

    def _received_majority_votes(self) -> bool:
        return len(self.votes_received) >= self._majority()

    def _time_for_heartbeat(self) -> bool:
        return self.clock.now_ms() >= self.heartbeat_deadline_ms

    def _emit(self, event_type: str, payload: dict[str, Any]) -> None:
        self.telemetry.emit(
            {
                "time_ms": self.clock.now_ms(),
                "node_id": self.node_id,
                "role": self.role.name,
                "term": self.current_term,
                "event": event_type,
                "payload": payload,
            }
        )
