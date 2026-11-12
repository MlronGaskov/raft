from __future__ import annotations

import asyncio
import logging
import random
import time
from collections.abc import Awaitable, Callable
from typing import Any

from storage import Entry, StateStore, validate_command

HEARTBEAT_INTERVAL = 0.15
RPC_TIMEOUT = 0.30
ELECTION_TIMEOUT = (0.9, 1.8)
TICK_INTERVAL = 0.03
APPEND_BATCH_SIZE = 32
CLIENT_TIMEOUT = 5.0

Message = dict[str, Any]
SendRPC = Callable[[str, str, Message], Awaitable[Message | None]]
LOG = logging.getLogger("raft")


class State:
    name = ""
    reply_field = ""
    retry_after: float | None = None

    def tick(self, node: RaftNode) -> None:
        if time.monotonic() >= node.election_deadline:
            node._begin_election_locked()

    def request(
        self,
        node: RaftNode,
        peer: str,
    ) -> tuple[str, Message] | None:
        return None

    def reply(
        self,
        node: RaftNode,
        peer: str,
        sent: Message,
        response: Message,
    ) -> None:
        pass

    def submit(
        self,
        node: RaftNode,
        commands: list[str],
    ) -> tuple[int, int] | None:
        return None

    def follow(self, node: RaftNode, leader_id: str) -> None:
        node._transition_locked(Follower(), leader_id)

    def leave(self, node: RaftNode) -> None:
        pass


class Follower(State):
    name = "follower"

    def follow(self, node: RaftNode, leader_id: str) -> None:
        node.leader_id = leader_id


class Candidate(State):
    name = "candidate"
    reply_field = "voteGranted"
    retry_after = HEARTBEAT_INTERVAL

    def __init__(self, node_id: str):
        self.votes = {node_id}

    def request(self, node: RaftNode, peer: str) -> tuple[str, Message]:
        last_index = len(node.log)
        return "/raft/request-vote", {
            "term": node.term,
            "candidateId": node.id,
            "lastLogIndex": last_index,
            "lastLogTerm": node._term_at(last_index),
        }

    def reply(
        self,
        node: RaftNode,
        peer: str,
        sent: Message,
        response: Message,
    ) -> None:
        if not response["voteGranted"]:
            return

        self.votes.add(peer)
        if len(self.votes) >= node.quorum:
            node._become_leader_locked()


class Leader(State):
    name = "leader"
    reply_field = "success"
    retry_after = HEARTBEAT_INTERVAL

    def __init__(self, node: RaftNode):
        self.next_index = {
            peer: len(node.log) + 1 for peer in node.peers
        }
        self.match_index = {peer: 0 for peer in node.peers}

    def tick(self, node: RaftNode) -> None:
        pass

    def leave(self, node: RaftNode) -> None:
        node._reset_election_timer()

    def request(self, node: RaftNode, peer: str) -> tuple[str, Message]:
        previous = self.next_index[peer] - 1
        return "/raft/append-entries", {
            "term": node.term,
            "leaderId": node.id,
            "prevLogIndex": previous,
            "prevLogTerm": node._term_at(previous),
            "entries": node.log[previous:previous + APPEND_BATCH_SIZE],
            "leaderCommit": node.commit_index,
        }

    def reply(
        self,
        node: RaftNode,
        peer: str,
        sent: Message,
        response: Message,
    ) -> None:
        if not response["success"]:
            self.next_index[peer] = max(1, sent["prevLogIndex"])
            node.wake[peer].set()
            return

        matched = sent["prevLogIndex"] + len(sent["entries"])
        self.match_index[peer] = max(self.match_index[peer], matched)
        self.next_index[peer] = max(self.next_index[peer], matched + 1)
        self.advance_commit(node)

        if self.next_index[peer] <= len(node.log):
            node.wake[peer].set()

    def advance_commit(self, node: RaftNode) -> None:
        replicated = sorted(
            [len(node.log), *self.match_index.values()],
            reverse=True,
        )
        index = replicated[node.quorum - 1]

        if index > node.commit_index and node._term_at(index) == node.term:
            node._commit_locked(index)
            node._wake_peers_locked()

    def submit(
        self,
        node: RaftNode,
        commands: list[str],
    ) -> tuple[int, int]:
        first = len(node.log) + 1
        entries = [
            {"term": node.term, "command": command}
            for command in commands
        ]
        node.store.save(
            node.term,
            node.voted_for,
            node.log + entries,
        )

        self.advance_commit(node)
        node._wake_peers_locked()
        return first, len(node.log)


class RaftNode:
    def __init__(self, store: StateStore, send_rpc: SendRPC):
        self.store = store
        self.id = store.node_id
        self.members = dict(store.members)
        self.peers = {
            key: url for key, url in self.members.items()
            if key != self.id
        }
        self.quorum = len(self.members) // 2 + 1
        self.send_rpc = send_rpc

        self.state: State = Follower()
        self.leader_id: str | None = None
        self.commit_index = 0
        self.last_applied = 0

        self.applied: list[str] = []

        self.lock = asyncio.Lock()
        self.changed = asyncio.Condition(self.lock)
        self.wake = {peer: asyncio.Event() for peer in self.peers}
        self._reset_election_timer()

    @property
    def term(self) -> int:
        return self.store.term

    @property
    def voted_for(self) -> str | None:
        return self.store.voted_for

    @property
    def log(self) -> list[Entry]:
        return self.store.log

    @property
    def role(self) -> str:
        return self.state.name

    def _term_at(self, index: int) -> int:
        return 0 if index == 0 else self.log[index - 1]["term"]

    def _reset_election_timer(self) -> None:
        self.election_deadline = (
            time.monotonic() + random.uniform(*ELECTION_TIMEOUT)
        )

    def _wake_peers_locked(self) -> None:
        for event in self.wake.values():
            event.set()

    def _transition_locked(
        self,
        state: State,
        leader_id: str | None = None,
    ) -> None:
        self.state.leave(self)
        self.state, self.leader_id = state, leader_id
        self.changed.notify_all()
        self._wake_peers_locked()
        LOG.info(
            "узел=%s term=%s role=%s leader=%s",
            self.id, self.term, self.role, self.leader_id,
        )

    def _observe_term_locked(self, term: int) -> None:
        if term <= self.term:
            return

        self.store.save(term, None)
        self._transition_locked(Follower())

    def _begin_election_locked(self) -> None:
        self.store.save(self.term + 1, self.id)
        self._reset_election_timer()

        candidate = Candidate(self.id)
        self._transition_locked(candidate)
        if len(candidate.votes) >= self.quorum:
            self._become_leader_locked()

    def _become_leader_locked(self) -> None:
        self.store.save(
            self.term,
            self.voted_for,
            self.log + [{"term": self.term, "command": None}],
        )

        leader = Leader(self)
        self._transition_locked(leader, self.id)
        leader.advance_commit(self)

    def _commit_locked(self, index: int) -> None:
        if index <= self.commit_index:
            return

        self.commit_index = index
        while self.last_applied < self.commit_index:
            command = self.log[self.last_applied]["command"]
            if command is not None:
                self.applied.append(command)
            self.last_applied += 1

        self.changed.notify_all()

    async def request_vote(
        self,
        term: int,
        candidate_id: str,
        last_log_index: int,
        last_log_term: int,
    ) -> Message:
        async with self.lock:
            self.store.ensure_healthy()
            self._observe_term_locked(term)

            local_last = len(self.log)
            up_to_date = (
                last_log_term,
                last_log_index,
            ) >= (
                self._term_at(local_last),
                local_last,
            )
            granted = (
                term == self.term
                and self.voted_for in (None, candidate_id)
                and up_to_date
            )

            if granted:
                if self.voted_for is None:
                    self.store.save(self.term, candidate_id)
                self._reset_election_timer()

            return {"term": self.term, "voteGranted": granted}

    async def append_entries(
        self,
        term: int,
        leader_id: str,
        prev_log_index: int,
        prev_log_term: int,
        entries: list[Entry],
        leader_commit: int,
    ) -> Message:
        async with self.lock:
            self.store.ensure_healthy()
            if term < self.term:
                return {"term": self.term, "success": False}

            self._observe_term_locked(term)
            self.state.follow(self, leader_id)
            self._reset_election_timer()

            if (
                prev_log_index > len(self.log)
                or self._term_at(prev_log_index) != prev_log_term
            ):
                return {"term": self.term, "success": False}

            for offset, entry in enumerate(entries):
                position = prev_log_index + offset

                if (
                    position < len(self.log)
                    and self.log[position]["term"] == entry["term"]
                ):
                    continue

                if position < self.commit_index:
                    return {"term": self.term, "success": False}

                self.store.save(
                    self.term,
                    self.voted_for,
                    self.log[:position] + entries[offset:],
                )
                break

            last_covered = prev_log_index + len(entries)
            self._commit_locked(min(leader_commit, last_covered))
            return {"term": self.term, "success": True}

    async def add_entries(
        self,
        commands: list[str],
        timeout: float = CLIENT_TIMEOUT,
    ) -> Message:
        if (
            not isinstance(commands, list)
            or not 1 <= len(commands) <= APPEND_BATCH_SIZE
        ):
            raise ValueError(
                f"entries должен содержать 1–{APPEND_BATCH_SIZE} команд"
            )
        for command in commands:
            validate_command(command)

        async with self.changed:
            self.store.ensure_healthy()
            state = self.state
            span = state.submit(self, commands)

            if span is None:
                return {
                    "status": "not_leader",
                    "leader_id": self.leader_id,
                    "leader_url": self.members.get(self.leader_id),
                }

            first, last = span
            term = self.term
            result = "committed"

            try:
                async with asyncio.timeout(timeout):
                    await self.changed.wait_for(
                        lambda: self.last_applied >= last
                        or self.state is not state
                    )
            except TimeoutError:
                result = "timeout"

            self.store.ensure_healthy()
            if self.state is not state:
                result = "leadership_lost"

            return {
                "status": result,
                "term": term,
                "first_index": first,
                "last_index": last,
                "commit_index": self.commit_index,
                "leader_id": self.leader_id,
                "leader_url": self.members.get(self.leader_id),
            }

    def _snapshot_locked(self) -> Message:
        return {
            "node_id": self.id,
            "role": self.role,
            "term": self.term,
            "voted_for": self.voted_for,
            "leader_id": self.leader_id,
            "leader_url": self.members.get(self.leader_id),
            "quorum": self.quorum,
            "members": dict(self.members),
            "log_length": len(self.log),
            "commit_index": self.commit_index,
            "last_applied": self.last_applied,
        }

    async def status(self) -> Message:
        async with self.lock:
            self.store.ensure_healthy()
            return self._snapshot_locked()

    async def show_log(self) -> Message:
        async with self.lock:
            self.store.ensure_healthy()
            return {
                **self._snapshot_locked(),
                "entries": [
                    {
                        "index": index,
                        **entry,
                        "committed": index <= self.commit_index,
                    }
                    for index, entry in enumerate(self.log, start=1)
                ],
            }

    async def _handle_reply(
        self,
        peer: str,
        sent_state: State,
        sent: Message,
        reply: Message,
    ) -> None:
        if (
            not isinstance(reply, dict)
            or type(reply.get("term")) is not int
            or reply["term"] < 0
            or type(reply.get(sent_state.reply_field)) is not bool
        ):
            return

        async with self.lock:
            self.store.ensure_healthy()

            self._observe_term_locked(reply["term"])

            if (
                self.state is not sent_state
                or self.term != sent["term"]
                or reply["term"] != sent["term"]
            ):
                return

            sent_state.reply(self, peer, sent, reply)

    async def _election_loop(self) -> None:
        while True:
            async with self.lock:
                self.store.ensure_healthy()
                self.state.tick(self)
            await asyncio.sleep(TICK_INTERVAL)

    async def _peer_loop(self, peer: str) -> None:
        event = self.wake[peer]

        while True:
            async with self.lock:
                self.store.ensure_healthy()
                event.clear()
                state = self.state
                outgoing = state.request(self, peer)

            if outgoing is not None:
                path, payload = outgoing
                reply = await self.send_rpc(peer, path, payload)
                if reply is not None:
                    await self._handle_reply(peer, state, payload, reply)

            try:
                await asyncio.wait_for(event.wait(), state.retry_after)
            except TimeoutError:
                pass

    async def run(self) -> None:
        async with asyncio.TaskGroup() as tasks:
            tasks.create_task(self._election_loop())
            for peer in self.peers:
                tasks.create_task(self._peer_loop(peer))