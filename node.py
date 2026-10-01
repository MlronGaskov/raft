from __future__ import annotations

import asyncio
import logging
import random
import time
from collections.abc import Awaitable, Callable
from typing import Any

from storage import StateStore

HEARTBEAT_INTERVAL = 0.15
RPC_TIMEOUT = 0.30
ELECTION_TIMEOUT = (0.9, 1.8)
TICK_INTERVAL = 0.03

Message = dict[str, Any]
SendRPC = Callable[[str, str, Message], Awaitable[Message | None]]
LOG = logging.getLogger("raft")


class RaftNode:
    def __init__(self, store: StateStore, send_rpc: SendRPC):
        self.store = store
        self.id = store.node_id
        self.members = dict(store.members)
        self.peers = {key: url for key, url in self.members.items() if key != self.id}
        self.quorum = len(self.members) // 2 + 1
        self.send_rpc = send_rpc

        self.term, self.voted_for = store.term, store.voted_for
        self.role = "follower"
        self.leader_id: str | None = None
        self.votes: set[str] = set()
        self.lock = asyncio.Lock()
        self.wake = {peer: asyncio.Event() for peer in self.peers}
        self._reset_election_timer()

    def _reset_election_timer(self) -> None:
        self.election_deadline = time.monotonic() + random.uniform(*ELECTION_TIMEOUT)

    def _wake_peers_locked(self) -> None:
        for event in self.wake.values():
            event.set()

    def _save_locked(self, term: int, voted_for: str | None) -> None:
        self.store.save(term, voted_for)
        self.term, self.voted_for = term, voted_for

    def _observe_term_locked(self, term: int) -> None:
        if term <= self.term:
            return
        was_leader = self.role == "leader"
        self._save_locked(term, None)
        self.role, self.leader_id = "follower", None
        self.votes.clear()
        if was_leader:
            self._reset_election_timer()
        self._wake_peers_locked()
        LOG.info("узел=%s term=%s follower: обнаружен больший term", self.id, self.term)

    def _become_leader_locked(self) -> None:
        self.role, self.leader_id = "leader", self.id
        self._wake_peers_locked()
        LOG.info("узел=%s term=%s leader: голосов=%s/%s",
                 self.id, self.term, len(self.votes), len(self.members))

    def _begin_election_locked(self) -> None:
        self._save_locked(self.term + 1, self.id)
        self.role, self.leader_id = "candidate", None
        self.votes = {self.id}
        self._reset_election_timer()
        self._wake_peers_locked()
        LOG.info("узел=%s term=%s candidate: начаты выборы", self.id, self.term)
        if len(self.votes) >= self.quorum:
            self._become_leader_locked()

    async def request_vote(self, term: int, candidate_id: str) -> Message:
        async with self.lock:
            self.store.ensure_healthy()
            self._observe_term_locked(term)
            granted = term == self.term and self.voted_for in (None, candidate_id)
            if granted:
                if self.voted_for is None:
                    self._save_locked(self.term, candidate_id)
                self._reset_election_timer()
            return {"term": self.term, "voteGranted": granted}

    async def append_entries(self, term: int, leader_id: str) -> Message:
        async with self.lock:
            self.store.ensure_healthy()
            if term < self.term:
                return {"term": self.term, "success": False}
            self._observe_term_locked(term)
            changed = self.role != "follower" or self.leader_id != leader_id
            self.role, self.leader_id = "follower", leader_id
            self.votes.clear()
            self._reset_election_timer()
            if changed:
                self._wake_peers_locked()
                LOG.info("узел=%s term=%s follower: лидер=%s",
                         self.id, self.term, leader_id)
            return {"term": self.term, "success": True}

    async def status(self) -> Message:
        async with self.lock:
            self.store.ensure_healthy()
            return {
                "node_id": self.id,
                "role": self.role,
                "term": self.term,
                "voted_for": self.voted_for,
                "leader_id": self.leader_id,
                "leader_url": self.members.get(self.leader_id),
                "quorum": self.quorum,
                "members": dict(self.members),
            }

    async def _handle_reply(self, peer: str, sent_role: str,
                            sent_term: int, reply: Message) -> None:
        flag = "voteGranted" if sent_role == "candidate" else "success"
        if (not isinstance(reply, dict)
                or type(reply.get("term")) is not int or reply["term"] < 0
                or type(reply.get(flag)) is not bool):
            return
        async with self.lock:
            self.store.ensure_healthy()
            self._observe_term_locked(reply["term"])
            if (self.role != sent_role or self.term != sent_term
                    or reply["term"] != sent_term):
                return
            if sent_role == "candidate" and reply["voteGranted"]:
                self.votes.add(peer)
                if len(self.votes) >= self.quorum:
                    self._become_leader_locked()

    async def _election_loop(self) -> None:
        while True:
            async with self.lock:
                self.store.ensure_healthy()
                if self.role != "leader" and time.monotonic() >= self.election_deadline:
                    self._begin_election_locked()
            await asyncio.sleep(TICK_INTERVAL)

    async def _peer_loop(self, peer: str) -> None:
        event = self.wake[peer]
        while True:
            async with self.lock:
                self.store.ensure_healthy()
                event.clear()
                role, term = self.role, self.term
                if role == "candidate":
                    path = "/raft/request-vote"
                    payload = {"term": term, "candidateId": self.id,
                               "lastLogIndex": 0, "lastLogTerm": 0}
                elif role == "leader":
                    path = "/raft/append-entries"
                    payload = {"term": term, "leaderId": self.id,
                               "prevLogIndex": 0, "prevLogTerm": 0,
                               "entries": [], "leaderCommit": 0}
                else:
                    payload = None
            if payload is not None:
                reply = await self.send_rpc(peer, path, payload)
                if reply is not None:
                    await self._handle_reply(peer, role, term, reply)
            try:
                timeout = None if role == "follower" else HEARTBEAT_INTERVAL
                await asyncio.wait_for(event.wait(), timeout)
            except TimeoutError:
                pass

    async def run(self) -> None:
        async with asyncio.TaskGroup() as tasks:
            tasks.create_task(self._election_loop())
            for peer in self.peers:
                tasks.create_task(self._peer_loop(peer))
