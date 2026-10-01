from __future__ import annotations

import json
import os
from pathlib import Path


class StorageError(RuntimeError):
    pass


class StateStore:
    def __init__(self, directory: Path, node_id: str, members: dict[str, str]):
        self.directory = directory
        self.node_id = node_id
        self.members = dict(members)
        self.path = directory / f"{node_id}.json"
        self.term = 0
        self.voted_for: str | None = None
        self.failed = False
        self.directory.mkdir(parents=True, exist_ok=True)

        try:
            if self.path.exists():
                data = json.loads(self.path.read_text(encoding="utf-8"))
                if not isinstance(data, dict):
                    raise ValueError("expected a JSON object")
                if data.get("node_id") != self.node_id or data.get("members") != self.members:
                    raise ValueError("node ID or cluster members do not match stored state")
                term, vote = data["current_term"], data["voted_for"]
                if type(term) is not int or term < 0:
                    raise ValueError("invalid current_term")
                if vote is not None and (not isinstance(vote, str) or vote not in self.members):
                    raise ValueError("invalid voted_for")
                if term == 0 and vote is not None:
                    raise ValueError("cannot vote in term zero")
                self.term, self.voted_for = term, vote
            else:
                self.save(0, None)
        except (OSError, ValueError, KeyError, StorageError) as exc:
            raise StorageError(f"Failed to open {self.path}: {exc}") from exc

    def ensure_healthy(self) -> None:
        if self.failed:
            raise StorageError("State write failed: node must stop")

    def save(self, term: int, voted_for: str | None) -> None:
        self.ensure_healthy()
        data = {
            "node_id": self.node_id,
            "members": self.members,
            "current_term": term,
            "voted_for": voted_for,
        }
        temporary = self.path.with_suffix(".tmp")
        self.failed = True
        try:
            with temporary.open("w", encoding="utf-8") as file:
                json.dump(data, file, ensure_ascii=False, indent=2)
                file.write("\n")
                file.flush()
                os.fsync(file.fileno())
            os.replace(temporary, self.path)
            if os.name != "nt":
                fd = os.open(self.directory, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
        except OSError as exc:
            raise StorageError(f"Failed to save {self.path}: {exc}") from exc
        self.term, self.voted_for = term, voted_for
        self.failed = False
