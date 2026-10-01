from __future__ import annotations

import json
import os
from pathlib import Path
from typing import BinaryIO


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
        self._lock_file: BinaryIO | None = None

    def __enter__(self) -> StateStore:
        self.directory.mkdir(parents=True, exist_ok=True)
        self._lock_file = (self.directory / f"{self.node_id}.lock").open("a+b")
        try:
            if os.name == "nt":
                import msvcrt

                self._lock_file.seek(0, os.SEEK_END)
                if self._lock_file.tell() == 0:
                    self._lock_file.write(b"\0")
                    self._lock_file.flush()
                self._lock_file.seek(0)
                msvcrt.locking(self._lock_file.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(self._lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            self.close()
            raise StorageError(f"Узел {self.node_id}: каталог состояния уже занят") from exc

        try:
            if self.path.exists():
                data = json.loads(self.path.read_text(encoding="utf-8"))
                if not isinstance(data, dict):
                    raise ValueError("ожидался JSON-объект")
                if data.get("node_id") != self.node_id or data.get("members") != self.members:
                    raise ValueError("id или состав кластера не совпадает с сохранённым")
                term, vote = data["current_term"], data["voted_for"]
                if type(term) is not int or term < 0:
                    raise ValueError("некорректный current_term")
                if vote is not None and (not isinstance(vote, str) or vote not in self.members):
                    raise ValueError("некорректный voted_for")
                if term == 0 and vote is not None:
                    raise ValueError("в нулевом term ещё нельзя голосовать")
                self.term, self.voted_for = term, vote
            else:
                self.save(0, None)
        except (OSError, ValueError, KeyError, StorageError) as exc:
            self.close()
            raise StorageError(f"Не удалось открыть {self.path}: {exc}") from exc
        return self

    def ensure_healthy(self) -> None:
        if self.failed:
            raise StorageError("Ошибка записи состояния: узел прекращает работу")

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
            raise StorageError(f"Не удалось сохранить {self.path}: {exc}") from exc
        self.term, self.voted_for = term, voted_for
        self.failed = False

    def close(self) -> None:
        if self._lock_file is not None:
            self._lock_file.close()
            self._lock_file = None

    def __exit__(self, *_: object) -> None:
        self.close()
