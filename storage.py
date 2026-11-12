from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, BinaryIO

Entry = dict[str, Any]
MAX_COMMAND_BYTES = 4096


class StorageError(RuntimeError):
    pass


def validate_command(command: object) -> None:
    if not isinstance(command, str) or not command:
        raise ValueError("Команда должна быть непустой строкой")
    if len(command.encode("utf-8")) > MAX_COMMAND_BYTES:
        raise ValueError(f"Команда длиннее {MAX_COMMAND_BYTES} байт")


def validate_entries(
    value: object,
    current_term: int,
    previous_term: int = 0,
) -> list[Entry]:
    if not isinstance(value, list):
        raise ValueError("entries должен быть массивом")

    result = []
    for entry in value:
        if not isinstance(entry, dict) or set(entry) != {"term", "command"}:
            raise ValueError("Запись должна содержать только term и command")

        term = entry["term"]
        if (
            type(term) is not int
            or not 1 <= term <= current_term
            or term < previous_term
        ):
            raise ValueError("Некорректная последовательность term в журнале")

        command = entry["command"]
        if command is not None:
            validate_command(command)

        result.append({"term": term, "command": command})
        previous_term = term

    return result


class StateStore:
    def __init__(
        self,
        directory: Path,
        node_id: str,
        members: dict[str, str],
    ):
        self.directory = directory
        self.node_id = node_id
        self.members = dict(members)
        self.path = directory / f"{node_id}.json"
        self.term = 0
        self.voted_for: str | None = None
        self.log: list[Entry] = []
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

                fcntl.flock(
                    self._lock_file,
                    fcntl.LOCK_EX | fcntl.LOCK_NB,
                )
        except OSError as exc:
            self.close()
            raise StorageError(
                f"Узел {self.node_id}: каталог состояния уже занят"
            ) from exc

        try:
            if self.path.exists():
                data = json.loads(self.path.read_text(encoding="utf-8"))
                if not isinstance(data, dict):
                    raise ValueError("ожидался JSON-объект")
                if (
                    data.get("node_id") != self.node_id
                    or data.get("members") != self.members
                ):
                    raise ValueError(
                        "id или состав кластера не совпадает с сохранённым"
                    )

                term, vote = data["current_term"], data["voted_for"]
                if type(term) is not int or term < 0:
                    raise ValueError("некорректный current_term")
                if vote is not None and (
                    not isinstance(vote, str) or vote not in self.members
                ):
                    raise ValueError("некорректный voted_for")
                if term == 0 and vote is not None:
                    raise ValueError("в нулевом term ещё нельзя голосовать")

                # Файлы этапа 1 не содержат поля log.
                self.log = validate_entries(data.get("log", []), term)
                self.term, self.voted_for = term, vote
            else:
                self.save(0, None)
        except (OSError, ValueError, KeyError, StorageError) as exc:
            self.close()
            raise StorageError(
                f"Не удалось открыть {self.path}: {exc}"
            ) from exc

        return self

    def ensure_healthy(self) -> None:
        if self.failed:
            raise StorageError(
                "Ошибка записи состояния: узел прекращает работу"
            )

    def save(
        self,
        term: int,
        voted_for: str | None,
        log: list[Entry] | None = None,
    ) -> None:
        self.ensure_healthy()
        entries = validate_entries(
            self.log if log is None else log,
            term,
        )
        data = {
            "node_id": self.node_id,
            "members": self.members,
            "current_term": term,
            "voted_for": voted_for,
            "log": entries,
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
            raise StorageError(
                f"Не удалось сохранить {self.path}: {exc}"
            ) from exc

        self.term, self.voted_for, self.log = term, voted_for, entries
        self.failed = False

    def close(self) -> None:
        if self._lock_file is not None:
            self._lock_file.close()
            self._lock_file = None

    def __exit__(self, *_: object) -> None:
        self.close()