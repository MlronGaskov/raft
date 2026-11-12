from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
import signal
from collections.abc import Awaitable, Callable
from pathlib import Path
from urllib.parse import urlsplit

from aiohttp import ClientError, ClientSession, ClientTimeout, web

from node import APPEND_BATCH_SIZE, RPC_TIMEOUT, Message, RaftNode
from storage import StateStore, StorageError, validate_entries

ROOT = Path(__file__).resolve().parent
NODE = web.AppKey("node", RaftNode)

CLIENT_STATUS_CODES = {
    "committed": 201,
    "not_leader": 409,
    "leadership_lost": 503,
    "timeout": 504,
}


def load_members(path: Path) -> dict[str, str]:
    members = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(members, dict) or not members:
        raise ValueError("Конфигурация должна содержать непустой объект id → URL")

    result = {}
    for node_id, url in members.items():
        if (
            not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", node_id)
            or not isinstance(url, str)
        ):
            raise ValueError("Некорректный id или URL в конфигурации")

        parsed = urlsplit(url)
        if (
            parsed.scheme != "http"
            or not parsed.hostname
            or not parsed.port
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path not in ("", "/")
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError(f"Некорректный адрес узла {node_id}: {url}")

        result[node_id] = url.rstrip("/")

    if len(set(result.values())) != len(result):
        raise ValueError("У каждого узла должен быть отдельный адрес")
    return result


@web.middleware
async def storage_errors(
    request: web.Request,
    handler: Callable[[web.Request], Awaitable[web.StreamResponse]],
) -> web.StreamResponse:
    try:
        return await handler(request)
    except StorageError as exc:
        return web.json_response({"error": str(exc)}, status=503)


async def json_object(request: web.Request) -> Message:
    try:
        data = await request.json()
    except (ValueError, UnicodeError) as exc:
        raise web.HTTPBadRequest(text="Нужен корректный JSON") from exc

    if not isinstance(data, dict):
        raise web.HTTPBadRequest(text="Ожидался JSON-объект")
    return data


async def parse_rpc(
    request: web.Request,
    sender_key: str,
    integer_fields: tuple[str, ...],
) -> Message:
    data = await json_object(request)

    if type(data.get("term")) is not int or data["term"] < 1:
        raise web.HTTPBadRequest(
            text="term должен быть положительным целым числом"
        )

    sender = data.get(sender_key)
    if not isinstance(sender, str) or sender not in request.app[NODE].peers:
        raise web.HTTPBadRequest(
            text=f"{sender_key}: неизвестный или собственный id"
        )

    if any(
        type(data.get(key)) is not int or data[key] < 0
        for key in integer_fields
    ):
        raise web.HTTPBadRequest(
            text="Индексы и term записей должны быть неотрицательными целыми"
        )
    return data


def validate_position(data: Message, index_key: str, term_key: str) -> None:
    if (
        (data[index_key] == 0) != (data[term_key] == 0)
        or data[term_key] > data["term"]
    ):
        raise web.HTTPBadRequest(text="Некорректная пара index/term")


async def status(request: web.Request) -> web.Response:
    snapshot = await request.app[NODE].status()
    snapshot["pid"] = os.getpid()
    return web.json_response(snapshot, headers={"Cache-Control": "no-store"})


async def request_vote(request: web.Request) -> web.Response:
    data = await parse_rpc(
        request,
        "candidateId",
        ("lastLogIndex", "lastLogTerm"),
    )
    validate_position(data, "lastLogIndex", "lastLogTerm")

    reply = await request.app[NODE].request_vote(
        data["term"],
        data["candidateId"],
        data["lastLogIndex"],
        data["lastLogTerm"],
    )
    return web.json_response(reply)


async def append_entries(request: web.Request) -> web.Response:
    data = await parse_rpc(
        request,
        "leaderId",
        ("prevLogIndex", "prevLogTerm", "leaderCommit"),
    )
    validate_position(data, "prevLogIndex", "prevLogTerm")

    try:
        entries = validate_entries(
            data.get("entries"),
            data["term"],
            data["prevLogTerm"],
        )
        if len(entries) > APPEND_BATCH_SIZE:
            raise ValueError(
                f"В одном RPC допускается до {APPEND_BATCH_SIZE} записей"
            )
    except ValueError as exc:
        raise web.HTTPBadRequest(text=str(exc)) from exc

    reply = await request.app[NODE].append_entries(
        data["term"],
        data["leaderId"],
        data["prevLogIndex"],
        data["prevLogTerm"],
        entries,
        data["leaderCommit"],
    )
    return web.json_response(reply)


async def add_entries(request: web.Request) -> web.Response:
    data = await json_object(request)
    try:
        reply = await request.app[NODE].add_entries(data.get("entries"))
    except ValueError as exc:
        raise web.HTTPBadRequest(text=str(exc)) from exc

    return web.json_response(
        reply,
        status=CLIENT_STATUS_CODES[reply["status"]],
    )


async def show_log(request: web.Request) -> web.Response:
    reply = await request.app[NODE].show_log()
    return web.json_response(reply, headers={"Cache-Control": "no-store"})


def create_app(node: RaftNode) -> web.Application:
    app = web.Application(
        middlewares=[storage_errors],
        client_max_size=1024 * 1024,
    )
    app[NODE] = node
    app.add_routes([
        web.get("/status", status),
        web.get("/log", show_log),
        web.post("/log", add_entries),
        web.post("/raft/request-vote", request_vote),
        web.post("/raft/append-entries", append_entries),
    ])
    return app


async def serve(
    node_id: str,
    members: dict[str, str],
    data_dir: Path,
) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    try:
        loop.add_signal_handler(signal.SIGTERM, stop.set)
    except NotImplementedError:
        pass

    with StateStore(data_dir, node_id, members) as store:
        async with ClientSession(
            timeout=ClientTimeout(total=RPC_TIMEOUT)
        ) as session:
            async def send_rpc(
                peer: str,
                path: str,
                payload: Message,
            ) -> Message | None:
                try:
                    async with session.post(
                        members[peer] + path,
                        json=payload,
                        allow_redirects=False,
                    ) as response:
                        response.raise_for_status()
                        if response.status != 200:
                            return None
                        return await response.json()
                except (ClientError, TimeoutError, OSError, ValueError):
                    return None

            node = RaftNode(store, send_rpc)
            runner = web.AppRunner(
                create_app(node),
                access_log=None,
                shutdown_timeout=1.0,
            )
            await runner.setup()
            tasks = []

            try:
                address = urlsplit(members[node_id])
                await web.TCPSite(
                    runner,
                    address.hostname,
                    address.port,
                ).start()

                logging.info(
                    "узел=%s pid=%s адрес=%s term=%s",
                    node_id, os.getpid(), members[node_id], node.term,
                )

                tasks = [
                    asyncio.create_task(node.run()),
                    asyncio.create_task(stop.wait()),
                ]
                done, _ = await asyncio.wait(
                    tasks,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for task in done:
                    await task
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                await runner.cleanup()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Raft: выборы лидера и репликация журнала"
    )
    parser.add_argument("--id", required=True, help="id узла из cluster.json")
    parser.add_argument("--config", type=Path, default=ROOT / "cluster.json")
    parser.add_argument("--data-dir", type=Path, default=ROOT / "data")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )
    try:
        members = load_members(args.config)
        if args.id not in members:
            parser.error(f"Узел {args.id} отсутствует в конфигурации")
        asyncio.run(serve(args.id, members, args.data_dir))
    except KeyboardInterrupt:
        pass
    except Exception:
        logging.exception("Узел остановлен из-за ошибки")
        raise SystemExit(1)


if __name__ == "__main__":
    main()