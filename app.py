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

from node import RPC_TIMEOUT, Message, RaftNode
from storage import StateStore, StorageError

ROOT = Path(__file__).resolve().parent
NODE = web.AppKey("node", RaftNode)


def load_members(path: Path) -> dict[str, str]:
    members = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(members, dict) or not members:
        raise ValueError("Конфигурация должна содержать непустой объект id → URL")
    result = {}
    for node_id, url in members.items():
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", node_id) or not isinstance(url, str):
            raise ValueError("Некорректный id или URL в конфигурации")
        parsed = urlsplit(url)
        if (parsed.scheme != "http" or not parsed.hostname or not parsed.port
                or parsed.username is not None or parsed.password is not None
                or parsed.path not in ("", "/") or parsed.query or parsed.fragment):
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


async def parse_rpc(request: web.Request, sender_key: str,
                    zero_fields: tuple[str, ...]) -> Message:
    try:
        data = await request.json()
    except (ValueError, UnicodeError) as exc:
        raise web.HTTPBadRequest(text="Нужен корректный JSON") from exc
    if not isinstance(data, dict) or type(data.get("term")) is not int or data["term"] < 1:
        raise web.HTTPBadRequest(text="term в RPC должен быть положительным целым числом")
    sender = data.get(sender_key)
    if not isinstance(sender, str) or sender not in request.app[NODE].peers:
        raise web.HTTPBadRequest(text=f"{sender_key}: неизвестный или собственный id")
    if any(type(data.get(key)) is not int or data[key] != 0 for key in zero_fields):
        raise web.HTTPBadRequest(text="На шаге 1 все журналы пусты: индексы и term записей равны 0")
    return data


async def status(request: web.Request) -> web.Response:
    snapshot = await request.app[NODE].status()
    snapshot["pid"] = os.getpid()
    return web.json_response(snapshot, headers={"Cache-Control": "no-store"})


async def request_vote(request: web.Request) -> web.Response:
    data = await parse_rpc(request, "candidateId", ("lastLogIndex", "lastLogTerm"))
    reply = await request.app[NODE].request_vote(data["term"], data["candidateId"])
    return web.json_response(reply)


async def append_entries(request: web.Request) -> web.Response:
    data = await parse_rpc(request, "leaderId", ("prevLogIndex", "prevLogTerm", "leaderCommit"))
    if data.get("entries") != []:
        raise web.HTTPBadRequest(text="На шаге 1 поддерживаются только пустые heartbeat")
    reply = await request.app[NODE].append_entries(data["term"], data["leaderId"])
    return web.json_response(reply)


def create_app(node: RaftNode) -> web.Application:
    app = web.Application(middlewares=[storage_errors], client_max_size=4096)
    app[NODE] = node
    app.add_routes([
        web.get("/status", status),
        web.post("/raft/request-vote", request_vote),
        web.post("/raft/append-entries", append_entries),
    ])
    return app


async def serve(node_id: str, members: dict[str, str], data_dir: Path) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    try:
        loop.add_signal_handler(signal.SIGTERM, stop.set)
    except NotImplementedError:  # Windows: Ctrl+C обрабатывает asyncio.run.
        pass
    with StateStore(data_dir, node_id, members) as store:
        async with ClientSession(timeout=ClientTimeout(total=RPC_TIMEOUT)) as session:
            async def send_rpc(peer: str, path: str, payload: Message) -> Message | None:
                try:
                    async with session.post(members[peer] + path, json=payload,
                                            allow_redirects=False) as response:
                        response.raise_for_status()
                        if response.status != 200:
                            return None
                        return await response.json()
                except (ClientError, TimeoutError, OSError, ValueError):
                    # Недоступный peer — штатная ситуация. Следующая попытка
                    # будет в его собственном цикле, не задерживая остальных.
                    return None

            node = RaftNode(store, send_rpc)
            runner = web.AppRunner(create_app(node), access_log=None, shutdown_timeout=1.0)
            await runner.setup()
            tasks = []
            try:
                address = urlsplit(members[node_id])
                await web.TCPSite(runner, address.hostname, address.port).start()
                logging.info("узел=%s pid=%s адрес=%s term=%s",
                             node_id, os.getpid(), members[node_id], node.term)
                # Выборы запускаются только после успешного занятия HTTP-порта.
                tasks = [asyncio.create_task(node.run()), asyncio.create_task(stop.wait())]
                done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    await task
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                await runner.cleanup()


def main() -> None:
    parser = argparse.ArgumentParser(description="Raft: только выборы лидера и REST status")
    parser.add_argument("--id", required=True, help="id узла из cluster.json")
    parser.add_argument("--config", type=Path, default=ROOT / "cluster.json")
    parser.add_argument("--data-dir", type=Path, default=ROOT / "data")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        datefmt="%H:%M:%S")
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
