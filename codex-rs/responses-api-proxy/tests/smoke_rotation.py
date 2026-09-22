"""Public HTTP/RPC contract for automatic rotation, auth acknowledgement and rollback."""

import argparse
import asyncio
import contextlib
import hashlib
import json
import os
import pathlib
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import websockets


def fingerprint(value):
    data = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(len(data).to_bytes(8, "little") + data).hexdigest()


async def scenario(binary, reject_target, no_candidate=False):
    with tempfile.TemporaryDirectory(prefix="rotation-") as directory:
        root = pathlib.Path(directory)
        accounts = {
            name: {
                "tokens": {
                    "account_id": name,
                    "access_token": "token-" + name,
                    "refresh_token": "refresh-" + name,
                    "id_token": "id-" + name,
                }
            }
            for name in ["a", "b"]
        }
        for name, auth in accounts.items():
            (root / "accounts" / name).mkdir(parents=True)
            (root / "accounts" / name / "auth.json").write_text(json.dumps(auth))
        auth_file = root / "auth.json"
        auth_file.write_text(json.dumps(accounts["a"]))
        now = int(time.time())
        cache = {}
        for name, remaining in [("a", 2), ("b", 4 if no_candidate else 80)]:
            cache[fingerprint(accounts[name])] = {
                "value": {
                    "fetched_at": now,
                    "limits": [
                        {
                            "name": "Codex",
                            "allowed": True,
                            "limit_reached": False,
                            "windows": [
                                {
                                    "remaining_percent": remaining,
                                    "used_percent": 100 - remaining,
                                    "seconds": 18000,
                                    "reset_at": now + 3600,
                                }
                            ],
                        }
                    ],
                },
                "failure": None,
                "retry_at": 0,
                "failures": 0,
            }
        (root / "account-quota-cache.json").write_text(json.dumps(cache))
        threads = {}
        loaded = set()
        calls = []
        auth_checks = []
        gate = asyncio.Event()
        released = asyncio.Event()

        def active():
            return json.loads(auth_file.read_text())["tokens"]["account_id"]

        async def rpc(ws):
            with contextlib.suppress(websockets.ConnectionClosed):
                async for data in ws:
                    req = json.loads(data)
                    method = req["method"]
                    params = req.get("params") or {}
                    if method == "initialized":
                        continue
                    if method == "initialize":
                        result = {"userAgent": "rotation-smoke"}
                    elif method == "getAuthStatus":
                        account = active()
                        auth_checks.append(account)
                        token = (
                            "old-token"
                            if account == "b"
                            and (reject_target or auth_checks.count("b") < 3)
                            else "token-" + account
                        )
                        result = {
                            "authMethod": "chatgpt",
                            "authToken": token,
                            "requiresOpenaiAuth": True,
                        }
                    elif method == "account/rateLimits/read":
                        result = {
                            "accountId": active(),
                            "rateLimits": {"primary": {"usedPercent": 20}},
                        }
                    elif method == "thread/start":
                        thread = "thread-" + str(len(threads))
                        threads[thread] = active()
                        loaded.add(thread)
                        result = {"thread": {"id": thread}}
                    elif method == "thread/name/set":
                        result = {}
                    elif method == "thread/resume":
                        loaded.add(params["threadId"])
                        result = {}
                    elif method in ["thread/unsubscribe", "thread/delete"]:
                        loaded.discard(params["threadId"])
                        result = {}
                    elif method == "thread/modelIdentity/list":
                        result = {
                            "data": [
                                {
                                    "threadId": t,
                                    "sessionId": t,
                                    "installationId": "install",
                                    "windowId": t,
                                }
                                for t in sorted(loaded)
                            ],
                            "nextCursor": None,
                        }
                    elif method == "turn/start":
                        thread = params["threadId"]
                        tag = params["rawResponses"]["tag"]
                        assert threads[thread] == active(), (
                            "request used a different account than its binding"
                        )
                        calls.append((tag, active(), thread))
                        if tag == "running":
                            gate.set()
                            await released.wait()
                            assert active() == "a", "switched during an active stream"
                        wire = b'data: {"type":"response.completed","response":{"id":"done","output":[]}}\n\n'
                        for event in [
                            {
                                "type": "started",
                                "status": 200,
                                "headers": {"content-type": "text/event-stream"},
                            },
                            {"type": "chunk", "data": list(wire)},
                        ]:
                            await ws.send(
                                json.dumps(
                                    {
                                        "method": "rawResponse/stream",
                                        "params": {
                                            "requestId": req["id"],
                                            "event": event,
                                        },
                                    }
                                )
                            )
                        result = {}
                    else:
                        raise AssertionError(method)
                    await ws.send(json.dumps({"id": req["id"], "result": result}))

        socket = root / "rpc.sock"
        info = root / "server.json"
        async with websockets.unix_serve(rpc, str(socket)):
            with (root / "proxy.log").open("w+") as log:
                process = subprocess.Popen(
                    [
                        binary,
                        "--queue",
                        "--queue-state",
                        str(root / "queue.json"),
                        "--app-server-socket",
                        str(socket),
                        "--server-info",
                        str(info),
                        "--worker-api-key",
                        "test-secret",
                        "--session-pool-size",
                        "2",
                        "--queue-max-running",
                        "2",
                        "--queue-start-gap-ms",
                        "0",
                        "--queue-conversation-gap-ms",
                        "0",
                        "--queue-tool-gap-ms",
                        "0",
                        "--queue-user-gap-ms",
                        "0",
                    ],
                    env={**os.environ, "CODEX_HOME": str(root)},
                    stdout=log,
                    stderr=log,
                )
                try:
                    async with asyncio.timeout(10):
                        while not info.exists():
                            await asyncio.sleep(0.01)
                    base = "http://127.0.0.1:" + str(
                        json.loads(info.read_text())["port"]
                    )

                    def http(path, body=None):
                        request = urllib.request.Request(
                            base + path,
                            data=json.dumps(body).encode()
                            if body is not None
                            else None,
                            headers={
                                "Authorization": "Bearer test-secret",
                                "Content-Type": "application/json",
                            },
                        )
                        opener = urllib.request.build_opener(
                            urllib.request.ProxyHandler({})
                        )
                        try:
                            with opener.open(request, timeout=110) as r:
                                return r.status, r.read()
                        except urllib.error.HTTPError as e:
                            return e.code, e.read()

                    async def get():
                        return json.loads(
                            (await asyncio.to_thread(http, "/admin/api/accounts"))[1]
                        )

                    async def until(predicate):
                        async with asyncio.timeout(35):
                            while True:
                                data = await get()
                                if predicate(data):
                                    return data
                                await asyncio.sleep(0.05)

                    async def send(tag, extra=None):
                        return await asyncio.to_thread(
                            http,
                            "/v1/responses",
                            {
                                "model": "mock",
                                "input": [],
                                "stream": True,
                                "tag": tag,
                                "client_metadata": {"session_id": tag},
                                **(extra or {}),
                            },
                        )

                    running = asyncio.create_task(send("running"))
                    await asyncio.wait_for(gate.wait(), 10)
                    assert (
                        await asyncio.to_thread(
                            http,
                            "/admin/api/rotation",
                            {"enabled": True, "priority": ["b"]},
                        )
                    )[0] == 200
                    await until(
                        lambda d: (
                            d["rotation"]["phase"] == "waiting"
                            if no_candidate
                            else d["queue"]["switching"]
                        )
                    )
                    assert active() == "a" and not auth_checks
                    queued = (
                        asyncio.create_task(send("queued"))
                        if not reject_target and not no_candidate
                        else None
                    )
                    if queued:
                        await until(lambda d: d["queue"]["pending"] == 1)
                    released.set()
                    assert (await running)[0] == 200
                    if no_candidate:
                        data = await get()
                        assert (
                            active() == "a"
                            and data["queue"]["rotation_hold"] is True
                            and not auth_checks
                        )
                        assert data["rotation"]["events"] == [] and len(calls) == 1
                        assert (
                            await asyncio.to_thread(
                                http,
                                "/admin/api/rotation",
                                {"enabled": False, "priority": ["b"]},
                            )
                        )[0] == 200
                        assert (await get())["queue"]["rotation_hold"] is False
                        print(
                            "PASS: unavailable candidates remain gated without repeated probes; disabling releases hold",
                            flush=True,
                        )
                        return
                    done = await until(lambda d: len(d["rotation"]["events"]) == 1)
                    assert (
                        process.poll() is None
                        and done["queue"]["worker_fault"] is False
                    )
                    if reject_target:
                        assert (
                            active() == "a"
                            and done["rotation"]["events"][0]["ok"] is False
                        )
                        assert done["queue"]["rotation_hold"] is True
                        assert calls == [("running", "a", "thread-0")]
                    else:
                        assert (await queued)[0] == 200 and active() == "b"
                        assert auth_checks.count("b") >= 3
                        assert calls[1][0:2] == ("queued", "b")
                        assert (
                            await send(
                                "running",
                                {"previous_response_id": "old-account-response"},
                            )
                        )[0] == 409
                        assert (
                            await send(
                                "new-encrypted",
                                {
                                    "input": [
                                        {
                                            "type": "reasoning",
                                            "encrypted_content": "old-account",
                                        }
                                    ]
                                },
                            )
                        )[0] == 409
                        assert len(calls) == 2, (
                            "dependent history reached the new account"
                        )
                    assert (root / "account-rotation.json").exists()
                    original_pid = process.pid
                    process.terminate()
                    await asyncio.to_thread(process.wait, timeout=10)
                    info.unlink()
                    process = subprocess.Popen(
                        process.args,
                        env={**os.environ, "CODEX_HOME": str(root)},
                        stdout=log,
                        stderr=log,
                    )
                    async with asyncio.timeout(10):
                        while not info.exists():
                            await asyncio.sleep(0.01)
                    base = "http://127.0.0.1:" + str(
                        json.loads(info.read_text())["port"]
                    )
                    restored = await get()
                    assert (
                        process.pid != original_pid
                        and restored["rotation"]["settings"]["enabled"] is True
                    )
                    assert len(restored["rotation"]["events"]) == 1

                    print(
                        "PASS:",
                        "failed target rolled back without replay"
                        if reject_target
                        else "drain, retained queue, auth acknowledgement, safe continuation, no restart",
                        flush=True,
                    )
                except BaseException:
                    log.seek(0)
                    print(log.read()[-12000:])
                    raise
                finally:
                    process.terminate()
                    await asyncio.to_thread(process.wait, timeout=10)


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--proxy", required=True)
    args = parser.parse_args()
    await scenario(args.proxy, False)
    await scenario(args.proxy, True)
    await scenario(args.proxy, False, True)


if __name__ == "__main__":
    asyncio.run(main())
