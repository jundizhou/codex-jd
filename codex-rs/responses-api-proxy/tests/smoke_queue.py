"""Real HTTP queue contract against a controlled app-server RPC peer."""

import argparse
import asyncio
import contextlib
import json
import os
from http.client import IncompleteRead
import pathlib
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

import websockets


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--proxy", required=True)
    args = parser.parse_args()
    identities, calls, active = [], [], set()
    loaded, persisted, rpc_calls = set(), set(), []
    request_bodies = {}
    gates = {}
    responses = {}
    end_times = {}
    probe_calls, probe_failures = [], []
    wire = b': original\r\ndata: {"type":"response.completed"}\r\n\r\n'

    async def rpc(ws):
        with contextlib.suppress(websockets.ConnectionClosed):
            async for raw in ws:
                request = json.loads(raw)
                method, params = request["method"], request.get("params", {})
                rpc_calls.append((method, params))
                if method == "initialized":
                    continue
                if method == "initialize":
                    result = {"userAgent": "queue-smoke"}
                elif method == "thread/start":
                    thread = f"thread-{len(identities)}"
                    identities.append(
                        {
                            "threadId": thread,
                            "sessionId": thread,
                            "installationId": "install",
                            "windowId": thread,
                        }
                    )
                    assert params["ephemeral"] is False
                    loaded.add(thread)
                    result = {"thread": {"id": thread}}
                elif method == "thread/name/set":
                    persisted.add(params["threadId"])
                    result = {}
                elif method == "thread/unsubscribe":
                    assert params["threadId"] not in active
                    loaded.discard(params["threadId"])
                    result = {}
                elif method == "thread/delete":
                    assert params["threadId"] not in active
                    persisted.discard(params["threadId"])
                    loaded.discard(params["threadId"])
                    result = {}
                elif method == "thread/resume":
                    assert params["threadId"] in persisted
                    loaded.add(params["threadId"])
                    result = {}
                elif method == "thread/modelIdentity/list":
                    result = {
                        "data": [i for i in identities if i["threadId"] in loaded],
                        "nextCursor": None,
                    }
                elif method == "account/rateLimits/read":
                    probe_calls.append(time.monotonic())
                    used = probe_failures.pop(0) if probe_failures else 20
                    result = {
                        "accountId": "test-account",
                        "rateLimits": {"primary": {"usedPercent": used}},
                    }
                elif method == "turn/start":
                    body, thread = params["rawResponses"], params["threadId"]
                    assert thread in persisted and thread in loaded
                    tag = body.get("test_tag", body["input"])
                    assert thread not in active, "concurrent calls on one conversation"
                    assert not any(
                        "queue" in name for name in params["rawResponsesHeaders"]
                    )
                    request_bodies[tag] = body
                    active.add(thread)
                    calls.append((tag, time.monotonic(), thread))
                    assert len(active) <= 2
                    if tag.startswith("concurrent-"):
                        await asyncio.sleep(0.15)
                    if tag in gates:
                        await gates[tag].wait()
                    status, headers, payload = responses.get(tag, (200, {}, wire))
                    for event in [
                        {"type": "started", "status": status, "headers": headers},
                        {"type": "chunk", "data": list(payload)},
                    ]:
                        await ws.send(
                            json.dumps(
                                {
                                    "method": "rawResponse/stream",
                                    "params": {
                                        "requestId": request["id"],
                                        "event": event,
                                    },
                                }
                            )
                        )
                    active.remove(thread)
                    end_times[tag] = time.monotonic()
                    result = {}
                else:
                    raise AssertionError(method)
                await ws.send(json.dumps({"id": request["id"], "result": result}))

    with tempfile.TemporaryDirectory(prefix="queue-") as directory:
        root = pathlib.Path(directory)
        (root / "auth.json").write_text(
            json.dumps({"tokens": {"account_id": "test-account"}})
        )
        env = {**os.environ, "CODEX_HOME": str(root)}
        socket, info = root / "rpc.sock", root / "info.json"
        async with websockets.unix_serve(rpc, str(socket)):
            with (root / "proxy.log").open("w+") as log:
                process = subprocess.Popen(
                    [
                        args.proxy,
                        "--queue",
                        "--queue-state",
                        str(root / "queue.json"),
                        "--queue-idle-ttl-secs",
                        "1",
                        "--queue-tool-gap-ms",
                        "100",
                        "--queue-user-gap-ms",
                        "500",
                        "--worker-api-key",
                        "test-secret",
                        "--session-pool-size",
                        "2",
                        "--queue-conversation-gap-ms",
                        "800",
                        "--queue-start-gap-ms",
                        "50",
                        "--app-server-socket",
                        str(socket),
                        "--server-info",
                        str(info),
                    ],
                    stdout=log,
                    stderr=log,
                    env=env,
                )
                try:

                    async def until(predicate):
                        async with asyncio.timeout(10):
                            while not predicate():
                                await asyncio.sleep(0.01)

                    await until(lambda: info.exists() or process.poll() is not None)
                    if not info.exists():
                        log.seek(0)
                        raise AssertionError(log.read())
                    base = f"http://127.0.0.1:{json.loads(info.read_text())['port']}"

                    def http(path, body=None, headers=None):
                        req = urllib.request.Request(
                            base + path,
                            data=body,
                            headers={
                                "Authorization": "Bearer test-secret",
                                "Content-Type": "application/json",
                                **(headers or {}),
                            },
                        )
                        try:
                            with urllib.request.urlopen(req, timeout=150) as response:
                                return response.status, response.read()
                        except urllib.error.HTTPError as error:
                            return error.code, error.read()

                    async def send(
                        tag, conversation, tenant="user", extra=None, queue_headers=None
                    ):
                        body = json.dumps(
                            {
                                "model": "mock",
                                "stream": True,
                                "input": tag,
                                "client_metadata": {"session_id": conversation},
                                "test_tag": tag,
                                **(extra or {}),
                            }
                        ).encode()
                        return await asyncio.to_thread(
                            http,
                            "/v1/responses",
                            body,
                            {
                                "X-Codex-Queue-Principal": tenant,
                                "X-Codex-Queue-Request-Id": tag,
                                **(queue_headers or {}),
                            },
                        )

                    gates["a"] = asyncio.Event()
                    gates["b"] = asyncio.Event()
                    a = asyncio.create_task(send("a", "A"))
                    b = asyncio.create_task(send("b", "B"))
                    await until(lambda: len(calls) == 2)
                    cancelled = asyncio.create_task(send("cancel-me", "C"))
                    follower = asyncio.create_task(send("a2", "A"))
                    explicit_budget = asyncio.create_task(
                        send(
                            "b2",
                            "B",
                            queue_headers={"X-Codex-Queue-Budget-Ms": "120000"},
                        )
                    )
                    short_budget = asyncio.create_task(
                        send(
                            "short-budget",
                            "C",
                            queue_headers={"X-Codex-Queue-Budget-Ms": "1000"},
                        )
                    )
                    async with asyncio.timeout(10):
                        while True:
                            _, data = await asyncio.to_thread(
                                http, "/internal/queue/status"
                            )
                            if json.loads(data)["pending"] == 4:
                                break
                            await asyncio.sleep(0.01)
                    status, _ = await asyncio.to_thread(
                        http,
                        "/internal/queue/cancel/cancel-me",
                        b"",
                        {"X-Codex-Queue-Principal": "user"},
                    )
                    assert status == 200
                    assert (await cancelled)[0] == 409
                    status, body = await short_budget
                    assert status == 503
                    assert (
                        json.loads(body)["error"]["type"] == "queue_deadline_exceeded"
                    )
                    # Both the default budget and an explicit 120-second budget
                    # must survive the former 20-second dispatch deadline.
                    await asyncio.sleep(21)
                    assert not follower.done()
                    assert not explicit_budget.done()
                    assert len(calls) == 2
                    gates["a"].set()
                    assert await a == (200, wire)
                    assert await follower == (200, wire)
                    assert calls[2][2] == next(
                        call[2] for call in calls if call[0] == "a"
                    )
                    assert calls[2][1] - end_times["a"] >= 0.19
                    gates["b"].set()
                    assert await b == (200, wire)
                    assert await explicit_budget == (200, wire)
                    print(
                        "PASS: default and explicit queue budgets survive 20 seconds; shorter budgets expire"
                    )
                    for i in range(5):
                        assert await send(f"new-{i}", "A", f"tenant-{i}") == (200, wire)
                    assert len({call[2] for call in calls}) == 7
                    assert await send("a3", "A") == (200, wire)
                    assert calls[-1][2] == next(
                        call[2] for call in calls if call[0] == "a"
                    )
                    assert all(
                        right[1] - left[1] >= 0.04
                        for left, right in zip(calls, calls[1:])
                    )
                    assert (
                        await asyncio.to_thread(
                            http,
                            "/internal/queue/status",
                            None,
                            {"Authorization": "Bearer wrong"},
                        )
                    )[0] == 401
                    # Match tool outputs against the preceding observed response.
                    tool_wire = b'data: {"type":"response.completed","response":{"id":"r-tool","output":[{"type":"function_call","call_id":"c-tool"}]}}\n\n'
                    responses["tool-first"] = (200, {}, tool_wire)
                    initial = [{"role": "user", "content": "first"}]
                    assert await send("tool-first", "A", extra={"input": initial}) == (
                        200,
                        tool_wire,
                    )
                    follow = initial + [
                        {
                            "type": "function_call_output",
                            "call_id": "c-tool",
                            "output": "ok",
                        }
                    ]
                    assert await send("tool-follow", "A", extra={"input": follow}) == (
                        200,
                        wire,
                    )
                    assert 0.09 <= calls[-1][1] - end_times["tool-first"] < 0.7
                    assert await send(
                        "new-user",
                        "A",
                        extra={"input": follow + [{"role": "user", "content": "next"}]},
                    ) == (200, wire)
                    assert calls[-1][1] - end_times["tool-follow"] >= 0.49
                    before = len(calls)
                    assert (await send("tool-first", "A", extra={"input": initial}))[
                        0
                    ] == 409
                    assert (await send("tool-first", "A", extra={"input": "changed"}))[
                        0
                    ] == 409
                    assert len(calls) == before

                    gates["running-cancel"] = asyncio.Event()
                    running = asyncio.create_task(send("running-cancel", "A"))
                    await until(lambda: calls[-1][0] == "running-cancel")
                    assert (
                        await asyncio.to_thread(
                            http,
                            "/internal/queue/cancel/running-cancel",
                            b"",
                            {"X-Codex-Queue-Principal": "user"},
                        )
                    )[0] == 200
                    gates["running-cancel"].set()
                    assert await running == (200, wire)
                    assert (
                        await asyncio.to_thread(http, "/internal/queue/pause", b"")
                    )[0] == 200
                    assert (await asyncio.to_thread(http, "/readyz"))[0] == 503
                    assert (await send("paused", "A"))[0] == 503
                    assert (
                        await asyncio.to_thread(http, "/internal/queue/resume", b"")
                    )[0] == 200

                    async def restart(quarantined=False):
                        nonlocal process, base
                        async with asyncio.timeout(10):
                            while (
                                json.loads(
                                    (
                                        await asyncio.to_thread(
                                            http, "/internal/queue/status"
                                        )
                                    )[1]
                                )["running"]
                                and not quarantined
                            ):
                                await asyncio.sleep(0.01)
                        command = process.args
                        process.terminate()
                        await asyncio.to_thread(process.wait, timeout=10)
                        info.unlink()
                        loaded.clear()
                        process = subprocess.Popen(
                            command, stdout=log, stderr=log, env=env
                        )
                        await until(lambda: info.exists() or process.poll() is not None)
                        if not info.exists():
                            log.seek(0)
                            raise AssertionError(log.read())
                        base = (
                            f"http://127.0.0.1:{json.loads(info.read_text())['port']}"
                        )

                    prior = calls[-1][2]
                    await asyncio.sleep(2.2)
                    assert not loaded
                    assert await send(
                        "idle-resume",
                        "A",
                        extra={"previous_response_id": "persisted-response"},
                    ) == (200, wire)
                    assert calls[-1][2] == prior
                    # More logical conversations than the loaded-thread limit.
                    concurrent = await asyncio.gather(
                        *(
                            send(
                                f"concurrent-{i}",
                                "shared-client-id",
                                f"concurrent-tenant-{i}",
                            )
                            for i in range(10)
                        )
                    )
                    assert concurrent == [(200, wire)] * 10
                    assert (
                        len(
                            {
                                thread
                                for tag, _, thread in calls
                                if tag.startswith("concurrent-")
                            }
                        )
                        == 10
                    )
                    assert len(loaded) <= 2
                    await restart()
                    assert (await send("tool-first", "A", extra={"input": initial}))[
                        0
                    ] == 409
                    assert await send("old-binding", "A", extra={"input": follow}) == (
                        200,
                        wire,
                    )
                    assert calls[-1][2] == prior
                    assert request_bodies["old-binding"]["input"] == follow
                    before = len(calls)
                    for i, extra in enumerate(
                        [
                            {"previous_response_id": "missing"},
                            {"conversation": "missing"},
                            {
                                "input": [
                                    {
                                        "type": "function_call_output",
                                        "call_id": "missing",
                                        "output": "ok",
                                    }
                                ]
                            },
                        ]
                    ):
                        assert (await send(f"lost-{i}", f"missing-{i}", extra=extra))[
                            0
                        ] == 409
                    assert (
                        await send(
                            "lost-routing",
                            "missing",
                            queue_headers={"X-Codex-Turn-State": "opaque"},
                        )
                    )[0] == 409
                    assert len(calls) == before
                    assert await send("fresh", "fresh") == (200, wire)
                    responses["redirect"] = (302, {}, b"redirect")
                    assert await send("redirect", "fresh") == (302, b"redirect")
                    assert (
                        await send("background", "fresh", extra={"background": True})
                    )[0] == 400
                    responses["truncated"] = (
                        200,
                        {},
                        b'data: {"type":"response.created"}\n\n',
                    )
                    try:
                        await send("truncated", "broken")
                        raise AssertionError("truncated model stream was accepted")
                    except IncompleteRead:
                        pass
                    async with asyncio.timeout(10):
                        while not json.loads(
                            (await asyncio.to_thread(http, "/internal/queue/status"))[1]
                        )["outcome_unknown"]:
                            await asyncio.sleep(0.01)
                    assert (await send("same-unknown-conversation", "broken"))[0] == 503
                    assert await send("unaffected-conversation", "fresh") == (200, wire)
                    assert (await asyncio.to_thread(http, "/readyz"))[0] == 200
                    await restart(quarantined=True)
                    assert (await send("same-unknown-after-restart", "broken"))[
                        0
                    ] == 503
                    assert await send("unaffected-after-restart", "fresh") == (
                        200,
                        wire,
                    )
                    responses["second-truncated"] = responses["truncated"]
                    try:
                        await send("second-truncated", "second-broken")
                        raise AssertionError("second truncated stream was accepted")
                    except IncompleteRead:
                        pass
                    assert (await send("no-capacity", "fresh"))[0] == 503
                    assert (await asyncio.to_thread(http, "/readyz"))[0] == 503
                    await restart(quarantined=True)
                    assert (await send("no-capacity-after-restart", "fresh"))[0] == 503
                    assert (
                        await asyncio.to_thread(http, "/internal/queue/pause", b"")
                    )[0] == 200
                    # The controlled RPC peer has already returned; model work in this
                    # fixture is known to be over, so explicitly acknowledge recovery.
                    assert (
                        await asyncio.to_thread(
                            http,
                            "/internal/queue/recover",
                            b"",
                            {"X-Codex-Queue-Confirm-Stopped": "true"},
                        )
                    )[0] == 200
                    assert (
                        await asyncio.to_thread(http, "/internal/queue/resume", b"")
                    )[0] == 200
                    assert (
                        await send(
                            "broken-continuation",
                            "broken",
                            extra={"previous_response_id": "unknown"},
                        )
                    )[0] == 409
                    assert await send("broken-new-history", "broken") == (200, wire)
                    # Enable recovery only for this stage, preserving earlier strict-mode checks.
                    process.args.append("--queue-auto-recover")
                    await restart()
                    responses["auto-truncated"] = responses["truncated"]
                    probe_failures.append(100)
                    try:
                        await send("auto-truncated", "auto-broken")
                        raise AssertionError("truncated stream was accepted")
                    except IncompleteRead:
                        pass
                    old_thread = calls[-1][2]
                    before = len(calls)
                    assert (
                        await send(
                            "auto-waiting",
                            "auto-broken",
                            queue_headers={"X-Codex-Queue-Budget-Ms": "1000"},
                        )
                    )[0] == 503
                    incremental = asyncio.create_task(
                        send(
                            "auto-incremental",
                            "auto-broken",
                            extra={"previous_response_id": "unknown"},
                        )
                    )
                    async with asyncio.timeout(5):
                        while (
                            json.loads(
                                (
                                    await asyncio.to_thread(
                                        http, "/internal/queue/status"
                                    )
                                )[1]
                            )["pending"]
                            != 1
                        ):
                            await asyncio.sleep(0.01)
                    waiting = asyncio.create_task(send("auto-waiting", "auto-broken"))
                    async with asyncio.timeout(45):
                        while True:
                            queue = json.loads(
                                (
                                    await asyncio.to_thread(
                                        http, "/internal/queue/status"
                                    )
                                )[1]
                            )
                            if queue["quarantined"] == 0:
                                break
                            await asyncio.sleep(0.1)
                    assert len(probe_calls) == 2
                    assert 19 <= probe_calls[1] - probe_calls[0] <= 24
                    assert queue["recovery"]["released_unknown"] == 1
                    assert (await incremental)[0] == 409
                    assert await waiting == (200, wire)
                    assert (
                        len(calls) == before + 1 and calls[-1][0] == "auto-waiting"
                    ), "only the waiting request may reach the model"
                    new_thread = calls[-1][2]
                    assert new_thread != old_thread
                    status, body = await send("auto-truncated", "auto-broken")
                    assert (
                        status == 409
                        and json.loads(body)["error"]["type"]
                        == "request_outcome_unknown"
                    )
                    await restart()
                    assert await send(
                        "auto-resumed",
                        "auto-broken",
                        extra={"previous_response_id": "known"},
                    ) == (200, wire)
                    assert calls[-1][2] == new_thread
                    assert (await send("auto-truncated", "auto-broken"))[0] == 409
                    (root / "auth.json").write_text(
                        json.dumps({"tokens": {"account_id": "different"}})
                    )
                    status, body = await send("hot-account-swap", "fresh")
                    assert (
                        status == 503
                        and json.loads(body)["error"]["type"]
                        == "worker_identity_unavailable"
                    )
                    assert (await asyncio.to_thread(http, "/readyz"))[0] == 503
                    (root / "auth.json").write_text(
                        json.dumps({"tokens": {"account_id": "test-account"}})
                    )
                    await restart()
                    # A failed durable write must never reach the model or permit more creations.
                    temporary = root / "queue.conversations.tmp"
                    temporary.mkdir()
                    before = len(calls)
                    assert (await send("disk-failure", "fresh"))[0] == 503
                    assert len(calls) == before
                    assert (await asyncio.to_thread(http, "/readyz"))[0] == 503
                    temporary.rmdir()
                    await restart()
                    error_wire = b'{"error":{"type":"rate_limit_exceeded"}}'
                    responses["limit"] = (
                        429,
                        {"retry-after": "120", "content-type": "application/json"},
                        error_wire,
                    )
                    assert await send("limit", "fresh") == (429, error_wire)
                    before = len(calls)
                    status, body = await send(
                        "after-limit",
                        "fresh",
                        queue_headers={"X-Codex-Queue-Budget-Ms": "1000"},
                    )
                    assert (
                        status == 429
                        and json.loads(body)["error"]["type"] == "account_cooldown"
                    )
                    assert len(calls) == before
                    await restart()
                    assert (await asyncio.to_thread(http, "/readyz"))[0] == 503
                    status, body = await send(
                        "still-cooling",
                        "brand-new",
                        queue_headers={"X-Codex-Queue-Budget-Ms": "1000"},
                    )
                    assert status == 429, (status, body)
                    assert len(calls) == before
                    print(
                        "PASS: queue fairness, cancellation, tool/user gaps, exact bytes, pause/resume, durable dedup/identity restoration/10 concurrent tenants, truncation quarantine/recovery and persistent 429 cooldown"
                    )

                finally:
                    for gate in gates.values():
                        gate.set()
                    process.terminate()
                    await asyncio.to_thread(process.wait, timeout=10)


if __name__ == "__main__":
    asyncio.run(main())
