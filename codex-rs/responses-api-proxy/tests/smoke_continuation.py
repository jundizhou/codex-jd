"""Exercise automatic continuation through real HTTP, including restart and isolation."""

import argparse
import asyncio
import contextlib
import json
import os
import pathlib
import subprocess
import tempfile
import urllib.error
import urllib.request

import websockets


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--proxy", required=True)
    args = parser.parse_args()
    identities, loaded, calls, outputs, wires = {}, set(), {}, {}, {}
    gate, delivered = asyncio.Event(), asyncio.Event()
    loop = asyncio.get_running_loop()

    async def rpc(ws):
        with contextlib.suppress(websockets.ConnectionClosed):
            async for raw in ws:
                request = json.loads(raw)
                method, params = request["method"], request.get("params", {})
                if method == "initialized":
                    continue
                if method == "initialize":
                    result = {"userAgent": "continuation-smoke"}
                elif method == "thread/start":
                    thread = f"thread-{len(identities)}"
                    identities[thread] = {
                        "threadId": thread,
                        "sessionId": thread,
                        "installationId": "install",
                        "windowId": thread,
                    }
                    loaded.add(thread)
                    result = {"thread": {"id": thread}}
                elif method == "thread/resume":
                    assert params["threadId"] in identities
                    loaded.add(params["threadId"])
                    result = {}
                elif method in ("thread/unsubscribe", "thread/delete"):
                    loaded.discard(params["threadId"])
                    result = {}
                elif method == "thread/name/set":
                    result = {}
                elif method == "thread/modelIdentity/list":
                    result = {
                        "data": [identities[t] for t in loaded],
                        "nextCursor": None,
                    }
                elif method == "turn/start":
                    body, thread = params["rawResponses"], params["threadId"]
                    tag = body["test_tag"]
                    calls[tag] = thread
                    assert body["stream"] is True and body["store"] is False
                    item = {
                        "type": "message",
                        "id": "msg-" + tag,
                        "role": "assistant",
                        "status": "completed",
                        "content": [{"type": "output_text", "text": tag}],
                    }
                    if tag in ("first", "early", "large"):
                        item = {
                            "type": "function_call",
                            "id": "item-" + tag,
                            "call_id": "call-" + tag,
                            "name": "lookup",
                            "arguments": "{}",
                            "encrypted_function_args": "args-" + tag,
                        }
                    reasoning = {
                        "type": "reasoning",
                        "id": "reason-" + tag,
                        "encrypted_content": "cipher-" + tag,
                        "summary": [],
                    }
                    if tag == "large":
                        reasoning["encrypted_content"] = "x" * (300 * 1024)
                    if tag.startswith("collision"):
                        item["id"] = "ambiguous-item"
                    outputs[tag] = [reasoning, item]

                    async def notify(event):
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

                    async def event(value):
                        wire = b"data: " + json.dumps(value).encode() + b"\r\n\r\n"
                        wires[tag] = wires.get(tag, b"") + wire
                        for start in range(0, len(wire), 16384):
                            await notify(
                                {
                                    "type": "chunk",
                                    "data": list(wire[start : start + 16384]),
                                }
                            )

                    await notify(
                        {
                            "type": "started",
                            "status": 200,
                            "headers": {"content-type": "text/event-stream"},
                        }
                    )
                    await event(
                        {"type": "response.created", "response": {"id": "resp-" + tag}}
                    )
                    for index, value in enumerate(outputs[tag]):
                        await event(
                            {
                                "type": "response.output_item.done",
                                "output_index": index,
                                "item": value,
                            }
                        )
                    if tag == "early":
                        await gate.wait()
                    if tag == "unfinished":
                        await ws.send(
                            json.dumps(
                                {
                                    "id": request["id"],
                                    "error": {
                                        "code": -32603,
                                        "message": "upstream disconnected",
                                    },
                                }
                            )
                        )
                        continue
                    await event(
                        {
                            "type": "response.completed",
                            "response": {
                                "id": "resp-" + tag,
                                "status": "completed",
                                "output": [],
                            },
                        }
                    )
                    result = {}
                else:
                    raise AssertionError(method)
                await ws.send(json.dumps({"id": request["id"], "result": result}))

    async def until(predicate):
        async with asyncio.timeout(15):
            while not predicate():
                await asyncio.sleep(0.02)

    with tempfile.TemporaryDirectory(prefix="continuation-") as directory:
        root = pathlib.Path(directory)
        auth, info, socket = root / "auth.json", root / "info.json", root / "rpc.sock"
        auth.write_text(json.dumps({"tokens": {"account_id": "account-a"}}))
        env = {**os.environ, "CODEX_HOME": str(root)}
        async with websockets.unix_serve(rpc, str(socket)):
            with (root / "proxy.log").open("w+") as log:
                process, base = None, None

                async def start():
                    nonlocal process, base
                    info.unlink(missing_ok=True)
                    process = subprocess.Popen(
                        [
                            args.proxy,
                            "--queue",
                            "--queue-state",
                            str(root / "queue.json"),
                            "--worker-api-key",
                            "secret",
                            "--session-pool-size",
                            "2",
                            "--queue-conversation-gap-ms",
                            "0",
                            "--queue-user-gap-ms",
                            "0",
                            "--queue-tool-gap-ms",
                            "0",
                            "--queue-start-gap-ms",
                            "0",
                            "--app-server-socket",
                            str(socket),
                            "--server-info",
                            str(info),
                        ],
                        env=env,
                        stdout=log,
                        stderr=log,
                    )
                    await until(lambda: info.exists() or process.poll() is not None)
                    assert info.exists(), "proxy failed to start"
                    base = f"http://127.0.0.1:{json.loads(info.read_text())['port']}"

                async def stop():
                    if process and process.poll() is None:
                        process.terminate()
                        await asyncio.to_thread(process.wait, 10)
                    loaded.clear()

                def http(path, body=None, principal="caller-a", early=False):
                    request = urllib.request.Request(
                        base + path,
                        data=None if body is None else json.dumps(body).encode(),
                        headers={
                            "Authorization": "Bearer secret",
                            "Content-Type": "application/json",
                            "x-codex-queue-principal": principal,
                        },
                    )
                    try:
                        with urllib.request.urlopen(request, timeout=30) as response:
                            if early:
                                chunks = []
                                for line in response:
                                    chunks.append(line)
                                    if b'"call_id": "call-early"' in line:
                                        loop.call_soon_threadsafe(delivered.set)
                                return response.status, b"".join(chunks)
                            return response.status, response.read()
                    except urllib.error.HTTPError as error:
                        return error.code, error.read()

                async def send(tag, *, principal="caller-a", early=False, **extra):
                    body = {
                        "model": "mock",
                        "test_tag": tag,
                        "input": [{"role": "user", "content": tag}],
                        **extra,
                    }
                    return await asyncio.to_thread(
                        http, "/v1/responses", body, principal, early
                    )

                async def rejection(tag, code, **extra):
                    status, body = await send(tag, **extra)
                    assert (status, json.loads(body)["error"]["type"]) == (409, code), (
                        tag,
                        status,
                        body,
                    )
                    assert tag not in calls

                try:
                    await start()
                    status, body = await send("first")
                    assert (status, json.loads(body)["output"]) == (
                        200,
                        outputs["first"],
                    )
                    status, wire = await send("other", stream=True)
                    assert (status, wire) == (200, wires["other"])
                    assert calls["first"] != calls["other"]
                    history = outputs["first"] + [
                        {
                            "type": "function_call_output",
                            "call_id": "call-first",
                            "output": "ok",
                        }
                    ]
                    assert (await send("tool-next", input=history))[0] == 200
                    for tag, extra in [
                        ("previous", {"previous_response_id": "resp-first"}),
                        (
                            "item",
                            {"input": [{"type": "item_reference", "id": "item-first"}]},
                        ),
                        (
                            "encrypted",
                            {
                                "input": [
                                    {
                                        "type": "reasoning",
                                        "encrypted_content": "cipher-first",
                                    }
                                ]
                            },
                        ),
                    ]:
                        assert (await send(tag, **extra))[0] == 200
                        assert calls[tag] == calls["first"]
                    assert calls["tool-next"] == calls["first"]
                    await rejection(
                        "foreign",
                        "conversation_binding_lost",
                        principal="caller-b",
                        input=history,
                    )
                    await rejection(
                        "unknown",
                        "conversation_binding_lost",
                        previous_response_id="missing",
                    )
                    await rejection(
                        "mixed",
                        "continuation_conflict",
                        previous_response_id="resp-other",
                        input=history,
                    )
                    for tag in ("collision-a", "collision-b"):
                        assert (await send(tag))[0] == 200
                    await rejection(
                        "collision",
                        "continuation_conflict",
                        input=[{"type": "item_reference", "id": "ambiguous-item"}],
                    )
                    explicit = {"client_metadata": {"session_id": "explicit"}}
                    assert (await send("explicit", **explicit))[0] == 200
                    assert (await send("explicit-next", input=history, **explicit))[
                        0
                    ] == 200
                    assert calls["explicit"] == calls["explicit-next"] != calls["first"]
                    assert (await send("large"))[0] == 200
                    assert (await send("large-next", input=outputs["large"]))[0] == 200
                    assert calls["large-next"] == calls["large"]
                    streaming = asyncio.create_task(
                        send("early", stream=True, early=True)
                    )
                    await asyncio.wait_for(delivered.wait(), 10)
                    following = asyncio.create_task(
                        send(
                            "early-next",
                            input=outputs["early"]
                            + [
                                {
                                    "type": "function_call_output",
                                    "call_id": "call-early",
                                    "output": "ok",
                                }
                            ],
                        )
                    )
                    for _ in range(100):
                        _, status = await asyncio.to_thread(
                            http, "/internal/queue/status"
                        )
                        if json.loads(status)["pending"] == 1:
                            break
                        await asyncio.sleep(0.02)
                    else:
                        raise AssertionError(
                            "continuation did not enter original conversation queue"
                        )
                    assert "early-next" not in calls
                    gate.set()
                    assert (await streaming) == (200, wires["early"])
                    assert (await following)[0] == 200
                    assert calls["early-next"] == calls["early"]
                    await stop()
                    await start()
                    assert (await send("restart", input=history))[0] == 200
                    assert calls["restart"] == calls["first"]
                    await stop()
                    auth.write_text(json.dumps({"tokens": {"account_id": "account-b"}}))
                    await start()
                    await rejection(
                        "account-change", "conversation_binding_lost", input=history
                    )
                    assert (await send("unfinished"))[0] == 502
                    status, body = await send(
                        "unfinished-next", input=outputs["unfinished"]
                    )
                    assert (status, json.loads(body)["error"]["type"]) == (
                        503,
                        "worker_outcome_unknown",
                    )
                    assert "unfinished-next" not in calls
                    await stop()
                    await start()
                    assert (
                        await send("unfinished-restart", input=outputs["unfinished"])
                    )[0] == 503
                    assert "unfinished-restart" not in calls
                    print(
                        json.dumps(
                            {
                                "result": "passed",
                                "upstream_calls": len(calls),
                                "checks": [
                                    "json",
                                    "exact_sse",
                                    "tool_continuation",
                                    "response_id",
                                    "item_id",
                                    "encrypted_content",
                                    "caller_isolation",
                                    "account_isolation",
                                    "conflicts",
                                    "explicit_priority",
                                    "large_event",
                                    "early_queue",
                                    "restart",
                                    "unknown_outcome_restart",
                                ],
                            }
                        )
                    )
                except BaseException:
                    log.seek(0)
                    print(log.read())
                    raise
                finally:
                    gate.set()
                    await stop()


if __name__ == "__main__":
    asyncio.run(main())
