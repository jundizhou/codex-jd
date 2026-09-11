"""Process-level pool validation. Requires the Python websockets package."""

import argparse
import asyncio
import concurrent.futures
import contextlib
import http.server
import json
import os
import pathlib
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request
import uuid

import websockets


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--proxy", required=True)
    parser.add_argument("--app-server")
    args = parser.parse_args()
    identities = []
    active = set()
    requests = []
    generated_call_ids = set()
    user_agents = set()
    failures = []
    lock = threading.Lock()
    five_entered = threading.Event()
    release = threading.Event()
    catalog_failure = threading.Event()
    catalog = [
        {"id": "picker-visible", "model": "visible", "hidden": False},
        {"id": "picker-hidden", "model": "hidden", "hidden": True},
    ]

    class Upstream(http.server.BaseHTTPRequestHandler):
        def do_POST(self):
            try:
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                if body.get("unknown", {}).get("smoke_disconnect"):
                    self.close_connection = True
                    return
                metadata = body["client_metadata"]
                nested = json.loads(metadata["x-codex-turn-metadata"])
                thread = metadata["thread_id"]
                assert thread != "caller-thread"
                assert self.headers["thread-id"] == thread == nested["thread_id"]
                assert self.headers["session-id"] == metadata["session_id"]
                assert nested["installation_id"] == metadata["x-codex-installation-id"]
                assert "root_turn_id" not in nested
                assert "parent_turn_id" not in nested
                assert body["input"] == "unchanged"
                expected_headers = body.get("unknown", {}).get("expected_headers", {})
                for name in ("x-codex-turn-state", "traceparent", "tracestate"):
                    assert self.headers.get(name) == expected_headers.get(name)
                call_id = self.headers.get("x-codex-inference-call-id")
                if "x-codex-inference-call-id" in expected_headers:
                    assert call_id == expected_headers["x-codex-inference-call-id"]
                elif args.app_server:
                    assert uuid.UUID(call_id).version == 4
                    with lock:
                        assert call_id not in generated_call_ids
                        generated_call_ids.add(call_id)
                if args.app_server:
                    window = self.headers["x-codex-window-id"]
                    assert window != "caller-window"
                    assert (
                        metadata["window_id"] == metadata["context_window_id"] == window
                    )
                    assert nested["window_id"] == nested["context_window_id"] == window
                    assert self.headers["User-Agent"] != "caller-agent"
                    with lock:
                        user_agents.add(self.headers["User-Agent"])
                        assert len(user_agents) == 1
                with lock:
                    assert thread not in active, "thread leased concurrently"
                    active.add(thread)
                    requests.append(thread)
                    if len(active) == 5:
                        five_entered.set()
                payload = b'data: {"type":"response.completed"}\n\n'
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Content-Length", str(len(payload)))
                if "x-codex-turn-state" in expected_headers:
                    self.send_header("x-codex-turn-state", "upstream-next-token")
                self.send_header("Set-Cookie", "must-not-leak")
                self.end_headers()
                self.wfile.flush()
                assert release.wait(20), "upstream release timed out"
                with lock:
                    active.remove(thread)
                self.wfile.write(payload)
            except Exception as error:
                failures.append(repr(error))
                self.close_connection = True

        def log_message(self, *_args):
            pass

    def call_upstream(body, headers):
        metadata = body["client_metadata"]
        request = urllib.request.Request(
            f"http://127.0.0.1:{upstream.server_port}/v1/responses",
            data=json.dumps(body).encode(),
            headers={
                "Content-Type": "application/json",
                "thread-id": metadata["thread_id"],
                "session-id": metadata["session_id"],
                **headers,
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return (
                    response.status,
                    response.read().decode(),
                    {
                        name: value
                        for name, value in response.headers.items()
                        if name.lower() == "x-codex-turn-state"
                    },
                )
        except urllib.error.HTTPError as error:
            return error.code, error.read().decode(), {}

    async def rpc(websocket):
        async for message in websocket:
            request = json.loads(message)
            method = request["method"]
            if method == "initialized":
                continue
            if method == "initialize":
                result = {"userAgent": "pool-smoke"}
            elif method == "thread/start":
                thread = f"host-thread-{len(identities)}"
                identities.append(
                    {
                        "threadId": thread,
                        "sessionId": thread,
                        "installationId": "host-installation",
                        "windowId": f"{thread}:0",
                        "turnId": None,
                        "rootTurnId": None,
                        "parentTurnId": None,
                        "parentThreadId": None,
                    }
                )
                result = {"thread": {"id": thread}}
            elif method == "thread/modelIdentity/list":
                result = {"data": identities, "nextCursor": None}
            elif method == "model/list":
                assert request["params"]["includeHidden"] is True
                if catalog_failure.is_set():
                    await websocket.send(
                        json.dumps(
                            {
                                "id": request["id"],
                                "error": {
                                    "code": -32603,
                                    "message": "catalog unavailable",
                                },
                            }
                        )
                    )
                    continue
                offset = int(request["params"].get("cursor") or 0)
                result = {
                    "data": catalog[offset : offset + 1],
                    "nextCursor": str(offset + 1)
                    if offset + 1 < len(catalog)
                    else None,
                }
            elif method == "turn/start":
                status, body, headers = await asyncio.to_thread(
                    call_upstream,
                    request["params"]["rawResponses"],
                    request["params"]["rawResponsesHeaders"],
                )
                result = {
                    "turn": {
                        "id": "smoke-turn",
                        "items": [],
                        "itemsView": "notLoaded",
                        "status": "completed",
                        "error": None,
                        "startedAt": None,
                        "completedAt": None,
                        "durationMs": None,
                    },
                    "rawResponseBody": body,
                    "rawResponseStatus": status,
                    "rawResponseHeaders": headers,
                }
            else:
                raise AssertionError(method)
            await websocket.send(json.dumps({"id": request["id"], "result": result}))

    with tempfile.TemporaryDirectory(prefix="proxy-pool-") as directory:
        root = pathlib.Path(directory)
        socket = root / "control.sock"
        info = root / "proxy.json"
        upstream = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Upstream)
        upstream_thread = threading.Thread(target=upstream.serve_forever, daemon=True)
        upstream_thread.start()
        app = None
        loop = None
        mock_thread = None
        with contextlib.ExitStack() as stack:
            if args.app_server:
                (root / "config.toml").write_text(
                    'model = "mock-model"\n'
                    'model_provider = "mock_provider"\n'
                    'approval_policy = "never"\n'
                    'sandbox_mode = "read-only"\n'
                    "[model_providers.mock_provider]\n"
                    'name = "Local smoke upstream"\n'
                    f'base_url = "http://127.0.0.1:{upstream.server_port}/v1"\n'
                    'wire_api = "responses"\n'
                    "supports_websockets = false\n"
                    "request_max_retries = 0\n"
                )
                app_log = stack.enter_context((root / "app.log").open("w+"))
                app = subprocess.Popen(
                    [args.app_server, "--listen", f"unix://{socket}"],
                    env={**os.environ, "CODEX_HOME": str(root)},
                    stdout=app_log,
                    stderr=app_log,
                )
            else:
                loop = asyncio.new_event_loop()

                async def serve():
                    return await websockets.unix_serve(rpc, str(socket))

                mock_server = loop.run_until_complete(serve())
                mock_thread = threading.Thread(target=loop.run_forever, daemon=True)
                mock_thread.start()
            proxy = None
            try:
                deadline = time.monotonic() + 60
                while not socket.exists():
                    if app and app.poll() is not None:
                        app_log.seek(0)
                        raise AssertionError(app_log.read())
                    assert time.monotonic() < deadline, "app-server startup timeout"
                    time.sleep(0.1)
                proxy_log = stack.enter_context((root / "proxy.log").open("w+"))
                proxy = subprocess.Popen(
                    [
                        args.proxy,
                        "--app-server-socket",
                        str(socket),
                        "--server-info",
                        str(info),
                    ],
                    stdout=proxy_log,
                    stderr=proxy_log,
                )
                deadline = time.monotonic() + 180
                while not info.exists():
                    if proxy.poll() is not None:
                        proxy_log.seek(0)
                        raise AssertionError(proxy_log.read())
                    assert time.monotonic() < deadline, "proxy startup timeout"
                    time.sleep(0.1)
                port = json.loads(info.read_text())["port"]
                url = f"http://127.0.0.1:{port}/v1/responses"

                def get_model(path="/v1/models"):
                    try:
                        with urllib.request.urlopen(
                            f"http://127.0.0.1:{port}{path}", timeout=5
                        ) as response:
                            assert (
                                response.headers.get_content_type()
                                == "application/json"
                            )
                            return response.status, json.load(response)
                    except urllib.error.HTTPError as error:
                        return error.code, json.load(error)

                if not args.app_server:
                    expected_models = [
                        {
                            "id": name,
                            "object": "model",
                            "created": 0,
                            "owned_by": "codex",
                            "shutdown_date": None,
                        }
                        for name in ("visible", "hidden")
                    ]
                    assert get_model() == (
                        200,
                        {"object": "list", "data": expected_models},
                    )
                    assert get_model("/v1/models/hidden") == (200, expected_models[1])
                    status, error = get_model("/v1/models/missing")
                    assert status == 404 and error["error"]["code"] == "model_not_found"
                    catalog_failure.set()
                    status, error = get_model()
                    assert (
                        status == 502
                        and error["error"]["code"] == "model_catalog_unavailable"
                    )
                    catalog_failure.clear()
                    assert get_model()[0] == 200
                    catalog.clear()
                    assert get_model() == (200, {"object": "list", "data": []})
                    catalog.extend(
                        [
                            {"model": "visible", "hidden": False},
                            {"model": "hidden", "hidden": True},
                        ]
                    )
                payload = json.dumps(
                    {
                        "model": "mock-model",
                        "stream": True,
                        "input": "unchanged",
                        "client_metadata": {
                            "thread_id": "caller-thread",
                            "window_id": "caller-window",
                            "context_window_id": "caller-window",
                            "root_turn_id": "old-root",
                            "parent_turn_id": "old-parent",
                            "x-codex-turn-metadata": json.dumps(
                                {
                                    "thread_id": "caller-thread",
                                    "window_id": "caller-window",
                                    "context_window_id": "caller-window",
                                    "root_turn_id": "old-root",
                                    "parent_turn_id": "old-parent",
                                }
                            ),
                        },
                    }
                ).encode()

                def send(data=payload, disconnect=False, headers=None):
                    request_data = data
                    if headers:
                        body = json.loads(data)
                        body.setdefault("unknown", {})["expected_headers"] = headers
                        request_data = json.dumps(body).encode()
                    if disconnect:
                        body = json.loads(data)
                        body.setdefault("unknown", {})["smoke_disconnect"] = True
                        request_data = json.dumps(body).encode()
                    request = urllib.request.Request(
                        url,
                        data=request_data,
                        headers={
                            "Content-Type": "application/json",
                            "thread-id": "caller-thread",
                            "User-Agent": "caller-agent",
                            **(headers or {}),
                        },
                    )
                    try:
                        with urllib.request.urlopen(request, timeout=30) as response:
                            assert response.headers.get("Set-Cookie") is None
                            assert response.headers.get("x-codex-turn-state") == (
                                "upstream-next-token"
                                if headers and "x-codex-turn-state" in headers
                                else None
                            )
                            return response.status, response.read()
                    except urllib.error.HTTPError as error:
                        return error.code, error.read()

                with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
                    futures = [executor.submit(send) for _ in range(6)]
                    assert five_entered.wait(15), (
                        f"five requests did not reach upstream: {failures}"
                    )
                    time.sleep(0.2)
                    with lock:
                        assert len(requests) == 5
                        assert len(set(requests)) == 5
                    if not args.app_server:
                        assert get_model() == (
                            200,
                            {"object": "list", "data": expected_models},
                        ), (
                            "catalog must remain available while all five sessions are leased"
                        )
                    release.set()
                    assert all(future.result()[0] == 200 for future in futures)
                assert send(b"invalid-json")[0] == 400
                assert send()[0] == 200, "slot was not released after malformed input"
                assert send(disconnect=True)[0] == 502
                assert send()[0] == 200, (
                    "slot was not released after upstream disconnect"
                )
                assert (
                    send(
                        headers={
                            "x-codex-turn-state": "caller-token",
                            "x-codex-inference-call-id": "caller-call-id",
                            "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
                            "tracestate": "vendor=value",
                        }
                    )[0]
                    == 200
                )
                for _ in range(5):
                    assert send()[0] == 200, (
                        "routing state leaked when reusing a pool slot"
                    )
                assert not failures, failures
                if not args.app_server:
                    assert len(identities) == 5
                    identities.clear()
                    assert send()[0] == 503, (
                        "missing identity must not use stale snapshot"
                    )
                print(
                    "PASS: five leases, sixth waits, SSE, error releases, identity rewrite, dynamic models, routing/trace headers and isolation"
                )
            finally:
                release.set()
                for process in (proxy, app):
                    if process and process.poll() is None:
                        process.terminate()
                        try:
                            process.wait(timeout=10)
                        except subprocess.TimeoutExpired:
                            process.kill()
                            process.wait()
                if loop:
                    loop.call_soon_threadsafe(mock_server.close)
                    asyncio.run_coroutine_threadsafe(
                        mock_server.wait_closed(), loop
                    ).result(10)
                    loop.call_soon_threadsafe(loop.stop)
                    mock_thread.join(10)
                    loop.close()
                upstream.shutdown()
                upstream.server_close()
                upstream_thread.join(10)


if __name__ == "__main__":
    main()
