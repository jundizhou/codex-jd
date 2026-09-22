# codex-responses-api-proxy

`codex-responses-api-proxy` exposes a local OpenAI-compatible `POST
/v1/responses` endpoint and sends requests through the local Codex app-server.
The app-server owns Codex authentication, provider selection, OAuth refresh, and
the model transport. The proxy does not read an API key from stdin and does not
call `api.openai.com` directly.

## Start

The app-server must already be running with the desired Codex login and
`CODEX_HOME`:

```shell
codex app-server --listen unix:///tmp/codex-app-server.sock
codex-responses-api-proxy \
  --port 8787 \
  --app-server-socket /tmp/codex-app-server.sock
```

For a worker reached by Sub2API on another machine, bind the proxy to the
worker's private interface and configure a shared secret:

```shell
codex-responses-api-proxy \
  --listen-address 0.0.0.0 \
  --port 8787 \
  --app-server-socket /run/codex/account-a.sock \
  --worker-api-key worker-secret-a
```

The same secret must be sent as `Authorization: Bearer worker-secret-a` by
Sub2API. Without `--worker-api-key` (or `CODEX_WORKER_API_KEY`), authentication
remains disabled for backwards-compatible local development.

If `--app-server-socket` is omitted, the proxy uses
`$CODEX_HOME/app-server-control/app-server-control.sock`.

The endpoint is then:

```shell
curl http://127.0.0.1:8787/v1/responses \
  -H 'Content-Type: application/json' \
  -d '{"model":"gpt-5","input":"hello"}'
```

`GET /healthz` returns `200 OK` with `ok` and is suitable for load-balancer
health checks. It does not require authentication.

The request must be valid for the Codex Responses backend. The proxy does not
turn a public OpenAI request into a Codex prompt or synthesize missing model
fields.

## Session Pool

At startup the proxy connects to app-server, creates five ephemeral threads,
and reads their generated identities through `thread/modelIdentity/list`.
Each incoming model request leases one idle thread. When the request contains a
stable client session or conversation identifier in its metadata, subsequent
requests with the same identifier are pinned to the same pool thread. The lease
is held until the model response has been collected and returned; a sixth
concurrent request waits for an available lease. The pool still contains five
threads and retains its five-request concurrency limit. A lost app-server
control connection is not replaced with stale identity data; restart the proxy
to recreate the pool. If more than five different client keys compete for the
pool, an idle slot may be rebound to a newer key; the pool capacity remains
five.

## Opt-in conversation queue

Enable `--queue` and a Worker API key. The default remains off. Queue mode
loads up to 32 conversation threads, with two running requests and a 300 ms account start
interval. Scheduling is tenant/conversation round-robin with one active request
per conversation. Tenants without an active request get the next available
opportunity. Within a tenant, verified tool continuations receive at most two
consecutive preferred starts, and imminent queue deadlines take precedence.
There are no automatic model retries or synthetic SSE events.

| Setting | Default | Meaning |
| --- | --- | --- |
| `--queue-max-running` | 2 | Manual upper bound; feedback may reduce it |
| `--queue-start-gap-ms` | 300 | Minimum interval between RPC dispatch attempts |
| `--queue-user-gap-ms` | 1500 | Verified newly appended user input |
| `--queue-tool-gap-ms` | 300 | New outputs matching preceding observed call IDs |
| `--queue-conversation-gap-ms` | 800 | Unclassified conversation continuation |
| `--queue-idle-ttl-secs` | 900 | Unload idle threads, retaining durable identities |
| `--session-pool-size` | 32 | Loaded thread limit, up to 256 in queue mode |
| `--queue-state FILE` | `$CODEX_HOME/responses-queue.json` | Exclusive durable dispatch/account journal |

Conversation intervals start at confirmed upstream completion. Time spent
executing client tools counts toward the gap. The bounded observer matches
history-prefix digests or a known previous response ID, then validates new tool
outputs. An old tool output somewhere in the history is insufficient. It keeps
at most 64 call IDs for five minutes, with a 256 KiB observation limit per event;
missing, oversized or malformed evidence falls back to the unknown interval.
Forwarded request content and response octets are not changed by observation.
A fully delimited named SSE terminal event can confirm completion even if its
JSON data exceeds the observation budget. Oversized non-streaming JSON or
data-only SSE without an observable terminal remains an unknown outcome.

Admission caps are 24 waiting requests, two per conversation, eight per tenant,
32 MiB per request, 64 MiB of queued bodies and 128 MiB of admitted raw bodies.
There are at most 26 admitted handlers with the default concurrency. Unknown
body lengths reserve 32 MiB. These raw-byte budgets do not bound total RSS.
Deploy behind Nginx for upload/write timeouts; direct tiny_http socket uploads
and downstream writes still do not have new absolute socket deadlines.

The default and maximum queue budget is 120 seconds including Worker
upload/preparation. Sub2API supplies the remaining budget after its Responses handler's earlier waits and
reserves 250 ms of transit headroom. This bounds dispatch eligibility, not the
upstream first-token or complete-response duration. Long transit delays beyond
the reserved headroom and middleware time before the handler remain outside
this shared budget. Existing user admission limits still apply.

Only trusted gateways may hold the Worker key. Sub2API strips client
`X-Codex-Queue-*` headers, supplies an authenticated principal and uses a stable
`Idempotency-Key` or inference-call ID when present, otherwise a random ID.
A stable ID is scoped to the principal and bound to the exact body, conversation
and routing token digest. Concurrent repeats return 409; changed bodies return
`idempotency_conflict`; completed repeats return `request_already_completed`.
Responses are not cached/replayed. Missing stable client IDs cannot protect
against client retries that obtain a new random ID.

Conversation keys retain the existing metadata affinity extraction, scoped to
the trusted principal. Requests with no stable conversation metadata are treated
independently; their real conversation order or interval cannot be guaranteed.
Direct callers without gateway metadata share the `direct` principal.

The journal holds up to 4096 dispatch records, retaining completed
ones for ten minutes. A dispatch record is flushed before sending the model RPC.
The file lock permits one local owner. A corrupt/unwritable journal fails closed;
an unfinished record after restart reserves capacity and isolates its conversation.
The journal stores hashes and states, never prompts or SSE. Preserve it across
restarts. This is bounded deduplication and conservative crash recovery, not an
exactly-once guarantee or distributed ownership.

Logical identities live in a separate atomic `*.conversations.json` index beside
this journal. Preserve both files and the app-server's CODEX_HOME. Identity threads
use durable legacy rollouts, explicitly materialized before dispatch. Idle expiry
unloads completed threads; subsequent requests resume the same persisted identity.
A normal Worker restart restores identities on demand. Queue permits, the bounded
timing-evidence cache, loaded threads and durable identities have separate limits.

The index retains up to 4096 identities for 30 days of inactivity. Retention/capacity
cleanup deletes only idle threads owned by this index. It stores no request bodies
or routing tokens. Authentication must be file-backed; account changes require a
drained Worker restart and cannot reuse another account's mapping. Missing mappings
may be rebuilt only for self-contained input. Requests using previous response IDs,
conversation references, routing state, item references or dangling tool outputs
still receive `conversation_binding_lost` when their identity is missing. Original
input and tool outputs are never modified to make a continuation appear complete.
Local identity-control failures return 503 `worker_identity_unavailable`.

Unknown outcomes each retain one execution slot and block their own conversation,
including after restart. Other conversations may use the remaining capacity.
`running` includes these `quarantined` slots; `worker_fault` identifies failures
that still block the whole Worker (such as an unreadable dispatch journal).
Their identities are invalidated
before operator recovery can finish; no original model request is replayed. In-process
unknown threads stay pinned until a Worker restart, so use a new conversation after
manual recovery. Deleting state files is not a supported recovery procedure.

A real 429 (including an observed SSE rate-limit failure) cools the account and
reduces dispatch concurrency to one. Valid Retry-After delta/date values are
honored without shortening, with at most 250 ms additional jitter. Missing hints
use 5/10/20/40/80/120-second backoff. Three consecutive 5xx responses also enter
cooldown; 401/402 or an observed quota-exhaustion error makes the account
unavailable pending operator recovery, while a request-specific 403 does
not disable the whole account. Recovery adds one slot only after 20 successful
terminal responses spanning at least 60 seconds. Cooldown and account state are
journaled. Requests whose queue deadline cannot survive cooldown get a local
429 plus Retry-After; the original rejected upstream call is never replayed.

Authenticated controls:

- `GET /admin/accounts`: the account management page. The HTML shell carries no
  account data; it prompts for the worker token and calls the JSON APIs below.
- `GET /admin/api/accounts`: saved account profiles (name, masked login
  identity, active marker, validity), the current account's masked identity,
  queue status and the concurrency capacity. The login identity is the
  id_token email when present, otherwise the account identifier; it is always
  masked (for example `j***@example.com` or `account-***0f1e`), and no endpoint
  ever returns credentials.
- `POST /admin/api/add` with `{"profile": name, "auth": <auth.json object>}`:
  stores an already-logged-in account under `CODEX_ACCOUNT_PROFILES_DIR`
  (default `<codex-home>/accounts`). Profiles are deduplicated by account
  identity, capped at 64, and written with 0600 permissions.
- `POST /admin/api/login-start` with `{"profile": name}`: returns the ChatGPT
  OAuth authorize URL (PKCE, redirecting to the standard
  `http://localhost:1455/auth/callback`). No state is created on the issuer
  and no local port is bound. Starting a new login supersedes a previous
  unfinished one.
- `POST /admin/api/login-callback` with `{"profile": name, "url": callback}`:
  validates the pasted callback URL's state, exchanges the single-use
  authorization code for tokens, and stores the credentials as the named
  profile (same validation, dedup and limits as file upload). The response
  blocks for one token-exchange round trip; a failed exchange ends the
  session and requires a fresh link because the code has been consumed.
- `GET /admin/api/login-status`: the pending login's profile and authorize
  URL, so the page can restore an in-progress flow after a refresh.
- `POST /admin/api/switch` with `{"profile": name}`: makes a stored profile the
  active account. The current login is saved back to its profile (or auto-backed
  up on its first switch) so refreshed tokens are not lost. Switching is refused
  while any request is running, queued or outcome-unknown.
- `POST /admin/api/delete` with `{"profile": name}`: removes a stored profile.
  The active account cannot be deleted; switch away first.
- `POST /admin/api/concurrency` with `{"limit": n}`: adjusts the maximum
  running requests between 1 and the session capacity. It takes effect
  immediately for new dispatches, survives restarts via
  `<codex-home>/admin-concurrency.json`, and never interrupts running streams.
- `GET /internal/queue/status`: pending/running counts, effective concurrency,
  body reservations, cached conversation timing entries, cooldown and fault state.
- `GET /readyz`: 503 while paused, cooling or unavailable; `/healthz` is liveness.
- `POST /internal/queue/cancel/{id}` with `X-Codex-Queue-Principal`: prevents
  dispatch if it wins the race, otherwise requests finalization within the original
  3600-second execution deadline.
  Acknowledgement means cancellation requested, not confirmed upstream shutdown.
- `POST /internal/queue/pause`: rejects pending/new dispatch; running calls finish.
- `POST /internal/queue/resume`: resumes a healthy account without clearing cooldown.
- `POST /internal/queue/recover` with `X-Codex-Queue-Confirm-Stopped: true`: only
  while paused with no active HTTP handlers. Use after independently confirming
  the upstream has stopped (and fixing authentication if relevant). It marks
  unknown records terminal without replaying them; affected identities remain invalid.

On downstream loss, the Worker continues observing within the original
3600-second raw RPC execution deadline. A model terminal event confirms completion
even if the trailing local RPC fails. First-response and stream-idle timeouts
use the thread provider's `stream_idle_timeout_ms` (default 300 seconds).
Sub2API also drains to collect real terminal usage within its own 15-second
window; missing/truncated/oversized usage is recorded as `usage_unknown`, never
as a successful zero-token response. An unconfirmed upstream outcome retains
one slot and blocks its conversation, including a 2xx stream ending without a
model terminal event. New conversations are rejected when all slots are quarantined;
`/readyz` stays healthy while there is usable capacity and no account/Worker fault. `background: true` requests are rejected before dispatch because
the queue cannot track computation detached from the HTTP response.
No unverified `turn/interrupt` call is used.
The raw RPC currently cannot prove upstream compute cancellation after timeout;
Manual confirmation/recovery remains the default in that case.

With `--queue-auto-recover` (migration image: `CODEX_WORKER_QUEUE_AUTO_RECOVER=1`),
each isolated conversation is checked after 10 seconds, then after 20, then 30,
repeating 10/20/30 on failure. Intervals start when the preceding check finishes;
deadlines and the cycle position survive restart. A successful check immediately
releases that conversation's reservation with no additional cooldown or success
streak. Checks use the live ChatGPT `account/rateLimits/read` RPC (10-second probe
timeout), unchanged account credentials, available quota, and a writable identity
index/journal. They do not generate model responses. API-key-only authentication
cannot pass this ChatGPT probe. Pauses, account blocks, Retry-After cooldowns and
Worker faults are never cleared by this mechanism.
Undispatched requests may wait within their existing queue budget (at most 120
seconds) for recovery, then dispatch without another client retry. The queue
still enforces its existing memory, pending-count and conversation-order bounds.

Recovery retires the invalid loaded binding; the next self-contained request in
the same client conversation creates a fresh backend identity. Incremental requests
still return `conversation_binding_lost` until the client supplies complete history.
Old requests retain an unknown-outcome tombstone and return 409
`request_outcome_unknown`, including across restart; they are never replayed or
marked completed. Tombstones share the 4096-record hard cap and are not silently
expired; exhausting it requires operator reconciliation. Status includes
`automatic_recovery`, `recovery.next_check_at` (Unix seconds) and
`recovery.released_unknown`. A successful availability check does **not** prove
old upstream computation ended, so actual upstream concurrency can exceed the
configured local limit. Disabling recovery does not re-reserve already released
unknowns; rolling back to older software conservatively quarantines them again.

For the migration image, rebuild **both the Linux app-server and proxy binaries**
then set `CODEX_WORKER_QUEUE=1`. The entrypoint reuses the existing API token,
stores the journal at `/data/queue-state.json`, pauses on shutdown and waits up
to 20 seconds before stopping processes. Other `CODEX_WORKER_QUEUE_*` settings
are listed in `.env.example`. In Sub2API, set account extras
`worker_queue_enabled=true` and `strict_raw_forward=true` only after deployment.
Account ingress concurrency should be 26 for this pilot; the adapter reserves
one more transport connection for cancellation. Shared proxy connection pools
must also leave that headroom (at least 27 connections).

One account must have one scheduler owner. Multiple machines sharing an account,
distributed leases, cost-weighted fairness and a billing/admin UI are separate
scale-out work, not enabled by the local journal lock. Compact and WebSocket
request paths are not covered by this HTTP `/v1/responses` queue.

The process test exercises exact SSE bytes, tool/user intervals, cancellation,
limits, authenticated controls and repeated proxy restarts with the journal:

```shell
python tests/smoke_queue.py --proxy /absolute/path/to/codex-responses-api-proxy
```

## Request Preservation

The proxy sends the complete incoming JSON object through the experimental
app-server `turn/start.rawResponses` path. The raw transport does not rebuild
the request from `input`.

Existing installation/session/thread/window identities are aligned with the
bound server thread. Turn and context-window IDs use stable mappings; parent
thread aliases and optional five-profile workspace metadata persist across
restart. Fields stay in their original headers or metadata objects. Missing
metadata fields are not added and explicit nulls are preserved. Other JSON
content, including input, tools and instructions, is unchanged.

The HTTP proxy and app-server share `codex_http_client::raw_responses_headers`.
Application headers are preserved by default, including unknown headers,
`x-openai-internal-codex-responses-lite` and `x-codex-beta-features`. Incoming
headers are bounded to 128 entries, 256 bytes per name, 8192 bytes per value and
64 KiB overall; duplicate names are rejected case-insensitively.

Caller credentials, account selection, cookies, User-Agent, originator,
session-id/thread-id/x-client-request-id, proxy provenance and internal
`x-codex-queue-*` headers are excluded. Codex's upstream transport supplies its
own credentials and identities. Caller `x-oai-attestation` is discarded because
its signature is bound to the original client; the raw adapter does not generate
a replacement. HTTP framing, compression negotiation and hop-by-hop headers,
including names nominated by Connection, belong to the outgoing transport.
See [the complete field rules](FIELD_HANDLING_SUMMARY.md) for exact exceptions.

Existing inference-call IDs and tracing headers are preserved. A missing
inference-call ID is generated once per request. An upstream `x-codex-turn-state`
is returned for client replay; it is never invented or cached globally or per
pool slot. The response header allowlist remains turn-state, retry-after and
content-type. Server-default headers added after the application capture boundary
may be absent from request-detail snapshots.

## Forwarding Boundary

For each accepted `POST /v1/responses`, the proxy performs one identity rewrite,
one app-server `turn/start.rawResponses` call, and one upstream Responses POST.
Raw forwarding disables transport/status retries and allows the app-server control
wait up to one hour. Streaming requests flush each upstream byte chunk through
the app-server raw stream notifications before the terminal RPC result. Upstream HTTP error statuses
and error bodies are returned instead of converting quota failures into 502s.
Control operations retain their 30-second timeout. Client-side timeouts or
explicit retries remain outside this forwarding boundary.
The proxy does not execute tools, append prompts, rebuild history, or run an
agent loop. A client that is itself an agent may send several independent HTTP
requests during one user turn; each request is forwarded independently.

## CLI

```text
codex-responses-api-proxy [--port <PORT>] [--server-info <FILE>]
  [--listen-address <IP>] [--http-shutdown] [--dump-dir <DIR>]
  [--app-server-socket <PATH>] [--worker-api-key <SECRET>]
```

- `--listen-address`: bind address; defaults to `127.0.0.1`. Use `0.0.0.0`
  only when the worker is protected by a private network or TLS proxy.
- `--port`: TCP port; omitted means an ephemeral port.
- `--server-info`: write `{ "port": <PORT>, "pid": <PID> }` after binding.
- `--http-shutdown`: enable `GET /shutdown` for local process management.
- `--dump-dir`: write redacted request/response dumps for accepted calls.
- `--app-server-socket`: path to the local app-server control socket.
- `--worker-api-key`: require this shared secret in the inbound Bearer token.
  The `CODEX_WORKER_API_KEY` environment variable is also accepted.

Accepted endpoints are `POST /v1/responses`, `GET /v1/models`,
`GET /v1/models/{id}`, and `GET /healthz`. Other paths and methods receive
`403`. When a worker API key is configured, model and response endpoints
require a matching Bearer token; `/healthz` remains unauthenticated.

The model endpoints read all pages of app-server `model/list` with
`includeHidden: true`, including hidden models. Queries use a separate control
connection and do not lease a pooled thread. Each request reads the current
app-server catalog (which may itself be cached); listing does not test model
inference access or quota. No additional proxy cache is used.

The OpenAI-compatible response uses each entry's callable `model` as `id`,
`created: 0` because creation dates are unavailable, and `owned_by: "codex"`
as the catalog source rather than an upstream ownership assertion. Unknown
model IDs return JSON `404`; failed catalog queries return JSON `502` rather
than an invented list. Catalog queries have a 30-second overall timeout.

## Verification

The Rust tests cover the five-session pool, identity rewriting, and deep
request preservation. The app-server integration test captures the actual
outbound model request and compares it with the original request after identity
substitution, and checks routing and tracing headers in both directions.

The process smoke test is:

```shell
python tests/smoke_pool.py --proxy /absolute/path/to/codex-responses-api-proxy
```

Use `--app-server` to exercise real ephemeral thread creation with an isolated
Codex home. The mock-only mode does not validate real OAuth or model access.
