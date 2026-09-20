# Portable Codex Responses Service

This package contains the patched Codex CLI, app-server and Responses proxy,
plus their Linux runtime and an authenticated Nginx entry point. No source
machine credentials, configuration, API token or conversation history is bundled.

## Target machine

Install Docker Engine/Desktop with Docker Compose v2 and start Docker first.
This image is Linux x86_64; ARM hosts require amd64 emulation. Windows users
can run the commands in WSL with Docker integration enabled.
The machine must be able to reach Codex login and model services.

Extract `codex-migration-2026-09-18-admin-ui-linux-amd64.tar.gz`, enter `codex-migration`, then run:

```sh
./migrate.sh
```

The script imports the image when missing and starts the service without
requiring a Codex account. Open `/admin/accounts`, enter the Worker Token,
then add and activate an account before making model requests. You can also
run `./migrate.sh login` for device-code login if enabled for your
account/workspace. Login is inside this deployment's data volume, not
automatically shared with an existing desktop login.

Base URL: `http://127.0.0.1:18876/v1`.
Get the newly generated Bearer API token with `./migrate.sh token`.
The API supports `/responses`, `/models`, and `/models/{id}`.

For optional settings, copy `.env.example` to `.env` before starting.
An outbound proxy address must be reachable inside Docker: host loopback
`127.0.0.1` refers to the container, not the host. Host proxies must accept
connections from Docker. No proxy credentials are bundled.

The default binding is local-only. For remote clients, use a TLS reverse proxy
or VPN. Setting `BIND_ADDRESS=0.0.0.0` exposes unencrypted HTTP; do not send
credentials or prompts over an untrusted network this way.

### Local Sub2API to a remote Codex host

For development, keep the server bound to loopback and create an SSH tunnel
from the machine running Sub2API:

```sh
ssh -N -L 18876:127.0.0.1:18876 user@codex-server
```

With the tunnel running, the local endpoint is
`http://127.0.0.1:18876/v1`. Configure a Sub2API OpenAI-compatible account with
that Base URL and the token printed by `./migrate.sh token` on the server. Check
the connection before sending a model request:

```sh
curl -fsS http://127.0.0.1:18876/healthz
curl -fsS http://127.0.0.1:18876/v1/models \
  -H "Authorization: Bearer $TOKEN"
```

If Sub2API runs in Docker on the local machine, `127.0.0.1` points to the
Sub2API container. Use `http://host.docker.internal:18876/v1` instead and add
this mapping to the Sub2API Compose service on Linux:

```yaml
extra_hosts:
  - "host.docker.internal:host-gateway"
```

The tunnel carries the request to the server's Codex proxy without exposing
the proxy port publicly. Close the SSH process when the test is complete.

## Operations

```sh
./migrate.sh status
./migrate.sh logs
./migrate.sh stop
./migrate.sh restart
./migrate.sh login
./migrate.sh token
./migrate.sh switch-account account-b
```

Login state and API token persist in the `codex-migration_codex-data` Docker
volume. Do not delete that volume unless intentionally removing credentials.
Health checks verify local process readiness, not model quota or inference
access. Model listing does not guarantee that every listed model is callable.

Open `http://127.0.0.1:18876/admin/accounts` and enter the Worker Token.
The page supports auth.json import, browser login links with pasted callbacks,
account deletion, active account selection, and live concurrency changes.
Profiles are stored in `/data/accounts` inside the persistent Docker volume.
Adding a profile does not activate it; select it explicitly in the page.
Switching requires an idle queue, then app-server reloads its authentication
without a restart. Concurrency changes take effect immediately and persist.
The default selectable capacity is 32; changing `CODEX_SESSION_POOL_SIZE` or
other container environment settings requires `./migrate.sh start` to recreate.

To enable automatic switching after a confirmed quota exhaustion, also set
`CODEX_WORKER_AUTO_SWITCH=1`. The worker selects the first profile (sorted by
name) different from the active profile. Leave this disabled if account
selection requires manual approval.

### Conversation queue

The `2026-09-18-admin-ui-bundle1` image enables the queue and administration
page by default. Keep `CODEX_WORKER_QUEUE=1` to use the administration page. Defaults allow two running requests, one per conversation, with a
300 ms account start interval. Verified new user turns wait 1500 ms after the
previous response finishes; verified tool continuations wait 300 ms and
unclassified continuations wait 800 ms. Other settings are in `.env.example`.

Enable the Sub2API account extras `strict_raw_forward=true` and
`worker_queue_enabled=true` after the Worker is ready, and set account ingress
concurrency to 26. The gateway reserves one additional transport connection
for cancellation. Existing Worker and gateway API keys remain unchanged.

Inspect the authenticated queue without printing its token:

```sh
docker compose exec -T api curl --noproxy '*' --silent --show-error \
  --config /tmp/worker-queue-curl.conf http://127.0.0.1:8787/internal/queue/status
```

`/healthz` checks liveness; authenticated `/readyz` also checks queue readiness.
The journal at `/data/queue-state.json` must persist across restarts. An unknown
upstream outcome keeps one execution slot occupied and blocks that conversation;
other conversations can use the remaining capacity. The reservation survives a
restart. Only recover it after independently confirming the upstream has stopped;
do not delete the journal to bypass this state. Healthy conversation identities
resume after restart; identities without proof of local request termination stay invalid. Completed request IDs
remain protected against replay for ten minutes.

Set `CODEX_WORKER_QUEUE_AUTO_RECOVER=1` to enable availability recovery for
ChatGPT accounts. Checks wait 10/20/30 seconds in a repeating cycle; one successful
account/identity check releases a reservation only when a terminal app-server RPC
was observed. Recovery resumes and verifies the original thread identity instead
of discarding its binding. Termination proof persists across proxy restarts. New
requests can wait within the 120-second queue budget, retaining their original
routing header and tool dependencies. A lost control connection without a terminal
RPC remains quarantined; a healthy account alone is not sufficient to recover it.
Recovery never replays an unknown request or clears account limits. The raw API
retries connection-establishment failures at most twice, with 250/500 ms backoff;
ambiguous network failures and partially returned responses are not replayed.
Old remote computation may still exist, so actual upstream concurrency can exceed
two. See the proxy README for persistence and status fields.

## Build on source machine

Place the three matching patched Linux x86_64 executables in `bin/`, then run
`bash build.sh`. The output is `dist/codex-migration-2026-09-18-admin-ui-linux-amd64.tar.gz`.
Only explicitly allowed binaries and entrypoint are sent to the Docker build;
the export copies a fixed allowlist and never includes `.env` or data volumes.
The image is based on Ubuntu 24.04. Building requires network access to the
base image and Ubuntu package repositories; importing the bundle does not.
