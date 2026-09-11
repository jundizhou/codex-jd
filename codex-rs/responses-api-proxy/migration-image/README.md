# Portable Codex Responses Service

This package contains the patched Codex CLI, app-server and Responses proxy,
plus their Linux runtime and an authenticated Nginx entry point. No source
machine credentials, configuration, API token or conversation history is bundled.

## Target machine

Install Docker Engine/Desktop with Docker Compose v2 and start Docker first.
This image is Linux x86_64; ARM hosts require amd64 emulation. Windows users
can run the commands in WSL with Docker integration enabled.
The machine must be able to reach Codex login and model services.

Extract `codex-migration-linux-amd64.tar.gz`, enter `codex-migration`, then run:

```sh
./migrate.sh
```

The script imports the image when missing, prompts for a Codex device login,
then starts the service. Complete login in your browser. Device-code login
must be enabled for your account/workspace. Login is inside this deployment's
data volume, not automatically shared with an existing desktop login.

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

## Operations

```sh
./migrate.sh status
./migrate.sh logs
./migrate.sh stop
./migrate.sh restart
./migrate.sh login
./migrate.sh token
```

Login state and API token persist in the `codex-migration_codex-data` Docker
volume. Do not delete that volume unless intentionally removing credentials.
After changing accounts, restart the service. Health checks verify local
process readiness, not model quota or inference access. Model listing does
not guarantee that every listed model is callable.

## Build on source machine

Place the three matching patched Linux x86_64 executables in `bin/`, then run
`bash build.sh`. The output is `dist/codex-migration-linux-amd64.tar.gz`.
Only explicitly allowed binaries and entrypoint are sent to the Docker build;
the export copies a fixed allowlist and never includes `.env` or data volumes.
The image is based on Ubuntu 24.04. Building requires network access to the
base image and Ubuntu package repositories; importing the bundle does not.
