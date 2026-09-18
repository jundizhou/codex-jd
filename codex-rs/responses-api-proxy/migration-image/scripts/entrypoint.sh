#!/usr/bin/env bash
set -euo pipefail
umask 077
# Empty proxy variables break reqwest (it treats "" as a proxy URL); drop them.
for proxy_var in HTTP_PROXY HTTPS_PROXY ALL_PROXY http_proxy https_proxy all_proxy; do
    if [[ -z "${!proxy_var:-}" ]]; then
        unset "$proxy_var"
    fi
done
mkdir -p "$CODEX_HOME" /data/workspace
if [[ ! -f "$CODEX_HOME/config.toml" ]]; then
    printf 'cli_auth_credentials_store = "file"\n' > "$CODEX_HOME/config.toml"
fi
case "${1:-serve}" in
    login) exec codex login --device-auth ;;
    login-status) exec codex login status ;;
    token) exec cat /data/api-token ;;
    serve) ;;
    *) exec "$@" ;;
esac
if ! codex login status; then
    echo 'Login required: run ./migrate.sh login first.' >&2
    exit 1
fi
if [[ ! -s /data/api-token ]]; then
    head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n' > /data/api-token
fi
token=$(< /data/api-token)
if [[ ! "$token" =~ ^[a-f0-9]{64}$ ]]; then
    echo 'Invalid /data/api-token: expected 64 lowercase hex characters.' >&2
    exit 1
fi
socket=/data/app-server.sock
printf 'header = "Authorization: Bearer %s"\n' "$token" > /tmp/worker-queue-curl.conf
rm -f "$socket"
pids=()
cleanup() {
    trap - EXIT TERM INT
    if [[ "${CODEX_WORKER_QUEUE:-0}" == 1 ]] && ((${#pids[@]} >= 2)); then
        curl --noproxy '*' --config /tmp/worker-queue-curl.conf --silent --max-time 2 \
            --request POST --output /dev/null http://127.0.0.1:8787/internal/queue/pause || true
        stop_deadline=$((SECONDS + 20))
        while ((SECONDS < stop_deadline)); do
            queue_status=$(curl --noproxy '*' --config /tmp/worker-queue-curl.conf --silent --max-time 1 \
                http://127.0.0.1:8787/internal/queue/status) || break
            [[ "$queue_status" == *'"running":0'* && "$queue_status" == *'"pending":0'* ]] && break
            sleep 1
        done
    fi
    if ((${#pids[@]})); then
        kill "${pids[@]}" 2>/dev/null || true
        wait "${pids[@]}" 2>/dev/null || true
    fi
}
trap cleanup EXIT
trap 'exit 143' TERM
trap 'exit 130' INT
cd /data/workspace
codex-app-server --listen "unix://$socket" --session-source vscode &
pids+=("$!")
for ((i=0; i<90; i++)); do
    [[ -S "$socket" ]] && break
    kill -0 "${pids[0]}" 2>/dev/null || exit 1
    sleep 1
done
[[ -S "$socket" ]] || { echo 'app-server startup timed out.' >&2; exit 1; }
queue_args=()
if [[ "${CODEX_WORKER_QUEUE:-0}" == 1 ]]; then
    if [[ "${CODEX_WORKER_QUEUE_AUTO_RECOVER:-0}" == 1 ]]; then
        queue_args+=(--queue-auto-recover)
    fi
    queue_args+=(--queue --queue-state /data/queue-state.json
        --queue-max-running "${CODEX_WORKER_QUEUE_MAX_RUNNING:-2}"
        --queue-conversation-gap-ms "${CODEX_WORKER_QUEUE_GAP_MS:-800}"
        --queue-user-gap-ms "${CODEX_WORKER_QUEUE_USER_GAP_MS:-1500}"
        --queue-tool-gap-ms "${CODEX_WORKER_QUEUE_TOOL_GAP_MS:-300}"
        --queue-start-gap-ms "${CODEX_WORKER_QUEUE_START_GAP_MS:-300}"
        --queue-idle-ttl-secs "${CODEX_WORKER_QUEUE_IDLE_TTL_SECS:-900}")
fi
CODEX_WORKER_API_KEY="$token" codex-responses-api-proxy "${queue_args[@]}" --port 8787 --app-server-socket "$socket" &
pids+=("$!")
if [[ "${CODEX_WORKER_AUTO_SWITCH:-0}" == 1 && "${CODEX_WORKER_QUEUE:-0}" == 1 ]]; then
    /usr/local/bin/account-switcher.sh &
    pids+=("$!")
fi
# Gate readiness on the proxy listener without requiring an upstream model call.
for ((i=0; i<90; i++)); do
    if curl --noproxy '*' --silent --output /dev/null http://127.0.0.1:8787/; then
        break
    fi
    kill -0 "${pids[1]}" 2>/dev/null || exit 1
    sleep 1
done
curl --noproxy '*' --silent --output /dev/null http://127.0.0.1:8787/ || exit 1
cat > /tmp/migration-nginx.conf <<EOF
worker_processes auto;
pid /tmp/migration-nginx.pid;
error_log /dev/stderr warn;
events { worker_connections 1024; }
http {
    map_hash_bucket_size 128;
    access_log off;
    client_body_temp_path /tmp/client_body;
    proxy_temp_path /tmp/proxy_temp;
    map \$http_authorization \$authorized {
        default 0;
        "Bearer $token" 1;
    }
    server {
        listen 8080;
        client_max_body_size 32m;
        client_body_timeout 20s;
        send_timeout 30s;
        location = /healthz { return 200 'ok'; }
        location = /admin/accounts {
            limit_except GET { deny all; }
            proxy_pass http://127.0.0.1:8787;
            proxy_set_header Authorization "";
            add_header Cache-Control "no-store" always;
        }
        location / {
            if (\$authorized = 0) { return 401; }
            proxy_pass http://127.0.0.1:8787;
            proxy_http_version 1.1;
            proxy_set_header Connection "";
            proxy_set_header Authorization \$http_authorization;
            proxy_buffering off;
            proxy_request_buffering off;
            proxy_read_timeout 3600s;
        }
    }
}
EOF
unset token
nginx -t -c /tmp/migration-nginx.conf
nginx -c /tmp/migration-nginx.conf -g 'daemon off;' &
pids+=("$!")
wait -n "${pids[@]}" || true
echo 'A service exited; stopping the container so it can restart.' >&2
exit 1
