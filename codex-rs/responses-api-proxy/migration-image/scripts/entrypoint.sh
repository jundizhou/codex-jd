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
rm -f "$socket"
pids=()
cleanup() {
    trap - EXIT TERM INT
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
codex-responses-api-proxy --port 8787 --app-server-socket "$socket" &
pids+=("$!")
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
        location = /healthz { return 200 'ok'; }
        location / {
            if (\$authorized = 0) { return 401; }
            proxy_pass http://127.0.0.1:8787;
            proxy_http_version 1.1;
            proxy_set_header Connection "";
            proxy_set_header Authorization "";
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
