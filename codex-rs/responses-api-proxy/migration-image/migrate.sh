#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
image=codex-migration:2026-09-18-admin-ui-bundle1
docker info >/dev/null
docker compose version >/dev/null
compose=(docker compose --project-name "${CODEX_MIGRATION_PROJECT:-codex-migration}" -f compose.yaml)
ensure_image() {
    if ! docker image inspect "$image" >/dev/null 2>&1; then
        [[ -f codex-image.tar.gz ]] || { echo 'Missing codex-image.tar.gz. Run build.sh on the source machine.' >&2; exit 1; }
        if command -v sha256sum >/dev/null 2>&1; then
            sha256sum -c SHA256SUMS
        else
            shasum -a 256 -c SHA256SUMS
        fi
        docker load -i codex-image.tar.gz
        docker image inspect "$image" >/dev/null
    fi
}
case "${1:-start}" in
    start)
        ensure_image
        "${compose[@]}" up -d --wait --wait-timeout 180
        echo 'Ready. Open /admin/accounts on the configured host and port.'
        echo 'Run ./migrate.sh token to view the Worker Token for the page and API.'
        ;;
    login)
        ensure_image
        "${compose[@]}" run --rm --no-deps api login
        ;;
    stop) "${compose[@]}" stop ;;
    restart) "${compose[@]}" restart ;;
    status) "${compose[@]}" ps ;;
    logs) "${compose[@]}" logs --tail 100 -f ;;
    token) "${compose[@]}" exec -T api cat /data/api-token ;;
    switch-account)
        profile=${2:-}
        [[ "$profile" =~ ^[A-Za-z0-9_-]{1,64}$ ]] || { echo 'Usage: ./migrate.sh switch-account PROFILE' >&2; exit 2; }
        [[ -z "${3:-}" ]] || { echo 'switch-account accepts exactly one profile name' >&2; exit 2; }
        # Use the same guarded, live activation path as the administration page.
        "${compose[@]}" exec -T api curl --noproxy '*' --config /tmp/worker-queue-curl.conf \
            --fail-with-body --silent --show-error --max-time 15 \
            --header 'Content-Type: application/json' \
            --data "{\"profile\":\"$profile\"}" http://127.0.0.1:8787/admin/api/switch
        echo "Activated account profile: $profile"
        ;;
    *) echo 'Usage: ./migrate.sh [start|login|stop|restart|status|logs|token|switch-account PROFILE]' >&2; exit 2 ;;
esac
