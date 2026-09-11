#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
image=codex-migration:2026-09-10
docker info >/dev/null
docker compose version >/dev/null
compose=(docker compose --project-name "${CODEX_MIGRATION_PROJECT:-codex-migration}" -f compose.yaml)
ensure_image() {
    if ! docker image inspect "$image" >/dev/null 2>&1; then
        [[ -f codex-image.tar.gz ]] || { echo 'Missing codex-image.tar.gz. Run build.sh on the source machine.' >&2; exit 1; }
        docker load -i codex-image.tar.gz
    fi
}
case "${1:-start}" in
    start)
        ensure_image
        if ! "${compose[@]}" run --rm --no-deps -T api login-status; then
            "${compose[@]}" run --rm --no-deps api login
        fi
        "${compose[@]}" up -d --wait --wait-timeout 180
        echo 'Ready. Run ./migrate.sh token to view the API token.'
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
    *) echo 'Usage: ./migrate.sh [start|login|stop|restart|status|logs|token]' >&2; exit 2 ;;
esac
