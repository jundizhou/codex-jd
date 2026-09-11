#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
for binary in codex codex-app-server codex-responses-api-proxy; do
    [[ -s "bin/$binary" ]] || { echo "Missing bin/$binary (patched Linux x86_64 build required)." >&2; exit 1; }
done
docker build --platform linux/amd64 -t codex-migration:2026-09-10 .
mkdir -p dist/codex-migration
docker save codex-migration:2026-09-10 | gzip > dist/codex-migration/codex-image.tar.gz
cp migrate.sh compose.yaml README.md .env.example dist/codex-migration/
cp 部署文档.md dist/codex-migration/
chmod 755 dist/codex-migration/migrate.sh
COPYFILE_DISABLE=1 tar -czf dist/codex-migration-linux-amd64.tar.gz -C dist codex-migration
echo "Bundle: $PWD/dist/codex-migration-linux-amd64.tar.gz"
