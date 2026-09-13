#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
for binary in codex codex-app-server codex-responses-api-proxy; do
    [[ -s "bin/$binary" ]] || { echo "Missing bin/$binary (patched Linux x86_64 build required)." >&2; exit 1; }
done
base_image=ubuntu:24.04
if docker image inspect -f '{{.Id}}' codex-migration:2026-09-10 >/dev/null 2>&1; then
    base_image=codex-migration:2026-09-10
fi
docker build --platform linux/amd64 --build-arg BASE_IMAGE="$base_image" \
    -t codex-migration:2026-09-11-token-fix .
mkdir -p dist/codex-migration
docker save codex-migration:2026-09-11-token-fix | gzip > dist/codex-migration/codex-image.tar.gz
cp migrate.sh compose.yaml README.md .env.example dist/codex-migration/
cp 部署文档.md dist/codex-migration/
cp ../FIELD_HANDLING_SUMMARY.md dist/codex-migration/
chmod 755 dist/codex-migration/migrate.sh
COPYFILE_DISABLE=1 tar -czf dist/codex-migration-linux-amd64.tar.gz -C dist codex-migration
echo "Bundle: $PWD/dist/codex-migration-linux-amd64.tar.gz"
