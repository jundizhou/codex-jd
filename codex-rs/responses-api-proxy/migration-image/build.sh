#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
image=codex-migration:2026-09-18-admin-ui-bundle1
for binary in codex codex-app-server codex-responses-api-proxy; do
    [[ -s "bin/$binary" ]] || { echo "Missing bin/$binary (patched Linux x86_64 build required)." >&2; exit 1; }
done
# Always use a clean runtime base; never inherit a previously deployed container.
docker build --platform linux/amd64 --build-arg BASE_IMAGE="${CODEX_BUNDLE_BASE_IMAGE:-ubuntu:24.04}" -t "$image" .
mkdir -p dist
staging=$(mktemp -d "$PWD/dist/.bundle.XXXXXX")
trap 'rm -rf "$staging"' EXIT
package="$staging/codex-migration"
mkdir "$package"
docker save "$image" | gzip > "$package/codex-image.tar.gz"
# Explicit allowlist: credentials, account profiles and deployment .env are excluded.
cp migrate.sh compose.yaml README.md .env.example 部署文档.md "$package/"
cp ../FIELD_HANDLING_SUMMARY.md "$package/"
chmod 755 "$package/migrate.sh"
(
    cd "$package"
    shasum -a 256 codex-image.tar.gz migrate.sh compose.yaml README.md .env.example 部署文档.md FIELD_HANDLING_SUMMARY.md > SHA256SUMS
)
archive=codex-migration-2026-09-18-admin-ui-linux-amd64.tar.gz
COPYFILE_DISABLE=1 tar -czf "dist/$archive.pending" -C "$staging" codex-migration
mv "dist/$archive.pending" "dist/$archive"
(cd dist && shasum -a 256 "$archive" > "$archive.sha256")
echo "Bundle: $PWD/dist/$archive"
