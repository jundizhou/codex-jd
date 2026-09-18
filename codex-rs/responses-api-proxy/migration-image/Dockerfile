ARG BASE_IMAGE=ubuntu:24.04
FROM ${BASE_IMAGE}
RUN if ! command -v nginx >/dev/null || ! command -v curl >/dev/null || ! command -v tini >/dev/null; then \
        apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates libssl3t64 libgcc-s1 nginx curl tini \
        && rm -rf /var/lib/apt/lists/*; \
    fi
COPY --chmod=755 bin/ /usr/local/bin/
COPY --chmod=755 scripts/entrypoint.sh /usr/local/bin/migration-entrypoint
COPY --chmod=755 scripts/account-switcher.sh /usr/local/bin/account-switcher.sh
ENV CODEX_HOME=/data/codex
WORKDIR /data
EXPOSE 8080
HEALTHCHECK --interval=15s --timeout=5s --start-period=90s --retries=3 \
    CMD curl --noproxy '*' --fail --silent http://127.0.0.1:8080/healthz || exit 1
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/migration-entrypoint"]
CMD ["serve"]
