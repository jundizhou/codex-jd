FROM ubuntu:24.04
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates libssl3t64 libgcc-s1 nginx curl tini \
    && rm -rf /var/lib/apt/lists/*
COPY bin/ /usr/local/bin/
COPY scripts/entrypoint.sh /usr/local/bin/migration-entrypoint
RUN chmod 755 /usr/local/bin/*
ENV CODEX_HOME=/data/codex
WORKDIR /data
EXPOSE 8080
HEALTHCHECK --interval=15s --timeout=5s --start-period=90s --retries=3 \
    CMD curl --noproxy '*' --fail --silent http://127.0.0.1:8080/healthz || exit 1
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/migration-entrypoint"]
CMD ["serve"]
