#!/usr/bin/env bash
set -euo pipefail

profiles=/data/accounts
active_file=/data/active-account
status_url=http://127.0.0.1:8787/internal/queue/status
pause_url=http://127.0.0.1:8787/internal/queue/pause
resume_url=http://127.0.0.1:8787/internal/queue/resume

profile_names() {
    find "$profiles" -mindepth 2 -maxdepth 2 -type f -name auth.json -printf '%h\n' 2>/dev/null \
        | sed 's#^.*/##' | LC_ALL=C sort
}

current_profile() {
    if [[ -s "$active_file" ]]; then
        tr -d '\n' < "$active_file"
    fi
}

switch_to() {
    local profile=$1
    local source="$profiles/$profile/auth.json"
    local mode
    test -s "$source"
    mode=$(stat -c %a "$source" 2>/dev/null || stat -f %Lp "$source")
    test "$mode" = 600
    cp "$source" /data/codex/auth.json.switching
    chmod 600 /data/codex/auth.json.switching
    mv -f /data/codex/auth.json.switching /data/codex/auth.json
    printf '%s\n' "$profile" > "$active_file"
    echo "automatic account switch activated profile: $profile" >&2
}

while :; do
    sleep 5
    status=$(curl --noproxy '*' --config /tmp/worker-queue-curl.conf --silent --show-error \
        --max-time 5 "$status_url") || continue
    [[ "$status" == *'"account_unavailable":true'* && "$status" == *'"account_unavailable_reason":"quota_exhausted"'* ]] || continue

    current=$(current_profile || true)
    next=''
    while IFS= read -r candidate; do
        [[ "$candidate" == "$current" ]] && continue
        next=$candidate
        break
    done < <(profile_names)
    [[ -n "$next" ]] || { echo 'automatic account switch: no alternate profile' >&2; sleep 30; continue; }

    curl --noproxy '*' --config /tmp/worker-queue-curl.conf --silent --show-error \
        --max-time 5 --request POST "$pause_url" >/dev/null || continue
    drained=false
    for _ in $(seq 1 60); do
        status=$(curl --noproxy '*' --config /tmp/worker-queue-curl.conf --silent --show-error \
            --max-time 5 "$status_url") || break
        if [[ "$status" == *'"running":0'* && "$status" == *'"pending":0'* ]]; then
            drained=true
            break
        fi
        sleep 1
    done
    if [[ "$drained" == true ]]; then
        switch_to "$next"
        # Give app-server's file watcher time to publish the new AuthManager state.
        sleep 2
        curl --noproxy '*' --config /tmp/worker-queue-curl.conf --silent --show-error \
            --max-time 5 --request POST "$resume_url" >/dev/null || true
    else
        echo 'automatic account switch: queue did not drain' >&2
    fi
done
