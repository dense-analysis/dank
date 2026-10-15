#!/usr/bin/env bash
set -euo pipefail

action=$1
mode=$2
shift 2

case "$action:$mode" in
    scrape:docker|scrape:native|process:docker|process:native) ;;
    *) printf '%s\n' 'Expected scrape/process and docker/native.' >&2; exit 2 ;;
esac

started=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
started_seconds=$SECONDS
log_dir="logs/$action"
mkdir -p "$log_dir"
log_file="$log_dir/${started//:/-}-$$.log"
# Never overwrite a previous run, even if its timestamp and PID are reused.
(umask 077; set -o noclobber; : > "$log_file")

revision=$(git rev-parse --verify HEAD 2>/dev/null || printf 'unknown')
if [ "$revision" != unknown ] && [ -n "$(git status --porcelain)" ]; then
    revision="$revision (dirty)"
fi

{
    printf 'DANK %s run\n' "$action"
    printf 'Started (UTC): %s\n' "$started"
    printf 'Mode: %s\n' "$mode"
    printf 'Working directory: %s\n' "$PWD"
    printf 'Checkout revision: %s\n' "$revision"
    printf 'Command:'
    printf ' %q' "$@"
    printf '\n\n'
} >> "$log_file"
printf 'Run log: %s\n' "$log_file"

finish() {
    local status=$?
    local capture_status=0
    trap - EXIT INT TERM

    # Drain both streams before writing the final status.
    wait "$stderr_pid" || capture_status=$?
    wait "$stdout_pid" || capture_status=$?
    rm -f "$pipe_dir/stdout" "$pipe_dir/stderr"
    rmdir "$pipe_dir"
    {
        printf '\nFinished (UTC): %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
        printf 'Duration (seconds): %d\n' "$((SECONDS - started_seconds))"
        printf 'Exit status: %d\n' "$status"
        printf 'Log capture status: %d\n' "$capture_status"
    } >> "$log_file" || capture_status=$?

    if [ "$status" -eq 0 ] && [ "$capture_status" -ne 0 ]; then
        status=$capture_status
    fi

    printf 'Run log: %s (exit %d)\n' "$log_file" "$status"
    exit "$status"
}

# Append complete lines so simultaneous streams do not split ordinary log lines.
forward_stream() {
    local line
    while IFS= read -r line; do
        printf '%s\n' "$line"
        printf '%s\n' "$line" >&3
    done
    if [ -n "$line" ]; then
        printf '%s' "$line"
        printf '%s' "$line" >&3
    fi
}

# FIFOs work with macOS Bash and keep terminal stdout and stderr separate.
pipe_dir=$(mktemp -d "${TMPDIR:-/tmp}/dank-run-log.XXXXXX")
mkfifo "$pipe_dir/stdout" "$pipe_dir/stderr"
forward_stream < "$pipe_dir/stdout" 3>> "$log_file" &
stdout_pid=$!
forward_stream < "$pipe_dir/stderr" 3>> "$log_file" >&2 &
stderr_pid=$!
trap finish EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

status=0
"$@" > "$pipe_dir/stdout" 2> "$pipe_dir/stderr" || status=$?
exit "$status"
