#!/usr/bin/env bash
set +x
set -euo pipefail
export -n login_username login_password

scrape=false
if [[ ${1:-} == --scrape ]]; then
    scrape=true
    shift
fi

compose=()
while (($#)) && [[ $1 != -- ]]; do
    compose+=("$1")
    shift
done
if ((${#compose[@]} == 0 || $# == 0)); then
    printf '%s\n' 'Expected a Compose command followed by -- and login arguments.' >&2
    exit 2
fi
shift

read_host_credentials() {
    local references username_reference password_reference
    references=$("${compose[@]}" run --rm --no-deps -T dank x-login --onepassword-references "$@") || return 1
    if [[ -z $references ]]; then
        return 3
    fi
    {
        IFS= read -r username_reference
        IFS= read -r password_reference
    } <<< "$references"
    if [[ $username_reference != op://* || $password_reference != op://* || $references != "$username_reference"$'\n'"$password_reference" ]]; then
        printf '%s\n' 'Could not read the configured 1Password references.' >&2
        return 1
    fi

    # Preserve trailing newlines so the receiver can reject them.
    if ! login_username=$(op read --no-newline "$username_reference" 2>/dev/null && printf '.'); then
        return 4
    fi
    if ! login_password=$(op read --no-newline "$password_reference" 2>/dev/null && printf '.'); then
        unset login_username
        return 4
    fi
}

write_host_credentials() {
    builtin printf '%s\0%s\0' "${login_username%.}" "${login_password%.}"
}

send_scrape_credentials() {
    local config=config.toml argument
    while (($#)); do
        argument=${1%%=*}
        if [[ $1 == -- ]]; then
            break
        elif [[ $argument == --c* && --config == "$argument"* ]]; then
            if [[ $1 == *=* ]]; then
                config=${1#*=}
            else
                config=$2
                shift
            fi
        fi
        shift
    done
    read_host_credentials --config "$config" && write_host_credentials
}

run_scrape() {
    run=("${compose[@]}" run --rm -T dank scrape --headless)
    if ! command -v op >/dev/null 2>&1; then
        exec "${run[@]}" "$@"
    fi

    pipe_dir=$(mktemp -d "${TMPDIR:-/tmp}/dank-x-credentials.XXXXXX")
    scrape_pid= credentials_pid= status=0
    trap '[[ -z $scrape_pid ]] || kill "$scrape_pid" 2>/dev/null || :
          [[ -z $credentials_pid ]] || kill "$credentials_pid" 2>/dev/null || :
          rm -f "$pipe_dir/input" "$pipe_dir/output"
          rmdir "$pipe_dir"' EXIT
    trap 'exit 130' INT
    trap 'exit 143' TERM

    mkfifo "$pipe_dir/input" "$pipe_dir/output"
    "${run[@]}" --x-credentials-stdio "$@" < "$pipe_dir/input" > "$pipe_dir/output" &
    scrape_pid=$!
    exec 3> "$pipe_dir/input"

    while IFS= read -r line; do
        if [[ $line == DANK_REQUEST_X_CREDENTIALS ]]; then
            if [[ -z $credentials_pid ]]; then
                # Keep streaming scrape output while the vault is unlocked.
                send_scrape_credentials "$@" </dev/null >&3 2>/dev/null &
                credentials_pid=$!
                exec 3>&-
            fi
        else
            printf '%s\n' "$line"
        fi
    done < "$pipe_dir/output"
    if [[ -n $line ]]; then
        printf '%s' "$line"
    fi

    wait "$scrape_pid" || status=$?
    scrape_pid=
    exit "$status"
}

if [[ $scrape == true ]]; then
    run_scrape "$@"
fi

manual=false
for argument in "$@"; do
    case "$argument" in
        --manual) manual=true ;;
        --help|-h) exec "${compose[@]}" run --build --rm --no-deps -T dank x-login "$@" ;;
    esac
done

# Build output stays separate from later references and credentials.
"${compose[@]}" build dank >&2
set +e
"${compose[@]}" run --rm --no-deps -T dank x-login --check-session "$@"
check_status=$?
set -e
case "$check_status" in
    0) exit 0 ;;
    3) ;; # No signed-in session; continue to interactive login.
    *) exit "$check_status" ;;
esac

run=("${compose[@]}" run --rm --no-deps -T -p 127.0.0.1:6080:6080 dank dank-x-login)
if [[ $manual == true ]] || ! command -v op >/dev/null 2>&1; then
    exec "${run[@]}" "$@"
fi

credential_status=0
read_host_credentials "$@" || credential_status=$?
case "$credential_status" in
    0) write_host_credentials | "${run[@]}" --credentials-stdin "$@" ;;
    3) exec "${run[@]}" "$@" ;;
    4)
        printf '%s\n' 'Could not read 1Password fields; continue sign-in manually.' >&2
        exec "${run[@]}" --manual "$@"
        ;;
    *) exit "$credential_status" ;;
esac
