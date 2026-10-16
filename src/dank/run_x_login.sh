#!/usr/bin/env bash
# This command deliberately does not use the archived run-log wrapper.
set +x
set -euo pipefail
export -n login_username login_password

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

# Only references leave this configuration-reader container.
references=$("${compose[@]}" run --rm --no-deps -T dank x-login --onepassword-references "$@")
if [[ -z $references ]]; then
    exec "${run[@]}" "$@"
fi
{
    IFS= read -r username_reference
    IFS= read -r password_reference
} <<< "$references"
if [[ $username_reference != op://* || $password_reference != op://* || $references != "$username_reference"$'\n'"$password_reference" ]]; then
    printf '%s\n' 'Could not read the configured 1Password references.' >&2
    exit 1
fi

# The sentinel preserves trailing newlines so the receiver can reject them.
# Values stay in shell memory, never environment variables, argv or files.
if ! login_username=$(op read --no-newline "$username_reference" 2>/dev/null && printf '.'); then
    printf '%s\n' 'Could not read 1Password fields; continue sign-in manually.' >&2
    exec "${run[@]}" --manual "$@"
fi
if ! login_password=$(op read --no-newline "$password_reference" 2>/dev/null && printf '.'); then
    unset login_username
    printf '%s\n' 'Could not read 1Password fields; continue sign-in manually.' >&2
    exec "${run[@]}" --manual "$@"
fi
login_username=${login_username%.}
login_password=${login_password%.}

builtin printf '%s\0%s\0' "$login_username" "$login_password" |
    "${run[@]}" --credentials-stdin "$@"
