#!/usr/bin/env bash
set -eu

children=()
login_pid=
desktop_log=$(mktemp)
display_file=$(mktemp)
cleanup() {
    trap - EXIT
    trap '' INT TERM
    # Let Chromium flush and remove its profile locks before stopping Xvfb.
    if [[ -n $login_pid ]]; then
        kill -TERM "$login_pid" 2>/dev/null || true
        wait "$login_pid" 2>/dev/null || true
    fi
    if ((${#children[@]})); then
        kill "${children[@]}" 2>/dev/null || true
        wait "${children[@]}" 2>/dev/null || true
    fi
    rm -f "$desktop_log" "$display_file"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# Xvfb writes the chosen display number only when it is ready for clients.
Xvfb -displayfd 3 -screen 0 1280x900x24 -nolisten tcp 3>"$display_file" >>"$desktop_log" 2>&1 &
children+=("$!")
for attempt in {1..100}; do
    if read -r display_number < "$display_file"; then
        break
    fi
    sleep 0.1
done
if [[ -z ${display_number:-} ]]; then
    printf '%s\n' 'The sign-in display did not start.' >&2
    cat "$desktop_log" >&2
    exit 1
fi
export DISPLAY=:$display_number

# Only the web client is published, on host loopback by make x-login.
x11vnc -display "$DISPLAY" -localhost -rfbport 5900 -forever -shared -nopw -quiet >>"$desktop_log" 2>&1 &
children+=("$!")
/usr/bin/websockify --web /usr/share/novnc 6080 localhost:5900 >>"$desktop_log" 2>&1 &
children+=("$!")

printf '%s\n' 'Open http://127.0.0.1:6080/vnc.html?autoconnect=1&resize=scale to sign in to X.'
x-login "$@" <&0 &
login_pid=$!

# Stop the whole temporary desktop if login or any display server exits.
set +e
wait -n -p finished "${children[@]}" "$login_pid"
status=$?
set -e
if [[ $finished != "$login_pid" ]]; then
    printf '%s\n' 'The sign-in browser service stopped unexpectedly.' >&2
    cat "$desktop_log" >&2
    exit 1
fi
exit "$status"
