#!/bin/sh
# Chromium namespaces are unavailable in the default Docker sandbox.
exec /usr/bin/chromium --no-sandbox "$@"
