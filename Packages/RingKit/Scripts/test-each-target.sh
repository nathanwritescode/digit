#!/bin/bash
# Runs RingKit's test targets one after another, each in a process of its own (#68).
#
# With Swift 6.3 (CI's toolchain in November 2026) `swift test` runs every test target in one
# process; Swift 6.4 already gives each target a process of its own. In the shared process, tests
# that hold threads (ringprobe's tests wait synchronously for `git`; the store derivation tests
# added for #69) delayed the transport tests' scripted replies by 80 to 180 ms per scheduling hop,
# past their 50 ms reply timeout, so requests timed out with the reply already queued. With one
# target per process no target's tests can starve another's, and every toolchain behaves the same.
#
# Run from Packages/RingKit after `swift build --build-tests`. With no arguments it runs every test
# target in Package.swift; otherwise only the targets named. Environment variables (such as
# LIBDISPATCH_COOPERATIVE_POOL_STRICT) pass through to the test processes. Every target is run
# even after one fails, and the script fails if any target failed or ran no tests.
set -uo pipefail

if [ $# -eq 0 ]; then
    targets=$(swift package dump-package | jq -r '.targets[] | select(.type == "test") | .name') || exit 1
    # Word splitting is intended: target names contain no spaces.
    set -- $targets
fi
if [ $# -eq 0 ]; then
    echo "error: no test targets found" >&2
    exit 1
fi

log=$(mktemp)
trap 'rm -f "$log"' EXIT
failed=()
for target in "$@"; do
    echo "=== $target"
    start=$SECONDS
    # Test identifiers start with the module name, which is the target name.
    swift test --skip-build --filter "^$target\." 2>&1 | tee "$log"
    status=$?
    echo "=== $target finished with status $status after $((SECONDS - start)) s"
    if [ $status -ne 0 ]; then
        failed+=("$target")
    elif ! grep -Eq "Test run with [1-9][0-9]* tests?" "$log"; then
        # A filter that matches nothing passes with only a warning. Every target uses Swift
        # Testing; a target with only XCTest tests would need this check changed.
        echo "error: $target ran no tests" >&2
        failed+=("$target")
    fi
done

if [ ${#failed[@]} -ne 0 ]; then
    echo "error: failed: ${failed[*]}" >&2
    exit 1
fi
