#!/bin/bash
# Fails unless every pin in RingKit's Package.resolved appears in the app's with the same
# identity, location and revision (#58). The app and RingKit are separate roots for package
# resolution, each with its own lockfile, and `-disableAutomaticPackageResolution` accepts any
# app pin inside Package.swift's requirement, so a dependency bumped in one lockfile only would
# otherwise leave `swift test` and the app building different revisions.
#
# A subset check, so an app-only dependency does not trip it. `originHash` is not compared: it
# differs between roots by design. Run from the repository root; the two paths can be passed as
# arguments (RingKit's lockfile, then the app's) for testing. Needs jq (in /usr/bin from macOS 15).
set -euo pipefail

ringkit=${1:-Packages/RingKit/Package.resolved}
app=${2:-App/Digit.xcodeproj/project.xcworkspace/xcshareddata/swiftpm/Package.resolved}

for f in "$ringkit" "$app"; do
    if [ ! -f "$f" ]; then
        echo "::error::$f not found; update this check if the lockfile moved."
        exit 1
    fi
done

# One "identity location revision" line per pin, sorted. A pin without a revision (a local or
# branch pin) shows as an empty revision and so must match too.
pins() {
    jq -r '.pins[] | [.identity, .location, (.state.revision // "")] | join(" ")' "$1" | sort
}

ringkit_pins=$(pins "$ringkit")
app_pins=$(pins "$app")

if [ -z "$ringkit_pins" ]; then
    echo "::error::$ringkit has no pins; this check would pass vacuously."
    exit 1
fi

# comm -23: lines only in RingKit's list, i.e. pins the app's lockfile lacks or pins differently.
missing=$(comm -23 <(printf '%s\n' "$ringkit_pins") <(printf '%s\n' "$app_pins"))
if [ -n "$missing" ]; then
    echo "::error::The app's lockfile does not match RingKit's for these pins (identity location revision):"
    printf '%s\n' "$missing"
    echo "Update both lockfiles together: $ringkit and $app."
    exit 1
fi
echo "Every pin in $ringkit appears in $app with the same revision."
