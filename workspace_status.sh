#!/usr/bin/env bash

set -o errexit -o nounset -o pipefail

VERSION="$(git describe --tags 2>/dev/null || echo "v0.0.0")"
STABLE_NBES_VERSION="${VERSION#v}"

cat <<EOF
STABLE_NBES_VERSION ${STABLE_NBES_VERSION}
EOF
