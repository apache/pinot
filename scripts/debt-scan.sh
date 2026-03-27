#!/bin/bash

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

set -e

# Script to scan Apache Pinot codebase for TODO and FIXME comments in main source code.
# Produces a per-module summary, excluding test resources, fixtures, and non-code files.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Display usage information
usage() {
    cat << EOF
Usage: $(basename "$0") [OPTIONS]

Scan Apache Pinot main source code for TODO and FIXME comments.
Produces a per-module summary table with counts and untracked TODOs.

OPTIONS:
  -h, --help      Show this help message
  -d, --dir DIR   Root directory to scan (default: current repo root)

OUTPUT:
  Per-module summary table with columns:
  - Module name
  - TODO count
  - FIXME count
  - Untracked TODOs (lacking issue reference like PINOT-*, #*, or issue)

EXCLUSIONS:
  The script excludes:
  - Test resources: */src/test/resources/*
  - Example data: */resources/examples/*, */docker/images/*/examples/*
  - Non-code files: *.xml, *.json, *.avro, *.csv, *.properties

EOF
    exit 0
}

# Parse command-line arguments
SCAN_ROOT="$REPO_ROOT"

while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help)
            usage
            ;;
        -d|--dir)
            SCAN_ROOT="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1" >&2
            usage
            exit 1
            ;;
    esac
done

# Verify scan root exists
if [[ ! -d "$SCAN_ROOT" ]]; then
    echo "Error: Scan directory does not exist: $SCAN_ROOT" >&2
    exit 1
fi

# Create a temporary directory for processing
TEMP_DIR=$(mktemp -d)
trap "rm -rf $TEMP_DIR" EXIT

# Find all modules with src/main/java structure
MODULES=$(find "$SCAN_ROOT" -maxdepth 4 -type d -path "*/src/main/java" 2>/dev/null | sed 's|/src/main/java$||' | sort)

if [[ -z "$MODULES" ]]; then
    echo "No modules with src/main/java found in $SCAN_ROOT" >&2
    exit 1
fi

# Process each module
declare -A TODO_COUNT
declare -A FIXME_COUNT
declare -A UNTRACKED_COUNT

for MODULE_PATH in $MODULES; do
    MODULE_NAME=$(basename "$MODULE_PATH")
    JAVA_DIR="$MODULE_PATH/src/main/java"

    if [[ ! -d "$JAVA_DIR" ]]; then
        continue
    fi

    # Count all TODO occurrences, excluding test resources and non-code files
    TODO_MATCHES=$(find "$JAVA_DIR" -type f -name "*.java" \
        ! -path "*/src/test/resources/*" \
        ! -path "*/resources/examples/*" \
        ! -path "*/docker/images/*/examples/*" \
        -exec grep -h "TODO" {} \; 2>/dev/null | wc -l)

    # Count all FIXME occurrences with same exclusions
    FIXME_MATCHES=$(find "$JAVA_DIR" -type f -name "*.java" \
        ! -path "*/src/test/resources/*" \
        ! -path "*/resources/examples/*" \
        ! -path "*/docker/images/*/examples/*" \
        -exec grep -h "FIXME" {} \; 2>/dev/null | wc -l)

    # Count untracked TODOs (those without issue references)
    # Pattern: TODO not followed by (PINOT-, #, or issue)
    UNTRACKED=$(find "$JAVA_DIR" -type f -name "*.java" \
        ! -path "*/src/test/resources/*" \
        ! -path "*/resources/examples/*" \
        ! -path "*/docker/images/*/examples/*" \
        -exec grep -h "TODO" {} \; 2>/dev/null | \
        grep -v -E "TODO\([^)]*((PINOT-|#|issue))" | wc -l)

    if [[ "$TODO_MATCHES" -gt 0 || "$FIXME_MATCHES" -gt 0 ]]; then
        TODO_COUNT["$MODULE_NAME"]=$TODO_MATCHES
        FIXME_COUNT["$MODULE_NAME"]=$FIXME_MATCHES
        UNTRACKED_COUNT["$MODULE_NAME"]=$UNTRACKED
    fi
done

# Print header
printf "%-40s %8s %8s %10s\n" "Module" "TODO" "FIXME" "Untracked"
printf "%-40s %8s %8s %10s\n" "$(printf '%.0s-' {1..40})" "$(printf '%.0s-' {1..8})" "$(printf '%.0s-' {1..8})" "$(printf '%.0s-' {1..10})"

# Print results sorted by module name
for MODULE_NAME in $(echo "${!TODO_COUNT[@]}" | tr ' ' '\n' | sort); do
    TODO="${TODO_COUNT[$MODULE_NAME]:-0}"
    FIXME="${FIXME_COUNT[$MODULE_NAME]:-0}"
    UNTRACKED="${UNTRACKED_COUNT[$MODULE_NAME]:-0}"

    # Highlight untracked TODOs with a marker
    MARKER=""
    if [[ "$UNTRACKED" -gt 0 ]]; then
        MARKER=" ⚠"
    fi

    printf "%-40s %8d %8d %10d%s\n" "$MODULE_NAME" "$TODO" "$FIXME" "$UNTRACKED" "$MARKER"
done

# Print footer with totals
TOTAL_TODO=0
TOTAL_FIXME=0
TOTAL_UNTRACKED=0

for MODULE_NAME in "${!TODO_COUNT[@]}"; do
    TOTAL_TODO=$((TOTAL_TODO + ${TODO_COUNT[$MODULE_NAME]:-0}))
    TOTAL_FIXME=$((TOTAL_FIXME + ${FIXME_COUNT[$MODULE_NAME]:-0}))
    TOTAL_UNTRACKED=$((TOTAL_UNTRACKED + ${UNTRACKED_COUNT[$MODULE_NAME]:-0}))
done

printf "%-40s %8s %8s %10s\n" "$(printf '%.0s-' {1..40})" "$(printf '%.0s-' {1..8})" "$(printf '%.0s-' {1..8})" "$(printf '%.0s-' {1..10})"
printf "%-40s %8d %8d %10d\n" "TOTAL" "$TOTAL_TODO" "$TOTAL_FIXME" "$TOTAL_UNTRACKED"

# Print legend
echo ""
echo "Legend:"
echo "  ⚠ = Module has untracked TODOs (lacking issue reference)"
echo ""
echo "Tracked TODOs should follow pattern: TODO(PINOT-XXXXX) or TODO(#XXXXX) or TODO(issue-xxx)"
