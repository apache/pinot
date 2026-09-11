#!/bin/sh
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
# Fixture tests for jfr-janitor.sh. Run: sh helm/pinot/scripts/jfr-janitor-test.sh

set -u

script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
janitor="$script_dir/jfr-janitor.sh"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

failures=0
checks=0

check() {
  checks=$(( checks + 1 ))
  if [ "$2" = "$3" ]; then
    echo "ok   - $1"
  else
    echo "FAIL - $1"
    echo "         expected: $3"
    echo "         actual:   $2"
    failures=$(( failures + 1 ))
  fi
}

# `date` takes a different flag for relative times on BSD (macOS) and GNU (the container).
if date -v-1M +%Y >/dev/null 2>&1; then
  minutes_ago() { date -v"-$1M" +%Y%m%d%H%M; }
else
  minutes_ago() { date -d "-$1 minutes" +%Y%m%d%H%M; }
fi

# make_repo <parent> <name> <kib> [minutes_in_the_past]
make_repo() {
  _dir="$1/$2"
  mkdir -p "$_dir"
  dd if=/dev/zero of="$_dir/chunk.jfr" bs=1024 count="$3" 2>/dev/null
  if [ "${4:-0}" -gt 0 ]; then
    _stamp=$(minutes_ago "$4")
    touch -t "$_stamp" "$_dir/chunk.jfr" "$_dir"
  fi
}

# Names of surviving entries, sorted, space separated. The fixtures use plain names, so `ls` is
# fine here and is the portable option.
# shellcheck disable=SC2012
survivors() {
  ls -1 "$1" 2>/dev/null | sort | tr '\n' ' ' | sed 's/ $//'
}

run() {
  _repo="$1"; shift
  env PINOT_JFR_REPOSITORY="$_repo" \
      PINOT_JFR_JANITOR_MAX_AGE_MINUTES="${AGE:-}" \
      PINOT_JFR_JANITOR_MAX_TOTAL_KIB="${BUDGET:-}" \
      PINOT_JFR_JANITOR_MIN_IDLE_MINUTES="${IDLE:-15}" \
      sh "$janitor" "$@" > "$work/out.txt" 2>&1
  echo "$?" > "$work/rc.txt"
}

# --- age pass -----------------------------------------------------------------------------------
r="$work/age"; mkdir -p "$r"
make_repo "$r" 2026_08_01_10_00_00_1 512 20000
make_repo "$r" 2026_08_20_10_00_00_1 512 60
AGE=10080 BUDGET='' IDLE=15 run "$r"
check "age pass drops only the aged-out repository" "$(survivors "$r")" "2026_08_20_10_00_00_1"
check "age pass exits 0" "$(cat "$work/rc.txt")" "0"

# --- size pass ----------------------------------------------------------------------------------
r="$work/size"; mkdir -p "$r"
make_repo "$r" 2026_08_01_10_00_00_1 1024 20000
make_repo "$r" 2026_08_02_10_00_00_1 1024 20000
make_repo "$r" 2026_08_03_10_00_00_1 1024 20000
AGE='' BUDGET=2048 IDLE=15 run "$r"
check "size pass trims oldest-first down to the budget" \
  "$(survivors "$r")" "2026_08_02_10_00_00_1 2026_08_03_10_00_00_1"

# --- non-repository entries are never touched ---------------------------------------------------
r="$work/foreign"; mkdir -p "$r/lost+found" "$r/operator-scratch"
: > "$r/lost+found/keep"; : > "$r/notes.txt"
make_repo "$r" 2026_08_01_10_00_00_1 1024 20000
AGE=1 BUDGET=1 IDLE=15 run "$r"
check "foreign files and directories survive" \
  "$(survivors "$r")" "lost+found notes.txt operator-scratch"

# --- a recently written repository is never deleted ---------------------------------------------
r="$work/live"; mkdir -p "$r"
make_repo "$r" 2026_08_01_10_00_00_1 1024 0
AGE=1 BUDGET=1 IDLE=15 run "$r"
check "a live repository survives both passes" "$(survivors "$r")" "2026_08_01_10_00_00_1"
check "and the overage is reported" \
  "$(grep -c 'still .* against' "$work/out.txt")" "1"

# --- a malformed budget must never mean 'delete everything' -------------------------------------
r="$work/badbudget"; mkdir -p "$r"
make_repo "$r" 2026_08_01_10_00_00_1 1024 20000
make_repo "$r" 2026_08_02_10_00_00_1 1024 20000
AGE='' BUDGET='4GiB' IDLE=15 run "$r"
check "unparseable budget skips the size pass instead of deleting" \
  "$(survivors "$r")" "2026_08_01_10_00_00_1 2026_08_02_10_00_00_1"
check "unparseable budget still exits 0" "$(cat "$work/rc.txt")" "0"
check "unparseable budget is reported" "$(grep -c 'unusable size budget' "$work/out.txt")" "1"

# --- empty budget simply skips the pass ---------------------------------------------------------
r="$work/nobudget"; mkdir -p "$r"
make_repo "$r" 2026_08_01_10_00_00_1 1024 20000
AGE='' BUDGET='' IDLE=15 run "$r"
check "empty budget skips the size pass" "$(survivors "$r")" "2026_08_01_10_00_00_1"

# --- degraded environments must not fail the init container -------------------------------------
AGE=1 BUDGET=1 IDLE=15 run "$work/does-not-exist"
check "missing repository directory exits 0" "$(cat "$work/rc.txt")" "0"

r="$work/readonly"; mkdir -p "$r"
make_repo "$r" 2026_08_01_10_00_00_1 1024 20000
chmod 500 "$r"
AGE=1 BUDGET=1 IDLE=15 run "$r"
rc=$(cat "$work/rc.txt")
chmod 700 "$r"
check "an unremovable repository still exits 0" "$rc" "0"

env PINOT_JFR_REPOSITORY= sh "$janitor" > "$work/out.txt" 2>&1
check "unset repository exits 0" "$?" "0"

echo
echo "$checks checks, $failures failure(s)"
[ "$failures" -eq 0 ]
