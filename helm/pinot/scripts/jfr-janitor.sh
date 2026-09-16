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
# Reclaims JFR repositories left behind by previous JVM runs.
#
# JFR's own `maxsize` bounds the repository of the JVM that is running. Nothing inside the JVM ever
# reclaims the repository of a JVM that has already exited, so with `preserve-repository=true` those
# directories accumulate on the volume until it is full. This script deletes them.
#
# It runs as an init container, which is what makes it safe: init containers finish before the Pinot
# container starts, so the repositories it sees belong to runs that are already over. It still
# refuses to touch anything written to recently, so that it stays safe if the volume is ever shared.
#
# Nothing here is required for Pinot to run. Every failure is tolerated and the script always exits
# 0: blocking a Pinot role from starting because a cleanup failed would be far worse than leaving a
# stale recording on disk.
#
# All sizes and durations arrive already converted to plain integers by the Helm chart, so this
# script parses no units. That keeps a single source of truth for the unit table and means a
# malformed value can never turn the size pass into "delete everything".
#
# Inputs (environment):
#   PINOT_JFR_REPOSITORY                 directory holding the per-run repositories (required)
#   PINOT_JFR_JANITOR_MAX_AGE_MINUTES    drop repositories older than this many minutes; empty skips
#   PINOT_JFR_JANITOR_MAX_TOTAL_KIB      trim oldest-first until under this many KiB; empty skips
#   PINOT_JFR_JANITOR_MIN_IDLE_MINUTES   never touch a repository written to this recently

repo="${PINOT_JFR_REPOSITORY:-}"
max_age_minutes="${PINOT_JFR_JANITOR_MAX_AGE_MINUTES:-}"
max_total_kib="${PINOT_JFR_JANITOR_MAX_TOTAL_KIB:-}"
min_idle="${PINOT_JFR_JANITOR_MIN_IDLE_MINUTES:-15}"

# A JFR repository directory is named `<yyyy_MM_dd_HH_mm_ss>_<pid>`. Matching that pattern keeps the
# janitor away from anything else sharing the volume (`lost+found`, an operator's scratch file), and
# makes a lexicographic sort a chronological one.
pattern='[0-9][0-9][0-9][0-9]_[0-9][0-9]_[0-9][0-9]_*'

log() {
  echo "jfr-janitor: $*"
}

# A whole number, and nothing else. Anything the chart failed to convert is treated as "not set"
# rather than as zero: a zero budget would mean "delete everything".
is_positive_int() {
  case "${1:-}" in
    '' | *[!0-9]*) return 1 ;;
    *) return 0 ;;
  esac
}

# Size of a path in KiB; 0 if it cannot be read.
kib() {
  size=$(du -sk "$1" 2>/dev/null | awk 'NR == 1 { print $1 }') || size=""
  is_positive_int "$size" || size=0
  echo "$size"
}

# True when a repository may still belong to a live JVM.
#
# JFR flushes at least once a second, so a live repository has a recently modified chunk file. The
# directory's own mtime is checked too, which covers the window between a JVM creating its
# repository and writing the first chunk into it.
in_use() {
  if [ -n "$(find "$1" -type f -mmin "-$min_idle" 2>/dev/null | head -1)" ]; then
    return 0
  fi
  [ -n "$(find "$1" -maxdepth 0 -mmin "-$min_idle" 2>/dev/null)" ]
}

reclaim_by_age() {
  is_positive_int "$max_age_minutes" || return 0
  log "dropping repositories older than $max_age_minutes minutes"
  find "$repo" -mindepth 1 -maxdepth 1 -type d -name "$pattern" -mmin "+$max_age_minutes" \
    2>/dev/null | sort > "$candidates" || return 0
  while IFS= read -r dir; do
    [ -d "$dir" ] || continue
    if in_use "$dir"; then
      log "WARN skipping $dir: written to within the last $min_idle minutes"
      continue
    fi
    log "removing $dir (aged out)"
    rm -rf "$dir" || log "WARN could not remove $dir"
  done < "$candidates"
}

reclaim_by_size() {
  if ! is_positive_int "$max_total_kib"; then
    if [ -n "$max_total_kib" ]; then
      log "WARN ignoring unusable size budget '$max_total_kib'; skipping the size pass"
    fi
    return 0
  fi
  used=$(kib "$repo")
  log "budget is $max_total_kib KiB, $used KiB in use"
  find "$repo" -mindepth 1 -maxdepth 1 -type d -name "$pattern" 2>/dev/null | sort \
    > "$candidates" || return 0
  matched=0
  skipped=0
  while IFS= read -r dir; do
    if [ "$used" -le "$max_total_kib" ]; then
      break
    fi
    [ -d "$dir" ] || continue
    matched=$(( matched + 1 ))
    if in_use "$dir"; then
      log "WARN skipping $dir: written to within the last $min_idle minutes"
      skipped=$(( skipped + 1 ))
      continue
    fi
    size=$(kib "$dir")
    log "removing $dir ($size KiB, over budget)"
    if rm -rf "$dir"; then
      used=$(( used - size ))
    else
      log "WARN could not remove $dir"
    fi
  done < "$candidates"
  if [ "$used" -gt "$max_total_kib" ]; then
    if [ "$matched" -eq 0 ]; then
      log "WARN still $used KiB over a $max_total_kib KiB budget and nothing matched '$pattern';" \
          "the JFR repository naming may have changed, or $repo holds data this script does not own"
    else
      log "WARN still $used KiB against a $max_total_kib KiB budget after cleanup" \
          "($skipped repositories skipped as recently written); the volume may fill"
    fi
  fi
}

main() {
  if [ -z "$repo" ]; then
    log "WARN PINOT_JFR_REPOSITORY is not set; nothing to do"
    return 0
  fi
  mkdir -p "$repo" 2>/dev/null || true
  if [ ! -d "$repo" ]; then
    log "WARN $repo does not exist and could not be created; nothing to do"
    return 0
  fi

  # Sorted candidates go to a file rather than a pipeline so the loops run in this shell and can
  # keep a running total.
  candidates="${TMPDIR:-/tmp}/jfr-janitor-candidates.$$"
  trap 'rm -f "$candidates"' EXIT

  log "$repo holds $(kib "$repo") KiB before cleanup"
  reclaim_by_age
  reclaim_by_size
  log "$repo holds $(kib "$repo") KiB after cleanup"
}

main || log "WARN cleanup did not complete; continuing so that Pinot can start"
exit 0
