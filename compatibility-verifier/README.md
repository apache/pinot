<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# Compatibility Regression Testing Scripts for Apache Pinot

## Usage

### Step 1: checkout source code and build targets for older commit and newer commit
```shell
Usage: checkoutAndBuild.sh [-o olderCommit] [-n newerCommit] -w workingDir
  -w, --working-dir                      Working directory where olderCommit and newCommit target files reside

  -o, --old-commit-hash                  git hash (or tag) for old commit

  -n, --new-commit-hash                  git hash (or tag) for new commit

If -n is not specified, then current commit is assumed
If -o is not specified, then previous commit is assumed (expected -n is also empty)
Examples:
    To compare this checkout with previous commit: 'checkoutAndBuild.sh -w /tmp/wd'
    To compare this checkout with some older tag or hash: 'checkoutAndBuild.sh -o release-0.7.1 -w /tmp/wd'
    To compare any two previous tags or hashes: 'checkoutAndBuild.sh -o release-0.7.1 -n 637cc3494 -w /tmp/wd

```

### Step 2: run compatibility regression test against the two targets build in step1
```shell
./compCheck.sh -h
Usage:  -w <workingDir> -t <testSuiteDir> [-k]
MANDATORY:
  -w, --working-dir                      Working directory where olderCommit and newCommit target files reside.
  -t, --test-suite-dir                   Test suite directory

OPTIONAL:
  -k, --keep-cluster-on-failure          Keep cluster on test failure
  -h, --help                             Prints this help
```

## T-digest migration

The Pinot digest implementation removes `com.tdunning:t-digest` while reading legacy verbose and compact BYTES.
Plugins that used tdunning types must recompile against `org.apache.pinot.segment.local.customobject.tdigest.TDigest`.
Create an empty general/Star-tree digest with `PercentileTDigestAccumulator.forLegacyAggregation(100)` or an empty
SQL reducer with `PercentileTDigestAccumulator.forReduction(100)`. `PercentileTDigestAccumulator.fromBytes(bytes)`
reads and retains stored input. Use `digest.add(value, doubleWeight)` and `byte[] bytes = digest.serialize()`;
each serialization returns independently owned bytes. Compact decoding and compatible output selection are automatic.
`TDigestCodec` handles encoding/validation; `SerializedTDigestInput` can be reused for group fanout. The digest classes,
including `NonFiniteAwareTDigest` and `SerializedTDigest`, reside in the same `customobject.tdigest` package.
Centroid mass is available through `Centroid.weight()` and `getTotalWeight()` without integer truncation.

Release note: an identifiable infinity tail from historical tdunning arithmetic remains usable. NaN extrema and
ambiguous infinity mixtures remain opaque. Other numerically
corrupted payloads retain their original bytes and return NaN statistics, including when their weights sum to zero
and null handling is enabled; they no longer appear empty or return SQL NULL. Mixing them with additional input fails
instead of subtracting invalid mass from a healthy distribution. Fresh fractional boundary mass below one, or a
fractional singleton below two, cannot satisfy legacy unit-endpoint requirements and is rejected on serialization;
unchanged historical encodings remain readable and can be retained byte-for-byte, even at a different configured
compression. When healthy input leaves an inherited fractional boundary unrepresentable, Pinot writes its exact
merged mass in the historical verbose form rather than failing intermediate-result serialization. This retains
the existing legacy limitation: assertion-enabled t-digest 3.3 can fail when recompressing these fractional forms,
including during the old Pinot wrapper's initial deserialization. Fresh unsupported boundaries remain rejected.

Operator remediation: an error starting with `Cannot merge or mutate a historically corrupted TDigest` identifies
a corrupt or ambiguous stored distribution. Mixing such a payload with nonempty input fails the query, merge-rollup
task, or TDigest star-tree build. Healthy empty inputs remain no-ops. Identify affected stored BYTES with
`PercentileTDigestAccumulator.fromBytes(bytes).hasValidStatistics()`, rebuild their digests from original measurements or
reingest affected segments from a known-good source, then rerun rollup or rebuild the star-tree. Retrying the same
stored bytes cannot repair the distribution. These payloads remain readable individually with NaN statistics and
their original bytes, including inverted extrema; structural errors such as truncated encodings still fail validation.

Release note: `percentileSmartTDigest` over multi-value columns with null handling enabled now respects each
non-null row range. Previously, a batch containing null rows replayed all rows for every non-null range,
including null rows and duplicating values across ranges. Queries now include each non-null row once and skip null
rows; this correction can change percentile results independently of the digest migration.

After the affected tests generate fixtures, run `compatibility-verifier/tdigest-compatibility/run.sh` to exercise
the real 3.2 and 3.3 readers, and `compatibility-verifier/tdigest-compatibility/generate-rank-errors.sh` to verify
the independent 3.3 accuracy oracle.
