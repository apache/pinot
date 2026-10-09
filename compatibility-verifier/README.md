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
Plugins that used tdunning types must recompile against `org.apache.pinot.segment.local.customobject.TDigest`;
use `TDigestUtils.createMergingDigest(100)`, `digest.add(value, doubleWeight)`, and `TDigestUtils.serialize(digest)`.
Centroid mass is available through `Centroid.weight()` and `getTotalWeight()` without integer truncation.

Release note: an identifiable infinity tail from historical tdunning arithmetic remains usable. NaN extrema and
ambiguous infinity mixtures remain opaque. Other numerically
corrupted payloads retain their original bytes and return NaN statistics, including when their weights sum to zero
and null handling is enabled; they no longer appear empty or return SQL NULL. Mixing them with additional input fails
instead of subtracting invalid mass from a healthy distribution. Fresh fractional boundary mass below one, or a
fractional singleton below two, cannot satisfy legacy unit-endpoint requirements and is rejected on serialization;
unchanged historical encodings remain readable and can be retained byte-for-byte.

After the affected tests generate fixtures, run `compatibility-verifier/tdigest-compatibility/run.sh` to exercise
the real 3.2 and 3.3 readers, and `compatibility-verifier/tdigest-compatibility/generate-rank-errors.sh` to verify
the independent 3.3 accuracy oracle.
