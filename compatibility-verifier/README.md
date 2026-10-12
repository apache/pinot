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

## T-digest accuracy baseline

Run `compatibility-verifier/tdigest-compatibility/generate-rank-errors.sh` from a checkout with JDK 25+ to verify the
270 seeded case/input fingerprints and rank-error envelopes in
`pinot-core/src/test/resources/data/tdigest-3.3-k1-rank-errors.csv`. The standalone script compiles only the generator,
Pinot's pinned `f818bae6d75a91228eb3aac95c921bca4aeb2128` utility and the genuine `com.tdunning:t-digest:3.3` jar;
it does not depend on the current accumulator or a Pinot build. It fetches the pinned commit and jar if missing.

The reference environment is Ubuntu x86_64 with Temurin 25. `StrictMath` makes seeded input fingerprints portable,
but the legacy library's `Math` trigonometric functions can change exact rank errors across architectures or JVMs.
Investigate regeneration differences on the reference environment before changing the CSV. Use `--update` only
after intentionally changing the seeded corpus; it writes an unverified reference that must be reviewed.
