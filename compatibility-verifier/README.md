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

## T-digest migration

Pinot removes `com.tdunning:t-digest` while retaining legacy verbose/compact BYTES and object type 10.
Plugins using tdunning types must recompile against `org.apache.pinot.segment.local.customobject.tdigest.TDigest`.
Use `TDigestAccumulator.forLegacyAggregation(compression)` for general/Star-tree aggregation,
`forReduction(compression)` for SQL reducers, or `fromBytes(bytes)` for stored input. Add values with
`digest.add(value, doubleWeight)`; `serialize()` returns independently owned bytes. `Centroid.weight()` and
`getTotalWeight()` expose fractional mass without integer truncation.

### Behavior changes

Byte compatibility does not imply identical quantile/CDF results:

- Duplicate-value plateaus are treated as point mass, changing interpolation within and adjacent to plateaus.
- Healthy stored mass between zero and one survives merges and returns its percentile instead of being dropped
  or returning SQL NULL/NaN; a stored centroid `(42, 0.5)` returns 42.
- Corrupt payloads remain present with NaN statistics, even for zero/NaN total mass with null handling enabled.
  They no longer appear empty or return SQL NULL; `IS NULL` and JDBC null mapping cannot identify them.
- SmartTDigest MV aggregation with null handling now includes each non-null row once. Previously, batches with
  null rows replayed all rows for each non-null range, including null rows and duplicate values.
- An empty SmartTDigest digest returns NULL with null handling, otherwise -Infinity, matching its empty list state
  (previously NaN without null handling). Ordinary TDigest still returns NaN without null handling.
- Native factories normalize finite compression below 10 to 10, including formerly rejected accumulator inputs
  such as `PERCENTILETDIGEST(col, 50, 0)` and SmartTDigest `compression=-5`. The standard SQL factory still rejects
  negative literals; NaN/infinite compression remains invalid.

Infinity interpolation also differs. Native CDF now retains defined values after negative-infinity tails and
uses 3.3 half-centroid/unit-exclusion rules before positive infinity. For centroids `(5, 4), (+Infinity, 1)`,
`cdf(6)` changes from the old Pinot accumulator's 0.8 to 0.4. The infinity wrapper keeps its finite/tail formula:
for `(-Infinity, 3), (10, 4), (20, 1)`, `cdf(0)` remains 3/8 while 3.3 returns NaN.
For `(-Infinity, 2), (+Infinity, 2)`, `quantile(0.5)` returns -Infinity (old Pinot: +Infinity; 3.3: NaN).
Zero interpolation weights avoid `0 * Infinity`: `(0, 1), (10, 4), (+Infinity, 3)` returns 10 at quantile 0.375
as old Pinot did, versus NaN in 3.3. Not every weighted-infinity quantile is defined.

### Historical payload limits and remediation

Identifiable infinity tails remain usable. NaN extrema, ambiguous infinity mixtures, non-finite/overflowing
weights and other numerical corruption remain opaque: individually readable with NaN statistics and byte-exact
serialization, but merging with nonempty input fails the query, rollup or star-tree build. There is no lenient
mode or per-row quarantine; healthy empty inputs remain no-ops. Structural errors such as truncation still fail
validation. RAW results use `Double.compare`: a NaN group sorts last ascending and first descending, so it can
occupy a descending LIMIT slot.

An error starting with `Cannot merge or mutate a historically corrupted TDigest` identifies this condition.
A rate-limited WARN from `TDigestAccumulator` includes encoding/size metadata, but no stored values or segment
identity. Locate affected data using the query's segment context and `fromBytes(bytes).hasValidStatistics()`.
Rebuild from original measurements or reingest from a known-good source before rerunning queries, rollup or
star-tree builds; retrying the same bytes cannot repair them.

Historical weighted boundaries retain their bytes until modified or rewritten, even at a different compression.
Unit-endpoint repair requires exact mass subtraction and bounded first-moment roundoff. If repair cannot
preserve these, unchanged bytes stay readable, but mutation/nonempty merges fail with
`Cannot merge or mutate a historical TDigest whose unit boundary repair cannot preserve its mass or first moment`.
Infinity-tail updates likewise reject absorbed positive mass or inexact unit subtraction (for example, adding
one to weight 2^53 or splitting weight 2^53 + 2). Rebuild these digests before mutation or nonempty merges.

Fresh fractional boundaries below unit mass, or a fractional singleton below mass two, cannot be serialized with legacy
unit endpoints. Fractional interior mass remains supported: `add(5, 0.5)` needs unit mass at 4 and 6 before
serialization. Historical fractional bytes remain readable and byte-exact; after a merge, inherited fractional
boundaries use exact verbose weights when unit repair is impossible. These forms retain t-digest 3.3's
assertion-enabled deserialization/recompression limitation; fresh unsupported boundaries still fail serialization.

Byte retention does not guarantee downgrade safety. Retained corrupt or unrepairable/fractional payloads can
exceed old readers' header-based capacity, even when statistics are valid (compression 100.5 with 212 centroids
exceeds 3.3's capacity of 211). Rebuild affected digests before downgrading or exposing them to old readers.

### Verification

Run `compatibility-verifier/tdigest-compatibility/run.sh` for genuine 3.2/3.3 reads and writes, including the
historical limitations above, and the frozen accuracy oracle described earlier. It reuses fixtures or runs
`LegacyTDigestCompatibilityTest` to generate them; missing fixtures remain fatal. Legacy jars are isolated from
the Pinot reader and are test-tool artifacts only. Uncached runs require network access. The oracle's
reference-environment and update rules above apply.
