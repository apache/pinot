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

The Pinot digest implementation removes `com.tdunning:t-digest` while reading legacy verbose and compact BYTES.
Plugins that used tdunning types must recompile against `org.apache.pinot.segment.local.customobject.tdigest.TDigest`.
Create an empty general/Star-tree digest with `TDigestAccumulator.forLegacyAggregation(100)` or an empty
SQL reducer with `TDigestAccumulator.forReduction(100)`. `TDigestAccumulator.fromBytes(bytes)`
reads and retains stored input. Use `digest.add(value, doubleWeight)` and `byte[] bytes = digest.serialize()`;
each serialization returns independently owned bytes. Compact decoding and compatible output selection are automatic.
`TDigestCodec` handles encoding/validation; `SerializedTDigestInput` can be reused for group fanout. The digest classes,
including `NonFiniteAwareTDigest` and `SerializedTDigest`, reside in the same `customobject.tdigest` package.
Centroid mass is available through `Centroid.weight()` and `getTotalWeight()` without integer truncation.

Release note: native digest constructors and factories normalize finite compression settings below 10 to 10,
matching tdunning 3.3. This changes previously rejected accumulator query inputs such as
`PERCENTILETDIGEST(col, 50, 0)` and SmartTDigest's `compression=-5` to use compression 10 once a digest is needed.
The standard SQL factory still rejects negative compression literals. NaN and infinite compression remain invalid.

Release note: an identifiable infinity tail from historical tdunning arithmetic remains usable. NaN extrema,
ambiguous infinity mixtures, non-finite weights and overflowing weight totals remain opaque. Other numerically
corrupted payloads retain their original bytes and return NaN statistics, including when their weights sum to zero
and null handling is enabled; they no longer appear empty or return SQL NULL. Mixing them with additional input fails
instead of subtracting invalid mass from a healthy distribution. Fresh fractional boundary mass below one, or a
fractional singleton below two, cannot satisfy legacy unit-endpoint requirements and is rejected on serialization;
unchanged historical encodings remain readable and can be retained byte-for-byte, even at a different configured
compression. When healthy input leaves an inherited fractional boundary unrepresentable, Pinot writes its exact
merged mass in the historical verbose form rather than failing intermediate-result serialization. This retains
the existing legacy limitation: assertion-enabled t-digest 3.3 can fail when recompressing these fractional forms,
including during the old Pinot wrapper's initial deserialization. Fresh unsupported boundaries remain rejected.

The native accumulator preserves historical weighted boundary centroids when read. Quantile/CDF values follow Pinot's
plateau interpolation rule described below. Unit boundary repair is deferred until the digest is modified or rewritten.
Repair requires exact endpoint mass subtraction and preserves the first moment within bounded floating-point roundoff.
Residual means may be clamped within four ULPs of the crossed neighbor; two-point means may differ by the sum of the
half-endpoint operand ULPs. Beyond those bounds, the unchanged payload remains readable and serializes to its original
bytes; modifying it or merging it with nonempty input fails with
`Cannot merge or mutate a historical TDigest whose unit boundary
repair cannot preserve its mass or first moment`. Rebuild these digests from the original measurements before merging them.

Infinity-tail additions and merges fail when double precision would absorb a positive tail contribution, such as
adding one infinity row to a tail of weight 2^53, or cannot subtract a unit endpoint exactly, such as a tail of
weight 2^53 + 2. These historical payloads remain readable and retain their original bytes; rebuild them before
mutation or merging with nonempty input. The native accumulator's
CDF retains defined values after negative-infinity tails; interpolation before a positive-infinity neighbor follows
the 3.3 half-centroid and unit-exclusion rules. This changes the old Pinot accumulator on unchanged stored bytes:
for centroids `(mean=5, weight=4)` and `(mean=+Infinity, weight=1)`, `cdf(6)` changes from 0.8 to 0.4.
The infinity wrapper retains its existing finite/tail CDF formula. For stored centroids `(-Infinity, 3)`, `(10, 4)`,
and `(20, 1)`, `cdf(0)` remains 3/8 as in the old Pinot accumulator; the genuine t-digest 3.3 reader returns NaN.

Weighted infinite quantile interpolation can also differ on unchanged bytes: for `(-Infinity, 2)` and
`(+Infinity, 2)`, `quantile(0.5)` returns -Infinity, versus +Infinity in the old Pinot accumulator and NaN in
t-digest 3.3. A zero interpolation weight also avoids `0 * Infinity`: for `(0, 1)`, `(10, 4)`,
and `(+Infinity, 3)`, `quantile(0.375)` returns 10, matching the old Pinot accumulator, versus NaN in 3.3.
Infinity handling does not guarantee that every historical weighted-infinity quantile is defined.

Weighted-add callers can use fractional interior mass. Whether a boundary is representable depends on the final
distribution: `add(5.0, 0.5)` succeeds, but serializing that fresh singleton fails; adding unit mass at 4.0 and 6.0
makes the fractional mass interior and serializable. Rejecting every fractional add would also reject valid interior mass.

Operator remediation: an error starting with `Cannot merge or mutate a historically corrupted TDigest` identifies
a corrupt or ambiguous stored distribution. Mixing such a payload with nonempty input fails the query, merge-rollup
task, or TDigest star-tree build, even when all other inputs are healthy. A single corrupt row in one segment can
therefore fail a query spanning many segments; there is no automatic per-row quarantine or NaN fallback during merges.
This replaces the approximate values older readers could return for negative weights, NaN means or far-out-of-range
means; there is no configuration to restore that lenient behavior. A rate-limited WARN from
`TDigestAccumulator` reports when an input first becomes degraded, with encoding and size metadata and
rebuild guidance. It does not contain stored values or segment identity; use the query's segment context to locate
affected data.
Healthy empty inputs remain no-ops. Identify affected stored BYTES with
`TDigestAccumulator.fromBytes(bytes).hasValidStatistics()`, rebuild their digests from original measurements or
reingest affected segments from a known-good source, then rerun rollup or rebuild the star-tree. Retrying the same
stored bytes cannot repair the distribution. These payloads remain readable individually with NaN statistics and
their original bytes, including inverted extrema; structural errors such as truncated encodings still fail validation.
Raw percentile results retain `Double.compare` ordering: a corrupted group's NaN quantile sorts last in ascending
order and first in descending order, ahead of positive infinity. Such a group can occupy an `ORDER BY ... DESC LIMIT`
slot; repair the affected data as described above before relying on that ranking.

Downgrade note: retaining an opaque payload does not make it safe for an old reader. For example, Pinot preserves
a verbose payload with compression 100, 300 centroids and a NaN interior mean byte-for-byte, while both tdunning
3.2 and 3.3 throw `ArrayIndexOutOfBoundsException` during decoding. A retained degraded header with compression 5
and 25 centroids also exceeds 3.2's raw-header allocation; 3.3 clamps the header before allocating and can decode it.
The isolated legacy-reader gate verifies both cases. Byte-exact historical fractional or unrepairable-boundary
payloads can also exceed an old reader's allocation despite having valid statistics: for example, compression 100.5 with 212 centroids exceeds
the 3.3 reader's capacity of 211. Rebuild affected stored digests from the original measurements before a downgrade
or before allowing them to reach old readers in a mixed-version cluster.

Release note: a healthy stored digest with total weight between zero and one now survives percentile merges and
returns its percentile, including with null handling enabled. Previously, integer truncation treated that digest
as empty, dropped it during merges and returned SQL NULL (or NaN without null handling). For example, a stored
centroid of weight 0.5 at 42 now returns 42. Numerically corrupted digests whose weights sum to zero or NaN remain
present and return NaN when read individually, including with null handling enabled, instead of the previous SQL NULL.
This is an intentional distinction between present invalid data and an empty input; SQL `IS NULL` and JDBC null
mapping no longer identify these degraded results as NULL. Merging them with healthy nonempty input fails as
described above; null handling does not suppress corruption.

Release note: `percentileSmartTDigest` over multi-value columns with null handling enabled now respects each
non-null row range. Previously, a batch containing null rows replayed all rows for every non-null range,
including null rows and duplicating values across ranges. Queries now include each non-null row once and skip null
rows; this correction can change percentile results independently of the digest migration.

Release note: an empty `percentileSmartTDigest` digest now returns the same result as its empty value-list state:
NULL with null handling enabled, otherwise -Infinity. Previously that digest state returned NaN without null
handling. `percentileTDigest` retains its existing NaN result without null handling.

Release note: duplicate-value plateaus are treated as point mass when interpolating quantile/CDF boundaries.
This improves accuracy near large repeated-value populations, but can change quantiles and CDF values across the upgrade,
including intervals adjacent to a plateau, even when evaluating the same stored centroid bytes. Legacy byte encodings
remain compatible; quantile/CDF values need not be bit-identical to tdunning's interpolation.

Run `compatibility-verifier/tdigest-compatibility/run.sh` to exercise
the real 3.2 and 3.3 readers and verify Pinot reads verbose/compact bytes written by those libraries. The script
reuses generated fixtures or explicitly runs `LegacyTDigestCompatibilityTest` if its manifest or isolated reader
classpath is missing. Generation failures remain fatal. The Pinot reader includes only Pinot classes and the
SLF4J/Guava runtime jars used by its diagnostics, and rejects any classpath containing tdunning classes. Legacy jars
remain isolated test-tool artifacts resolved through the cached Maven repository; uncached runs need Maven Central.
The script also runs `compatibility-verifier/tdigest-compatibility/generate-rank-errors.sh` to verify the independent 3.3
accuracy oracle.

The oracle pins Pinot's `f818bae6d75a91228eb3aac95c921bca4aeb2128` utility and t-digest 3.3. Its reference CI
environment is Ubuntu x86_64 with Temurin 25. `StrictMath` makes the seeded input fingerprints portable, but the
legacy library uses `Math` trigonometric functions, so exact rank-error output is not guaranteed across architectures
or JVM implementations. Investigate a regeneration difference on that reference environment before updating the CSV;
do not automatically replace the frozen reference after a mismatch on another architecture.
