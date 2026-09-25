/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.query.distinct;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import javax.annotation.Nullable;
import org.apache.datasketches.theta.UpdatableThetaSketch;
import org.apache.pinot.core.query.distinct.table.DistinctTable;
import org.apache.pinot.spi.query.QueryThreadContext;


/// Tracks how many distinct values a streaming distinct leaf has emitted so far, across flush windows.
///
/// [org.apache.pinot.core.operator.streaming.StreamingDistinctCombineOperator] flushes its accumulated
/// [DistinctTable] every `streamingDistinctFlushThreshold` values, which empties the accumulator and therefore
/// prevents [DistinctTable#isSatisfied()] from ever firing. This tracker restores that early exit by measuring the
/// cardinality of the *union* of everything flushed so far.
///
/// A running count of emitted rows would not do: flush windows overlap. A window emitting `{a,b,c}` followed by one
/// emitting `{b,c,d}` is 6 rows but 4 distinct values, so a row counter overshoots LIMIT and would let the leaf exit
/// having emitted fewer than LIMIT distinct values -- a silently truncated result that nothing downstream can
/// repair.
///
/// ## Two regimes, and only one of them is sound
///
/// The split is at `streamingDistinctMaxTrackedCardinality`, and the difference between the two sides is a
/// difference in kind, not in accuracy:
///
/// - **LIMIT at or below the bound -- exact, and provably sound.** An exact set of value hashes, capped at LIMIT
///   entries since nothing past LIMIT changes the answer. Memory is ~8 bytes per tracked value, bounded by the
///   configured value. Because equal values hash equally, the set's size can never exceed the number of distinct
///   values emitted, so reaching LIMIT is a *counted fact*: the exit fires at exactly LIMIT and can never stop
///   short. This regime is on by default.
/// - **LIMIT above the bound -- estimated, and NOT sound.** A theta sketch of that many nominal entries, so memory
///   stays constant whatever LIMIT is. A sketch lower bound is a confidence bound, not a guarantee: DataSketches
///   documents `getLowerBound` as the "approximate lower error bound", computed by `BinomialBoundsN`, which
///   documents itself as *estimating* error bounds. **There is no setting that makes this regime sound**; there is
///   only a choice of how unlikely an unsound exit is. It is therefore **off by default**, gated behind
///   `streamingDistinctEstimatedExitStdDev`, and when off these queries read every segment exactly as they did
///   before this feature existed.
///
/// ## What the estimating regime risks, and how likely it is
///
/// [#reachedLimit()] reads the lower bound rather than the point estimate, which makes the error one-sided in the
/// usual case: sketch error is two-sided, and an overestimate means exiting with fewer than LIMIT distinct values
/// emitted -- a result that is short by a few rows, with no error raised and no partial-result flag. Reading the
/// lower bound turns most of that into the harmless direction, exiting a little later than necessary (~1.02x LIMIT).
///
/// What remains is a ceiling, not a rate. The bound is evaluated once per flush window, but those evaluations share
/// one cumulative sketch and are strongly correlated, so the per-query probability is dominated by the single
/// evaluation closest below LIMIT rather than compounding over the windows. Measured over 20k trials per
/// configuration with a flush boundary landing one value below LIMIT (the worst alignment): **~2.2% of queries at 2
/// standard deviations and ~0.12% at 3**, and it does not grow with LIMIT.
///
/// Raising the nominal entries does **not** reduce this -- it only reduces the overshoot. The configured bound is
/// the lever for memory and lateness; moving a query into the exact regime is the only lever that removes the risk.
///
/// Not thread-safe: the streaming combine operator owns one of these as main-thread-only state.
public abstract class DistinctCardinalityTracker {
  /// Bounds accepted by the theta sketch for its nominal entries.
  private static final int MIN_NOMINAL_ENTRIES = 16;
  private static final int MAX_NOMINAL_ENTRIES = 1 << 26;

  /// Ceiling on the exact regime, whatever `streamingDistinctMaxTrackedCardinality` asks for. The exact set holds
  /// one 8-byte hash per tracked value, so an unclamped request would let a query option drive server heap without
  /// limit -- on the very operator that exists to bound it. At this ceiling the set costs ~16 MiB at its widest.
  ///
  /// Clamping degrades safely rather than failing: a LIMIT above the ceiling falls through to the estimating
  /// branch, which is off unless opted into and otherwise yields no tracker at all, leaving the leaf to read every
  /// segment exactly as it does without this feature.
  private static final int MAX_TRACKED_CARDINALITY = 1 << 20;

  private static final String ADD_SCOPE = "DistinctCardinalityTracker#add";

  protected final int _limit;

  private int _numHashed;

  /// Returns a tracker for a leaf with the given LIMIT, or `null` when tracking cannot help:
  ///
  /// - `limit <= flushThreshold`: the accumulator reaches LIMIT inside a single window, so
  ///   [DistinctTable#isSatisfied()] already short-circuits and nothing needs to be carried across windows. The gate
  ///   in [org.apache.pinot.core.plan.CombinePlanNode] keeps such queries off the streaming operator entirely, but
  ///   that operator is public and constructible directly, so this check stands on its own.
  /// - `limit == Integer.MAX_VALUE`: an MSE leaf with no LIMIT pushed down. No cardinality can ever reach it, so a
  ///   tracker would be pure overhead.
  /// - `hasOrderBy`: an ORDER BY DISTINCT must never exit early. The top-LIMIT is not known until every segment has
  ///   been seen, so stopping after LIMIT distinct values would return an arbitrary LIMIT rather than the ordered
  ///   top-LIMIT. This mirrors [DistinctTable#isSatisfied()], which returns `false` outright when ordered. Unlike
  ///   the other gates, violating this one yields silently wrong rows rather than duplicates.
  /// - `maxTrackedCardinality <= 0`: the early exit is switched off by configuration. Values above
  ///   [#MAX_TRACKED_CARDINALITY] are clamped to it rather than honoured, so a query option cannot drive unbounded
  ///   server heap.
  /// - `limit > maxTrackedCardinality && estimatedExitStdDev <= 0`: the query would need the estimating regime,
  ///   which is unsound and off by default. The leaf then reads every segment, as it does without this feature.
  @Nullable
  public static DistinctCardinalityTracker createIfUseful(int limit, int flushThreshold, boolean hasOrderBy,
      int maxTrackedCardinality, int estimatedExitStdDev) {
    if (hasOrderBy || limit <= flushThreshold || limit == Integer.MAX_VALUE || maxTrackedCardinality <= 0) {
      return null;
    }
    int bound = Math.min(maxTrackedCardinality, MAX_TRACKED_CARDINALITY);
    if (limit <= bound) {
      return new Exact(limit);
    }
    return estimatedExitStdDev > 0 ? new Estimating(limit, bound, estimatedExitStdDev) : null;
  }

  protected DistinctCardinalityTracker(int limit) {
    _limit = limit;
  }

  /// Folds the values of a flushed table into the running measurement.
  ///
  /// [DistinctTable#forEachValueHash] walks the table's own typed value set, so nothing is boxed and this class
  /// needs no knowledge of stored types. It enumerates exactly what the block streams downstream, which is what
  /// makes this a count of values actually emitted.
  public final void add(DistinctTable distinctTable) {
    distinctTable.forEachValueHash(hash -> {
      QueryThreadContext.checkTerminationAndSampleUsagePeriodically(_numHashed++, ADD_SCOPE);
      addHash(hash);
    });
  }

  /// Returns `true` once the leaf has emitted at least LIMIT distinct values.
  ///
  /// The guarantee this underwrites is the same one the non-streaming path relies on: it is per-server, and a server
  /// that has emitted at least LIMIT distinct values guarantees the global result holds at least LIMIT, so applying
  /// LIMIT downstream yields a full result.
  public final boolean hasReachedLimit() {
    return reachedLimit();
  }

  protected abstract void addHash(long hash);

  protected abstract boolean reachedLimit();

  /// Exact regime: holds the 64-bit hash of every emitted value, capped at LIMIT entries since nothing beyond LIMIT
  /// can change the answer. Cheaper than a sketch held in its exact range -- no nominal-entry array to size, and no
  /// second hash per value -- and it removes the confidence-bound reasoning entirely.
  private static final class Exact extends DistinctCardinalityTracker {
    private final LongOpenHashSet _hashes;

    private Exact(int limit) {
      super(limit);
      _hashes = new LongOpenHashSet(Math.min(limit, DistinctTable.MAX_INITIAL_CAPACITY));
    }

    @Override
    protected void addHash(long hash) {
      if (_hashes.size() < _limit) {
        _hashes.add(hash);
      }
    }

    @Override
    protected boolean reachedLimit() {
      return _hashes.size() >= _limit;
    }
  }

  /// Estimating regime, for LIMITs too large to hold exactly within the configured bound.
  private static final class Estimating extends DistinctCardinalityTracker {
    private final UpdatableThetaSketch _sketch;
    private final int _numStdDev;

    private Estimating(int limit, int maxTrackedCardinality, int numStdDev) {
      super(limit);
      _sketch = UpdatableThetaSketch.builder().setNominalEntries(nominalEntriesFor(maxTrackedCardinality)).build();
      _numStdDev = numStdDev;
    }

    /// Rounds the configured bound *down* to a power of two (the sketch requires one) so the sketch never costs more
    /// than was asked for, then clamps it to the range the sketch accepts.
    private static int nominalEntriesFor(int maxTrackedCardinality) {
      int nominalEntries = Integer.highestOneBit(maxTrackedCardinality);
      return Math.min(MAX_NOMINAL_ENTRIES, Math.max(MIN_NOMINAL_ENTRIES, nominalEntries));
    }

    @Override
    protected void addHash(long hash) {
      _sketch.update(hash);
    }

    /// Reads the lower bound, never [UpdatableThetaSketch#getEstimate()]. Note that DataSketches clamps the
    /// returned value below by the retained-entry count, so while the sketch is still in exact mode this is the
    /// true count and the exit is sound; it becomes a confidence bound only once the sketch starts discarding.
    @Override
    protected boolean reachedLimit() {
      return _sketch.getLowerBound(_numStdDev) >= _limit;
    }
  }
}
