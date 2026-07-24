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
package org.apache.pinot.core.query.aggregation.utils;

import it.unimi.dsi.fastutil.longs.AbstractLongSet;
import it.unimi.dsi.fastutil.longs.LongCollection;
import it.unimi.dsi.fastutil.longs.LongIterator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.pinot.spi.query.QueryThreadContext;


/// A `LongSet` for exact DISTINCT aggregation over LONG columns that stores distinct values as sorted runs and unions
/// them lazily.
///
/// Distinct LONG values read from a dictionary are duplicate-free, and value-sorted for immutable segments, so they
/// are kept as a `long[]` run without hashing (mutable/realtime dictionaries are insertion-ordered and get sorted
/// first). Merging two of these sets during the (serial) combine phase appends the other set's runs instead of
/// inserting every element into a hash table; the accumulated runs are unioned into one sorted, duplicate-free array
/// on demand, the first time [#size()], [#iterator()] or [#contains(long)] is called (i.e. when the result is
/// serialized or its distinct count / sum / average is extracted). The union runs on the calling query thread as a
/// multi-pass merge between two pre-sized buffers -- a single up-front allocation instead of one per pairwise merge,
/// no shared ForkJoin/common-pool usage -- and checks query termination between passes. To bound retained memory when
/// many segments carry overlapping values, pending runs are eagerly compacted (deduplicated) once their total length
/// crosses an internal threshold, so pending memory does not grow unboundedly with segment count.
///
/// This replaces per-element hashing (which dominated the wall-clock cost of high-cardinality DISTINCT_COUNT queries)
/// with sorted merging. Serialization as a `LongSet` and DISTINCT_SUM / DISTINCT_AVG value iteration work unchanged.
/// Inputs that are not already sorted-and-distinct (e.g. a `LongOpenHashSet` arriving via a mixed merge) are sorted
/// and de-duplicated before being added. One documented contract deviation: [#addAll(LongCollection)] returns `true`
/// for any non-empty operand, even if every element was already present -- computing the exact "changed" answer would
/// force the union eagerly. No caller on the aggregation path reads the return value.
///
/// Not thread-safe, and more strongly so than a typical mutable collection: the read accessors [#size()],
/// [#contains(long)] and [#iterator()] are *not* pure reads -- the first such call triggers `materialize()`, which
/// reassigns internal state to union the pending runs. Instances are therefore built per segment and both mutated and
/// first-read only by the single-threaded combine phase; a finished result must be safely published before any other
/// thread reads it, and the first read must not race a mutation.
///
/// Element removal is unsupported: `remove`/`rem` and the iterator's `remove()` are not implemented, so the inherited
/// `removeAll`/`retainAll` throw [UnsupportedOperationException]. The DISTINCT aggregation path never removes
/// elements (it only builds, merges, counts, sums, iterates and serializes).
public final class SortedLongDistinctSet extends AbstractLongSet {
  private static final long[] EMPTY = new long[0];
  private static final String SCOPE = "SortedLongDistinctSet";

  // Hard cap on the total pending values: beyond this the exact distinct result cannot be represented in one array.
  private static final long MAX_PENDING_TOTAL = Integer.MAX_VALUE - 8;

  // When the pending runs' total length crosses this, they are eagerly compacted into one deduplicated run inside
  // addAll(). This bounds peak retained memory to roughly the threshold plus the incoming run, instead of the sum of
  // all per-segment distinct counts -- which, when many segments carry overlapping values, can far exceed the global
  // distinct count. Large enough that it never triggers for typical segment counts and cardinalities.
  private static final long COMPACT_THRESHOLD = 8L << 20;

  // Pending sorted, duplicate-free, exact-length runs awaiting union. Non-null exactly when not yet materialized.
  private List<long[]> _runs;
  private long _pendingTotal;
  // Next pending total that triggers eager compaction. Doubles with the compacted size (geometric backoff) so that
  // when the true distinct count exceeds COMPACT_THRESHOLD, appends do not each trigger a full re-merge (which would
  // be quadratic in segment count); amortized compaction work stays O(n log n) and retained memory stays proportional
  // to the actual distinct count rather than the number of segments.
  private long _compactMin = COMPACT_THRESHOLD;

  // Materialized sorted, duplicate-free values in [0, _size). Non-null exactly when materialized. The array may be
  // longer than _size when the dedupe waste was immaterial (< 25%); every consumer bounds accesses by _size.
  private long[] _values;
  private int _size;

  /// Wraps a single run that is already sorted ascending and duplicate-free. Private so the checked [#fromValues]
  /// factory is the only entry point: an unsorted or duplicate-bearing array here would silently corrupt results
  /// ([#size()] over-counts, [#contains(long)] misreports) rather than fail loudly.
  private SortedLongDistinctSet(long[] sortedDistinct) {
    _runs = new ArrayList<>(4);
    if (sortedDistinct.length > 0) {
      _runs.add(sortedDistinct);
      _pendingTotal = sortedDistinct.length;
    }
  }

  /// Creates a set from the prefix `[0, size)` of `values`, which must be duplicate-free (dictionary values are, by
  /// construction). When `sorted` is false (mutable/realtime dictionaries are insertion-ordered) the prefix is sorted
  /// first; when true the caller guarantees it is already sorted ascending. The set takes ownership of `values` (it
  /// is retained without copying when `size == values.length`), so the caller must not modify the array after this
  /// call.
  public static SortedLongDistinctSet fromValues(long[] values, int size, boolean sorted) {
    if (!sorted) {
      Arrays.sort(values, 0, size);
    }
    return new SortedLongDistinctSet(size == values.length ? values : Arrays.copyOf(values, size));
  }

  /// Unions two distinct-value sets when segments may mix the no-scan path (which produces this class) with the scan
  /// path (which produces hash sets), e.g. a filter that matches all documents in only some segments: whichever side
  /// the `SortedLongDistinctSet` arrives on, it absorbs the other operand. Merge order of segment blocks is
  /// nondeterministic, and letting a hash set absorb a sorted set would re-hash every dictionary-sourced value,
  /// silently losing the sorted-run optimization. For any other type combination this behaves exactly like
  /// `set1.addAll(set2)`.
  @SuppressWarnings({"rawtypes", "unchecked"})
  public static Set union(Set set1, Set set2) {
    if (set2 instanceof SortedLongDistinctSet && !(set1 instanceof SortedLongDistinctSet)
        && set1 instanceof LongCollection) {
      ((SortedLongDistinctSet) set2).addAll((LongCollection) set1);
      return set2;
    }
    set1.addAll(set2);
    return set1;
  }

  /// Removes consecutive duplicates from the sorted prefix `[0, size)`; returns the distinct count.
  private static int dedupeSorted(long[] values, int size) {
    if (size <= 1) {
      return size;
    }
    int w = 1;
    for (int r = 1; r < size; r++) {
      if (values[r] != values[w - 1]) {
        values[w++] = values[r];
      }
    }
    return w;
  }

  /// Appends a sorted, duplicate-free, exact-length run to the pending list, guarding the representable-size cap and
  /// eagerly compacting when pending memory crosses [#COMPACT_THRESHOLD].
  private void appendRun(long[] run) {
    if (run.length == 0) {
      return;
    }
    if (_pendingTotal + run.length > MAX_PENDING_TOTAL) {
      throw new IllegalStateException(
          "Too many pending values for exact DISTINCT aggregation: " + (_pendingTotal + run.length)
              + ", consider an approximate function such as DISTINCT_COUNT_HLL");
    }
    _runs.add(run);
    _pendingTotal += run.length;
    if (_pendingTotal >= _compactMin && _runs.size() > 1) {
      long[] compacted = mergeRunsExact(_runs);
      _runs.clear();
      _runs.add(compacted);
      _pendingTotal = compacted.length;
      _compactMin = Math.max(COMPACT_THRESHOLD, 2L * _pendingTotal);
    }
  }

  /// Unions sorted, duplicate-free runs into one sorted, duplicate-free sequence via a multi-pass adjacent-pair merge
  /// between two pre-sized buffers on the calling thread: exactly two array allocations regardless of run count, no
  /// per-merge garbage, and a query-termination check between passes. Returns the buffer holding the result; the
  /// logical size is written to `sizeOut[0]` and may be smaller than the buffer where duplicates collapsed.
  private static long[] mergeRuns(List<long[]> runs, int[] sizeOut) {
    int numRuns = runs.size();
    if (numRuns == 1) {
      long[] only = runs.get(0);
      sizeOut[0] = only.length;
      return only;
    }
    long totalLong = 0;
    for (long[] run : runs) {
      totalLong += run.length;
    }
    int total = (int) totalLong; // guarded by MAX_PENDING_TOTAL in appendRun
    long[] src = new long[total];
    long[] dst = new long[total];
    int[] bounds = new int[numRuns + 1];
    int[] nextBounds = new int[numRuns + 1];
    int p = 0;
    int b = 0;
    for (long[] run : runs) {
      System.arraycopy(run, 0, src, p, run.length);
      p += run.length;
      bounds[++b] = p;
    }
    while (numRuns > 1) {
      QueryThreadContext.checkTerminationAndSampleUsage(SCOPE);
      int w = 0;
      int nb = 0;
      for (int i = 0; i + 1 < numRuns; i += 2) {
        w = unionInto(src, bounds[i], bounds[i + 1], bounds[i + 2], dst, w);
        nextBounds[++nb] = w;
      }
      if ((numRuns & 1) == 1) {
        int from = bounds[numRuns - 1];
        int len = bounds[numRuns] - from;
        System.arraycopy(src, from, dst, w, len);
        w += len;
        nextBounds[++nb] = w;
      }
      long[] tmp = src;
      src = dst;
      dst = tmp;
      int[] tmpBounds = bounds;
      bounds = nextBounds;
      nextBounds = tmpBounds;
      numRuns = nb;
    }
    sizeOut[0] = bounds[1];
    return src;
  }

  /// Like [#mergeRuns] but always returns an exact-length array, preserving the run-length invariant.
  private static long[] mergeRunsExact(List<long[]> runs) {
    int[] sizeOut = new int[1];
    long[] merged = mergeRuns(runs, sizeOut);
    return sizeOut[0] == merged.length ? merged : Arrays.copyOf(merged, sizeOut[0]);
  }

  /// Two-pointer union with dedupe of the adjacent sorted, duplicate-free chunks `[aFrom, aTo)` and `[aTo, bTo)` of
  /// `src` into `dst` starting at `w`; returns the new write position.
  private static int unionInto(long[] src, int aFrom, int aTo, int bTo, long[] dst, int w) {
    int i = aFrom;
    int j = aTo;
    while (i < aTo && j < bTo) {
      long av = src[i];
      long bv = src[j];
      if (av < bv) {
        dst[w++] = av;
        i++;
      } else if (av > bv) {
        dst[w++] = bv;
        j++;
      } else {
        dst[w++] = av;
        i++;
        j++;
      }
    }
    while (i < aTo) {
      dst[w++] = src[i++];
    }
    while (j < bTo) {
      dst[w++] = src[j++];
    }
    return w;
  }

  /// Unions all pending runs into the materialized array, trimming when the dedupe waste is material (>= 25%).
  private void materialize() {
    if (_runs == null) {
      return;
    }
    List<long[]> runs = _runs;
    _runs = null;
    if (runs.isEmpty()) {
      _values = EMPTY;
      _size = 0;
      return;
    }
    int[] sizeOut = new int[1];
    long[] merged = mergeRuns(runs, sizeOut);
    int size = sizeOut[0];
    _values = size + (size >> 2) < merged.length ? Arrays.copyOf(merged, size) : merged;
    _size = size;
  }

  /// Converts a materialized set back into a single-run pending state so more runs can be appended cheaply.
  private void demoteToRuns() {
    List<long[]> runs = new ArrayList<>(4);
    if (_size > 0) {
      runs.add(_size == _values.length ? _values : Arrays.copyOf(_values, _size));
    }
    _runs = runs;
    _pendingTotal = _size;
    _values = null;
    _size = 0;
  }

  @Override
  public int size() {
    materialize();
    return _size;
  }

  @Override
  public boolean isEmpty() {
    return _runs != null ? _pendingTotal == 0 : _size == 0;
  }

  @Override
  public boolean contains(long k) {
    materialize();
    return Arrays.binarySearch(_values, 0, _size, k) >= 0;
  }

  @Override
  public LongIterator iterator() {
    materialize();
    return new LongIterator() {
      private int _idx;

      @Override
      public boolean hasNext() {
        return _idx < _size;
      }

      @Override
      public long nextLong() {
        return _values[_idx++];
      }
    };
  }

  /// Merges another collection of longs into this set by appending its runs. Contract deviation (documented in the
  /// class doc): returns `true` for any non-empty operand even if no new element was added.
  @Override
  public boolean addAll(LongCollection c) {
    if (c.isEmpty()) {
      return false;
    }
    if (_runs == null) {
      demoteToRuns();
    }
    if (c instanceof SortedLongDistinctSet) {
      SortedLongDistinctSet other = (SortedLongDistinctSet) c;
      if (other._runs != null) {
        // Sharing run references is safe: runs are never mutated in place once appended.
        for (long[] run : other._runs) {
          appendRun(run);
        }
      } else {
        appendRun(Arrays.copyOf(other._values, other._size));
      }
      return true;
    }
    long[] run = c.toLongArray();
    Arrays.sort(run);
    int n = dedupeSorted(run, run.length);
    appendRun(n == run.length ? run : Arrays.copyOf(run, n));
    return true;
  }

  /// Inserts a single value, keeping the sorted invariant. This is an O(n) operation (materialize + binary search +
  /// array copy) provided only to honor the `LongSet` contract; it is not used on the aggregation path (which builds
  /// via [#fromValues] and merges via [#addAll]). Do not call it in a loop -- repeated single inserts are O(n^2). Use
  /// [#addAll] to combine sets.
  @Override
  public boolean add(long k) {
    materialize();
    int pos = Arrays.binarySearch(_values, 0, _size, k);
    if (pos >= 0) {
      return false;
    }
    int insert = -(pos + 1);
    // Build a fresh array rather than mutating in place: after a merge, _values may alias a run still referenced by
    // another set, so an in-place shift could corrupt it.
    long[] updated = new long[_size + 1];
    System.arraycopy(_values, 0, updated, 0, insert);
    updated[insert] = k;
    System.arraycopy(_values, insert, updated, insert + 1, _size - insert);
    _values = updated;
    _size++;
    return true;
  }
}
