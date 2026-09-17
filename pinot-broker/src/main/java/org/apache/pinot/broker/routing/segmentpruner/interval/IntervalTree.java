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
package org.apache.pinot.broker.routing.segmentpruner.interval;

import it.unimi.dsi.fastutil.ints.IntArrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;


/// The `IntervalTree` class represents a read-only balanced binary interval tree map (from intervals to values).
///
/// The distinct intervals are held sorted in [#_intervals], and the balanced tree over them is implicit: the root of
/// the index range `[start, end)` is `start + (end - start) / 2`, its left child is the root of `[start, mid)` and its
/// right child the root of `[mid + 1, end)`. A typical balanced tree:
/// ```
///                              [10, 20]
///                              /       \
///                       [8, 15]         [12, 20]
///                          /            /
///                   [5, 10]       [10, 30]
/// ```
/// is held as the sorted array `{ [5, 10], [8, 15], [10, 20], [10, 30], [12, 20] }`.
///
/// The tree is held implicitly and its payload in flat arrays because a tree is rebuilt whenever the segments of a
/// table change, and on a table with hundreds of thousands of segments an object per interval dominates broker
/// allocation. See [org.apache.pinot.broker.routing.segmentpruner.TimeSegmentPruner] for how rebuilds are batched.
///
/// Instances are immutable and safe to publish to readers through a volatile field.
public class IntervalTree<VALUE> {
  /// The distinct intervals, sorted ascending by `(min, max)`.
  private final Interval[] _intervals;
  /// Max interval end of the subtree rooted at each node, used to skip subtrees that cannot match.
  private final long[] _subtreeMaxs;
  /// The values mapped to `_intervals[i]` are `_values[_valueOffsets[i]]` (inclusive) through
  /// `_values[_valueOffsets[i + 1]]` (exclusive). Holds one extra trailing entry so the last node has an end offset.
  private final int[] _valueOffsets;
  /// Every key of the map the tree was built from, grouped by interval. Typed as `Object[]` because a `VALUE[]`
  /// cannot be created from a type parameter; only values put here by the constructor are ever read back out.
  private final Object[] _values;

  public IntervalTree(Map<VALUE, Interval> valueToIntervalMap) {
    int numValues = valueToIntervalMap.size();
    Object[] values = new Object[numValues];
    Interval[] intervals = new Interval[numValues];
    int index = 0;
    for (Map.Entry<VALUE, Interval> entry : valueToIntervalMap.entrySet()) {
      values[index] = entry.getKey();
      intervals[index] = entry.getValue();
      index++;
    }

    // Sort a permutation of the indexes rather than the entries themselves, so that no object is allocated per entry.
    // This sorts every value instead of grouping by interval first and sorting only the distinct ones. Grouping first
    // needs a hash structure over the values, which costs more than it saves unless most values share an interval:
    // measured over 250k values, grouping first is 35 ms / 12 MB against 20 ms / 3 MB when every interval is distinct,
    // and 3 ms / 1 MB against 12 ms / 3 MB when 64 values share each interval.
    int[] sortedIndexes = new int[numValues];
    for (int i = 0; i < numValues; i++) {
      sortedIndexes[i] = i;
    }
    IntArrays.quickSort(sortedIndexes, (i, j) -> intervals[i].compareTo(intervals[j]));

    // Equal intervals are adjacent after the sort, so a single scan gives the number of distinct intervals
    int numIntervals = 0;
    Interval previousInterval = null;
    for (int i = 0; i < numValues; i++) {
      Interval interval = intervals[sortedIndexes[i]];
      if (!interval.equals(previousInterval)) {
        numIntervals++;
        previousInterval = interval;
      }
    }

    _intervals = new Interval[numIntervals];
    _subtreeMaxs = new long[numIntervals];
    _valueOffsets = new int[numIntervals + 1];
    _values = new Object[numValues];
    int numIntervalsAdded = 0;
    previousInterval = null;
    for (int i = 0; i < numValues; i++) {
      int sortedIndex = sortedIndexes[i];
      Interval interval = intervals[sortedIndex];
      if (!interval.equals(previousInterval)) {
        _intervals[numIntervalsAdded] = interval;
        _valueOffsets[numIntervalsAdded] = i;
        numIntervalsAdded++;
        previousInterval = interval;
      }
      _values[i] = values[sortedIndex];
    }
    _valueOffsets[numIntervals] = numValues;

    buildSubtreeMaxs(0, numIntervals);
  }

  /// Fills [#_subtreeMaxs] for the subtree covering `[start, end)` and returns its max interval end.
  private long buildSubtreeMaxs(int start, int end) {
    if (start >= end) {
      return Long.MIN_VALUE;
    }
    int mid = start + (end - start) / 2;
    long max = Math.max(_intervals[mid]._max, Math.max(buildSubtreeMaxs(start, mid), buildSubtreeMaxs(mid + 1, end)));
    _subtreeMaxs[mid] = max;
    return max;
  }

  /// Find all values whose intervals intersect with the input interval.
  ///
  /// @param searchInterval search interval
  /// @return list of all qualified values.
  public List<VALUE> searchAll(@Nullable Interval searchInterval) {
    List<VALUE> values = new ArrayList<>();
    if (searchInterval != null) {
      searchAll(0, _intervals.length, searchInterval, values);
    }
    return values;
  }

  private void searchAll(int start, int end, Interval searchInterval, List<VALUE> values) {
    if (start >= end) {
      return;
    }
    int mid = start + (end - start) / 2;

    // Search the left subtree unless every interval in it ends before the search interval starts
    if (start < mid && _subtreeMaxs[start + (mid - start) / 2] >= searchInterval._min) {
      searchAll(start, mid, searchInterval, values);
    }

    Interval interval = _intervals[mid];
    if (searchInterval.intersects(interval)) {
      int valueEndOffset = _valueOffsets[mid + 1];
      for (int i = _valueOffsets[mid]; i < valueEndOffset; i++) {
        @SuppressWarnings("unchecked")
        VALUE value = (VALUE) _values[i];
        values.add(value);
      }
    }

    // Intervals are sorted by start, so nothing in the right subtree can match once this one starts after the search
    // interval ends
    if (interval._min <= searchInterval._max) {
      searchAll(mid + 1, end, searchInterval, values);
    }
  }
}
