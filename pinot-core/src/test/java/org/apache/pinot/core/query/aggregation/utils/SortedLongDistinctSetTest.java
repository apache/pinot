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

import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.query.aggregation.function.DistinctCountAggregationFunction;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/**
 * Unit tests for {@link SortedLongDistinctSet}, the sorted-run backed {@code LongSet} used by the no-scan DISTINCT
 * aggregation path. These verify that it upholds the {@code LongSet} contract and, critically, that its lazy sorted
 * union produces exactly the same distinct set as a {@link LongOpenHashSet} across the code paths the combine phase
 * exercises: single run, multi-run merges, merges with a non-sorted collection, unsorted input, duplicates spanning
 * runs, empty inputs, and post-materialization mutation.
 */
public class SortedLongDistinctSetTest {

  /** Builds a set from an already sorted, duplicate-free run (the constructor is private; use the checked factory). */
  private static SortedLongDistinctSet sortedSet(long[] sortedDistinct) {
    return SortedLongDistinctSet.fromValues(sortedDistinct, sortedDistinct.length, true);
  }

  private static long[] sortedDistinct(long... values) {
    // Input is authored already sorted and distinct; assert that so tests stay honest.
    for (int i = 1; i < values.length; i++) {
      assertTrue(values[i] > values[i - 1], "test input must be sorted and distinct");
    }
    return values;
  }

  private static void assertSameAsHashSet(SortedLongDistinctSet actual, LongOpenHashSet expected) {
    assertEquals(actual.size(), expected.size());
    assertEquals(actual.isEmpty(), expected.isEmpty());
    // Values match (order-independent).
    LongOpenHashSet actualValues = new LongOpenHashSet(actual);
    assertEquals(actualValues, expected);
    // Iteration yields exactly `size` ascending, distinct values.
    long prev = Long.MIN_VALUE;
    int count = 0;
    for (long v : actual) {
      assertTrue(count == 0 || v > prev, "iteration must be strictly ascending");
      assertTrue(expected.contains(v));
      assertTrue(actual.contains(v));
      prev = v;
      count++;
    }
    assertEquals(count, expected.size());
    // Serialization round-trips to the same distinct set (this is how the server ships the intermediate result).
    LongOpenHashSet roundTrip = (LongOpenHashSet) ObjectSerDeUtils.LONG_SET_SER_DE.deserialize(
        ObjectSerDeUtils.LONG_SET_SER_DE.serialize(actual));
    assertEquals(roundTrip, expected);
  }

  @Test
  public void testEmpty() {
    SortedLongDistinctSet set = sortedSet(new long[0]);
    assertTrue(set.isEmpty());
    assertEquals(set.size(), 0);
    assertFalse(set.contains(1L));
    assertFalse(set.iterator().hasNext());
  }

  @Test
  public void testSingleRun() {
    SortedLongDistinctSet set = sortedSet(sortedDistinct(-5L, 0L, 3L, 9L));
    assertSameAsHashSet(set, new LongOpenHashSet(new long[]{-5L, 0L, 3L, 9L}));
    assertTrue(set.contains(3L));
    assertFalse(set.contains(4L));
  }

  @Test
  public void testFromValuesUnsorted() {
    // fromValues must sort when told the input is not already sorted (mutable/realtime dictionaries are
    // insertion-ordered). Input is duplicate-free per the dictionary contract.
    long[] raw = {7L, 3L, 1L, 9L, -2L};
    SortedLongDistinctSet set = SortedLongDistinctSet.fromValues(raw, raw.length, false);
    assertSameAsHashSet(set, new LongOpenHashSet(new long[]{-2L, 1L, 3L, 7L, 9L}));
  }

  @Test
  public void testFromValuesPrefix() {
    long[] raw = {1L, 2L, 3L, 999L, 998L};
    SortedLongDistinctSet set = SortedLongDistinctSet.fromValues(raw, 3, true);
    assertSameAsHashSet(set, new LongOpenHashSet(new long[]{1L, 2L, 3L}));
  }

  @Test
  public void testMergeDisjointRuns() {
    SortedLongDistinctSet a = sortedSet(sortedDistinct(1L, 3L, 5L));
    SortedLongDistinctSet b = sortedSet(sortedDistinct(2L, 4L, 6L));
    assertTrue(a.addAll(b));
    assertSameAsHashSet(a, new LongOpenHashSet(new long[]{1L, 2L, 3L, 4L, 5L, 6L}));
  }

  @Test
  public void testMergeOverlappingRuns() {
    SortedLongDistinctSet a = sortedSet(sortedDistinct(1L, 2L, 3L, 4L));
    SortedLongDistinctSet b = sortedSet(sortedDistinct(3L, 4L, 5L, 6L));
    a.addAll(b);
    assertSameAsHashSet(a, new LongOpenHashSet(new long[]{1L, 2L, 3L, 4L, 5L, 6L}));
  }

  @Test
  public void testMergeManyRunsLikeCombinePhase() {
    // Mirrors the combine phase: one run per segment appended one at a time, unioned lazily at the end.
    SortedLongDistinctSet acc = sortedSet(new long[0]);
    LongOpenHashSet expected = new LongOpenHashSet();
    Random random = new Random(42);
    for (int segment = 0; segment < 11; segment++) {
      TreeSet<Long> run = new TreeSet<>();
      for (int i = 0; i < 500; i++) {
        long v = random.nextInt(2000);
        run.add(v);
        expected.add(v);
      }
      long[] runArray = new long[run.size()];
      int idx = 0;
      for (long v : run) {
        runArray[idx++] = v;
      }
      acc.addAll(sortedSet(runArray));
    }
    assertSameAsHashSet(acc, expected);
  }

  @Test
  public void testMergeWithNonSortedLongCollection() {
    // A mixed merge where the other operand is a plain hash set (unordered) must still produce the correct union.
    SortedLongDistinctSet a = sortedSet(sortedDistinct(1L, 5L, 9L));
    LongOpenHashSet other = new LongOpenHashSet(new long[]{9L, 2L, 5L, 7L});
    a.addAll(other);
    assertSameAsHashSet(a, new LongOpenHashSet(new long[]{1L, 2L, 5L, 7L, 9L}));
  }

  @Test
  public void testMergeEmptyOperands() {
    SortedLongDistinctSet a = sortedSet(sortedDistinct(1L, 2L));
    assertFalse(a.addAll(sortedSet(new long[0])));
    assertFalse(a.addAll(new LongOpenHashSet()));
    assertSameAsHashSet(a, new LongOpenHashSet(new long[]{1L, 2L}));

    SortedLongDistinctSet empty = sortedSet(new long[0]);
    empty.addAll(sortedSet(sortedDistinct(3L, 4L)));
    assertSameAsHashSet(empty, new LongOpenHashSet(new long[]{3L, 4L}));
  }

  @Test
  public void testAddSingleElements() {
    SortedLongDistinctSet set = sortedSet(sortedDistinct(2L, 4L, 6L));
    assertTrue(set.add(5L));
    assertFalse(set.add(4L));
    assertTrue(set.add(1L));
    assertTrue(set.add(9L));
    assertSameAsHashSet(set, new LongOpenHashSet(new long[]{1L, 2L, 4L, 5L, 6L, 9L}));
  }

  @Test
  public void testLargeMultiPassMerge() {
    // Many large overlapping runs exercise the multi-pass ping-pong merge (several passes, odd run counts, dedupe
    // shrinking between passes); results must be identical to a hash set.
    SortedLongDistinctSet acc = sortedSet(new long[0]);
    LongOpenHashSet expected = new LongOpenHashSet();
    Random random = new Random(7);
    for (int segment = 0; segment < 4; segment++) {
      int n = 300_000;
      long[] run = new long[n];
      long v = random.nextLong() % 1_000_000_000L;
      for (int i = 0; i < n; i++) {
        v += 1 + random.nextInt(5); // strictly ascending, overlapping ranges across segments
        run[i] = v;
        expected.add(v);
      }
      acc.addAll(SortedLongDistinctSet.fromValues(run, n, true));
    }
    assertTrue(expected.size() > 1 << 20, "test must be large enough for a multi-pass merge");
    assertSameAsHashSet(acc, expected);
  }

  @Test
  public void testMergeFromMaterializedArgumentWithDedupeSlack() {
    // Materializing overlapping runs with immaterial dedupe waste (< 25%) leaves _values longer than _size (trailing
    // garbage beyond the logical size). Using such a set as the addAll argument, and as the receiver (demote path),
    // must copy exactly [0, _size) -- copying the full array would silently inflate DISTINCT counts.
    long[] runA = new long[100];
    long[] runB = new long[100];
    LongOpenHashSet expected = new LongOpenHashSet();
    for (int i = 0; i < 100; i++) {
      runA[i] = i + 1; // 1..100
      runB[i] = i + 95; // 95..194, overlap of 6 values
      expected.add(runA[i]);
      expected.add(runB[i]);
    }
    SortedLongDistinctSet slacked = sortedSet(runA);
    slacked.addAll(sortedSet(runB));
    assertEquals(slacked.size(), 194); // materializes: buffer 200, size 194 -> slack retained (waste < 25%)

    // As the argument of a merge into a fresh set
    SortedLongDistinctSet fresh = sortedSet(sortedDistinct(1000L, 2000L));
    fresh.addAll(slacked);
    LongOpenHashSet expectedFresh = new LongOpenHashSet(expected);
    expectedFresh.add(1000L);
    expectedFresh.add(2000L);
    assertSameAsHashSet(fresh, expectedFresh);

    // As the receiver (demoteToRuns must also honor _size, not _values.length)
    slacked.addAll(sortedSet(sortedDistinct(3000L, 4000L)));
    LongOpenHashSet expectedReceiver = new LongOpenHashSet(expected);
    expectedReceiver.add(3000L);
    expectedReceiver.add(4000L);
    assertSameAsHashSet(slacked, expectedReceiver);
  }

  @Test
  public void testEagerCompactionOfPendingRuns() {
    // Three 3M-value runs cross the internal compaction threshold (8M pending), forcing eager dedupe inside addAll.
    // Union size is arithmetically known: evens < 6M (3M) + odds < 6M (3M) = all ints < 6M, plus multiples of 3 in
    // [6M, 9M) (1M) = 7M distinct.
    int n = 3_000_000;
    long[] evens = new long[n];
    long[] mult3 = new long[n];
    long[] odds = new long[n];
    for (int i = 0; i < n; i++) {
      evens[i] = 2L * i;
      mult3[i] = 3L * i;
      odds[i] = 2L * i + 1;
    }
    SortedLongDistinctSet acc = sortedSet(evens);
    acc.addAll(sortedSet(mult3));
    acc.addAll(sortedSet(odds));
    assertEquals(acc.size(), 7_000_000);
    assertTrue(acc.contains(0L));
    assertTrue(acc.contains(5_999_999L));
    assertTrue(acc.contains(6_000_000L)); // multiple of 3 above the even/odd range
    assertFalse(acc.contains(6_000_001L)); // not a multiple of 3, above the even/odd range
    assertTrue(acc.contains(8_999_997L));
    assertFalse(acc.contains(8_999_998L));
  }

  @Test
  public void testUnionNormalizesMixedTypes() {
    // Whichever side the sorted set arrives on, it must absorb the hash set — a hash set absorbing a sorted set
    // would re-hash every value and lose the sorted-run optimization (block merge order is nondeterministic when
    // scan-based and no-scan segments mix within one query).
    LongOpenHashSet expected = new LongOpenHashSet(new long[]{1L, 2L, 3L, 4L, 5L});

    Set merged = SortedLongDistinctSet.union(new LongOpenHashSet(new long[]{2L, 4L}),
        sortedSet(sortedDistinct(1L, 3L, 5L)));
    assertTrue(merged instanceof SortedLongDistinctSet);
    assertEquals(new LongOpenHashSet(merged), expected);

    merged = SortedLongDistinctSet.union(sortedSet(sortedDistinct(1L, 3L, 5L)),
        new LongOpenHashSet(new long[]{2L, 4L}));
    assertTrue(merged instanceof SortedLongDistinctSet);
    assertEquals(new LongOpenHashSet(merged), expected);

    // Same-type unions keep the plain addAll behavior
    merged = SortedLongDistinctSet.union(new LongOpenHashSet(new long[]{1L, 2L, 3L}),
        new LongOpenHashSet(new long[]{4L, 5L}));
    assertTrue(merged instanceof LongOpenHashSet);
    assertEquals(merged, expected);

    SortedLongDistinctSet first = sortedSet(sortedDistinct(1L, 2L, 3L));
    merged = SortedLongDistinctSet.union(first, sortedSet(sortedDistinct(4L, 5L)));
    assertSame(merged, first);
    assertEquals(new LongOpenHashSet(merged), expected);
  }

  @Test
  public void testAggregationFunctionMergeNormalizesMixedTypes() {
    // End-to-end through DistinctCountAggregationFunction.merge, the path the combine phase actually uses
    DistinctCountAggregationFunction function =
        new DistinctCountAggregationFunction(List.of(ExpressionContext.forIdentifier("col")), false);
    LongOpenHashSet expected = new LongOpenHashSet(new long[]{1L, 2L, 3L, 4L, 5L});

    Set merged = function.merge(new LongOpenHashSet(new long[]{2L, 4L}), sortedSet(sortedDistinct(1L, 3L, 5L)));
    assertTrue(merged instanceof SortedLongDistinctSet);
    assertEquals(new LongOpenHashSet(merged), expected);
    assertEquals((int) function.extractFinalResult(merged), 5);

    merged = function.merge(sortedSet(sortedDistinct(1L, 3L, 5L)), new LongOpenHashSet(new long[]{2L, 4L}));
    assertTrue(merged instanceof SortedLongDistinctSet);
    assertEquals((int) function.extractFinalResult(merged), 5);
  }

  @Test
  public void testRemovalUnsupported() {
    SortedLongDistinctSet set = sortedSet(sortedDistinct(1L, 2L, 3L));
    assertThrows(UnsupportedOperationException.class, () -> set.rem(2L));
    assertThrows(UnsupportedOperationException.class, () -> {
      LongIterator it = set.iterator();
      it.nextLong();
      it.remove();
    });
    assertThrows(UnsupportedOperationException.class, () -> set.removeAll(new LongOpenHashSet(new long[]{2L})));
    assertThrows(UnsupportedOperationException.class, () -> set.retainAll(new LongOpenHashSet(new long[]{2L})));
    // The set must be untouched by the failed removals
    assertSameAsHashSet(set, new LongOpenHashSet(new long[]{1L, 2L, 3L}));
  }

  @Test
  public void testMergeAfterMaterialization() {
    // size() materializes; a subsequent addAll must demote back to runs and still union correctly.
    SortedLongDistinctSet a = sortedSet(sortedDistinct(1L, 2L, 3L));
    assertEquals(a.size(), 3);
    a.addAll(sortedSet(sortedDistinct(3L, 4L, 5L)));
    assertSameAsHashSet(a, new LongOpenHashSet(new long[]{1L, 2L, 3L, 4L, 5L}));
  }
}
