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
package org.apache.pinot.perf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.apache.pinot.query.runtime.operator.utils.SortUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;


/// Compares the two ways the multi-stage engine can produce a fully sorted result when nothing bounds it - i.e. an
/// ORDER BY with no LIMIT.
///
///   - [#unboundedHeap] reproduces the SortOperator path before the split: an unbounded PriorityQueue fed one row at
///     a time through [SelectionOperatorUtils#addToPriorityQueue], then drained by polling every row. Both halves
///     pay a sift.
///   - [#accumulateAndSort] reproduces FullSortOperator: append every row to an ArrayList, then sort once.
///   - [#accumulateAndSortNotPreSized] is the same, from a default-capacity list, to isolate what pre-sizing buys.
///
/// Two input distributions are measured, because the shape of the input is what decides whether the comparison is
/// close. RANDOM is the neutral case. SORTED_RUNS is what a receive stage actually sees once the senders sort - the
/// concatenation of one sorted run per sender - and TimSort detects such runs while a heap cannot.
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Benchmark)
public class BenchmarkMseSortImplementations {
  private static final int NUM_COLUMNS = 3;
  private static final int NUM_SENDERS = 64;
  /// Matches SelectionOperatorUtils.MAX_ROW_HOLDER_INITIAL_CAPACITY, which the old operator used.
  private static final int HOLDER_CAPACITY = 10_000;
  /// Rows per arriving block, so the accumulation pattern matches what an operator actually sees.
  private static final int ROWS_PER_BLOCK = 2_048;

  @Param({"10000", "1000000"})
  private int _numRows;

  @Param({"RANDOM", "SORTED_RUNS"})
  private String _distribution;

  private Object[][] _rows;
  private Comparator<Object[]> _forward;
  private Comparator<Object[]> _reversed;

  @Setup
  public void setUp() {
    List<RelFieldCollation> collations = List.of(
        new RelFieldCollation(0, RelFieldCollation.Direction.ASCENDING, RelFieldCollation.NullDirection.LAST));
    _forward = new SortUtils.SortComparator(collations, false);
    // The old path used the inverted comparator so the heap head is the row to evict.
    _reversed = new SortUtils.SortComparator(collations, true);

    Random random = new Random(42);
    _rows = new Object[_numRows][];
    if ("RANDOM".equals(_distribution)) {
      for (int i = 0; i < _numRows; i++) {
        _rows[i] = row(random.nextInt(), i);
      }
    } else {
      // One sorted run per sender, concatenated in mailbox order: what the receiver sees when senders sort.
      int perSender = (_numRows + NUM_SENDERS - 1) / NUM_SENDERS;
      int written = 0;
      for (int sender = 0; sender < NUM_SENDERS && written < _numRows; sender++) {
        int[] keys = new int[Math.min(perSender, _numRows - written)];
        for (int i = 0; i < keys.length; i++) {
          keys[i] = random.nextInt();
        }
        Arrays.sort(keys);
        for (int key : keys) {
          _rows[written] = row(key, written);
          written++;
        }
      }
    }
  }

  private static Object[] row(int key, int seq) {
    Object[] row = new Object[NUM_COLUMNS];
    row[0] = key;
    row[1] = (long) seq;
    row[2] = seq;
    return row;
  }

  /// The SortOperator path before the split, with `numRowsToKeep == Integer.MAX_VALUE` so nothing is ever evicted.
  @Benchmark
  public void unboundedHeap(Blackhole bh) {
    PriorityQueue<Object[]> queue = new PriorityQueue<>(HOLDER_CAPACITY, _reversed);
    for (Object[] row : _rows) {
      SelectionOperatorUtils.addToPriorityQueue(row, queue, Integer.MAX_VALUE);
    }
    int resultSize = queue.size();
    Object[][] result = new Object[resultSize][];
    for (int i = resultSize - 1; i >= 0; i--) {
      result[i] = queue.poll();
    }
    bh.consume(result);
  }

  /// FullSortOperator: accumulate into an ArrayList, sort once.
  ///
  /// Rows arrive one block at a time, so this appends in block-sized chunks rather than in one addAll - that is what
  /// decides whether the backing array grows repeatedly, and therefore whether pre-sizing the list is worth anything.
  @Benchmark
  public void accumulateAndSort(Blackhole bh) {
    ArrayList<Object[]> rows = new ArrayList<>(HOLDER_CAPACITY);
    for (int start = 0; start < _rows.length; start += ROWS_PER_BLOCK) {
      int end = Math.min(start + ROWS_PER_BLOCK, _rows.length);
      rows.addAll(Arrays.asList(_rows).subList(start, end));
    }
    rows.sort(_forward);
    bh.consume(rows);
  }

  /// Same, but from a default-capacity list, to isolate what pre-sizing buys.
  @Benchmark
  public void accumulateAndSortNotPreSized(Blackhole bh) {
    ArrayList<Object[]> rows = new ArrayList<>();
    for (int start = 0; start < _rows.length; start += ROWS_PER_BLOCK) {
      int end = Math.min(start + ROWS_PER_BLOCK, _rows.length);
      rows.addAll(Arrays.asList(_rows).subList(start, end));
    }
    rows.sort(_forward);
    bh.consume(rows);
  }

  public static void main(String[] args)
      throws RunnerException {
    new Runner(new OptionsBuilder().include(BenchmarkMseSortImplementations.class.getSimpleName()).build()).run();
  }
}
