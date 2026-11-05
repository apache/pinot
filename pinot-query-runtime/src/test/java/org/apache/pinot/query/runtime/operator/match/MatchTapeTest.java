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
package org.apache.pinot.query.runtime.operator.match;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.query.planner.plannode.PatternSymbol;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/// Tests the storage [MatchTape] retains between match attempts, which the matcher reuses for every match of every
/// partition.
public class MatchTapeTest {
  private static final int NUM_SYMBOLS = 16;
  private static final int LARGE_PARTITION_ROWS = 50_000;
  private static final Object[] ROW = {0, "A", 0};

  /// `PATTERN (A+ | B+ | ...)` over successive large partitions that are each mapped entirely to a different
  /// variable: every per-variable position list grows to the size of a whole partition once. Cleared lists keep their
  /// backing arrays, so without a bound the tape would end up retaining one large partition per variable even though
  /// it only ever processes one partition at a time.
  @Test
  public void testRetainedCapacityIsBoundedAfterLargePartitionsOfDifferentVariables() {
    MatchTape tape = new MatchTape(symbols(NUM_SYMBOLS));
    List<Object[]> largePartition = Collections.nCopies(LARGE_PARTITION_ROWS, ROW);
    for (int symbol = 0; symbol < NUM_SYMBOLS; symbol++) {
      matchWholePartition(tape, largePartition, symbol);
      // The partition boundary, as PartitionMatcher#releasePartition() reaches it.
      tape.reset(List.of(), 0, 0);
    }

    // A small partition follows: it needs almost nothing, and the large ones before it must not still be paid for.
    matchWholePartition(tape, Collections.nCopies(2, ROW), 0);

    long retainedPositions = tape.getRetainedSymbolPositions();
    assertTrue(retainedPositions <= 4L * LARGE_PARTITION_ROWS,
        "The per-variable lists must retain at most a few partitions' worth of positions, however many variables "
            + "the pattern has, but they retain " + retainedPositions + " after " + NUM_SYMBOLS + " partitions of "
            + LARGE_PARTITION_ROWS + " rows");
  }

  /// The bound must not turn into a reallocation per attempt: a list that one long match needs is kept for the next.
  @Test
  public void testCapacityNeededByOneMatchIsKeptForTheNext() {
    MatchTape tape = new MatchTape(symbols(NUM_SYMBOLS));
    List<Object[]> largePartition = Collections.nCopies(LARGE_PARTITION_ROWS, ROW);
    matchWholePartition(tape, largePartition, 0);

    tape.reset(largePartition, 0, 2);

    assertTrue(tape.getRetainedSymbolPositions() >= LARGE_PARTITION_ROWS,
        "A single long match must not have its storage reclaimed, retained: " + tape.getRetainedSymbolPositions());
  }

  /// Teardown releases everything, including the storage a partition boundary keeps for reuse.
  @Test
  public void testReleaseDropsThePartitionAndAllRetainedStorage() {
    MatchTape tape = new MatchTape(symbols(NUM_SYMBOLS));
    matchWholePartition(tape, Collections.nCopies(LARGE_PARTITION_ROWS, ROW), 3);
    assertTrue(tape.hasBufferedState());

    tape.release();

    assertFalse(tape.hasBufferedState());
    assertEquals(tape.getRetainedSymbolPositions(), 0L);
    assertTrue(tape.getPartitionRows().isEmpty());
    assertEquals(tape.getLength(), 0);
  }

  private static void matchWholePartition(MatchTape tape, List<Object[]> partition, int symbolOrdinal) {
    tape.reset(partition, 0, 1);
    for (int i = 0; i < partition.size(); i++) {
      tape.push(symbolOrdinal);
    }
    assertEquals(tape.rowCount(symbolOrdinal), partition.size());
  }

  private static List<PatternSymbol> symbols(int numSymbols) {
    List<PatternSymbol> symbols = new ArrayList<>(numSymbols);
    for (int i = 0; i < numSymbols; i++) {
      symbols.add(new PatternSymbol(String.valueOf((char) ('A' + i)), null));
    }
    return symbols;
  }
}
