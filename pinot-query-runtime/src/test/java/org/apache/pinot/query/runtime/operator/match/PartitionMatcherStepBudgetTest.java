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

import java.util.AbstractList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.PatternSymbol;
import org.apache.pinot.query.planner.plannode.RowPattern;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryException;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.testng.annotations.Test;

import static org.apache.pinot.query.runtime.operator.match.MatchTestFixtures.INPUT_SCHEMA;
import static org.apache.pinot.query.runtime.operator.match.MatchTestFixtures.anySymbol;
import static org.apache.pinot.query.runtime.operator.match.MatchTestFixtures.quantifier;
import static org.apache.pinot.query.runtime.operator.match.MatchTestFixtures.symbol;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Tests `maxStepsPerMatchAttempt` against the linear, zero-backtracking cost of a match rather than against the
/// backtracking it is meant to bound.
///
/// The step counter counts every automaton transition, including the ones that advance the scan position, so
/// `PATTERN (A+)` with `A` matching every row costs exactly `3n + 4` steps over `n` rows: one
/// REPEAT, one MATCH and one EPSILON per iteration. At the old default of one million steps that capped a linear match
/// at 333,332 rows while [MatchLimits#DEFAULT_MAX_ROWS_IN_MATCH_PARTITION] explicitly admitted three times that,
/// and the failure text blamed an ambiguity that was not there.
public class PartitionMatcherStepBudgetTest {
  /// The largest partition admitted by default; the repeated row object keeps the test's input allocation negligible.
  private static final int NUM_ROWS = MatchLimits.DEFAULT_MAX_ROWS_IN_MATCH_PARTITION;

  /// `PATTERN (A+)` where `A` has no DEFINE, so it matches every row: no backtracking is possible.
  private static final RowPattern A_PLUS = quantifier(symbol(0), 1, RowPattern.Quantifier.UNBOUNDED, true);
  private static final List<PatternSymbol> ANY_A = List.of(anySymbol("A"));

  @Test
  public void testDefaultBudgetAdmitsALinearMatchOverALargePartition() {
    PartitionMatcher matcher = new PartitionMatcher(PatternToNfaCompiler.compile(A_PLUS), ANY_A, INPUT_SCHEMA,
        MatchLimits.DEFAULT_MAX_STEPS_PER_MATCH_ATTEMPT);

    // The greedy quantifier consumes the whole partition, which is exactly what the partition row limit allows.
    assertEquals(matcher.match(matchingRows(NUM_ROWS), 0, 1), NUM_ROWS);
    assertTrue(matcher.getRetainedBacktrackingBytes() <= 32L * 1024 * 1024,
        "One-million-row A+ must retain at most 32 MiB of choice/undo state, retained: "
            + matcher.getRetainedBacktrackingBytes());
  }

  @Test
  public void testExceedingTheBudgetReportsThePartitionSizeAndTheRowsConsumed() {
    // Only the diagnostics change: a budget that a linear match cannot fit in still throws rather than truncating.
    PartitionMatcher matcher =
        new PartitionMatcher(PatternToNfaCompiler.compile(A_PLUS), ANY_A, INPUT_SCHEMA, 100L);

    QueryException exception =
        expectThrows(QueryException.class, () -> matcher.match(matchingRows(1000), 0, 1));
    String message = exception.getMessage();
    assertTrue(message.contains("maximum of 100 pattern matching steps"), message);
    // The partition size and the consumed row count are what distinguish a linear blowup from a backtracking one.
    assertTrue(message.contains("of a 1000-row partition"), message);
    assertTrue(message.contains("rows consumed so far"), message);
    assertTrue(message.contains(QueryOptionKey.MAX_STEPS_PER_MATCH_ATTEMPT), message);
  }

  @Test
  public void testTerminationCallbackInterruptsTheTransitionLoop() {
    AtomicInteger checks = new AtomicInteger();
    PartitionMatcher matcher = new PartitionMatcher(PatternToNfaCompiler.compile(A_PLUS), ANY_A, INPUT_SCHEMA,
        Long.MAX_VALUE, () -> {
          checks.incrementAndGet();
          throw QueryErrorCode.QUERY_CANCELLATION.asException("cancelled during MATCH_RECOGNIZE");
        });

    QueryException exception =
        expectThrows(QueryException.class, () -> matcher.match(matchingRows(10_000), 0, 1));
    assertEquals(exception.getErrorCode(), QueryErrorCode.QUERY_CANCELLATION);
    assertEquals(checks.get(), 1);
  }

  /// An aggregate inside DEFINE is evaluated with running semantics, so the predicate of the k-th candidate row scans
  /// the k rows mapped so far. Between two of the transition loop's checks that is a quadratic number of row
  /// evaluations, so the aggregate scan has to check for termination itself.
  @Test
  public void testTerminationCallbackInterruptsADefineAggregateScan() {
    // DEFINE A AS COUNT(A.value) >= 0: always true, but every candidate row recounts the rows already mapped to A.
    RexExpression count = new RexExpression.FunctionCall(ColumnDataType.LONG, "COUNT",
        List.of(new RexExpression.PatternFieldRef(MatchTestFixtures.VALUE_INDEX, 0, "A")));
    PatternSymbol a = new PatternSymbol("A", new RexExpression.FunctionCall(ColumnDataType.BOOLEAN,
        "GREATER_THAN_OR_EQUAL", List.of(count, new RexExpression.Literal(ColumnDataType.LONG, 0L))));
    PartitionMatcher matcher = new PartitionMatcher(PatternToNfaCompiler.compile(A_PLUS), List.of(a), INPUT_SCHEMA,
        Long.MAX_VALUE, () -> {
          throw QueryErrorCode.QUERY_CANCELLATION.asException("cancelled during MATCH_RECOGNIZE");
        });
    AtomicInteger rowReads = new AtomicInteger();
    Object[] row = {0, "A", 0};
    List<Object[]> rows = new AbstractList<>() {
      @Override
      public Object[] get(int index) {
        rowReads.incrementAndGet();
        return row;
      }

      @Override
      public int size() {
        return 10_000;
      }
    };

    QueryException exception = expectThrows(QueryException.class, () -> matcher.match(rows, 0, 1));
    assertEquals(exception.getErrorCode(), QueryErrorCode.QUERY_CANCELLATION);
    int checkInterval = QueryThreadContext.CHECK_TERMINATION_AND_SAMPLE_USAGE_RECORD_MASK + 1;
    assertTrue(rowReads.get() <= checkInterval,
        "The first termination check must come within " + checkInterval + " aggregated rows, but the DEFINE "
            + "aggregate had already read " + rowReads.get());
  }

  @Test
  public void testRetainedBacktrackingStateHasAHardMemoryLimit() {
    // 2 KiB blocks the first undo-log growth; 3 KiB admits it but blocks the subsequent choice-stack growth.
    for (long maxRetainedBytes : List.of(2L * 1024, 3L * 1024)) {
      PartitionMatcher matcher = new PartitionMatcher(PatternToNfaCompiler.compile(A_PLUS), ANY_A, INPUT_SCHEMA,
          Long.MAX_VALUE, () -> { }, maxRetainedBytes);

      QueryException exception =
          expectThrows(QueryException.class, () -> matcher.match(matchingRows(100), 0, 1));
      assertEquals(exception.getErrorCode(), QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED);
      assertTrue(exception.getMessage().contains("hard limit"), exception.getMessage());
      assertTrue(exception.getMessage().contains("retained pattern backtracking state"), exception.getMessage());
      assertTrue(matcher.getRetainedBacktrackingBytes() <= maxRetainedBytes,
          "Matcher must reject growth before retaining more than its hard cap");
    }
  }

  /// The choice stack and counter undo log grow with the longest match and keep that capacity across partitions,
  /// up to the hard limit above. [PartitionMatcher#releasePartition()] keeps them on purpose, for the next partition;
  /// [PartitionMatcher#release()] is the final teardown and must hand them back as well as the tape.
  @Test
  public void testReleaseDropsTheGrownBacktrackingArrays() {
    PartitionMatcher matcher =
        new PartitionMatcher(PatternToNfaCompiler.compile(A_PLUS), ANY_A, INPUT_SCHEMA, Long.MAX_VALUE);
    long initialBytes = matcher.getRetainedBacktrackingBytes();
    assertEquals(matcher.match(matchingRows(1000), 0, 1), 1000);
    long grownBytes = matcher.getRetainedBacktrackingBytes();
    assertTrue(grownBytes > initialBytes, "A 1000-row A+ match must grow the backtracking arrays past "
        + initialBytes + " bytes, retained: " + grownBytes);

    matcher.releasePartition();
    assertEquals(matcher.getRetainedBacktrackingBytes(), grownBytes, "backtracking arrays after releasePartition()");
    assertTrue(matcher.hasBufferedState(), "grown backtracking arrays are buffered state");

    matcher.release();
    assertEquals(matcher.getRetainedBacktrackingBytes(), initialBytes, "backtracking arrays after release()");
    assertFalse(matcher.hasBufferedState(), "release() must leave nothing to release");
  }

  private static List<Object[]> matchingRows(int numRows) {
    return Collections.nCopies(numRows, new Object[]{0, "A", 0});
  }
}
