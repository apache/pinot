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

import java.math.BigDecimal;
import java.util.function.Supplier;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.query.distinct.table.BigDecimalDistinctTable;
import org.apache.pinot.core.query.distinct.table.BytesDistinctTable;
import org.apache.pinot.core.query.distinct.table.DistinctTable;
import org.apache.pinot.core.query.distinct.table.DoubleDistinctTable;
import org.apache.pinot.core.query.distinct.table.FloatDistinctTable;
import org.apache.pinot.core.query.distinct.table.IntDistinctTable;
import org.apache.pinot.core.query.distinct.table.LongDistinctTable;
import org.apache.pinot.core.query.distinct.table.MultiColumnDistinctTable;
import org.apache.pinot.core.query.distinct.table.StringDistinctTable;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/// Test for [DistinctCardinalityTracker].
public class DistinctCardinalityTrackerTest {
  private static final DataSchema INT_SCHEMA =
      new DataSchema(new String[]{"c"}, new ColumnDataType[]{ColumnDataType.INT});
  private static final int UNBOUNDED = Integer.MAX_VALUE;
  /// Default bound: LIMIT at or below it is tracked exactly, above it by sketch.
  /// Any positive value enables the estimating regime; 3 is what the measurements in the class Javadoc use.
  private static final int STD_DEV = 3;
  private static final int DEFAULT_BOUND =
      CommonConstants.Broker.Request.QueryOptionValue.DEFAULT_STREAMING_DISTINCT_MAX_TRACKED_CARDINALITY;

  /// The tracker is only worth building for a leaf whose LIMIT it could actually reach and that could not already
  /// short-circuit inside a single flush window.
  @Test
  public void testCreateIfUsefulSkipsTheCasesThatCannotBenefit() {
    // LIMIT at or below the flush threshold: the accumulated table reaches LIMIT within one window, so
    // DistinctTable#isSatisfied() already fires and there is nothing to carry across windows.
    assertNull(DistinctCardinalityTracker.createIfUseful(100, 100, false, DEFAULT_BOUND, STD_DEV));
    assertNull(DistinctCardinalityTracker.createIfUseful(99, 100, false, DEFAULT_BOUND, STD_DEV));
    // An MSE leaf with no LIMIT pushed down. No cardinality can ever reach it.
    assertNull(DistinctCardinalityTracker.createIfUseful(UNBOUNDED, 100, false, DEFAULT_BOUND, STD_DEV));
    // ORDER BY DISTINCT must never exit early: the top-LIMIT is unknown until every segment has been seen, so
    // stopping after LIMIT distinct values returns an arbitrary LIMIT instead of the ordered top-LIMIT. This is the
    // one gate whose violation yields silently wrong rows rather than duplicates.
    assertNull(DistinctCardinalityTracker.createIfUseful(101, 100, true, DEFAULT_BOUND, STD_DEV));
    // The case the feature exists for.
    assertNotNull(DistinctCardinalityTracker.createIfUseful(101, 100, false, DEFAULT_BOUND, STD_DEV));
  }

  /// The bound selects the regime, and `0` switches the early exit off entirely.
  @Test
  public void testConfiguredBoundSelectsTheRegime() {
    // Exact regime: the exit lands on exactly LIMIT, never early, never late.
    DistinctCardinalityTracker exact = DistinctCardinalityTracker.createIfUseful(5000, 100, false, 8192, STD_DEV);
    assertNotNull(exact);
    exact.add(intTable(0, 4999));
    assertFalse(exact.hasReachedLimit());
    exact.add(intTable(4999, 5000));
    assertTrue(exact.hasReachedLimit());

    // Same LIMIT, bound lowered below it: now estimating, so it must still not fire below LIMIT.
    DistinctCardinalityTracker estimating = DistinctCardinalityTracker.createIfUseful(5000, 100, false, 1024, STD_DEV);
    assertNotNull(estimating);
    estimating.add(intTable(0, 4999));
    assertFalse(estimating.hasReachedLimit(), "The lower bound must not clear LIMIT at limit-1 distinct values");

    // Zero max-tracked-cardinality disables the early exit outright.
    assertNull(DistinctCardinalityTracker.createIfUseful(5000, 100, false, 0, STD_DEV));
  }

  /// The estimating regime is unsound -- a sketch lower bound is a confidence bound, not a guarantee -- so it must
  /// be off unless explicitly opted into. A LIMIT above the bound then gets no tracker at all, which leaves the leaf
  /// reading every segment exactly as it does without this feature.
  @Test
  public void testEstimatingRegimeIsOffByDefault() {
    int aboveBound = DEFAULT_BOUND + 1;
    assertNull(DistinctCardinalityTracker.createIfUseful(aboveBound, 100, false, DEFAULT_BOUND, 0),
        "A LIMIT above the bound must not get an estimating tracker unless opted in");
    assertNotNull(DistinctCardinalityTracker.createIfUseful(aboveBound, 100, false, DEFAULT_BOUND, STD_DEV),
        "Opting in must produce the estimating tracker");
    // The exact regime is provably sound, so it stays on without any opt-in.
    assertNotNull(DistinctCardinalityTracker.createIfUseful(DEFAULT_BOUND, 100, false, DEFAULT_BOUND, 0),
        "A LIMIT within the bound is counted exactly and needs no opt-in");
  }

  /// Tiny LIMITs and tiny bounds both have to produce a usable tracker: the sketch rejects fewer than 16 nominal
  /// entries, so the estimating regime has to clamp rather than pass the bound straight through.
  @Test
  public void testTinyLimitsAreStillExact() {
    for (int limit = 2; limit <= 32; limit++) {
      DistinctCardinalityTracker tracker =
          DistinctCardinalityTracker.createIfUseful(limit, 1, false, DEFAULT_BOUND, STD_DEV);
      assertNotNull(tracker);
      tracker.add(intTable(0, limit - 1));
      assertFalse(tracker.hasReachedLimit(), "limit=" + limit + " satisfied by limit-1 distinct values");
      tracker.add(intTable(limit - 1, limit));
      assertTrue(tracker.hasReachedLimit(), "limit=" + limit + " not satisfied by limit distinct values");
    }
  }

  /// The heart of the ticket: flush windows overlap, so a running count of emitted rows overshoots LIMIT and would
  /// let the leaf exit having emitted fewer than LIMIT distinct values. Feeding the same 50 values four times over
  /// (which is what four identical segments produce) must not move the estimate past 50.
  @Test
  public void testOverlappingFlushWindowsAreNotDoubleCounted() {
    DistinctCardinalityTracker tracker =
        DistinctCardinalityTracker.createIfUseful(60, 10, false, DEFAULT_BOUND, STD_DEV);
    assertNotNull(tracker);
    for (int window = 0; window < 4; window++) {
      tracker.add(intTable(0, 50));
      assertFalse(tracker.hasReachedLimit(),
          "200 rows over 50 distinct values must not satisfy a LIMIT of 60 (window " + window + ")");
    }
    // Ten genuinely new values do take it over the line.
    tracker.add(intTable(50, 60));
    assertTrue(tracker.hasReachedLimit());
  }

  /// Sizing the sketch's nominal entries against LIMIT keeps it in exact mode, so for any LIMIT at or below the cap
  /// the exit fires at exactly LIMIT -- no overshoot, and no chance of exiting below it.
  @Test
  public void testExactAtOrBelowTheConfiguredBound() {
    for (int limit : new int[]{101, 1000, 4096, 10_001, DEFAULT_BOUND}) {
      DistinctCardinalityTracker tracker =
          DistinctCardinalityTracker.createIfUseful(limit, 100, false, DEFAULT_BOUND, STD_DEV);
      assertNotNull(tracker);
      tracker.add(intTable(0, limit - 1));
      assertFalse(tracker.hasReachedLimit(), "limit=" + limit + " must not be satisfied by limit-1 distinct values");
      tracker.add(intTable(limit - 1, limit));
      assertTrue(tracker.hasReachedLimit(), "limit=" + limit + " must be satisfied by exactly limit distinct values");
    }
  }

  /// Above the cap the sketch is in estimation mode and the exit reads a lower confidence bound. The property that
  /// matters is one-sided: it must not claim LIMIT has been reached while it has not, because that truncates the
  /// result. Exiting late merely costs a few more segments, so no upper bound is asserted here.
  @Test
  public void testNeverClaimsLimitReachedTooEarlyAboveTheBound() {
    for (int limit : new int[]{20_000, 100_000, 1_000_000}) {
      DistinctCardinalityTracker tracker =
          DistinctCardinalityTracker.createIfUseful(limit, 10_000, false, DEFAULT_BOUND, STD_DEV);
      assertNotNull(tracker);
      tracker.add(intTable(0, limit - 1));
      assertFalse(tracker.hasReachedLimit(),
          "limit=" + limit + " must not be satisfied by limit-1 distinct values");
    }
  }

  /// Sanity bound on the other side: the estimation-mode exit must still fire reasonably close to LIMIT, or the
  /// early exit would never pay for itself. Measured overshoot at the cap is ~1.02x.
  @Test
  public void testOvershootAboveTheBoundStaysSmall() {
    int limit = 100_000;
    DistinctCardinalityTracker tracker =
        DistinctCardinalityTracker.createIfUseful(limit, 10_000, false, DEFAULT_BOUND, STD_DEV);
    assertNotNull(tracker);
    int fed = (int) (limit * 1.1);
    tracker.add(intTable(0, fed));
    assertTrue(tracker.hasReachedLimit(), "1.1x LIMIT distinct values must satisfy a LIMIT of " + limit);
  }

  /// Every stored value type reaching the combine operator must hash injectively, or the tracker undercounts and the
  /// early exit silently stops working for that column type. One table per DistinctTable subtype that the streaming
  /// operator can see (DictIdDistinctTable is converted to a typed table before it gets there).
  @Test
  public void testAllStoredValueTypesAreDistinguished() {
    assertCounts(() -> {
      LongDistinctTable table = new LongDistinctTable(schema(ColumnDataType.LONG), UNBOUNDED, false, null);
      for (int i = 0; i < 200; i++) {
        table.addUnbounded(i);
      }
      return table;
    });
    assertCounts(() -> {
      FloatDistinctTable table = new FloatDistinctTable(schema(ColumnDataType.FLOAT), UNBOUNDED, false, null);
      for (int i = 0; i < 200; i++) {
        table.addUnbounded(i + 0.5f);
      }
      return table;
    });
    assertCounts(() -> {
      DoubleDistinctTable table = new DoubleDistinctTable(schema(ColumnDataType.DOUBLE), UNBOUNDED, false, null);
      for (int i = 0; i < 200; i++) {
        table.addUnbounded(i + 0.5d);
      }
      return table;
    });
    assertCounts(() -> {
      StringDistinctTable table = new StringDistinctTable(schema(ColumnDataType.STRING), UNBOUNDED, false, null);
      for (int i = 0; i < 200; i++) {
        table.addUnbounded("value" + i);
      }
      return table;
    });
    assertCounts(() -> {
      BytesDistinctTable table = new BytesDistinctTable(schema(ColumnDataType.BYTES), UNBOUNDED, false, null);
      for (int i = 0; i < 200; i++) {
        table.addUnbounded(new ByteArray(new byte[]{(byte) i, (byte) (i >>> 8)}));
      }
      return table;
    });
    assertCounts(() -> {
      BigDecimalDistinctTable table =
          new BigDecimalDistinctTable(schema(ColumnDataType.BIG_DECIMAL), UNBOUNDED, false, null);
      for (int i = 0; i < 200; i++) {
        table.addUnbounded(new BigDecimal(i + ".25"));
      }
      return table;
    });
    // Multi-column: the per-column hashes have to be combined order-sensitively, or (1,2) and (2,1) collide.
    assertCounts(() -> {
      DataSchema schema =
          new DataSchema(new String[]{"a", "b"}, new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING});
      MultiColumnDistinctTable table = new MultiColumnDistinctTable(schema, UNBOUNDED, false, null);
      for (int i = 0; i < 200; i++) {
        table.addUnbounded(new Record(new Object[]{i, "v" + i}));
      }
      return table;
    });
  }

  /// BigDecimal equality is scale-sensitive, so 1.0 and 1.00 are two entries in the set and must stay two in the
  /// tracker. Hashing via toString() is what preserves that; unscaledValue() would not.
  @Test
  public void testBigDecimalScaleIsSignificant() {
    BigDecimalDistinctTable table =
        new BigDecimalDistinctTable(schema(ColumnDataType.BIG_DECIMAL), UNBOUNDED, false, null);
    table.addUnbounded(new BigDecimal("1.0"));
    table.addUnbounded(new BigDecimal("1.00"));
    assertEquals(table.size(), 2, "Fixture assumption: BigDecimal.equals() is scale-sensitive");

    DistinctCardinalityTracker tracker = DistinctCardinalityTracker.createIfUseful(2, 1, false, DEFAULT_BOUND, STD_DEV);
    assertNotNull(tracker);
    tracker.add(table);
    assertTrue(tracker.hasReachedLimit());
  }

  /// A multi-column tuple is ordered, so swapping the columns is a different distinct value.
  @Test
  public void testMultiColumnOrderIsSignificant() {
    DataSchema schema =
        new DataSchema(new String[]{"a", "b"}, new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.INT});
    MultiColumnDistinctTable table = new MultiColumnDistinctTable(schema, UNBOUNDED, false, null);
    table.addUnbounded(new Record(new Object[]{1, 2}));
    table.addUnbounded(new Record(new Object[]{2, 1}));
    assertEquals(table.size(), 2);

    DistinctCardinalityTracker tracker = DistinctCardinalityTracker.createIfUseful(2, 1, false, DEFAULT_BOUND, STD_DEV);
    assertNotNull(tracker);
    tracker.add(table);
    assertTrue(tracker.hasReachedLimit(), "(1,2) and (2,1) must hash differently");
  }

  /// The null marker is a flag on the DistinctTable rather than a set entry, but it counts towards size() and is
  /// materialized as a null-valued row, so it must count towards the cardinality too -- once, across every window
  /// that re-emits it.
  @Test
  public void testNullMarkerCountsExactlyOnce() {
    DistinctCardinalityTracker tracker = DistinctCardinalityTracker.createIfUseful(3, 1, false, DEFAULT_BOUND, STD_DEV);
    assertNotNull(tracker);
    for (int window = 0; window < 4; window++) {
      IntDistinctTable table = new IntDistinctTable(INT_SCHEMA, UNBOUNDED, true, null);
      table.addNull();
      table.addUnbounded(1);
      tracker.add(table);
      assertFalse(tracker.hasReachedLimit(), "{null, 1} repeated must stay at 2 distinct values");
    }
    IntDistinctTable table = new IntDistinctTable(INT_SCHEMA, UNBOUNDED, true, null);
    table.addUnbounded(2);
    tracker.add(table);
    assertTrue(tracker.hasReachedLimit());
  }

  /// A stored type with no hash defined (arrays, MAP, OBJECT, UNKNOWN) must collapse to a single hash rather than
  /// be given something derived from the value. Collapsing undercounts, so the leaf scans more; anything
  /// value-derived risks hashing two EQUAL values apart, which would exit below LIMIT and truncate.
  @Test
  public void testUnhashableStoredTypeUndercountsRatherThanInflates() {
    DataSchema schema = new DataSchema(new String[]{"a"}, new ColumnDataType[]{ColumnDataType.INT_ARRAY});
    MultiColumnDistinctTable table = new MultiColumnDistinctTable(schema, UNBOUNDED, false, null);
    table.addUnbounded(new Record(new Object[]{new int[]{1}}));
    table.addUnbounded(new Record(new Object[]{new int[]{2}}));
    table.addUnbounded(new Record(new Object[]{new int[]{3}}));
    assertEquals(table.size(), 3, "Fixture assumption: three distinct records");

    DistinctCardinalityTracker tracker = DistinctCardinalityTracker.createIfUseful(3, 1, false, DEFAULT_BOUND, STD_DEV);
    assertNotNull(tracker);
    tracker.add(table);
    assertFalse(tracker.hasReachedLimit(),
        "Unhashable values must collapse to one, leaving the leaf to scan on rather than exiting short");
  }

  /// Multi-column nulls live inside the records rather than on the table's null flag, so they go through the
  /// per-column branch of the dispatch -- which only runs with null handling on. Without this the branch had no
  /// coverage in the only mode that reaches it.
  @Test
  public void testMultiColumnNullsCountAsOneDistinctValue() {
    DataSchema schema =
        new DataSchema(new String[]{"a", "b"}, new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING});
    MultiColumnDistinctTable table = new MultiColumnDistinctTable(schema, UNBOUNDED, true, null);
    table.addUnbounded(new Record(new Object[]{1, null}));
    table.addUnbounded(new Record(new Object[]{1, "x"}));
    table.addUnbounded(new Record(new Object[]{null, "x"}));
    assertEquals(table.size(), 3, "Fixture assumption: nulls make distinct records");

    // A null in one column must not collapse the row onto a row that differs only there.
    DistinctCardinalityTracker tracker = DistinctCardinalityTracker.createIfUseful(3, 1, false, DEFAULT_BOUND,
        STD_DEV);
    assertNotNull(tracker);
    tracker.add(table);
    assertTrue(tracker.hasReachedLimit(), "(1,null), (1,x) and (null,x) must hash as three distinct rows");

    // ... and re-feeding the same records must not inflate the count.
    DistinctCardinalityTracker repeated = DistinctCardinalityTracker.createIfUseful(4, 1, false, DEFAULT_BOUND,
        STD_DEV);
    assertNotNull(repeated);
    repeated.add(table);
    repeated.add(table);
    assertFalse(repeated.hasReachedLimit(), "Three distinct records repeated must stay at three");
  }

  /// An empty table must be a no-op rather than an error, which is what the operator's final flush can hand it.
  @Test
  public void testEmptyTableIsANoOp() {
    DistinctCardinalityTracker tracker = DistinctCardinalityTracker.createIfUseful(2, 1, false, DEFAULT_BOUND, STD_DEV);
    assertNotNull(tracker);
    tracker.add(new IntDistinctTable(INT_SCHEMA, UNBOUNDED, false, null));
    assertFalse(tracker.hasReachedLimit());
  }

  /// Feeds 200 distinct values of one type and checks the tracker sees all 200: it must not be satisfied by a LIMIT
  /// of 200 until the last one lands, which fails if any two of them hash alike.
  /// Pins the measurement against the true distinct count from both sides, which is the invariant the early exit
  /// rests on:
  ///
  /// - it must not UNDERCOUNT by enough to miss LIMIT (that would merely cost scanning, but also means the type is
  ///   hashing badly), so 200 distinct values must satisfy a LIMIT of 200;
  /// - it must never OVERCOUNT, because that exits below LIMIT and truncates the result, so the same 200 values
  ///   must NOT satisfy a LIMIT of 201.
  ///
  /// The second is the direction that matters: it fails if two EQUAL values ever hash differently.
  private static void assertCounts(Supplier<DistinctTable> supplier) {
    DistinctTable table = supplier.get();
    assertEquals(table.size(), 200, "Fixture assumption");
    String type = table.getClass().getSimpleName();

    DistinctCardinalityTracker exactlyEnough =
        DistinctCardinalityTracker.createIfUseful(200, 10, false, DEFAULT_BOUND, STD_DEV);
    assertNotNull(exactlyEnough);
    exactlyEnough.add(table);
    assertTrue(exactlyEnough.hasReachedLimit(),
        "200 distinct values of " + type + " must satisfy a LIMIT of 200; a collision would leave it short");

    DistinctCardinalityTracker oneShort =
        DistinctCardinalityTracker.createIfUseful(201, 10, false, DEFAULT_BOUND, STD_DEV);
    assertNotNull(oneShort);
    oneShort.add(supplier.get());
    assertFalse(oneShort.hasReachedLimit(),
        "200 distinct values of " + type + " must NOT satisfy a LIMIT of 201 -- overcounting truncates results");
  }

  private static DataSchema schema(ColumnDataType columnDataType) {
    return new DataSchema(new String[]{"c"}, new ColumnDataType[]{columnDataType});
  }

  /// Builds a table holding the integers in `[fromInclusive, toExclusive)`.
  private static IntDistinctTable intTable(int fromInclusive, int toExclusive) {
    IntDistinctTable table = new IntDistinctTable(INT_SCHEMA, UNBOUNDED, false, null);
    for (int i = fromInclusive; i < toExclusive; i++) {
      table.addUnbounded(i);
    }
    return table;
  }
}
