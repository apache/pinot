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
package org.apache.pinot.query.planner.spi.stats;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// Covers the rollup semantics both stores fold through, so they cannot drift apart.
public class StatsAggregationsTest {

  private static final String COLUMN = "col";

  private static SegmentColumnStatsRow row(String segment, long ndv, String min, String max, boolean minTrusted,
      double avgBytes, double nullFraction, ColumnValueType valueType) {
    return new SegmentColumnStatsRow(segment, COLUMN, ndv, min, max, minTrusted, avgBytes, nullFraction, valueType);
  }

  @Test
  public void testEmptyAccumulatorReportsNoStatistics() {
    assertTrue(new StatsAggregations.ColumnStatsAccumulator().isEmpty());
  }

  @Test
  public void testBoundsUseTheRecordedOrdering() {
    StatsAggregations.ColumnStatsAccumulator acc = new StatsAggregations.ColumnStatsAccumulator();
    acc.add(100, row("s1", 5, "9", "9", true, 4, 0.0, ColumnValueType.LONG));
    acc.add(100, row("s2", 7, "10", "10", true, 4, 0.0, ColumnValueType.LONG));
    ColumnStatistics stats = acc.build(COLUMN);
    // Lexically "10" < "9"; numerically it is not. The recorded type decides.
    assertEquals(stats.getMinValue(), 9L);
    assertEquals(stats.getMaxValue(), 10L);
    assertTrue(stats.isMinTrusted());
  }

  @Test
  public void testStringColumnKeepsLexicalOrdering() {
    StatsAggregations.ColumnStatsAccumulator acc = new StatsAggregations.ColumnStatsAccumulator();
    acc.add(100, row("s1", 5, "9", "9", true, 4, 0.0, ColumnValueType.STRING));
    acc.add(100, row("s2", 7, "10", "10", true, 4, 0.0, ColumnValueType.STRING));
    ColumnStatistics stats = acc.build(COLUMN);
    assertEquals(stats.getMinValue(), "10");
    assertEquals(stats.getMaxValue(), "9");
  }

  @Test
  public void testUnrecordedTypeDropsBothBounds() {
    StatsAggregations.ColumnStatsAccumulator acc = new StatsAggregations.ColumnStatsAccumulator();
    acc.add(100, row("s1", 5, "1", "500", true, 4, 0.0, null));
    ColumnStatistics stats = acc.build(COLUMN);
    // Bounds folded without a known ordering are neither a true minimum nor a true maximum, and
    // isMinTrusted says nothing about the maximum -- so neither is reported at all.
    assertNull(stats.getMinValue());
    assertNull(stats.getMaxValue());
    assertFalse(stats.isMinTrusted());
    // NDV is unaffected: it needs no ordering.
    assertEquals(stats.getNdv(), 5);
  }

  @Test
  public void testConflictingTypesAcrossSegmentsDropBothBounds() {
    StatsAggregations.ColumnStatsAccumulator acc = new StatsAggregations.ColumnStatsAccumulator();
    acc.add(100, row("s1", 5, "1", "500", true, 4, 0.0, ColumnValueType.LONG));
    acc.add(100, row("s2", 5, "a", "z", true, 4, 0.0, ColumnValueType.STRING));
    ColumnStatistics stats = acc.build(COLUMN);
    assertNull(stats.getMinValue());
    assertNull(stats.getMaxValue());
    assertFalse(stats.isMinTrusted());
  }

  @Test
  public void testUntrustedMinInAnySegmentTaintsTheAggregate() {
    StatsAggregations.ColumnStatsAccumulator acc = new StatsAggregations.ColumnStatsAccumulator();
    acc.add(100, row("s1", 5, "1", "5", true, 4, 0.0, ColumnValueType.LONG));
    acc.add(100, row("s2", 5, "2", "6", false, 4, 0.0, ColumnValueType.LONG));
    assertFalse(acc.build(COLUMN).isMinTrusted());
  }

  @Test
  public void testWeightedAveragesAreDocumentWeighted() {
    StatsAggregations.ColumnStatsAccumulator acc = new StatsAggregations.ColumnStatsAccumulator();
    acc.add(300, row("s1", 5, "1", "5", true, 8, 0.4, ColumnValueType.LONG));
    acc.add(100, row("s2", 5, "1", "5", true, 4, 0.0, ColumnValueType.LONG));
    ColumnStatistics stats = acc.build(COLUMN);
    assertEquals(stats.getAvgBytesPerValue(), (8 * 300 + 4 * 100) / 400.0, 1e-9);
    assertEquals(stats.getNullFraction(), (0.4 * 300) / 400.0, 1e-9);
  }

  @Test
  public void testUnknownSentinelsAreExcludedFromAverages() {
    StatsAggregations.ColumnStatsAccumulator acc = new StatsAggregations.ColumnStatsAccumulator();
    acc.add(1000, row("s1", 5, "1", "5", true, -1, -1, ColumnValueType.LONG));
    acc.add(1000, row("s2", 5, "1", "5", true, 4, 0.2, ColumnValueType.LONG));
    ColumnStatistics stats = acc.build(COLUMN);
    // Weighting the -1 sentinel in as a measurement would yield 1.5 bytes and a NEGATIVE null
    // fraction -- neither a legal value nor the sentinel a consumer tests for.
    assertEquals(stats.getAvgBytesPerValue(), 4.0, 1e-9);
    assertEquals(stats.getNullFraction(), 0.2, 1e-9);
  }

  @Test
  public void testAveragesReportUnknownWhenEverySegmentIsUnknown() {
    StatsAggregations.ColumnStatsAccumulator acc = new StatsAggregations.ColumnStatsAccumulator();
    acc.add(1000, row("s1", 5, "1", "5", true, -1, -1, ColumnValueType.LONG));
    ColumnStatistics stats = acc.build(COLUMN);
    assertEquals(stats.getAvgBytesPerValue(), -1.0, 1e-9);
    assertEquals(stats.getNullFraction(), -1.0, 1e-9);
  }

  @Test
  public void testNdvTakesTheLargestSegmentValue() {
    StatsAggregations.ColumnStatsAccumulator acc = new StatsAggregations.ColumnStatsAccumulator();
    acc.add(100, row("s1", -1, "1", "5", true, 4, 0.0, ColumnValueType.LONG));
    acc.add(100, row("s2", 12, "1", "5", true, 4, 0.0, ColumnValueType.LONG));
    // MAX over segments is a lower bound on the table-wide NDV; the -1 sentinel cannot win it.
    assertEquals(acc.build(COLUMN).getNdv(), 12);
  }

  @Test
  public void testOverlapRowsInterpolatesPartialOverlap() {
    // Fully contained.
    assertEquals(StatsAggregations.overlapRows(100, 10, 20, 0, 100), 100);
    // No overlap at all.
    assertEquals(StatsAggregations.overlapRows(100, 10, 20, 30, 40), 0);
    // Half the segment's span falls in the range, so half its rows are attributed to it.
    assertEquals(StatsAggregations.overlapRows(100, 0, 100, 0, 50), 50);
  }
}
