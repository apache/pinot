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
package org.apache.pinot.core.query.pruner;

import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


public class SelectionQuerySegmentPrunerTest {
  public static final String ORDER_BY_COLUMN = "testColumn";

  public static final String FILTER_COLUMN = "foo";

  // Schema with a LONG order-by column and a STRING column, used by the filter-aware tests. Null handling is off, so
  // these columns are not treated as null-handling-active even though they are nullable by default.
  private static final Schema SCHEMA = new Schema.SchemaBuilder()
      .addSingleValueDimension(ORDER_BY_COLUMN, FieldSpec.DataType.LONG)
      .addSingleValueDimension(FILTER_COLUMN, FieldSpec.DataType.STRING)
      .build();

  private final SelectionQuerySegmentPruner _segmentPruner = new SelectionQuerySegmentPruner();

  @Test
  public void testLimit0() {
    List<IndexSegment> indexSegments =
        Arrays.asList(getIndexSegment(null, null, 10), getIndexSegment(0L, 10L, 10), getIndexSegment(-5L, 5L, 15));

    // Should keep only the first segment
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext("SELECT * FROM testTable LIMIT 0");
    List<IndexSegment> result = _segmentPruner.prune(indexSegments, queryContext);
    assertEquals(result.size(), 1);
    assertSame(result.get(0), indexSegments.get(0));

    queryContext =
        QueryContextConverterUtils.getQueryContext("SELECT * FROM testTable ORDER BY testColumn LIMIT 0");
    result = _segmentPruner.prune(indexSegments, queryContext);
    assertEquals(result.size(), 1);
    assertSame(result.get(0), indexSegments.get(0));
  }

  @Test
  public void testSelectionOnly() {
    List<IndexSegment> indexSegments =
        Arrays.asList(getIndexSegment(null, null, 10), getIndexSegment(0L, 10L, 10), getIndexSegment(-5L, 5L, 15));

    // Should keep enough documents to fulfill the LIMIT requirement
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext("SELECT * FROM testTable LIMIT 5");
    List<IndexSegment> result = _segmentPruner.prune(indexSegments, queryContext);
    assertEquals(result.size(), 1);
    assertSame(result.get(0), indexSegments.get(0));

    queryContext = QueryContextConverterUtils.getQueryContext("SELECT * FROM testTable LIMIT 10");
    result = _segmentPruner.prune(indexSegments, queryContext);
    assertEquals(result.size(), 1);
    assertSame(result.get(0), indexSegments.get(0));

    queryContext = QueryContextConverterUtils.getQueryContext("SELECT * FROM testTable LIMIT 15");
    result = _segmentPruner.prune(indexSegments, queryContext);
    assertEquals(result.size(), 2);
    assertSame(result.get(0), indexSegments.get(0));
    assertSame(result.get(1), indexSegments.get(1));

    queryContext = QueryContextConverterUtils.getQueryContext("SELECT * FROM testTable LIMIT 25");
    result = _segmentPruner.prune(indexSegments, queryContext);
    assertEquals(result.size(), 3);
    assertSame(result.get(0), indexSegments.get(0));
    assertSame(result.get(1), indexSegments.get(1));
    assertSame(result.get(2), indexSegments.get(2));

    queryContext = QueryContextConverterUtils.getQueryContext("SELECT * FROM testTable LIMIT 100");
    result = _segmentPruner.prune(indexSegments, queryContext);
    assertEquals(result.size(), 3);
    assertSame(result.get(0), indexSegments.get(0));
    assertSame(result.get(1), indexSegments.get(1));
    assertSame(result.get(2), indexSegments.get(2));
  }

  @Test
  public void testSelectionOrderBy() {
    List<IndexSegment> indexSegments = Arrays.asList(
        getIndexSegment(0L, 10L, 10),     // 0
        getIndexSegment(-5L, 5L, 15),     // 1
        getIndexSegment(15L, 50L, 30),    // 2
        getIndexSegment(5L, 15L, 20),     // 3
        getIndexSegment(20L, 30L, 5),     // 4
        getIndexSegment(null, null, 5),   // 5
        getIndexSegment(5L, 10L, 10),     // 6
        getIndexSegment(15L, 30L, 15));   // 7

    Schema schema = mock(Schema.class);
    // Should keep segments: [null, null], [-5, 5], [0, 10]
    QueryContext queryContext =
        QueryContextConverterUtils.getQueryContext("SELECT * FROM testTable ORDER BY testColumn LIMIT 5");
    queryContext.setSchema(schema);
    List<IndexSegment> result = _segmentPruner.prune(indexSegments, queryContext);
    assertEquals(result.size(), 3);
    assertSame(result.get(0), indexSegments.get(5));  // [null, null], 5
    assertSame(result.get(1), indexSegments.get(1));  // [-5, 5], 15
    assertSame(result.get(2), indexSegments.get(0));  // [0, 10], 10

    // Should keep segments: [null, null], [-5, 5], [0, 10], [5, 10], [5, 15]
    queryContext =
        QueryContextConverterUtils.getQueryContext("SELECT * FROM testTable ORDER BY testColumn LIMIT 15, 20");
    queryContext.setSchema(schema);
    result = _segmentPruner.prune(indexSegments, queryContext);
    assertEquals(result.size(), 5);
    assertSame(result.get(0), indexSegments.get(5));  // [null, null], 5
    assertSame(result.get(1), indexSegments.get(1));  // [-5, 5], 15
    // [0, 10], 10 & [5, 10], 10
    assertTrue(result.get(2) == indexSegments.get(0) || result.get(2) == indexSegments.get(6));
    assertTrue(result.get(3) == indexSegments.get(0) || result.get(3) == indexSegments.get(6));
    assertSame(result.get(4), indexSegments.get(3));  // [5, 15], 20

    // Should keep segments: [null, null], [-5, 5], [0, 10], [5, 10], [5, 15], [15, 30], [15, 50]
    queryContext =
        QueryContextConverterUtils.getQueryContext("SELECT * FROM testTable ORDER BY testColumn, foo LIMIT 40");
    queryContext.setSchema(schema);
    result = _segmentPruner.prune(indexSegments, queryContext);
    assertEquals(result.size(), 7);
    assertSame(result.get(0), indexSegments.get(5));  // [null, null], 5
    assertSame(result.get(1), indexSegments.get(1));  // [-5, 5], 15
    // [0, 10], 10 & [5, 10], 10
    assertTrue(result.get(2) == indexSegments.get(0) || result.get(2) == indexSegments.get(6));
    assertTrue(result.get(3) == indexSegments.get(0) || result.get(3) == indexSegments.get(6));
    assertSame(result.get(4), indexSegments.get(3));  // [5, 15], 20
    assertSame(result.get(5), indexSegments.get(7));  // [15, 30], 15
    assertSame(result.get(6), indexSegments.get(2));  // [15, 50], 30

    // Should keep segments: [null, null], [20, 30], [15, 50], [15, 30]
    queryContext =
        QueryContextConverterUtils.getQueryContext("SELECT * FROM testTable ORDER BY testColumn DESC LIMIT 5");
    queryContext.setSchema(schema);
    result = _segmentPruner.prune(indexSegments, queryContext);
    assertEquals(result.size(), 4);
    assertSame(result.get(0), indexSegments.get(5));  // [null, null], 5
    assertSame(result.get(1), indexSegments.get(4));  // [20, 30], 5
    // [15, 50], 30 & [15, 30], 15
    assertTrue(result.get(2) == indexSegments.get(2) || result.get(2) == indexSegments.get(7));
    assertTrue(result.get(3) == indexSegments.get(2) || result.get(3) == indexSegments.get(7));

    // Should keep segments: [null, null], [20, 30], [15, 50], [15, 30]
    queryContext = QueryContextConverterUtils
        .getQueryContext("SELECT * FROM testTable ORDER BY testColumn DESC LIMIT 5, 30");
    queryContext.setSchema(schema);
    result = _segmentPruner.prune(indexSegments, queryContext);
    assertEquals(result.size(), 4);
    assertSame(result.get(0), indexSegments.get(5));  // [null, null], 5
    assertSame(result.get(1), indexSegments.get(4));  // [20, 30], 5
    // [15, 50], 30 & [15, 30], 15
    assertTrue(result.get(2) == indexSegments.get(2) || result.get(2) == indexSegments.get(7));
    assertTrue(result.get(3) == indexSegments.get(2) || result.get(3) == indexSegments.get(7));

    // Should keep segments: [null, null], [20, 30], [15, 50], [15, 30], [5, 15], [5, 10], [0, 10], [-5, 5]
    queryContext = QueryContextConverterUtils
        .getQueryContext("SELECT * FROM testTable ORDER BY testColumn DESC, foo LIMIT 60");
    queryContext.setSchema(schema);
    result = _segmentPruner.prune(indexSegments, queryContext);
    assertEquals(result.size(), 8);
    assertSame(result.get(0), indexSegments.get(5));  // [null, null], 5
    assertSame(result.get(1), indexSegments.get(4));  // [20, 30], 5
    // [15, 50], 30 & [15, 30], 15
    assertTrue(result.get(2) == indexSegments.get(2) || result.get(2) == indexSegments.get(7));
    assertTrue(result.get(3) == indexSegments.get(2) || result.get(3) == indexSegments.get(7));
    // [5, 15], 20 & [5, 10], 10
    assertTrue(result.get(4) == indexSegments.get(3) || result.get(4) == indexSegments.get(6));
    assertTrue(result.get(5) == indexSegments.get(3) || result.get(5) == indexSegments.get(6));
    assertSame(result.get(6), indexSegments.get(0));  // [0, 10], 10
    assertSame(result.get(7), indexSegments.get(1));  // [-5, 5], 15
  }

  @Test
  public void testUpsertTable() {
    List<IndexSegment> indexSegments = Arrays
        .asList(getIndexSegment(0L, 10L, 10, true), getIndexSegment(20L, 30L, 10, true),
            getIndexSegment(40L, 50L, 10, true));

    // Should not prune any segment for upsert table
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext("SELECT * FROM testTable LIMIT 5");
    List<IndexSegment> result = _segmentPruner.prune(indexSegments, queryContext);
    assertEquals(result.size(), 3);

    queryContext =
        QueryContextConverterUtils.getQueryContext("SELECT * FROM testTable ORDER BY testColumn LIMIT 5");
    result = _segmentPruner.prune(indexSegments, queryContext);
    assertEquals(result.size(), 3);
  }

  /**
   * Range-partitioned, non-overlapping segments on {@code testColumn}: [0,9], [10,19], [20,29], [30,39], [40,49].
   */
  private List<IndexSegment> rangePartitionedSegments() {
    return Arrays.asList(
        getIndexSegment(0L, 9L, 10),     // 0
        getIndexSegment(10L, 19L, 10),   // 1
        getIndexSegment(20L, 29L, 10),   // 2
        getIndexSegment(30L, 39L, 10),   // 3
        getIndexSegment(40L, 49L, 10));  // 4
  }

  private List<IndexSegment> prune(List<IndexSegment> segments, String query) {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
    queryContext.setSchema(SCHEMA);
    return _segmentPruner.prune(segments, queryContext);
  }

  @Test
  public void testSelectionOrderByWithFilterDesc() {
    List<IndexSegment> segments = rangePartitionedSegments();
    // DESC, want largest 5 with col > 25: all live in [40,49]. Only the top fully-matching segment is kept.
    List<IndexSegment> result =
        prune(segments, "SELECT * FROM testTable WHERE testColumn > 25 ORDER BY testColumn DESC LIMIT 5");
    assertEquals(result, List.of(segments.get(4)));

    // >= behaves the same here (min 40 >= 30).
    result = prune(segments, "SELECT * FROM testTable WHERE testColumn >= 30 ORDER BY testColumn DESC LIMIT 5");
    assertEquals(result, List.of(segments.get(4)));
  }

  @Test
  public void testSelectionOrderByWithFilterAsc() {
    List<IndexSegment> segments = rangePartitionedSegments();
    // ASC, want smallest 15 with col < 25: live in [0,9] (0-9) and [10,19] (10-14). Higher segments are pruned.
    List<IndexSegment> result =
        prune(segments, "SELECT * FROM testTable WHERE testColumn < 25 ORDER BY testColumn LIMIT 15");
    assertEquals(result, List.of(segments.get(0), segments.get(1)));

    // <= behaves the same here (max 19 <= 24).
    result = prune(segments, "SELECT * FROM testTable WHERE testColumn <= 24 ORDER BY testColumn LIMIT 15");
    assertEquals(result, List.of(segments.get(0), segments.get(1)));
  }

  @Test
  public void testFilterStraddlingSegmentIsKept() {
    // Correctness counterexample for the lower-bound rule: DESC, col < 35, LIMIT 5. The answer (34..30) lives in the
    // straddling segment [30,39], which is counted as 0 matching docs. A naive getTotalDocs() accumulation would let
    // the boundary advance past it and prune it; the lower-bound rule must keep it.
    List<IndexSegment> segments = rangePartitionedSegments();
    List<IndexSegment> result =
        prune(segments, "SELECT * FROM testTable WHERE testColumn < 35 ORDER BY testColumn DESC LIMIT 5");
    assertTrue(result.contains(segments.get(3)), "straddling segment [30,39] holding the top-n must be kept");
  }

  @Test
  public void testFilterEqAndNotEq() {
    // EQ fully matches only a single-valued segment; here add one collapsed to 25.
    List<IndexSegment> segments = Arrays.asList(
        getIndexSegment(0L, 9L, 10),     // 0
        getIndexSegment(25L, 25L, 10),   // 1 (single value 25)
        getIndexSegment(40L, 49L, 10));  // 2
    // col = 25 ORDER BY col DESC LIMIT 5 -> only [25,25] fully matches; [40,49] (max 49 >= 25 boundary) pruned.
    List<IndexSegment> result =
        prune(segments, "SELECT * FROM testTable WHERE testColumn = 25 ORDER BY testColumn DESC LIMIT 5");
    assertTrue(result.contains(segments.get(1)));

    // col <> 25 DESC LIMIT 5 -> largest != 25 live in [40,49]; that segment fully matches (25 outside [40,49]).
    result = prune(segments, "SELECT * FROM testTable WHERE testColumn <> 25 ORDER BY testColumn DESC LIMIT 5");
    assertEquals(result, List.of(segments.get(2)));
  }

  @Test
  public void testAndConjunctOnNonProvableColumnDisablesPruning() {
    // col > 25 is provably-full on the high segments, but the ANDed predicate on an unanalyzable column ('foo' is not
    // in the schema) cannot be proven full for any segment -> 0 lower bound everywhere -> nothing is pruned (safe).
    List<IndexSegment> segments = rangePartitionedSegments();
    List<IndexSegment> result = prune(segments,
        "SELECT * FROM testTable WHERE testColumn > 25 AND foo = 'x' ORDER BY testColumn DESC LIMIT 5");
    assertEquals(result.size(), segments.size());
  }

  @Test
  public void testOrFilterFullMatch() {
    // LIMIT exceeds the total doc count (5 segments * 10 docs), so guaranteedMatchingDocs() -> fullyMatches() is
    // evaluated for every segment: the OR is fully matched when ANY child is (seg [30,39], [40,49] via testColumn > 25)
    // and not matched otherwise. No segment can be pruned (the limit needs them all), so the result is unchanged.
    List<IndexSegment> segments = rangePartitionedSegments();
    List<IndexSegment> result = prune(segments,
        "SELECT * FROM testTable WHERE testColumn > 25 OR testColumn > 1000 ORDER BY testColumn LIMIT 100");
    assertEquals(result.size(), segments.size());
  }

  @Test
  public void testFilterNotProvablyFullCases() {
    // Each of these filters cannot be proven full-match from min/max metadata, so every segment contributes 0 and
    // (with a LIMIT larger than the data) nothing is pruned. They exercise the conservative fall-through branches.
    List<IndexSegment> segments = rangePartitionedSegments();
    // Predicate on a non-identifier (function) expression.
    assertEquals(prune(segments,
        "SELECT * FROM testTable WHERE testColumn + 1 > 5 ORDER BY testColumn LIMIT 100").size(), segments.size());
    // Unsupported predicate type (IN) for the full-match analysis.
    assertEquals(prune(segments,
        "SELECT * FROM testTable WHERE testColumn IN (1, 2, 3) ORDER BY testColumn LIMIT 100").size(), segments.size());
    // NOT filter is never treated as full-match.
    assertEquals(prune(segments,
        "SELECT * FROM testTable WHERE NOT (testColumn IN (1, 2, 3)) ORDER BY testColumn LIMIT 100").size(),
        segments.size());
    // Bound larger than any value: no segment is fully contained, so nothing is pruned.
    assertEquals(prune(segments,
        "SELECT * FROM testTable WHERE testColumn > 99999999999999999999999 ORDER BY testColumn LIMIT 100").size(),
        segments.size());
  }

  @Test
  public void testAndAllChildrenFullMatch() {
    // AND where every child is provably full on the top segment -> AND returns true and the set is limit-pruned.
    List<IndexSegment> segments = rangePartitionedSegments();
    List<IndexSegment> result = prune(segments,
        "SELECT * FROM testTable WHERE testColumn > 25 AND testColumn < 1000 ORDER BY testColumn DESC LIMIT 5");
    assertEquals(result, List.of(segments.get(4)));
  }

  @Test
  public void testRangeInclusiveBoundsNotFull() {
    // Inclusive bounds where some processed segments are not fully contained, exercising the inclusive "not full"
    // branches. LIMIT exceeds the data so nothing is pruned.
    List<IndexSegment> segments = rangePartitionedSegments();
    assertEquals(prune(segments,
        "SELECT * FROM testTable WHERE testColumn >= 25 ORDER BY testColumn LIMIT 100").size(), segments.size());
    assertEquals(prune(segments,
        "SELECT * FROM testTable WHERE testColumn <= 25 ORDER BY testColumn LIMIT 100").size(), segments.size());
  }

  @Test
  public void testPruneCalledWithNullHandlingActive() {
    // prune() invoked directly (bypassing isApplicableTo) with null handling on: the predicate column is
    // null-handling-active, so it is never provably full and nothing is pruned.
    List<IndexSegment> segments = rangePartitionedSegments();
    List<IndexSegment> result = prune(segments, "SET enableNullHandling=true; "
        + "SELECT * FROM testTable WHERE testColumn > 25 ORDER BY testColumn DESC LIMIT 100");
    assertEquals(result.size(), segments.size());
  }

  @Test
  public void testFilterPredicateColumnWithoutMinMax() {
    // The filter column has no min/max metadata, so it cannot be proven full-match -> 0 contribution, nothing pruned.
    List<IndexSegment> segments = Arrays.asList(
        getIndexSegment(0L, 9L, 10, false, null, null),
        getIndexSegment(10L, 19L, 10, false, null, null));
    List<IndexSegment> result =
        prune(segments, "SELECT * FROM testTable WHERE foo >= 'a' ORDER BY testColumn DESC LIMIT 5");
    assertEquals(result.size(), segments.size());
  }

  @Test
  public void testFilteredSelectionOnlyNotPruned() {
    // Selection-only (no ORDER BY) with a filter: count-based pruning is unsafe, so prune() keeps every segment.
    List<IndexSegment> segments = rangePartitionedSegments();
    List<IndexSegment> result = prune(segments, "SELECT * FROM testTable WHERE testColumn > 25 LIMIT 5");
    assertEquals(result.size(), segments.size());
  }

  @Test
  public void testFilterWithOffset() {
    List<IndexSegment> segments = rangePartitionedSegments();
    // DESC, col > 25, LIMIT 5 OFFSET 30. remainingDocs = 35; the two fully-matching segments [30,39] and [40,49]
    // provide only 20 matching docs, so the boundary never advances enough to prune them; both are kept.
    List<IndexSegment> result =
        prune(segments, "SELECT * FROM testTable WHERE testColumn > 25 ORDER BY testColumn DESC LIMIT 5, 30");
    assertEquals(result.size(), segments.size());
  }

  @Test
  public void testFloatNaNNotTreatedAsFullMatch() {
    // Regression: an all-NaN DOUBLE segment must not be counted as fully matching 'col > 5'. NaN sorts as the largest
    // value, so without the NaN guard the all-NaN segment would (a) be counted as 10 matching docs and (b) sort first
    // (DESC), advancing the boundary and wrongly pruning the segment [10, 20] that actually holds the top-n.
    Schema doubleSchema =
        new Schema.SchemaBuilder().addSingleValueDimension(ORDER_BY_COLUMN, FieldSpec.DataType.DOUBLE).build();
    IndexSegment nanSegment = getDoubleSegment(Double.NaN, Double.NaN, 10);
    IndexSegment realSegment = getDoubleSegment(10.0, 20.0, 10);
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT * FROM testTable WHERE testColumn > 5 ORDER BY testColumn DESC LIMIT 5");
    queryContext.setSchema(doubleSchema);
    List<IndexSegment> result = _segmentPruner.prune(Arrays.asList(nanSegment, realSegment), queryContext);
    assertTrue(result.contains(realSegment), "segment [10, 20] holding the top-n must not be pruned");
  }

  private IndexSegment getDoubleSegment(Double minValue, Double maxValue, int totalDocs) {
    IndexSegment indexSegment = mock(IndexSegment.class);
    DataSource dataSource = mock(DataSource.class);
    when(indexSegment.getDataSource(eq(ORDER_BY_COLUMN), any(Schema.class))).thenReturn(dataSource);
    DataSourceMetadata dataSourceMetadata = mock(DataSourceMetadata.class);
    when(dataSource.getDataSourceMetadata()).thenReturn(dataSourceMetadata);
    when(dataSourceMetadata.getMinValue()).thenReturn(minValue);
    when(dataSourceMetadata.getMaxValue()).thenReturn(maxValue);
    when(dataSourceMetadata.getDataType()).thenReturn(FieldSpec.DataType.DOUBLE);
    SegmentMetadata segmentMetadata = mock(SegmentMetadata.class);
    when(indexSegment.getSegmentMetadata()).thenReturn(segmentMetadata);
    when(segmentMetadata.getTotalDocs()).thenReturn(totalDocs);
    return indexSegment;
  }

  @Test
  public void testIsApplicableTo() {
    // No filter: applicable (existing behavior), with or without order by.
    assertTrue(_segmentPruner.isApplicableTo(queryWithSchema("SELECT * FROM testTable LIMIT 5")));
    assertTrue(_segmentPruner.isApplicableTo(queryWithSchema("SELECT * FROM testTable ORDER BY testColumn LIMIT 5")));
    // LIMIT 0 with a filter: applicable (just keeps one segment for the schema).
    assertTrue(_segmentPruner.isApplicableTo(
        queryWithSchema("SELECT * FROM testTable WHERE testColumn > 5 ORDER BY testColumn LIMIT 0")));
    // Filtered order-by on a non-nullable identifier: applicable.
    assertTrue(_segmentPruner.isApplicableTo(
        queryWithSchema("SELECT * FROM testTable WHERE testColumn > 5 ORDER BY testColumn DESC LIMIT 5")));
    // Filtered selection-only (no order by): not applicable (count-based pruning unsafe with a filter).
    assertFalse(_segmentPruner.isApplicableTo(queryWithSchema("SELECT * FROM testTable WHERE testColumn > 5 LIMIT 5")));
    // Filtered order-by on a non-identifier: not applicable.
    assertFalse(_segmentPruner.isApplicableTo(
        queryWithSchema("SELECT * FROM testTable WHERE testColumn > 5 ORDER BY testColumn + 1 DESC LIMIT 5")));
    // Filtered order-by but null handling enabled -> column treated as nullable -> not applicable.
    assertFalse(_segmentPruner.isApplicableTo(queryWithSchema(
        "SET enableNullHandling=true; SELECT * FROM testTable WHERE testColumn > 5 ORDER BY testColumn DESC LIMIT 5")));
    // Null handling on but no schema available -> conservatively treated as null-handling-active -> not applicable.
    assertFalse(_segmentPruner.isApplicableTo(QueryContextConverterUtils.getQueryContext(
        "SET enableNullHandling=true; SELECT * FROM testTable WHERE testColumn > 5 ORDER BY testColumn DESC LIMIT 5")));
  }

  @Test
  public void testIsApplicableToColumnBasedNullHandling() {
    String query = "SET enableNullHandling=true; "
        + "SELECT * FROM testTable WHERE testColumn > 5 ORDER BY testColumn DESC LIMIT 5";

    // Column-based null handling + non-nullable column: nulls cannot occur, so the optimization still applies even
    // though null handling is enabled.
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
    queryContext.setSchema(columnBasedNullHandlingSchema(false));
    assertTrue(_segmentPruner.isApplicableTo(queryContext));

    // Column-based null handling + nullable column: skip (min/max may be polluted by nulls).
    queryContext = QueryContextConverterUtils.getQueryContext(query);
    queryContext.setSchema(columnBasedNullHandlingSchema(true));
    assertFalse(_segmentPruner.isApplicableTo(queryContext));
  }

  private static Schema columnBasedNullHandlingSchema(boolean orderByColumnNullable) {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(ORDER_BY_COLUMN, FieldSpec.DataType.LONG)
        .setEnableColumnBasedNullHandling(true)
        .build();
    schema.getFieldSpecFor(ORDER_BY_COLUMN).setNullable(orderByColumnNullable);
    return schema;
  }

  private QueryContext queryWithSchema(String query) {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
    queryContext.setSchema(SCHEMA);
    return queryContext;
  }

  private IndexSegment getIndexSegment(@Nullable Long minValue, @Nullable Long maxValue, int totalDocs) {
    return getIndexSegment(minValue, maxValue, totalDocs, false);
  }

  private IndexSegment getIndexSegment(@Nullable Long minValue, @Nullable Long maxValue, int totalDocs,
      boolean upsert) {
    // A STRING filter column whose [min, max] = ["a", "z"] never collapses to a single value, so a predicate like
    // 'foo = x' is never provably full-match -> exercises the "AND with a non-provable conjunct" path.
    return getIndexSegment(minValue, maxValue, totalDocs, upsert, "a", "z");
  }

  private IndexSegment getIndexSegment(@Nullable Long minValue, @Nullable Long maxValue, int totalDocs,
      boolean upsert, @Nullable String filterMinValue, @Nullable String filterMaxValue) {
    IndexSegment indexSegment = mock(IndexSegment.class);
    when(indexSegment.getColumnNames()).thenReturn(ImmutableSet.of("foo", "testColumn"));
    DataSource dataSource = mock(DataSource.class);
    when(indexSegment.getDataSource(eq(ORDER_BY_COLUMN), any(Schema.class))).thenReturn(dataSource);
    DataSourceMetadata dataSourceMetadata = mock(DataSourceMetadata.class);
    when(dataSource.getDataSourceMetadata()).thenReturn(dataSourceMetadata);
    when(dataSourceMetadata.getMinValue()).thenReturn(minValue);
    when(dataSourceMetadata.getMaxValue()).thenReturn(maxValue);
    when(dataSourceMetadata.getDataType()).thenReturn(FieldSpec.DataType.LONG);
    DataSource filterDataSource = mock(DataSource.class);
    when(indexSegment.getDataSource(eq(FILTER_COLUMN), any(Schema.class))).thenReturn(filterDataSource);
    DataSourceMetadata filterMetadata = mock(DataSourceMetadata.class);
    when(filterDataSource.getDataSourceMetadata()).thenReturn(filterMetadata);
    when(filterMetadata.getMinValue()).thenReturn(filterMinValue);
    when(filterMetadata.getMaxValue()).thenReturn(filterMaxValue);
    when(filterMetadata.getDataType()).thenReturn(FieldSpec.DataType.STRING);
    SegmentMetadata segmentMetadata = mock(SegmentMetadata.class);
    when(indexSegment.getSegmentMetadata()).thenReturn(segmentMetadata);
    when(segmentMetadata.getTotalDocs()).thenReturn(totalDocs);
    if (upsert) {
      ThreadSafeMutableRoaringBitmap validDocIds = mock(ThreadSafeMutableRoaringBitmap.class);
      when(indexSegment.getValidDocIds()).thenReturn(validDocIds);
    }
    return indexSegment;
  }
}
