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
package org.apache.pinot.core.query.aggregation.function;

import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.operator.transform.function.TextScoreTransformFunction;
import org.apache.pinot.core.plan.AggregationPlanNode;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.reader.TextIndexReader;
import org.apache.pinot.segment.spi.index.reader.TextScoreStats;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;


/// Unit tests of [TextScoreStatsAggregationFunction] outside of query execution: argument validation, merging of
/// partial results and the rejection of the scan-based code paths.
public class TextScoreStatsAggregationFunctionTest {
  private static final ExpressionContext COLUMN = ExpressionContext.forIdentifier("title");
  private static final ExpressionContext QUERY = ExpressionContext.forLiteral(DataType.STRING, "usb");
  private static final String EMPTY_STATS = new TextScoreStats().toJson();

  @Test
  public void testArgumentValidation() {
    expectThrows(IllegalArgumentException.class, () -> new TextScoreStatsAggregationFunction(List.of(), false));
    expectThrows(IllegalArgumentException.class, () -> new TextScoreStatsAggregationFunction(
        List.of(COLUMN, ExpressionContext.forLiteral(DataType.INT, 123)), false));
    expectThrows(IllegalArgumentException.class, () -> new TextScoreStatsAggregationFunction(
        List.of(COLUMN, QUERY, ExpressionContext.forLiteral(DataType.UNKNOWN, null)), false));
    expectThrows(IllegalArgumentException.class, () -> new TextScoreStatsAggregationFunction(List.of(COLUMN), false));
    expectThrows(IllegalArgumentException.class,
        () -> new TextScoreStatsAggregationFunction(List.of(COLUMN, QUERY, QUERY, QUERY), false));
    expectThrows(IllegalArgumentException.class,
        () -> new TextScoreStatsAggregationFunction(List.of(QUERY, QUERY), false));
    expectThrows(IllegalArgumentException.class,
        () -> new TextScoreStatsAggregationFunction(List.of(COLUMN, COLUMN), false));
    expectThrows(IllegalArgumentException.class,
        () -> new TextScoreStatsAggregationFunction(List.of(COLUMN, QUERY, COLUMN), false));
  }

  @Test
  public void testResultTypes() {
    TextScoreStatsAggregationFunction function = newFunction();
    assertEquals(function.getType(), AggregationFunctionType.TEXTSCORESTATS);
    assertEquals(function.getResultColumnName(), "textscorestats(title,'usb')");
    assertEquals(new TextScoreStatsAggregationFunction(List.of(COLUMN, QUERY, QUERY), false).getResultColumnName(),
        "textscorestats(title,'usb','usb')");
    assertEquals(function.getIntermediateResultColumnType(), ColumnDataType.STRING);
    assertEquals(function.getFinalResultColumnType(), ColumnDataType.STRING);
  }

  @Test
  public void testRowFilterPreventsTextIndexAccess() {
    QueryContext context = QueryContextConverterUtils.getQueryContext("SELECT TEXT_SCORE_STATS(title, 'usb') FROM t");
    context.setRowFilter(FilterContext.CONSTANT_FALSE);
    AggregationPlanNode node = new AggregationPlanNode(new SegmentContext(mock(IndexSegment.class)), context);
    assertEquals(expectThrows(IllegalArgumentException.class, node::run).getMessage(),
        "TEXT_SCORE_STATS is not supported when row-level security filters apply");
    assertEquals(expectThrows(IllegalArgumentException.class,
        () -> new TextScoreTransformFunction().init(List.of(), Map.of(), context)).getMessage(),
        "TEXT_SCORE is not supported when row-level security filters apply");
  }

  @Test
  public void testMerge() {
    TextScoreStatsAggregationFunction function = newFunction();
    String stats1 = stats(5, "usb", 2);
    String stats2 = stats(3, "usb", 1);
    assertEquals(function.merge(null, stats1), stats1);
    assertEquals(function.merge("", stats1), stats1);
    assertEquals(function.merge(stats1, null), stats1);
    assertEquals(function.merge(stats1, ""), stats1);
    assertEquals(function.merge(stats1, stats2), stats(8, "usb", 3));
    assertEquals(function.mergeFinalResult(stats1, stats2), stats(8, "usb", 3));
  }

  @Test
  public void testEmptyResults() {
    TextScoreStatsAggregationFunction function = newFunction();
    // No segment processed (for example every segment pruned)
    assertEquals(function.extractFinalResult(null), EMPTY_STATS);
    assertEquals(function.extractAggregationResult(function.createAggregationResultHolder()), EMPTY_STATS);
    String stats = stats(5, "usb", 2);
    assertEquals(function.extractFinalResult(stats), stats);
  }

  @Test
  public void testScanPathsAreRejected() {
    TextScoreStatsAggregationFunction function = newFunction();
    expectThrows(UnsupportedOperationException.class, () -> function.createGroupByResultHolder(1, 1));
    expectThrows(UnsupportedOperationException.class,
        () -> function.aggregate(1, function.createAggregationResultHolder(), Map.of()));
    expectThrows(UnsupportedOperationException.class,
        () -> function.aggregateGroupBySV(1, new int[1], null, Map.of()));
    expectThrows(UnsupportedOperationException.class,
        () -> function.aggregateGroupByMV(1, new int[1][], null, Map.of()));
    expectThrows(UnsupportedOperationException.class, () -> function.extractGroupByResult(null, 0));
  }

  @Test
  public void testColumnValidation() {
    DataSource dataSource = mock(DataSource.class);
    assertEquals(expectThrows(IllegalArgumentException.class, () -> newFunction().getSegmentStats(dataSource))
        .getMessage(), "Cannot apply TEXT_SCORE_STATS on column: title without text index");

    DataSourceMetadata metadata = mock(DataSourceMetadata.class);
    when(dataSource.getDataSourceMetadata()).thenReturn(metadata);
    when(dataSource.getTextIndex()).thenReturn(mock(TextIndexReader.class));
    when(metadata.getDataType()).thenReturn(DataType.STRING);
    assertEquals(expectThrows(IllegalArgumentException.class, () -> newFunction().getSegmentStats(dataSource))
        .getMessage(), "TEXT_SCORE_STATS requires a single-valued STRING column: title");
  }

  private static TextScoreStatsAggregationFunction newFunction() {
    return new TextScoreStatsAggregationFunction(List.of(COLUMN, QUERY), false);
  }

  private static String stats(long docCount, String term, long docFreq) {
    TextScoreStats stats = new TextScoreStats();
    TextScoreStats.FieldStats fieldStats = stats.getOrCreateField("title");
    fieldStats.addCollectionStats(docCount, docCount, docCount * 2, docCount);
    fieldStats.addTermStats(term, docFreq, docFreq);
    return stats.toJson();
  }
}
