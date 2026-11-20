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
package org.apache.pinot.queries;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.startree.v2.builder.MultipleTreesBuilder;
import org.apache.pinot.segment.local.startree.v2.builder.MultipleTreesBuilder.BuildMode;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.StarTreeAggregationConfig;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// Queries answered from a null-aware star-tree, where the dimension itself contains nulls.
///
/// `NullAwareStarTreeBuilderTest` only inspects what the builder stored. These go through the query path instead,
/// which is where a null row has to read back as a null rather than as the value standing in for it.
///
/// The segment is built twice, once per build mode, and both copies are queried together: the harness fans the
/// segment list out to two servers, so every aggregate is scaled by [#SEGMENT_COPIES], and a builder that disagreed
/// with the other would show up as a wrong total. Each segment holds a null-aware and a regular star-tree over the
/// same columns, so routing between the two is exercised as well.
public class NullAwareStarTreeQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "NullAwareStarTreeQueriesTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final String DIMENSION = "d";
  private static final String METRIC = "m";
  /// A metric that is null for every row of one dimension group, so that group has no aggregated value.
  private static final String SPARSE_METRIC = "m2";
  /// A metric that is null in every row, so no group has an aggregated value and the column has no size to derive.
  private static final String PRECISE_METRIC = "m3";

  private static final Map<String, String> NULL_HANDLING_ENABLED = Map.of("enableNullHandling", "true");

  /// Two segments per server and two servers.
  private static final int SEGMENT_COPIES = 4;

  /// One record per leaf, so each distinct dimension value gets its own pre-aggregated document.
  private static final int MAX_LEAF_RECORDS = 1;

  /// `d` is null for two rows, so the null-aware star-tree stores those under the reserved dictionary id. The metric
  /// sums are distinct per group so a group picking up the wrong rows is visible in the answer.
  private static final Integer[] DIMENSION_VALUES = {1, 1, 1, 1, null, null, 2, 2, 2, 2};
  private static final int[] METRIC_VALUES = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  private static final Integer[] SPARSE_METRIC_VALUES = {10, null, 30, 40, 50, null, null, null, null, null};
  private static final int SUM_WHERE_D_IS_1 = 1 + 2 + 3 + 4;
  private static final int SUM_WHERE_D_IS_NULL = 5 + 6;
  private static final int SUM_WHERE_D_IS_2 = 7 + 8 + 9 + 10;
  private static final int SPARSE_SUM_WHERE_D_IS_1 = 10 + 30 + 40;
  private static final int SPARSE_COUNT_WHERE_D_IS_1 = 3;
  private static final int SPARSE_SUM_WHERE_D_IS_NULL = 50;
  private static final int SPARSE_COUNT_WHERE_D_IS_NULL = 1;
  /// A value no row holds, so a `<>` against it is true over every real value and UNKNOWN over a null.
  private static final int ABSENT_VALUE = 99999;

  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegments;

  @Override
  protected String getFilter() {
    return "";
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  @Override
  protected List<IndexSegment> getIndexSegments() {
    return _indexSegments;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);
    _indexSegments = new ArrayList<>(2);
    for (BuildMode buildMode : BuildMode.values()) {
      _indexSegments.add(buildSegment(buildMode));
    }
    _indexSegment = _indexSegments.get(0);
  }

  private static ImmutableSegment buildSegment(BuildMode buildMode)
      throws Exception {
    Schema schema = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
        .addSingleValueDimension(DIMENSION, DataType.INT)
        .addMetric(METRIC, DataType.INT)
        .addMetric(SPARSE_METRIC, DataType.INT)
        .addMetric(PRECISE_METRIC, DataType.BIG_DECIMAL)
        .build();
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setNullHandlingEnabled(true).build();

    List<GenericRow> rows = new ArrayList<>(DIMENSION_VALUES.length);
    for (int i = 0; i < DIMENSION_VALUES.length; i++) {
      GenericRow row = new GenericRow();
      row.putValue(DIMENSION, DIMENSION_VALUES[i]);
      row.putValue(METRIC, METRIC_VALUES[i]);
      row.putValue(SPARSE_METRIC, SPARSE_METRIC_VALUES[i]);
      row.putValue(PRECISE_METRIC, null);
      rows.add(row);
    }

    File outDir = new File(INDEX_DIR, buildMode.name());
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
    segmentGeneratorConfig.setTableName(RAW_TABLE_NAME);
    segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
    segmentGeneratorConfig.setDefaultNullHandlingEnabled(true);
    segmentGeneratorConfig.setOutDir(outDir.getPath());
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(rows));
    driver.build();

    File indexDir = new File(outDir, SEGMENT_NAME);
    StarTreeIndexConfig nullAwareConfig = new StarTreeIndexConfig(List.of(DIMENSION), null,
        List.of("SUM__" + METRIC, "SUM__" + SPARSE_METRIC, "COUNT__" + SPARSE_METRIC, "AVG__" + SPARSE_METRIC,
            "SUMPRECISION__" + PRECISE_METRIC, "COUNT__*"),
        List.of(new StarTreeAggregationConfig(SPARSE_METRIC, "arrayAgg")), MAX_LEAF_RECORDS, true);
    StarTreeIndexConfig regularConfig = new StarTreeIndexConfig(List.of(DIMENSION), null,
        List.of("SUM__" + METRIC, "SUM__" + SPARSE_METRIC, "COUNT__*"), null, MAX_LEAF_RECORDS);
    try (MultipleTreesBuilder builder =
        new MultipleTreesBuilder(List.of(nullAwareConfig, regularConfig), false, indexDir, buildMode)) {
      builder.build();
    }
    return ImmutableSegmentLoader.load(indexDir, ReadMode.mmap);
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    for (IndexSegment indexSegment : _indexSegments) {
      indexSegment.destroy();
    }
    FileUtils.deleteDirectory(INDEX_DIR);
  }

  /// A null row is stored under the column's default null value and marked in the dimension's null vector, so the
  /// group key generator has to read that vector and give the row a group of its own.
  @Test
  public void groupingByANullDimensionReturnsTheNullGroup() {
    BrokerResponseNative response = getBrokerResponse(
        String.format("SELECT %s, SUM(%s) FROM testTable GROUP BY %s", DIMENSION, METRIC, DIMENSION),
        NULL_HANDLING_ENABLED);

    assertStarTreeWasUsed(response);
    ResultTable resultTable = response.getResultTable();
    assertEquals(resultTable.getRows().size(), 3);
    assertEquals(aggregateForGroup(resultTable, 1), (double) SUM_WHERE_D_IS_1 * SEGMENT_COPIES);
    assertEquals(aggregateForGroup(resultTable, 2), (double) SUM_WHERE_D_IS_2 * SEGMENT_COPIES);
    assertEquals(aggregateForGroup(resultTable, null), (double) SUM_WHERE_D_IS_NULL * SEGMENT_COPIES,
        "Rows whose dimension is null must form their own group rather than joining a real value's group");
  }

  /// A group-by on a transform over the dimension reads its values rather than its dictionary ids, so every id the
  /// star-tree's forward index holds has to resolve against the dictionary it shares with the segment.
  @Test
  public void groupingByATransformOverANullDimensionReturnsTheNullGroup() {
    BrokerResponseNative response = getBrokerResponse(
        String.format("SELECT %s + 1, SUM(%s) FROM testTable GROUP BY %s + 1", DIMENSION, METRIC, DIMENSION),
        NULL_HANDLING_ENABLED);

    assertStarTreeWasUsed(response);
    ResultTable resultTable = response.getResultTable();
    assertEquals(resultTable.getRows().size(), 3);
    assertEquals(aggregateForGroup(resultTable, 2.0), (double) SUM_WHERE_D_IS_1 * SEGMENT_COPIES);
    assertEquals(aggregateForGroup(resultTable, 3.0), (double) SUM_WHERE_D_IS_2 * SEGMENT_COPIES);
    assertEquals(aggregateForGroup(resultTable, null), (double) SUM_WHERE_D_IS_NULL * SEGMENT_COPIES,
        "A null dimension makes the transform null, so those rows must form their own group");
  }

  /// A predicate that is always true over real values is UNKNOWN over a null, so the null rows are not selected.
  @Test
  public void anAlwaysTruePredicateExcludesNullRows() {
    BrokerResponseNative response = getBrokerResponse(
        String.format("SELECT SUM(%s) FROM testTable WHERE %s <> %d", METRIC, DIMENSION, ABSENT_VALUE),
        NULL_HANDLING_ENABLED);

    assertStarTreeWasUsed(response);
    assertEquals(response.getResultTable().getRows().get(0)[0],
        (double) (SUM_WHERE_D_IS_1 + SUM_WHERE_D_IS_2) * SEGMENT_COPIES,
        "A null dimension makes the predicate UNKNOWN, so the row must not be aggregated");
  }

  /// A predicate excluding one real value matches every other one but not a null, whose reserved id lies one past
  /// the last real value and which the predicate is UNKNOWN over anyway. The negated form reaches the star-tree as a
  /// negated composite predicate rather than as an exclusive one, so both shapes are covered.
  @Test
  public void aPredicateExcludingOneValueAlsoExcludesNullRows() {
    for (String predicate : List.of(DIMENSION + " <> 1", "NOT (" + DIMENSION + " = 1)")) {
      BrokerResponseNative response =
          getBrokerResponse(String.format("SELECT SUM(%s) FROM testTable WHERE %s", METRIC, predicate),
              NULL_HANDLING_ENABLED);

      assertStarTreeWasUsed(response);
      assertEquals(response.getResultTable().getRows().get(0)[0], (double) SUM_WHERE_D_IS_2 * SEGMENT_COPIES,
          "Wrong answer for predicate: " + predicate);
    }
  }

  /// With a predicate on the dimension, the null rows are UNKNOWN and no null group may appear.
  @Test
  public void aFilteredGroupByHasNoNullGroup() {
    BrokerResponseNative response = getBrokerResponse(
        String.format("SELECT %s, SUM(%s) FROM testTable WHERE %s <> 1 GROUP BY %s", DIMENSION, METRIC, DIMENSION,
            DIMENSION), NULL_HANDLING_ENABLED);

    assertStarTreeWasUsed(response);
    ResultTable resultTable = response.getResultTable();
    assertEquals(resultTable.getRows().size(), 1);
    assertEquals(aggregateForGroup(resultTable, 2), (double) SUM_WHERE_D_IS_2 * SEGMENT_COPIES);
  }

  /// A group whose metric is null in every row has no aggregated value: `SUM` is `NULL` and `COUNT(column)` is 0,
  /// which the null-aware star-tree stores per column rather than as the row count.
  @Test
  public void anAllNullMetricGroupAggregatesToNull() {
    BrokerResponseNative response = getBrokerResponse(
        String.format("SELECT %s, SUM(%s), COUNT(%s) FROM testTable GROUP BY %s", DIMENSION, SPARSE_METRIC,
            SPARSE_METRIC, DIMENSION), NULL_HANDLING_ENABLED);

    assertStarTreeWasUsed(response);
    ResultTable resultTable = response.getResultTable();
    assertEquals(resultTable.getRows().size(), 3);
    assertEquals(aggregateForGroup(resultTable, 1), (double) SPARSE_SUM_WHERE_D_IS_1 * SEGMENT_COPIES);
    assertEquals(countForGroup(resultTable, 1), (long) SPARSE_COUNT_WHERE_D_IS_1 * SEGMENT_COPIES);
    assertEquals(aggregateForGroup(resultTable, null), (double) SPARSE_SUM_WHERE_D_IS_NULL * SEGMENT_COPIES);
    assertEquals(countForGroup(resultTable, null), (long) SPARSE_COUNT_WHERE_D_IS_NULL * SEGMENT_COPIES);
    assertNull(rowForGroup(resultTable, 2)[1], "A group with no non-null value has no sum");
    assertEquals(countForGroup(resultTable, 2), 0L);
  }

  /// `AVG` reads its pre-aggregated pairs straight from the forward index, where a group that aggregated over no
  /// non-null input holds a placeholder no aggregator can decode, so it has to skip the rows the null vector marks.
  @Test
  public void anAllNullMetricGroupAveragesToNull() {
    BrokerResponseNative response = getBrokerResponse(
        String.format("SELECT %s, AVG(%s) FROM testTable GROUP BY %s", DIMENSION, SPARSE_METRIC, DIMENSION),
        NULL_HANDLING_ENABLED);

    assertStarTreeWasUsed(response);
    ResultTable resultTable = response.getResultTable();
    assertEquals(resultTable.getRows().size(), 3);
    // An average needs no scaling by the segment copies: both its sum and its count scale with them
    assertEquals(aggregateForGroup(resultTable, 1), (double) SPARSE_SUM_WHERE_D_IS_1 / SPARSE_COUNT_WHERE_D_IS_1);
    assertEquals(aggregateForGroup(resultTable, null),
        (double) SPARSE_SUM_WHERE_D_IS_NULL / SPARSE_COUNT_WHERE_D_IS_NULL);
    assertNull(rowForGroup(resultTable, 2)[1], "A group with no non-null value has no average");
  }

  /// `arrayAgg` reads its pre-aggregated cells as serialized sets, and a group that aggregated over no non-null input
  /// holds a placeholder that is not one, so it has to skip the rows the null vector marks. Only the distinct variant
  /// is stored in a star-tree, and a distinct set absorbs the repeated segment copies rather than scaling with them.
  @Test
  public void anAllNullMetricGroupArrayAggregatesToAnEmptyArray() {
    BrokerResponseNative response = getBrokerResponse(
        String.format("SELECT %s, arrayAgg(%s, 'INT', true) FROM testTable GROUP BY %s", DIMENSION, SPARSE_METRIC,
            DIMENSION), NULL_HANDLING_ENABLED);

    assertStarTreeWasUsed(response);
    ResultTable resultTable = response.getResultTable();
    assertEquals(resultTable.getRows().size(), 3);
    assertEquals(arrayForGroup(resultTable, 1), List.of(10, 30, 40));
    assertEquals(arrayForGroup(resultTable, null), List.of(50));
    assertEquals(arrayForGroup(resultTable, 2), List.of(),
        "A group with no non-null value aggregates to an empty array");
  }

  /// A metric that is null in every row leaves the star-tree with no aggregated value anywhere, so its column holds
  /// nothing but placeholders and has no value to size itself from.
  @Test
  public void anAllNullMetricColumnAggregatesToNull() {
    BrokerResponseNative response =
        getBrokerResponse(String.format("SELECT SUMPRECISION(%s) FROM testTable", PRECISE_METRIC),
            NULL_HANDLING_ENABLED);

    assertStarTreeWasUsed(response);
    assertNull(response.getResultTable().getRows().get(0)[0]);
  }

  /// A query without null handling is served by the regular star-tree in the same segment, which folds a null
  /// dimension into the column's default value and a null metric into `0`: the answers null handling disabled asks
  /// for, and different from the null-aware ones above.
  @Test
  public void aQueryWithoutNullHandlingUsesTheRegularStarTree() {
    BrokerResponseNative response = getBrokerResponse(
        String.format("SELECT %s, SUM(%s) FROM testTable GROUP BY %s", DIMENSION, SPARSE_METRIC, DIMENSION));

    assertStarTreeWasUsed(response);
    ResultTable resultTable = response.getResultTable();
    assertEquals(resultTable.getRows().size(), 3);
    assertEquals(aggregateForGroup(resultTable, 1), (double) SPARSE_SUM_WHERE_D_IS_1 * SEGMENT_COPIES);
    assertEquals(aggregateForGroup(resultTable, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT),
        (double) SPARSE_SUM_WHERE_D_IS_NULL * SEGMENT_COPIES,
        "Without null handling the null rows fold into the dimension's default value");
    assertEquals(aggregateForGroup(resultTable, 2), 0d, "Without null handling a null metric folds into 0");
  }

  /// Guards against the checks above passing because the query silently fell back to a raw scan, which would answer
  /// correctly and prove nothing. A star-tree reads one pre-aggregated document per group rather than every row.
  private static void assertStarTreeWasUsed(BrokerResponseNative response) {
    long numRowsScanned = (long) DIMENSION_VALUES.length * SEGMENT_COPIES;
    assertTrue(response.getNumDocsScanned() < numRowsScanned,
        "Expected the star-tree to be used, but " + response.getNumDocsScanned() + " documents were scanned out of "
            + numRowsScanned);
  }

  private static double aggregateForGroup(ResultTable resultTable, @Nullable Object groupKey) {
    return ((Number) rowForGroup(resultTable, groupKey)[1]).doubleValue();
  }

  private static long countForGroup(ResultTable resultTable, @Nullable Object groupKey) {
    return ((Number) rowForGroup(resultTable, groupKey)[2]).longValue();
  }

  /// Returns the `arrayAgg` cell of a group, sorted, because the aggregate is a set whose order is arbitrary. The
  /// cell is read reflectively since a multi-value result column arrives as a primitive array.
  private static List<Integer> arrayForGroup(ResultTable resultTable, @Nullable Object groupKey) {
    Object cell = rowForGroup(resultTable, groupKey)[1];
    int length = Array.getLength(cell);
    List<Integer> values = new ArrayList<>(length);
    for (int i = 0; i < length; i++) {
      values.add(((Number) Array.get(cell, i)).intValue());
    }
    values.sort(Comparator.naturalOrder());
    return values;
  }

  /// Returns the row whose group key matches, or fails when no such group exists.
  private static Object[] rowForGroup(ResultTable resultTable, @Nullable Object groupKey) {
    for (Object[] row : resultTable.getRows()) {
      if (Objects.equals(groupKey, row[0])) {
        return row;
      }
    }
    throw new AssertionError("No group for dimension value: " + groupKey + " in " + resultTable.getRows().size()
        + " rows");
  }
}
