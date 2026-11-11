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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.startree.v2.builder.MultipleTreesBuilder;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Star-tree usage for queries that enable null handling.
///
/// A star-tree folds a null row into the column's default value and counts it, which is the answer null handling
/// disabled asks for and the wrong answer when it is enabled. So a star-tree may only serve a null handling query
/// when nothing the query touches actually holds a null, and the interesting cases are the ones where that check
/// could be skipped.
public class StarTreeNullHandlingQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "StarTreeNullHandlingQueriesTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";
  /// Holds a null value.
  private static final String NULLABLE_DIMENSION = "d1";
  /// Holds no null value.
  private static final String DIMENSION = "d2";
  private static final String METRIC = "m";

  private static final Map<String, String> QUERY_OPTIONS = Map.of("enableNullHandling", "true");

  /// `_indexSegments` holds two copies and the harness queries two instances, so every aggregate is scaled by this.
  private static final int SEGMENT_COPIES = 4;
  private static final int NUM_ROWS = 4;

  /// A value no row holds, so a `<>` against it is always true over the real values and is dropped from the star-tree
  /// predicate map. It is not true over a null, where it is UNKNOWN and the row is not selected.
  private static final int ABSENT_VALUE = 99999;

  private static final Integer[] NULLABLE_DIMENSION_VALUES = {1, null, 2, 2};
  private static final int[] DIMENSION_VALUES = {5, 5, 6, 6};
  private static final int[] METRIC_VALUES = {10, 20, 30, 40};
  private static final int SUM_OF_ALL_ROWS = 10 + 20 + 30 + 40;
  private static final int NUM_ROWS_WITH_A_NON_NULL_DIMENSION = 3;
  private static final int SUM_OF_ROWS_WITH_A_NON_NULL_DIMENSION = 10 + 30 + 40;

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

    Schema schema = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
        .addSingleValueDimension(NULLABLE_DIMENSION, DataType.INT)
        .addSingleValueDimension(DIMENSION, DataType.INT)
        .addMetric(METRIC, DataType.INT)
        .build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();

    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      GenericRow row = new GenericRow();
      row.putValue(NULLABLE_DIMENSION, NULLABLE_DIMENSION_VALUES[i]);
      row.putValue(DIMENSION, DIMENSION_VALUES[i]);
      row.putValue(METRIC, METRIC_VALUES[i]);
      rows.add(row);
    }

    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
    segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
    segmentGeneratorConfig.setDefaultNullHandlingEnabled(true);
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getPath());
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(rows));
    driver.build();

    File indexDir = new File(INDEX_DIR, SEGMENT_NAME);
    StarTreeIndexConfig starTreeIndexConfig =
        new StarTreeIndexConfig(List.of(NULLABLE_DIMENSION, DIMENSION), null, List.of("SUM__" + METRIC), null, 1);
    try (MultipleTreesBuilder builder = new MultipleTreesBuilder(List.of(starTreeIndexConfig), false, indexDir,
        MultipleTreesBuilder.BuildMode.OFF_HEAP)) {
      builder.build();
    }

    ImmutableSegment segment = ImmutableSegmentLoader.load(indexDir, ReadMode.mmap);
    _indexSegment = segment;
    _indexSegments = List.of(segment, segment);
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    _indexSegment.destroy();
    FileUtils.deleteDirectory(INDEX_DIR);
  }

  /// A predicate that is always true over a column's real values is UNKNOWN over a null, so with null handling it is
  /// only truly always true for a column without nulls. Dropped anyway, the column would escape the null checks and
  /// the star-tree would answer with the null row folded into the column's default value.
  @Test
  public void testAlwaysTruePredicateOnANullableColumnRefusesTheStarTree() {
    String query = String.format("SELECT SUM(%s) FROM testTable WHERE %s <> %d", METRIC, NULLABLE_DIMENSION,
        ABSENT_VALUE);

    BrokerResponseNative brokerResponse = getBrokerResponse(query, QUERY_OPTIONS);

    assertEquals(brokerResponse.getResultTable().getRows().get(0)[0],
        (double) SUM_OF_ROWS_WITH_A_NON_NULL_DIMENSION * SEGMENT_COPIES,
        "A null makes the predicate UNKNOWN, so the row must not be aggregated");
    assertEquals(brokerResponse.getNumDocsScanned(), (long) NUM_ROWS_WITH_A_NON_NULL_DIMENSION * SEGMENT_COPIES,
        "The rows the filter selects should be scanned one by one, because the star-tree cannot answer this query");
  }

  /// A negated predicate that is always false over the real values is the same verdict reached through the NOT
  /// branch, and has to follow the same rule.
  @Test
  public void testNegatedAlwaysFalsePredicateOnANullableColumnRefusesTheStarTree() {
    String query = String.format("SELECT SUM(%s) FROM testTable WHERE NOT (%s = %d)", METRIC, NULLABLE_DIMENSION,
        ABSENT_VALUE);

    BrokerResponseNative brokerResponse = getBrokerResponse(query, QUERY_OPTIONS);

    assertEquals(brokerResponse.getResultTable().getRows().get(0)[0],
        (double) SUM_OF_ROWS_WITH_A_NON_NULL_DIMENSION * SEGMENT_COPIES);
    assertEquals(brokerResponse.getNumDocsScanned(), (long) NUM_ROWS_WITH_A_NON_NULL_DIMENSION * SEGMENT_COPIES);
  }

  /// An OR clause is dropped as a whole when one of its branches is always true, which is the same decision made on
  /// a different path, and has to follow the same rule.
  @Test
  public void testAlwaysTrueOrClauseOnANullableColumnRefusesTheStarTree() {
    String query = String.format("SELECT SUM(%s) FROM testTable WHERE %s = 1 OR %s <> %d", METRIC, NULLABLE_DIMENSION,
        NULLABLE_DIMENSION, ABSENT_VALUE);

    BrokerResponseNative brokerResponse = getBrokerResponse(query, QUERY_OPTIONS);

    assertEquals(brokerResponse.getResultTable().getRows().get(0)[0],
        (double) SUM_OF_ROWS_WITH_A_NON_NULL_DIMENSION * SEGMENT_COPIES);
    assertEquals(brokerResponse.getNumDocsScanned(), (long) NUM_ROWS_WITH_A_NON_NULL_DIMENSION * SEGMENT_COPIES);
  }

  /// The same shape over a column that holds no null keeps the star-tree: nothing the query touches can turn the
  /// predicate UNKNOWN, so folding is not a risk and the optimization stands.
  @Test
  public void testAlwaysTruePredicateOnANonNullableColumnKeepsTheStarTree() {
    String query =
        String.format("SELECT SUM(%s) FROM testTable WHERE %s <> %d", METRIC, DIMENSION, ABSENT_VALUE);

    BrokerResponseNative brokerResponse = getBrokerResponse(query, QUERY_OPTIONS);

    assertEquals(brokerResponse.getResultTable().getRows().get(0)[0],
        (double) SUM_OF_ALL_ROWS * SEGMENT_COPIES);
    assertTrue(brokerResponse.getNumDocsScanned() < (long) NUM_ROWS * SEGMENT_COPIES,
        "Expected the star-tree to answer from pre-aggregated documents, but every row was scanned");
  }
}
