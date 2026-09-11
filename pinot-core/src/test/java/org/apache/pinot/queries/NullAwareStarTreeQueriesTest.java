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
import org.apache.pinot.common.response.broker.ResultTable;
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


/// Queries answered from a null-aware star-tree, where the dimension itself contains nulls.
///
/// `NullAwareStarTreeBuilderTest` only inspects what the builder stored. These go through the query path instead,
/// which is where the reserved null dictionary id has to survive being read back.
public class NullAwareStarTreeQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "NullAwareStarTreeQueriesTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final String DIMENSION = "d";
  private static final String METRIC = "m";

  private static final Map<String, String> QUERY_OPTIONS = Map.of("enableNullHandling", "true");

  /// `_indexSegments` holds two copies and the harness queries two instances, so every aggregate is scaled by this.
  private static final int SEGMENT_COPIES = 4;

  /// One record per leaf, so each distinct dimension value gets its own pre-aggregated document.
  private static final int MAX_LEAF_RECORDS = 1;

  /// `d` is null for two rows, so the null-aware star-tree stores those under the reserved dictionary id. The metric
  /// sums are distinct per group so a group picking up the wrong rows is visible in the answer.
  private static final Integer[] DIMENSION_VALUES = {1, 1, 1, 1, null, null, 2, 2, 2, 2};
  private static final int[] METRIC_VALUES = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  private static final int SUM_WHERE_D_IS_1 = 1 + 2 + 3 + 4;
  private static final int SUM_WHERE_D_IS_NULL = 5 + 6;
  private static final int SUM_WHERE_D_IS_2 = 7 + 8 + 9 + 10;

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
        .addSingleValueDimension(DIMENSION, DataType.INT)
        .addMetric(METRIC, DataType.INT)
        .build();
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setNullHandlingEnabled(true).build();

    List<GenericRow> rows = new ArrayList<>(DIMENSION_VALUES.length);
    for (int i = 0; i < DIMENSION_VALUES.length; i++) {
      GenericRow row = new GenericRow();
      row.putValue(DIMENSION, DIMENSION_VALUES[i]);
      row.putValue(METRIC, METRIC_VALUES[i]);
      rows.add(row);
    }

    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
    segmentGeneratorConfig.setTableName(RAW_TABLE_NAME);
    segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
    segmentGeneratorConfig.setDefaultNullHandlingEnabled(true);
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getPath());
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(rows));
    driver.build();

    File indexDir = new File(INDEX_DIR, SEGMENT_NAME);
    StarTreeIndexConfig starTreeIndexConfig =
        new StarTreeIndexConfig(List.of(DIMENSION), null, List.of("SUM__" + METRIC), null, MAX_LEAF_RECORDS, true);
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

  /// The reserved null dictionary id has no entry in the dictionary the star-tree shares with the segment, so
  /// resolving it reads past the end of the dictionary's value buffer.
  @Test
  public void groupingByANullDimensionReturnsTheNullGroup() {
    BrokerResponseNative response =
        getBrokerResponse("SELECT " + DIMENSION + ", SUM(" + METRIC + ") FROM testTable GROUP BY " + DIMENSION,
            QUERY_OPTIONS);

    assertStarTreeWasUsed(response);
    ResultTable resultTable = response.getResultTable();
    assertEquals(sumForGroup(resultTable, 1), (double) SUM_WHERE_D_IS_1 * SEGMENT_COPIES);
    assertEquals(sumForGroup(resultTable, 2), (double) SUM_WHERE_D_IS_2 * SEGMENT_COPIES);
    assertEquals(sumForGroup(resultTable, null), (double) SUM_WHERE_D_IS_NULL * SEGMENT_COPIES,
        "Rows whose dimension is null must form their own group rather than joining a real value's group");
  }

  /// A predicate that is always true over real values is not always true over nulls: it is UNKNOWN there, so the row
  /// is not selected. Dropping the predicate as always true loses that, and the star-tree then counts the null rows.
  @Test
  public void anAlwaysTruePredicateStillExcludesNullRows() {
    BrokerResponseNative response =
        getBrokerResponse("SELECT SUM(" + METRIC + ") FROM testTable WHERE " + DIMENSION + " <> 99999", QUERY_OPTIONS);

    assertStarTreeWasUsed(response);
    assertEquals(response.getResultTable().getRows().get(0)[0],
        (double) (SUM_WHERE_D_IS_1 + SUM_WHERE_D_IS_2) * SEGMENT_COPIES,
        "A null dimension makes the predicate UNKNOWN, so the row must not be aggregated");
  }

  /// Guards against the checks above passing because the query silently fell back to a raw scan, which would answer
  /// correctly and prove nothing. A star-tree reads one pre-aggregated document per group rather than every row.
  private static void assertStarTreeWasUsed(BrokerResponseNative response) {
    long numRowsScanned = (long) DIMENSION_VALUES.length * SEGMENT_COPIES;
    assertTrue(response.getNumDocsScanned() < numRowsScanned,
        "Expected the star-tree to be used, but " + response.getNumDocsScanned() + " documents were scanned out of "
            + numRowsScanned);
  }

  /// Returns the aggregate of the row whose group key matches, or fails when no such group exists.
  private static double sumForGroup(ResultTable resultTable, Integer groupKey) {
    for (Object[] row : resultTable.getRows()) {
      if (groupKey == null ? row[0] == null : groupKey.equals(row[0])) {
        return ((Number) row[1]).doubleValue();
      }
    }
    throw new AssertionError("No group for dimension value: " + groupKey + " in " + resultTable.getRows().size()
        + " rows");
  }
}
