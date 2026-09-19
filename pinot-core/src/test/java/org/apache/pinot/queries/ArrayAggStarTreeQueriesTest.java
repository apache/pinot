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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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


/// End-to-end SQL tests for distinct `arrayAgg` served from a star-tree, driving the real query engine (unlike
/// [org.apache.pinot.core.startree.v2.ArrayAggStarTreeV2Test], which exercises the ValueAggregator directly). Each
/// query is run twice — once with `useStarTree=true` and once with `useStarTree=false` — and the distinct sets are
/// asserted equal, so both the aggregate() and group-by star-tree BYTES read paths are covered.
public class ArrayAggStarTreeQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "ArrayAggStarTreeQueriesTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final String DIMENSION = "d";
  private static final String LONG_METRIC = "lm";
  private static final String STRING_METRIC = "sm";
  private static final String BYTES_METRIC = "bm";

  private static final Map<String, String> STAR_TREE_ON = Map.of("useStarTree", "true");
  private static final Map<String, String> STAR_TREE_OFF = Map.of("useStarTree", "false");

  private static final int NUM_ROWS = 1000;
  private static final int DIMENSION_CARDINALITY = 10;
  private static final int VALUE_CARDINALITY = 20;

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
        .addSingleValueDimension(LONG_METRIC, DataType.LONG)
        .addSingleValueDimension(STRING_METRIC, DataType.STRING)
        .addSingleValueDimension(BYTES_METRIC, DataType.BYTES)
        .build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();

    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      GenericRow row = new GenericRow();
      int value = i % VALUE_CARDINALITY;
      row.putValue(DIMENSION, i % DIMENSION_CARDINALITY);
      row.putValue(LONG_METRIC, (long) value);
      row.putValue(STRING_METRIC, "v" + value);
      row.putValue(BYTES_METRIC, new byte[]{(byte) value, (byte) (value + 1)});
      rows.add(row);
    }

    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
    segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getPath());
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(rows));
    driver.build();

    File indexDir = new File(INDEX_DIR, SEGMENT_NAME);
    StarTreeIndexConfig starTreeIndexConfig = new StarTreeIndexConfig(List.of(DIMENSION), null, null,
        List.of(
            new org.apache.pinot.spi.config.table.StarTreeAggregationConfig(LONG_METRIC, "arrayAgg", null, null, null,
                null, null, null),
            new org.apache.pinot.spi.config.table.StarTreeAggregationConfig(STRING_METRIC, "arrayAgg", null, null, null,
                null, null, null),
            new org.apache.pinot.spi.config.table.StarTreeAggregationConfig(BYTES_METRIC, "arrayAgg", null, null, null,
                null, null, null)),
        100);
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

  @Test
  public void testDistinctArrayAggNoGroupByLong() {
    assertStarTreeMatchesRawScan(
        String.format("SELECT arrayAgg(%s, 'LONG', true) FROM %s", LONG_METRIC, RAW_TABLE_NAME));
  }

  @Test
  public void testDistinctArrayAggGroupByLong() {
    assertStarTreeMatchesRawScan(
        String.format("SELECT %s, arrayAgg(%s, 'LONG', true) FROM %s GROUP BY %s ORDER BY %s", DIMENSION, LONG_METRIC,
            RAW_TABLE_NAME, DIMENSION, DIMENSION));
  }

  @Test
  public void testDistinctArrayAggGroupByString() {
    assertStarTreeMatchesRawScan(
        String.format("SELECT %s, arrayAgg(%s, 'STRING', true) FROM %s GROUP BY %s ORDER BY %s", DIMENSION,
            STRING_METRIC, RAW_TABLE_NAME, DIMENSION, DIMENSION));
  }

  /// BYTES raw values and star-tree cells are structurally identical (both BYTES-SV blocks); the provenance marker is
  /// what routes each correctly. Parity between the two paths proves raw values are never misread as cells and cells
  /// are never collected as raw values.
  @Test
  public void testDistinctArrayAggNoGroupByBytes() {
    assertStarTreeMatchesRawScan(
        String.format("SELECT arrayAgg(%s, 'BYTES', true) FROM %s", BYTES_METRIC, RAW_TABLE_NAME));
  }

  @Test
  public void testDistinctArrayAggGroupByBytes() {
    assertStarTreeMatchesRawScan(
        String.format("SELECT %s, arrayAgg(%s, 'BYTES', true) FROM %s GROUP BY %s ORDER BY %s", DIMENSION,
            BYTES_METRIC, RAW_TABLE_NAME, DIMENSION, DIMENSION));
  }

  /// The star-tree must actually serve the BYTES distinct query, not silently fall back to the raw scan.
  @Test
  public void testDistinctArrayAggBytesUsesStarTree() {
    String query = String.format("SELECT %s, arrayAgg(%s, 'BYTES', true) FROM %s GROUP BY %s", DIMENSION, BYTES_METRIC,
        RAW_TABLE_NAME, DIMENSION);
    BrokerResponseNative starTree = getBrokerResponse(query, STAR_TREE_ON);
    BrokerResponseNative rawScan = getBrokerResponse(query, STAR_TREE_OFF);
    assertTrue(starTree.getNumDocsScanned() < rawScan.getNumDocsScanned(),
        "Expected the star-tree to serve the distinct BYTES group-by query from pre-aggregated documents");
  }

  /// The star-tree must actually be used for the distinct group-by query (fewer docs scanned than the raw scan),
  /// which guarantees the group-by BYTES read path is exercised rather than silently falling back.
  @Test
  public void testDistinctArrayAggGroupByUsesStarTree() {
    String query = String.format("SELECT %s, arrayAgg(%s, 'LONG', true) FROM %s GROUP BY %s", DIMENSION, LONG_METRIC,
        RAW_TABLE_NAME, DIMENSION);
    BrokerResponseNative starTree = getBrokerResponse(query, STAR_TREE_ON);
    BrokerResponseNative rawScan = getBrokerResponse(query, STAR_TREE_OFF);
    assertTrue(starTree.getNumDocsScanned() < rawScan.getNumDocsScanned(),
        "Expected the star-tree to serve the distinct group-by query from pre-aggregated documents");
  }

  /// Non-distinct arrayAgg is not associative under merge and must never be served from the star-tree; it should scan
  /// every row instead.
  @Test
  public void testNonDistinctArrayAggDoesNotUseStarTree() {
    String query = String.format("SELECT %s, arrayAgg(%s, 'LONG') FROM %s GROUP BY %s", DIMENSION, LONG_METRIC,
        RAW_TABLE_NAME, DIMENSION);
    BrokerResponseNative starTree = getBrokerResponse(query, STAR_TREE_ON);
    BrokerResponseNative rawScan = getBrokerResponse(query, STAR_TREE_OFF);
    assertEquals(starTree.getNumDocsScanned(), rawScan.getNumDocsScanned(),
        "Non-distinct arrayAgg must not use the star-tree, so it scans the same docs with the option on or off");
  }

  /// Runs the query with the star-tree enabled and disabled and asserts each arrayAgg cell (an unordered MV of
  /// distinct values) is set-equal between the two, per group.
  private void assertStarTreeMatchesRawScan(String query) {
    ResultTable starTree = getBrokerResponse(query, STAR_TREE_ON).getResultTable();
    ResultTable rawScan = getBrokerResponse(query, STAR_TREE_OFF).getResultTable();
    List<Object[]> starTreeRows = starTree.getRows();
    List<Object[]> rawScanRows = rawScan.getRows();
    assertEquals(starTreeRows.size(), rawScanRows.size());
    for (int i = 0; i < starTreeRows.size(); i++) {
      Object[] starTreeRow = starTreeRows.get(i);
      Object[] rawScanRow = rawScanRows.get(i);
      assertEquals(starTreeRow.length, rawScanRow.length);
      for (int c = 0; c < starTreeRow.length; c++) {
        Object starTreeValue = starTreeRow[c];
        Object rawScanValue = rawScanRow[c];
        if (starTreeValue != null && starTreeValue.getClass().isArray()) {
          // arrayAgg result column (MV, possibly a primitive array): order-independent set comparison.
          assertEquals(asSet(starTreeValue), asSet(rawScanValue), "Mismatch at row " + i + " col " + c);
        } else {
          assertEquals(starTreeValue, rawScanValue, "Mismatch at row " + i + " col " + c);
        }
      }
    }
  }

  /// Normalizes an MV result cell (either an Object[] or a primitive array such as long[]) into a Set for
  /// order-independent comparison. byte[] elements are wrapped in [org.apache.pinot.spi.utils.ByteArray] so they
  /// compare by content.
  private static Set<Object> asSet(Object array) {
    Set<Object> set = new HashSet<>();
    int length = java.lang.reflect.Array.getLength(array);
    for (int i = 0; i < length; i++) {
      Object element = java.lang.reflect.Array.get(array, i);
      set.add(element instanceof byte[] ? new org.apache.pinot.spi.utils.ByteArray((byte[]) element) : element);
    }
    return set;
  }
}
