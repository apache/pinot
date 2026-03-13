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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.blocks.results.DistinctResultsBlock;
import org.apache.pinot.core.query.distinct.table.DistinctTable;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/**
 * Tests for the inverted-index-based distinct cost heuristic and the query option override.
 *
 * <p>Setup: 10,000 records with 100 unique INT values (each value appears 100 times).
 * Data is interleaved (0,1,2,...,99,0,1,...) to prevent the column from being auto-sorted.
 * Dictionary cardinality = 100.
 *
 * <p>The heuristic: use inverted index when {@code dictCardinality * costRatio <= filteredDocCount}.
 *
 * <p>Test strategy: enable via {@code useInvertedIndexDistinct=true}, use
 * {@code invertedIndexDistinctCostRatio} to control the heuristic, then verify which path
 * was taken via {@code toExplainString()}.
 */
public class InvertedIndexDistinctCostHeuristicTest extends BaseQueriesTest {
  private static final File INDEX_DIR =
      new File(FileUtils.getTempDirectory(), "InvertedIndexDistinctCostHeuristicTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final String INT_COLUMN = "intColumn";

  private static final int NUM_UNIQUE_VALUES = 100;
  private static final int RECORDS_PER_VALUE = 100;
  private static final int NUM_RECORDS = NUM_UNIQUE_VALUES * RECORDS_PER_VALUE; // 10,000

  private static final Schema SCHEMA = new Schema.SchemaBuilder()
      .addSingleValueDimension(INT_COLUMN, DataType.INT)
      .build();

  // Table with inverted index on INT_COLUMN
  private static final TableConfig TABLE_CONFIG = new TableConfigBuilder(TableType.OFFLINE)
      .setTableName(RAW_TABLE_NAME)
      .setInvertedIndexColumns(List.of(INT_COLUMN))
      .build();

  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegments;

  @Override
  protected String getFilter() {
    throw new UnsupportedOperationException();
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
    FileUtils.deleteQuietly(INDEX_DIR);

    // Interleave values to prevent the column from being auto-sorted
    // (sorted columns use SortedIndexBasedInvertedIndex which returns IntPair, not RoaringBitmap)
    List<GenericRow> records = new ArrayList<>(NUM_RECORDS);
    for (int j = 0; j < RECORDS_PER_VALUE; j++) {
      for (int i = 0; i < NUM_UNIQUE_VALUES; i++) {
        GenericRow record = new GenericRow();
        record.putValue(INT_COLUMN, i);
        records.add(record);
      }
    }

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    config.setTableName(RAW_TABLE_NAME);
    config.setSegmentName(SEGMENT_NAME);
    config.setOutDir(INDEX_DIR.getAbsolutePath());

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(records));
    driver.build();

    ImmutableSegment segment = ImmutableSegmentLoader.load(
        new File(INDEX_DIR, SEGMENT_NAME), new IndexLoadingConfig(TABLE_CONFIG, SCHEMA));
    _indexSegment = segment;
    _indexSegments = List.of(segment);
  }

  @AfterClass
  public void tearDown() {
    for (IndexSegment segment : _indexSegments) {
      segment.destroy();
    }
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  // Helper to run a DISTINCT query and return the operator (after consuming nextBlock)
  private BaseOperator<DistinctResultsBlock> runDistinct(String query) {
    BaseOperator<DistinctResultsBlock> op = getOperator(query);
    op.nextBlock();
    return op;
  }

  private boolean usedInvertedIndex(BaseOperator<DistinctResultsBlock> op) {
    return op.toExplainString().contains("DISTINCT_INVERTED_INDEX");
  }

  /**
   * Without the query option, the old DistinctOperator is used (no inverted index path).
   */
  @Test
  public void testDisabledByDefaultWithoutQueryOption() {
    String query = "SELECT DISTINCT intColumn FROM testTable WHERE intColumn >= 0";
    BaseOperator<DistinctResultsBlock> op = runDistinct(query);
    assertFalse(usedInvertedIndex(op), "Without useInvertedIndexDistinct=true, should use scan");
  }

  /**
   * costRatio=1 with wide filter (10K docs): dictCard(100)*1=100 <= 10000 → inverted index.
   */
  @Test
  public void testLowCostRatioForcesInvertedIndex() {
    String query = "SELECT DISTINCT intColumn FROM testTable WHERE intColumn >= 0 "
        + "OPTION(useInvertedIndexDistinct=true, invertedIndexDistinctCostRatio=1)";
    BaseOperator<DistinctResultsBlock> op = runDistinct(query);
    assertTrue(usedInvertedIndex(op), "costRatio=1, 10K docs → should use inverted index");
  }

  /**
   * costRatio=200 with wide filter (10K docs): dictCard(100)*200=20000 > 10000 → scan.
   */
  @Test
  public void testHighCostRatioForcesScan() {
    String query = "SELECT DISTINCT intColumn FROM testTable WHERE intColumn >= 0 "
        + "OPTION(useInvertedIndexDistinct=true, invertedIndexDistinctCostRatio=200)";
    BaseOperator<DistinctResultsBlock> op = runDistinct(query);
    assertFalse(usedInvertedIndex(op), "costRatio=200, 10K docs → should use scan");
  }

  /**
   * costRatio=1 with selective filter (intColumn=0, 100 docs): dictCard(100)*1=100 <= 100 → inverted index.
   */
  @Test
  public void testCostRatio1SelectiveFilterUsesInvertedIndex() {
    String query = "SELECT DISTINCT intColumn FROM testTable WHERE intColumn = 0 "
        + "OPTION(useInvertedIndexDistinct=true, invertedIndexDistinctCostRatio=1)";
    BaseOperator<DistinctResultsBlock> op = runDistinct(query);
    assertTrue(usedInvertedIndex(op), "costRatio=1, 100 docs, dictCard=100 → inverted index");
  }

  /**
   * costRatio=2 with selective filter (intColumn=0, 100 docs): dictCard(100)*2=200 > 100 → scan.
   */
  @Test
  public void testCostRatio2SelectiveFilterUsesScan() {
    String query = "SELECT DISTINCT intColumn FROM testTable WHERE intColumn = 0 "
        + "OPTION(useInvertedIndexDistinct=true, invertedIndexDistinctCostRatio=2)";
    BaseOperator<DistinctResultsBlock> op = runDistinct(query);
    assertFalse(usedInvertedIndex(op), "costRatio=2, 100 docs, dictCard=100 → scan");
  }

  /**
   * Verify correctness: inverted index path and scan path produce the same results with ORDER BY.
   */
  @Test
  public void testInvertedIndexAndScanProduceSameResults() {
    // Force inverted index path with costRatio=1
    String invertedQuery = "SELECT DISTINCT intColumn FROM testTable WHERE intColumn < 10 "
        + "ORDER BY intColumn LIMIT 100 "
        + "OPTION(useInvertedIndexDistinct=true, invertedIndexDistinctCostRatio=1)";
    BaseOperator<DistinctResultsBlock> invertedOp = getOperator(invertedQuery);
    DistinctTable invertedTable = invertedOp.nextBlock().getDistinctTable();
    assertTrue(usedInvertedIndex(invertedOp), "Should use inverted index with costRatio=1");

    // Force scan path with costRatio=10000
    String scanQuery = "SELECT DISTINCT intColumn FROM testTable WHERE intColumn < 10 "
        + "ORDER BY intColumn LIMIT 100 "
        + "OPTION(useInvertedIndexDistinct=true, invertedIndexDistinctCostRatio=10000)";
    BaseOperator<DistinctResultsBlock> scanOp = getOperator(scanQuery);
    DistinctTable scanTable = scanOp.nextBlock().getDistinctTable();
    assertFalse(usedInvertedIndex(scanOp), "Should use scan with costRatio=10000");

    // Both should return the same values: 0..9
    assertEquals(invertedTable.size(), scanTable.size());
    Set<Integer> invertedValues = new HashSet<>();
    for (Object[] row : invertedTable.getRows()) {
      invertedValues.add(((Number) row[0]).intValue());
    }
    Set<Integer> scanValues = new HashSet<>();
    for (Object[] row : scanTable.getRows()) {
      scanValues.add(((Number) row[0]).intValue());
    }
    assertEquals(invertedValues, scanValues);

    Set<Integer> expected = new HashSet<>();
    for (int i = 0; i < 10; i++) {
      expected.add(i);
    }
    assertEquals(invertedValues, expected);
  }

  /**
   * Verify correctness without ORDER BY — both paths produce the same distinct count.
   */
  @Test
  public void testInvertedIndexAndScanSameCountNoOrderBy() {
    // Force inverted index
    String invertedQuery = "SELECT DISTINCT intColumn FROM testTable WHERE intColumn >= 0 LIMIT 200 "
        + "OPTION(useInvertedIndexDistinct=true, invertedIndexDistinctCostRatio=1)";
    BaseOperator<DistinctResultsBlock> invertedOp = getOperator(invertedQuery);
    DistinctTable invertedTable = invertedOp.nextBlock().getDistinctTable();
    assertTrue(usedInvertedIndex(invertedOp));

    // Force scan
    String scanQuery = "SELECT DISTINCT intColumn FROM testTable WHERE intColumn >= 0 LIMIT 200 "
        + "OPTION(useInvertedIndexDistinct=true, invertedIndexDistinctCostRatio=10000)";
    BaseOperator<DistinctResultsBlock> scanOp = getOperator(scanQuery);
    DistinctTable scanTable = scanOp.nextBlock().getDistinctTable();
    assertFalse(usedInvertedIndex(scanOp));

    assertEquals(invertedTable.size(), NUM_UNIQUE_VALUES);
    assertEquals(scanTable.size(), NUM_UNIQUE_VALUES);
  }

  /**
   * With default costRatio=5 and 10K docs, inverted index should be chosen when opted in.
   */
  @Test
  public void testDefaultCostRatioUsesInvertedIndex() {
    String query = "SELECT DISTINCT intColumn FROM testTable WHERE intColumn >= 0 "
        + "OPTION(useInvertedIndexDistinct=true)";
    BaseOperator<DistinctResultsBlock> op = runDistinct(query);
    assertTrue(usedInvertedIndex(op), "Default costRatio=5: 100*5=500 <= 10K → inverted index");
  }

  /**
   * Verify the default cost ratio boundary behavior:
   * dictCard=100, default costRatio=5 → threshold is 500 docs.
   * With a wide filter (10K docs), inverted index should be chosen.
   * With costRatio=100 (threshold=10K), matching all 10K docs is at the boundary → inverted index.
   * With costRatio=101 (threshold=10100), 10K docs < threshold → scan.
   */
  @Test
  public void testDefaultRatioBoundary() {
    // Default costRatio=5: dictCard(100)*5=500 <= 10K docs → inverted index
    String defaultQuery = "SELECT DISTINCT intColumn FROM testTable WHERE intColumn >= 0 "
        + "OPTION(useInvertedIndexDistinct=true)";
    assertTrue(usedInvertedIndex(runDistinct(defaultQuery)),
        "Default costRatio=5: 100*5=500 <= 10K → inverted index");

    // costRatio=100: dictCard(100)*100=10000 <= 10K docs → inverted index (boundary)
    String boundaryQuery = "SELECT DISTINCT intColumn FROM testTable WHERE intColumn >= 0 "
        + "OPTION(useInvertedIndexDistinct=true, invertedIndexDistinctCostRatio=100)";
    assertTrue(usedInvertedIndex(runDistinct(boundaryQuery)),
        "costRatio=100: 100*100=10000 <= 10000 → inverted index at boundary");

    // costRatio=101: dictCard(100)*101=10100 > 10K docs → scan
    String aboveBoundaryQuery = "SELECT DISTINCT intColumn FROM testTable WHERE intColumn >= 0 "
        + "OPTION(useInvertedIndexDistinct=true, invertedIndexDistinctCostRatio=101)";
    assertFalse(usedInvertedIndex(runDistinct(aboveBoundaryQuery)),
        "costRatio=101: 100*101=10100 > 10000 → scan");
  }
}
