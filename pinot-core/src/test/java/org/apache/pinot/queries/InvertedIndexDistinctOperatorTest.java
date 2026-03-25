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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.blocks.results.DistinctResultsBlock;
import org.apache.pinot.core.query.distinct.table.DistinctTable;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.indexsegment.mutable.MutableSegmentImplTestUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.MutableSegment;
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
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/**
 * Tests for {@link org.apache.pinot.core.operator.query.InvertedIndexDistinctOperator}.
 *
 * <p>Six segments exercise distinct operator features:
 * <ul>
 *   <li><b>INT segment</b>: 10K records, 100 unique INT values (interleaved), inverted index.
 *       Tests cost heuristic path selection and inverted-vs-scan correctness.</li>
 *   <li><b>MV segment</b>: 1K records, MV INT column (50 unique values), inverted index.
 *       Tests multi-value column support.</li>
 *   <li><b>Sorted segment</b>: 10K records, sorted INT column (100 unique), sorted forward index.
 *       Tests sorted index path.</li>
 *   <li><b>Mutable segment</b>: consuming segment with unsorted dictionary + inverted index.
 *       Tests ORDER BY correctness without relying on sorted dictIds.</li>
 *   <li><b>STRING segment</b>: 5K records, STRING column (50 unique), inverted index.
 *       Tests STRING data type handling.</li>
 *   <li><b>Null segment</b>: 1K records, INT column with nulls, inverted index.
 *       Tests null handling.</li>
 * </ul>
 */
public class InvertedIndexDistinctOperatorTest extends BaseQueriesTest {
  private static final File INDEX_DIR =
      new File(FileUtils.getTempDirectory(), "InvertedIndexDistinctOperatorTest");
  private static final String RAW_TABLE_NAME = "testTable";

  // Active segment — swapped per test group
  private IndexSegment _activeSegment;
  private final List<IndexSegment> _allSegments = new ArrayList<>();

  // --- INT segment (cost heuristic tests) ---
  private static final String INT_COLUMN = "intColumn";
  private static final int INT_NUM_UNIQUE = 100;
  private static final int INT_RECORDS_PER_VALUE = 100;
  private static final int INT_NUM_RECORDS = INT_NUM_UNIQUE * INT_RECORDS_PER_VALUE;
  private IndexSegment _intSegment;

  // --- MV segment ---
  private static final String MV_INT_COLUMN = "mvIntColumn";
  private static final String SV_FILTER_COLUMN = "svFilterColumn";
  private static final int MV_NUM_RECORDS = 1000;
  private static final int MV_CARDINALITY = 50;
  private IndexSegment _mvSegment;
  private Set<Integer> _allMvValues;
  private Set<Integer> _filteredMvValues;

  // --- Sorted segment ---
  private static final String SORTED_COLUMN = "sortedColumn";
  private static final String FILTER_COLUMN = "filterColumn";
  private static final int SORTED_NUM_UNIQUE = 100;
  private static final int SORTED_RECORDS_PER_VALUE = 100;
  private IndexSegment _sortedSegment;

  // --- Mutable segment with unsorted dictionary ---
  private static final int MUTABLE_NUM_UNIQUE = 10;
  private static final int MUTABLE_RECORDS_PER_VALUE = 100;
  private IndexSegment _mutableSegment;

  // --- STRING segment ---
  private static final String STRING_COLUMN = "stringColumn";
  private static final int STRING_NUM_UNIQUE = 50;
  private static final int STRING_RECORDS_PER_VALUE = 100;
  private IndexSegment _stringSegment;

  // --- Null segment ---
  private static final int NULL_NUM_RECORDS = 1000;
  private static final int NULL_NUM_NON_NULL = 950;
  private static final int NULL_NUM_UNIQUE = 50;
  private IndexSegment _nullSegment;

  @Override
  protected String getFilter() {
    throw new UnsupportedOperationException();
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _activeSegment;
  }

  @Override
  protected List<IndexSegment> getIndexSegments() {
    return List.of(_activeSegment);
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);
    _intSegment = buildIntSegment();
    _mvSegment = buildMvSegment();
    _sortedSegment = buildSortedSegment();
    _mutableSegment = buildMutableSegment();
    _stringSegment = buildStringSegment();
    _nullSegment = buildNullSegment();
  }

  @AfterClass
  public void tearDown() {
    for (IndexSegment segment : _allSegments) {
      segment.destroy();
    }
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  // ==================== Segment Builders ====================

  private IndexSegment buildIntSegment()
      throws Exception {
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension(INT_COLUMN, DataType.INT).build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(RAW_TABLE_NAME).setInvertedIndexColumns(List.of(INT_COLUMN)).build();

    List<GenericRow> records = new ArrayList<>(INT_NUM_RECORDS);
    for (int j = 0; j < INT_RECORDS_PER_VALUE; j++) {
      for (int i = 0; i < INT_NUM_UNIQUE; i++) {
        GenericRow record = new GenericRow();
        record.putValue(INT_COLUMN, i);
        records.add(record);
      }
    }
    return buildSegment("intSegment", schema, tableConfig, records);
  }

  private IndexSegment buildMvSegment()
      throws Exception {
    Schema schema = new Schema.SchemaBuilder()
        .addMultiValueDimension(MV_INT_COLUMN, DataType.INT)
        .addSingleValueDimension(SV_FILTER_COLUMN, DataType.INT).build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(RAW_TABLE_NAME)
        .setInvertedIndexColumns(List.of(MV_INT_COLUMN, SV_FILTER_COLUMN)).build();

    _allMvValues = new HashSet<>();
    _filteredMvValues = new HashSet<>();
    List<GenericRow> records = new ArrayList<>(MV_NUM_RECORDS);
    for (int i = 0; i < MV_NUM_RECORDS; i++) {
      GenericRow record = new GenericRow();
      List<Integer> mvValues = new ArrayList<>();
      mvValues.add(i % MV_CARDINALITY);
      mvValues.add((i + 1) % MV_CARDINALITY);
      if (i % 3 == 0) {
        mvValues.add((i + 2) % MV_CARDINALITY);
      }
      record.putValue(MV_INT_COLUMN, mvValues.toArray(new Integer[0]));
      record.putValue(SV_FILTER_COLUMN, i);
      records.add(record);
      for (int v : mvValues) {
        _allMvValues.add(v);
        if (i < 500) {
          _filteredMvValues.add(v);
        }
      }
    }
    return buildSegment("mvSegment", schema, tableConfig, records);
  }

  private IndexSegment buildSortedSegment()
      throws Exception {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(SORTED_COLUMN, DataType.INT)
        .addSingleValueDimension(FILTER_COLUMN, DataType.INT).build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(RAW_TABLE_NAME).setSortedColumn(SORTED_COLUMN).build();

    List<GenericRow> records = new ArrayList<>();
    for (int i = 0; i < SORTED_NUM_UNIQUE; i++) {
      for (int j = 0; j < SORTED_RECORDS_PER_VALUE; j++) {
        GenericRow record = new GenericRow();
        record.putValue(SORTED_COLUMN, i);
        record.putValue(FILTER_COLUMN, i * SORTED_RECORDS_PER_VALUE + j);
        records.add(record);
      }
    }
    return buildSegment("sortedSegment", schema, tableConfig, records);
  }

  private IndexSegment buildMutableSegment()
      throws Exception {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(INT_COLUMN, DataType.INT)
        .addSingleValueDimension(FILTER_COLUMN, DataType.INT).build();
    MutableSegment mutableSegment = MutableSegmentImplTestUtils.createMutableSegmentImpl(schema, Set.of(), Set.of(),
        Set.of(INT_COLUMN, FILTER_COLUMN), false);
    int[] insertionOrder = new int[]{5, 1, 9, 0, 8, 2, 7, 3, 6, 4};
    int docId = 0;
    for (int i = 0; i < MUTABLE_RECORDS_PER_VALUE; i++) {
      for (int value : insertionOrder) {
        GenericRow record = new GenericRow();
        record.putValue(INT_COLUMN, value);
        record.putValue(FILTER_COLUMN, docId++);
        mutableSegment.index(record, null);
      }
    }
    assertFalse(mutableSegment.getDataSource(INT_COLUMN).getDictionary().isSorted());
    _allSegments.add(mutableSegment);
    return mutableSegment;
  }

  private IndexSegment buildStringSegment()
      throws Exception {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(STRING_COLUMN, DataType.STRING)
        .addSingleValueDimension(FILTER_COLUMN, DataType.INT).build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(RAW_TABLE_NAME)
        .setInvertedIndexColumns(List.of(STRING_COLUMN, FILTER_COLUMN)).build();

    List<GenericRow> records = new ArrayList<>();
    for (int j = 0; j < STRING_RECORDS_PER_VALUE; j++) {
      for (int i = 0; i < STRING_NUM_UNIQUE; i++) {
        GenericRow record = new GenericRow();
        record.putValue(STRING_COLUMN, String.format("val_%02d", i));
        record.putValue(FILTER_COLUMN, j * STRING_NUM_UNIQUE + i);
        records.add(record);
      }
    }
    return buildSegment("stringSegment", schema, tableConfig, records);
  }

  private IndexSegment buildNullSegment()
      throws Exception {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(INT_COLUMN, DataType.INT)
        .addSingleValueDimension(FILTER_COLUMN, DataType.INT).build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(RAW_TABLE_NAME)
        .setInvertedIndexColumns(List.of(INT_COLUMN, FILTER_COLUMN))
        .setNullHandlingEnabled(true).build();

    List<GenericRow> records = new ArrayList<>(NULL_NUM_RECORDS);
    for (int i = 0; i < NULL_NUM_RECORDS; i++) {
      GenericRow record = new GenericRow();
      record.putValue(INT_COLUMN, i >= NULL_NUM_NON_NULL ? null : i % NULL_NUM_UNIQUE);
      record.putValue(FILTER_COLUMN, i);
      records.add(record);
    }

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setTableName(RAW_TABLE_NAME);
    config.setSegmentName("nullSegment");
    config.setOutDir(new File(INDEX_DIR, "nullSegment_dir").getAbsolutePath());
    config.setDefaultNullHandlingEnabled(true);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(records));
    driver.build();

    ImmutableSegment segment = ImmutableSegmentLoader.load(
        new File(new File(INDEX_DIR, "nullSegment_dir"), "nullSegment"),
        new IndexLoadingConfig(tableConfig, schema));
    _allSegments.add(segment);
    return segment;
  }

  private IndexSegment buildSegment(String segmentName, Schema schema, TableConfig tableConfig,
      List<GenericRow> records)
      throws Exception {
    File segmentDir = new File(INDEX_DIR, segmentName + "_dir");
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setTableName(RAW_TABLE_NAME);
    config.setSegmentName(segmentName);
    config.setOutDir(segmentDir.getAbsolutePath());

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(records));
    driver.build();

    ImmutableSegment segment = ImmutableSegmentLoader.load(
        new File(segmentDir, segmentName), new IndexLoadingConfig(tableConfig, schema));
    _allSegments.add(segment);
    return segment;
  }

  // ==================== Helpers ====================

  private BaseOperator<DistinctResultsBlock> runDistinct(String query) {
    BaseOperator<DistinctResultsBlock> op = getOperator(query);
    op.nextBlock();
    return op;
  }

  private boolean usedInvertedIndex(BaseOperator<DistinctResultsBlock> op) {
    return op.toExplainString().contains("DISTINCT_INVERTED_INDEX");
  }

  private boolean usedSortedIndex(BaseOperator<DistinctResultsBlock> op) {
    return op.toExplainString().contains("DISTINCT_SORTED_INDEX");
  }

  private Set<Integer> extractIntValues(DistinctTable table) {
    Set<Integer> values = new HashSet<>();
    for (Object[] row : table.getRows()) {
      if (row[0] != null) {
        values.add(((Number) row[0]).intValue());
      }
    }
    return values;
  }

  private List<Integer> extractOrderedIntValues(ResultTable resultTable) {
    List<Integer> values = new ArrayList<>();
    for (Object[] row : resultTable.getRows()) {
      values.add((Integer) row[0]);
    }
    return values;
  }

  private List<String> extractOrderedStringValues(ResultTable resultTable) {
    List<String> values = new ArrayList<>();
    for (Object[] row : resultTable.getRows()) {
      values.add((String) row[0]);
    }
    return values;
  }

  private boolean containsNull(DistinctTable table) {
    for (Object[] row : table.getRows()) {
      if (row[0] == null) {
        return true;
      }
    }
    return false;
  }

  private boolean containsNull(ResultTable resultTable) {
    for (Object[] row : resultTable.getRows()) {
      if (row[0] == null) {
        return true;
      }
    }
    return false;
  }

  private static final String OPT = "OPTION(useIndexBasedDistinctOperator=true";
  private static final String OPT_INV = OPT + ", invertedIndexDistinctCostRatio=1)";
  private static final String OPT_SCAN = OPT + ", invertedIndexDistinctCostRatio=100000)";

  // ==================== Cost Heuristic Tests ====================

  @Test
  public void testCostRatioPathSelection() {
    _activeSegment = _intSegment;

    // Without the query option → old DistinctOperator
    assertFalse(usedInvertedIndex(runDistinct(
        "SELECT DISTINCT intColumn FROM testTable WHERE intColumn >= 0")));

    // costRatio=1, wide filter (10K docs): 100*1 <= 10000 → inverted
    assertTrue(usedInvertedIndex(runDistinct(
        "SELECT DISTINCT intColumn FROM testTable WHERE intColumn >= 0 "
            + OPT + ", invertedIndexDistinctCostRatio=1)")));

    // costRatio=200, wide filter: 100*200=20000 > 10000 → scan
    assertFalse(usedInvertedIndex(runDistinct(
        "SELECT DISTINCT intColumn FROM testTable WHERE intColumn >= 0 "
            + OPT + ", invertedIndexDistinctCostRatio=200)")));

    // costRatio=1, selective filter (100 docs): 100*1 <= 100 → inverted
    assertTrue(usedInvertedIndex(runDistinct(
        "SELECT DISTINCT intColumn FROM testTable WHERE intColumn = 0 "
            + OPT + ", invertedIndexDistinctCostRatio=1)")));

    // costRatio=2, selective filter: 100*2=200 > 100 → scan
    assertFalse(usedInvertedIndex(runDistinct(
        "SELECT DISTINCT intColumn FROM testTable WHERE intColumn = 0 "
            + OPT + ", invertedIndexDistinctCostRatio=2)")));

    // Default costRatio=30: 100*30=3000 <= 10K → inverted
    assertTrue(usedInvertedIndex(runDistinct(
        "SELECT DISTINCT intColumn FROM testTable WHERE intColumn >= 0 " + OPT + ")")));

    // Boundary: costRatio=100: 100*100=10000 <= 10000 → inverted
    assertTrue(usedInvertedIndex(runDistinct(
        "SELECT DISTINCT intColumn FROM testTable WHERE intColumn >= 0 "
            + OPT + ", invertedIndexDistinctCostRatio=100)")));

    // Above boundary: costRatio=101: 100*101=10100 > 10000 → scan
    assertFalse(usedInvertedIndex(runDistinct(
        "SELECT DISTINCT intColumn FROM testTable WHERE intColumn >= 0 "
            + OPT + ", invertedIndexDistinctCostRatio=101)")));
  }

  @Test
  public void testInvertedIndexVsScanCorrectness() {
    _activeSegment = _intSegment;

    // With ORDER BY
    BaseOperator<DistinctResultsBlock> invertedOp = getOperator(
        "SELECT DISTINCT intColumn FROM testTable WHERE intColumn IN "
            + "(0,1,2,3,4,5,6,7,8,9) ORDER BY intColumn LIMIT 100 " + OPT_INV);
    DistinctTable invertedTable = invertedOp.nextBlock().getDistinctTable();
    assertTrue(usedInvertedIndex(invertedOp));

    BaseOperator<DistinctResultsBlock> scanOp = getOperator(
        "SELECT DISTINCT intColumn FROM testTable WHERE intColumn IN "
            + "(0,1,2,3,4,5,6,7,8,9) ORDER BY intColumn LIMIT 100 " + OPT_SCAN);
    DistinctTable scanTable = scanOp.nextBlock().getDistinctTable();
    assertFalse(usedInvertedIndex(scanOp));

    Set<Integer> expected = new HashSet<>();
    for (int i = 0; i < 10; i++) {
      expected.add(i);
    }
    assertEquals(extractIntValues(invertedTable), expected);
    assertEquals(extractIntValues(scanTable), expected);

    // Without ORDER BY — same count
    BaseOperator<DistinctResultsBlock> inv2 = getOperator(
        "SELECT DISTINCT intColumn FROM testTable WHERE intColumn >= 0 LIMIT 200 " + OPT_INV);
    assertEquals(inv2.nextBlock().getDistinctTable().size(), INT_NUM_UNIQUE);

    BaseOperator<DistinctResultsBlock> scan2 = getOperator(
        "SELECT DISTINCT intColumn FROM testTable WHERE intColumn >= 0 LIMIT 200 " + OPT_SCAN);
    assertEquals(scan2.nextBlock().getDistinctTable().size(), INT_NUM_UNIQUE);
  }

  // ==================== Multi-Value Tests ====================

  @Test
  public void testMvColumnWithFilter() {
    _activeSegment = _mvSegment;

    BaseOperator<DistinctResultsBlock> op = getOperator(
        "SELECT DISTINCT mvIntColumn FROM testTable WHERE svFilterColumn < 500 LIMIT 1000 " + OPT_INV);
    DistinctTable table = op.nextBlock().getDistinctTable();
    assertTrue(usedInvertedIndex(op));
    assertEquals(extractIntValues(table), _filteredMvValues);
  }

  @Test
  public void testMvColumnInvertedVsScan() {
    _activeSegment = _mvSegment;

    BaseOperator<DistinctResultsBlock> invertedOp = getOperator(
        "SELECT DISTINCT mvIntColumn FROM testTable WHERE svFilterColumn < 500 "
            + "ORDER BY mvIntColumn LIMIT 1000 " + OPT_INV);
    DistinctTable invertedTable = invertedOp.nextBlock().getDistinctTable();
    assertTrue(usedInvertedIndex(invertedOp));

    BaseOperator<DistinctResultsBlock> scanOp = getOperator(
        "SELECT DISTINCT mvIntColumn FROM testTable WHERE svFilterColumn < 500 "
            + "ORDER BY mvIntColumn LIMIT 1000 " + OPT_SCAN);
    DistinctTable scanTable = scanOp.nextBlock().getDistinctTable();
    assertFalse(usedInvertedIndex(scanOp));

    assertEquals(extractIntValues(invertedTable), extractIntValues(scanTable));
  }

  @Test
  public void testMvColumnMatchAll() {
    _activeSegment = _mvSegment;

    BaseOperator<DistinctResultsBlock> matchAllOp = getOperator(
        "SELECT DISTINCT mvIntColumn FROM testTable WHERE svFilterColumn >= 0 LIMIT 1000 " + OPT_INV);
    DistinctTable matchAllTable = matchAllOp.nextBlock().getDistinctTable();
    assertEquals(extractIntValues(matchAllTable), _allMvValues);
  }

  @Test
  public void testMvColumnLimit() {
    _activeSegment = _mvSegment;

    BaseOperator<DistinctResultsBlock> limitOp = getOperator(
        "SELECT DISTINCT mvIntColumn FROM testTable WHERE svFilterColumn >= 0 LIMIT 10 " + OPT_INV);
    assertEquals(limitOp.nextBlock().getDistinctTable().size(), 10);
  }

  @Test
  public void testMvColumnOrderByDesc() {
    _activeSegment = _mvSegment;

    BaseOperator<DistinctResultsBlock> descOp = getOperator(
        "SELECT DISTINCT mvIntColumn FROM testTable WHERE svFilterColumn < 500 "
            + "ORDER BY mvIntColumn DESC LIMIT 1000 " + OPT_INV);
    BaseOperator<DistinctResultsBlock> descScanOp = getOperator(
        "SELECT DISTINCT mvIntColumn FROM testTable WHERE svFilterColumn < 500 "
            + "ORDER BY mvIntColumn DESC LIMIT 1000 " + OPT_SCAN);
    assertEquals(extractOrderedIntValues(descOp.nextBlock().getDistinctTable().toResultTable()),
        extractOrderedIntValues(descScanOp.nextBlock().getDistinctTable().toResultTable()));
  }

  @Test
  public void testMvColumnSelectiveFilter() {
    _activeSegment = _mvSegment;

    BaseOperator<DistinctResultsBlock> selectiveOp = getOperator(
        "SELECT DISTINCT mvIntColumn FROM testTable WHERE svFilterColumn < 3 "
            + "ORDER BY mvIntColumn LIMIT 100 " + OPT_INV);
    assertEquals(extractIntValues(selectiveOp.nextBlock().getDistinctTable()),
        new HashSet<>(Arrays.asList(0, 1, 2, 3)));
  }

  @Test
  public void testMvColumnEmptyFilter() {
    _activeSegment = _mvSegment;

    BaseOperator<DistinctResultsBlock> emptyOp = getOperator(
        "SELECT DISTINCT mvIntColumn FROM testTable WHERE svFilterColumn > 99999 LIMIT 1000 " + OPT_INV);
    assertEquals(emptyOp.nextBlock().getDistinctTable().size(), 0);
  }

  // ==================== Sorted Index Tests ====================

  @Test
  public void testSortedColumnPath() {
    _activeSegment = _sortedSegment;

    // Should use sorted index path
    BaseOperator<DistinctResultsBlock> op = getOperator(
        "SELECT DISTINCT sortedColumn FROM testTable WHERE filterColumn >= 0 LIMIT 1000 " + OPT + ")");
    DistinctTable table = op.nextBlock().getDistinctTable();
    assertTrue(usedSortedIndex(op));
    assertEquals(table.size(), SORTED_NUM_UNIQUE);
  }

  @Test
  public void testSortedColumnFilters() {
    _activeSegment = _sortedSegment;

    // Selective filter: filterColumn < 500 → sorted values 0..4
    BaseOperator<DistinctResultsBlock> selOp = getOperator(
        "SELECT DISTINCT sortedColumn FROM testTable WHERE filterColumn < 500 LIMIT 1000 " + OPT + ")");
    Set<Integer> expected = new HashSet<>();
    for (int i = 0; i < 5; i++) {
      expected.add(i);
    }
    assertEquals(extractIntValues(selOp.nextBlock().getDistinctTable()), expected);

    // Sparse filter: filterColumn=50 (value 0) OR filterColumn=150 (value 1)
    BaseOperator<DistinctResultsBlock> sparseOp = getOperator(
        "SELECT DISTINCT sortedColumn FROM testTable WHERE filterColumn = 50 OR filterColumn = 150 "
            + "LIMIT 1000 " + OPT + ")");
    assertEquals(extractIntValues(sparseOp.nextBlock().getDistinctTable()), Set.of(0, 1));

    // Empty filter
    BaseOperator<DistinctResultsBlock> emptyOp = getOperator(
        "SELECT DISTINCT sortedColumn FROM testTable WHERE filterColumn > 99999 LIMIT 1000 " + OPT + ")");
    DistinctTable emptyTable = emptyOp.nextBlock().getDistinctTable();
    assertTrue(usedSortedIndex(emptyOp));
    assertEquals(emptyTable.size(), 0);
  }

  @Test
  public void testSortedColumnLimit() {
    _activeSegment = _sortedSegment;

    BaseOperator<DistinctResultsBlock> limitOp = getOperator(
        "SELECT DISTINCT sortedColumn FROM testTable WHERE filterColumn >= 0 LIMIT 10 " + OPT + ")");
    assertEquals(limitOp.nextBlock().getDistinctTable().size(), 10);
  }

  @Test
  public void testSortedColumnMatchesScan() {
    _activeSegment = _sortedSegment;

    BaseOperator<DistinctResultsBlock> sortedOp = getOperator(
        "SELECT DISTINCT sortedColumn FROM testTable WHERE filterColumn < 500 LIMIT 1000 " + OPT + ")");
    BaseOperator<DistinctResultsBlock> scanOp = getOperator(
        "SELECT DISTINCT sortedColumn FROM testTable WHERE filterColumn < 500 LIMIT 1000");
    assertEquals(extractIntValues(sortedOp.nextBlock().getDistinctTable()),
        extractIntValues(scanOp.nextBlock().getDistinctTable()));
  }

  @Test
  public void testSortedColumnOrderByDesc() {
    _activeSegment = _sortedSegment;

    BaseOperator<DistinctResultsBlock> descOp = getOperator(
        "SELECT DISTINCT sortedColumn FROM testTable WHERE filterColumn < 500 "
            + "ORDER BY sortedColumn DESC LIMIT 1000 " + OPT + ")");
    BaseOperator<DistinctResultsBlock> descScanOp = getOperator(
        "SELECT DISTINCT sortedColumn FROM testTable WHERE filterColumn < 500 "
            + "ORDER BY sortedColumn DESC LIMIT 1000");
    assertEquals(extractOrderedIntValues(descOp.nextBlock().getDistinctTable().toResultTable()),
        extractOrderedIntValues(descScanOp.nextBlock().getDistinctTable().toResultTable()));
  }

  @Test
  public void testSortedColumnOrderByDescWithLimit() {
    _activeSegment = _sortedSegment;

    int limit = 5;
    BaseOperator<DistinctResultsBlock> descLimitOp = getOperator(
        "SELECT DISTINCT sortedColumn FROM testTable WHERE filterColumn >= 0 "
            + "ORDER BY sortedColumn DESC LIMIT " + limit + " " + OPT + ")");
    BaseOperator<DistinctResultsBlock> descLimitScanOp = getOperator(
        "SELECT DISTINCT sortedColumn FROM testTable WHERE filterColumn >= 0 "
            + "ORDER BY sortedColumn DESC LIMIT " + limit);
    DistinctTable descLimitTable = descLimitOp.nextBlock().getDistinctTable();
    ResultTable descLimitResultTable = descLimitTable.toResultTable();
    assertEquals(extractOrderedIntValues(descLimitResultTable),
        extractOrderedIntValues(descLimitScanOp.nextBlock().getDistinctTable().toResultTable()));
    assertEquals(descLimitResultTable.getRows().size(), limit);
  }

  @Test
  public void testMutableSegmentOrderByUsesInvertedIndex() {
    _activeSegment = _mutableSegment;
    String bitmapCapableAllDocsFilter = "intColumn IN (0,1,2,3,4,5,6,7,8,9)";

    BaseOperator<DistinctResultsBlock> invertedOp = getOperator(
        "SELECT DISTINCT intColumn FROM testTable WHERE " + bitmapCapableAllDocsFilter + ' '
            + "ORDER BY intColumn DESC LIMIT 5 " + OPT_INV);
    DistinctTable invertedTable = invertedOp.nextBlock().getDistinctTable();
    assertTrue(usedInvertedIndex(invertedOp));

    BaseOperator<DistinctResultsBlock> scanOp = getOperator(
        "SELECT DISTINCT intColumn FROM testTable WHERE " + bitmapCapableAllDocsFilter + ' '
            + "ORDER BY intColumn DESC LIMIT 5 " + OPT_SCAN);
    DistinctTable scanTable = scanOp.nextBlock().getDistinctTable();
    assertFalse(usedInvertedIndex(scanOp));

    ResultTable invertedResultTable = invertedTable.toResultTable();
    ResultTable scanResultTable = scanTable.toResultTable();
    assertEquals(extractOrderedIntValues(invertedResultTable), extractOrderedIntValues(scanResultTable));
    assertEquals(extractOrderedIntValues(invertedResultTable), List.of(9, 8, 7, 6, 5));
  }

  // ==================== STRING Tests ====================

  @Test
  public void testStringColumnWithFilter() {
    _activeSegment = _stringSegment;

    BaseOperator<DistinctResultsBlock> op = getOperator(
        "SELECT DISTINCT stringColumn FROM testTable WHERE filterColumn < 500 LIMIT 1000 " + OPT_INV);
    DistinctTable table = op.nextBlock().getDistinctTable();
    assertTrue(usedInvertedIndex(op));
    assertEquals(table.size(), STRING_NUM_UNIQUE);
  }

  @Test
  public void testStringColumnInvertedVsScan() {
    _activeSegment = _stringSegment;

    BaseOperator<DistinctResultsBlock> invertedOp = getOperator(
        "SELECT DISTINCT stringColumn FROM testTable WHERE filterColumn < 200 "
            + "ORDER BY stringColumn LIMIT 1000 " + OPT_INV);
    DistinctTable invertedTable = invertedOp.nextBlock().getDistinctTable();
    assertTrue(usedInvertedIndex(invertedOp));

    BaseOperator<DistinctResultsBlock> scanOp = getOperator(
        "SELECT DISTINCT stringColumn FROM testTable WHERE filterColumn < 200 "
            + "ORDER BY stringColumn LIMIT 1000 " + OPT_SCAN);
    assertEquals(extractOrderedStringValues(invertedTable.toResultTable()),
        extractOrderedStringValues(scanOp.nextBlock().getDistinctTable().toResultTable()));
  }

  @Test
  public void testStringColumnOrderByDesc() {
    _activeSegment = _stringSegment;

    BaseOperator<DistinctResultsBlock> descOp = getOperator(
        "SELECT DISTINCT stringColumn FROM testTable WHERE filterColumn >= 0 "
            + "ORDER BY stringColumn DESC LIMIT 1000 " + OPT_INV);
    BaseOperator<DistinctResultsBlock> descScanOp = getOperator(
        "SELECT DISTINCT stringColumn FROM testTable WHERE filterColumn >= 0 "
            + "ORDER BY stringColumn DESC LIMIT 1000 " + OPT_SCAN);
    assertEquals(extractOrderedStringValues(descOp.nextBlock().getDistinctTable().toResultTable()),
        extractOrderedStringValues(descScanOp.nextBlock().getDistinctTable().toResultTable()));
  }

  @Test
  public void testStringColumnOrderByDescWithLimit() {
    _activeSegment = _stringSegment;

    BaseOperator<DistinctResultsBlock> descLimitOp = getOperator(
        "SELECT DISTINCT stringColumn FROM testTable WHERE filterColumn >= 0 "
            + "ORDER BY stringColumn DESC LIMIT 5 " + OPT_INV);
    BaseOperator<DistinctResultsBlock> descLimitScanOp = getOperator(
        "SELECT DISTINCT stringColumn FROM testTable WHERE filterColumn >= 0 "
            + "ORDER BY stringColumn DESC LIMIT 5");
    DistinctTable descLimitStrTable = descLimitOp.nextBlock().getDistinctTable();
    ResultTable descLimitResultTable = descLimitStrTable.toResultTable();
    assertEquals(extractOrderedStringValues(descLimitResultTable),
        extractOrderedStringValues(descLimitScanOp.nextBlock().getDistinctTable().toResultTable()));
    assertEquals(descLimitResultTable.getRows().size(), 5);
  }

  @Test
  public void testStringColumnEmptyFilter() {
    _activeSegment = _stringSegment;

    BaseOperator<DistinctResultsBlock> emptyOp = getOperator(
        "SELECT DISTINCT stringColumn FROM testTable WHERE filterColumn > 99999 LIMIT 1000 " + OPT_INV);
    assertEquals(emptyOp.nextBlock().getDistinctTable().size(), 0);
  }

  @Test
  public void testStringColumnSelectiveFilter() {
    _activeSegment = _stringSegment;

    BaseOperator<DistinctResultsBlock> selectiveOp = getOperator(
        "SELECT DISTINCT stringColumn FROM testTable WHERE filterColumn < 100 "
            + "ORDER BY stringColumn LIMIT 100 " + OPT_INV);
    BaseOperator<DistinctResultsBlock> selectiveScanOp = getOperator(
        "SELECT DISTINCT stringColumn FROM testTable WHERE filterColumn < 100 "
            + "ORDER BY stringColumn LIMIT 100 " + OPT_SCAN);
    assertEquals(extractOrderedStringValues(selectiveOp.nextBlock().getDistinctTable().toResultTable()),
        extractOrderedStringValues(selectiveScanOp.nextBlock().getDistinctTable().toResultTable()));
  }

  // ==================== Null Handling Tests ====================

  @Test
  public void testNullIncludedWithWideFilter() {
    _activeSegment = _nullSegment;

    BaseOperator<DistinctResultsBlock> op = getOperator(
        "SELECT DISTINCT intColumn FROM testTable WHERE filterColumn >= 0 LIMIT 1000 "
            + OPT + ", invertedIndexDistinctCostRatio=1, enableNullHandling=true)");
    DistinctTable table = op.nextBlock().getDistinctTable();
    assertTrue(usedInvertedIndex(op));
    assertEquals(table.size(), NULL_NUM_UNIQUE + 1);
    assertTrue(containsNull(table));
  }

  @Test
  public void testNullExcludedWithSelectiveFilter() {
    _activeSegment = _nullSegment;

    // filterColumn < 500 → docs 0-499, all non-null
    BaseOperator<DistinctResultsBlock> op = getOperator(
        "SELECT DISTINCT intColumn FROM testTable WHERE filterColumn < 500 LIMIT 1000 "
            + OPT + ", invertedIndexDistinctCostRatio=1, enableNullHandling=true)");
    DistinctTable table = op.nextBlock().getDistinctTable();
    assertTrue(usedInvertedIndex(op));
    assertFalse(containsNull(table));
    assertEquals(table.size(), NULL_NUM_UNIQUE);
  }

  @Test
  public void testNullWithPartialFilter() {
    _activeSegment = _nullSegment;

    // filterColumn >= 940 → docs 940-999 (10 non-null + 50 null)
    BaseOperator<DistinctResultsBlock> op = getOperator(
        "SELECT DISTINCT intColumn FROM testTable WHERE filterColumn >= 940 LIMIT 1000 "
            + OPT + ", invertedIndexDistinctCostRatio=1, enableNullHandling=true)");
    DistinctTable table = op.nextBlock().getDistinctTable();
    assertTrue(usedInvertedIndex(op));
    assertTrue(containsNull(table));
    assertEquals(table.size(), 11); // 10 unique non-null + null
  }

  @Test
  public void testNullHandlingOrderBy() {
    _activeSegment = _nullSegment;

    BaseOperator<DistinctResultsBlock> orderOp = getOperator(
        "SELECT DISTINCT intColumn FROM testTable WHERE filterColumn >= 0 "
            + "ORDER BY intColumn LIMIT 1000 "
            + OPT + ", invertedIndexDistinctCostRatio=1, enableNullHandling=true)");
    ResultTable resultTable = orderOp.nextBlock().getDistinctTable().toResultTable();
    assertEquals(resultTable.getRows().size(), NULL_NUM_UNIQUE + 1);
    assertEquals(resultTable.getRows().get(0)[0], 0);
    assertNull(resultTable.getRows().get(resultTable.getRows().size() - 1)[0]);
  }

  @Test
  public void testNullHandlingOrderByNullsFirstLimit() {
    _activeSegment = _nullSegment;

    BaseOperator<DistinctResultsBlock> limitOp = getOperator(
        "SELECT DISTINCT intColumn FROM testTable WHERE filterColumn >= 0 "
            + "ORDER BY intColumn NULLS FIRST LIMIT 10 "
            + OPT + ", invertedIndexDistinctCostRatio=1, enableNullHandling=true)");
    ResultTable resultTable = limitOp.nextBlock().getDistinctTable().toResultTable();
    assertEquals(resultTable.getRows().size(), 10);
    assertNull(resultTable.getRows().get(0)[0]);
    assertTrue(containsNull(resultTable));
  }

  @Test
  public void testNullHandlingOrderByNullsLastLimit() {
    _activeSegment = _nullSegment;

    BaseOperator<DistinctResultsBlock> limitOp = getOperator(
        "SELECT DISTINCT intColumn FROM testTable WHERE filterColumn >= 0 "
            + "ORDER BY intColumn LIMIT 10 "
            + OPT + ", invertedIndexDistinctCostRatio=1, enableNullHandling=true)");
    ResultTable resultTable = limitOp.nextBlock().getDistinctTable().toResultTable();
    assertEquals(resultTable.getRows().size(), 10);
    assertFalse(containsNull(resultTable));
    assertEquals(resultTable.getRows().get(0)[0], 0);
    assertEquals(resultTable.getRows().get(resultTable.getRows().size() - 1)[0], 9);
  }

  @Test
  public void testNullHandlingOrderByDescNullsLastLimit() {
    _activeSegment = _nullSegment;

    BaseOperator<DistinctResultsBlock> limitOp = getOperator(
        "SELECT DISTINCT intColumn FROM testTable WHERE filterColumn >= 0 "
            + "ORDER BY intColumn DESC NULLS LAST LIMIT 10 "
            + OPT + ", invertedIndexDistinctCostRatio=1, enableNullHandling=true)");
    ResultTable resultTable = limitOp.nextBlock().getDistinctTable().toResultTable();
    assertEquals(resultTable.getRows().size(), 10);
    assertFalse(containsNull(resultTable));
    assertEquals(resultTable.getRows().get(0)[0], 49);
    assertEquals(resultTable.getRows().get(resultTable.getRows().size() - 1)[0], 40);
  }

  @Test
  public void testNullHandlingInvertedVsScan() {
    _activeSegment = _nullSegment;

    BaseOperator<DistinctResultsBlock> invertedOp = getOperator(
        "SELECT DISTINCT intColumn FROM testTable WHERE filterColumn >= 940 "
            + "ORDER BY intColumn LIMIT 1000 "
            + OPT + ", invertedIndexDistinctCostRatio=1, enableNullHandling=true)");
    ResultTable invertedResultTable = invertedOp.nextBlock().getDistinctTable().toResultTable();
    assertTrue(usedInvertedIndex(invertedOp));

    BaseOperator<DistinctResultsBlock> scanOp = getOperator(
        "SELECT DISTINCT intColumn FROM testTable WHERE filterColumn >= 940 "
            + "ORDER BY intColumn LIMIT 1000 "
            + OPT + ", invertedIndexDistinctCostRatio=100000, enableNullHandling=true)");
    ResultTable scanResultTable = scanOp.nextBlock().getDistinctTable().toResultTable();
    assertFalse(usedInvertedIndex(scanOp));

    assertEquals(extractOrderedIntValues(invertedResultTable), extractOrderedIntValues(scanResultTable));
    assertTrue(containsNull(invertedResultTable));
  }

  @Test
  public void testNullPreservedInBrokerResultWithoutOrderByLimit() {
    _activeSegment = _nullSegment;

    BaseOperator<DistinctResultsBlock> op = getOperator(
        "SELECT DISTINCT intColumn FROM testTable WHERE filterColumn >= 940 LIMIT 10 "
            + OPT + ", invertedIndexDistinctCostRatio=1, enableNullHandling=true)");
    DistinctTable table = op.nextBlock().getDistinctTable();
    assertTrue(usedInvertedIndex(op));

    ResultTable resultTable = table.toResultTable();
    assertEquals(resultTable.getRows().size(), 10);
    assertTrue(containsNull(resultTable));

    int nonNullValues = 0;
    for (Object[] row : resultTable.getRows()) {
      if (row[0] != null) {
        nonNullValues++;
      }
    }
    assertEquals(nonNullValues, 9);
  }

  @Test
  public void testPlaceholderWithoutNullHandling() {
    _activeSegment = _nullSegment;

    BaseOperator<DistinctResultsBlock> op = getOperator(
        "SELECT DISTINCT intColumn FROM testTable WHERE filterColumn >= 0 LIMIT 1000 " + OPT_INV);
    DistinctTable table = op.nextBlock().getDistinctTable();
    assertTrue(usedInvertedIndex(op));
    assertFalse(containsNull(table));
    // Without null handling: 50 real values + Integer.MIN_VALUE placeholder = 51
    assertEquals(table.size(), NULL_NUM_UNIQUE + 1);
  }
}
