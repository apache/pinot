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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.SelectionResults;
import org.apache.pinot.common.segment.ReadMode;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.core.data.manager.SegmentDataManager;
import org.apache.pinot.core.data.manager.offline.ImmutableSegmentDataManager;
import org.apache.pinot.core.data.readers.GenericRowRecordReader;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegment;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.query.AggregationOperator;
import org.apache.pinot.core.query.aggregation.DistinctTable;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.pql.parsers.Pql2Compiler;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.collections.Lists;


/**
 * Class to test DISTINCT queries.
 * Generates custom data set with explicitly generating
 * duplicate rows/keys
 */
public class DistinctQueriesTest extends BaseQueriesTest {
  private static final int NUM_ROWS = 1_000_000;

  private List<GenericRow> _rows = new ArrayList<>();

  private static String D1 = "STRING_COL1";
  private static String D2 = "STRING_COL2";
  private static String M1 = "INT_COL";
  private static String M2 = "LONG_COL";

  // in the custom data set, each row is repeated after 20 rows
  private static final int TUPLE_REPEAT_INTERVAL = 20;
  // in the custom data set, each row is repeated 5 times, total 200k unique rows in dataset
  private static final int PER_TUPLE_REPEAT_FREQUENCY = 5;
  private static final int NUM_UNIQUE_TUPLES = NUM_ROWS / PER_TUPLE_REPEAT_FREQUENCY;

  private static final int INT_BASE_VALUE = 10000;
  private static final int INT_INCREMENT = 500;
  private static final long LONG_BASE_VALUE = 100000000;
  private static final long LONG_INCREMENT = 5500;

  private static final String TABLE_NAME = "DistinctTestTable";
  private static final int NUM_SEGMENTS = 2;
  private static final String SEGMENT_NAME_1 = TABLE_NAME + "_100000000_200000000";
  private static final String SEGMENT_NAME_2 = TABLE_NAME + "_300000000_400000000";
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "DistinctQueryTest");

  private List<IndexSegment> _indexSegments = new ArrayList<>(NUM_SEGMENTS);
  private List<SegmentDataManager> _segmentDataManagers;
  private final Set<Record> _expectedAddTransformResults = new HashSet<>();
  private final Set<Record> _expectedSubTransformResults = new HashSet<>();
  private final Set<Record> _expectedAddSubTransformResults = new HashSet<>();
  private final Set<Record> _expectedResults = new HashSet<>();
  private final FieldSpec.DataType[] _dataTypes =
      new FieldSpec.DataType[]{FieldSpec.DataType.STRING, FieldSpec.DataType.STRING, FieldSpec.DataType.INT, FieldSpec.DataType.LONG};

  private Schema _schema;

  @BeforeClass
  public void setUp() {
    Pql2Compiler.ENABLE_DISTINCT = true;
    createPinotTableSchema();
    createTestData();
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  private void createPinotTableSchema() {
    _schema =
        new Schema.SchemaBuilder().setSchemaName(TABLE_NAME).addSingleValueDimension(D1, FieldSpec.DataType.STRING)
            .addSingleValueDimension(D2, FieldSpec.DataType.STRING).addMetric(M1, FieldSpec.DataType.INT)
            .addMetric(M2, FieldSpec.DataType.LONG).build();
  }

  /**
   * Custom data generator that explicitly generates duplicate
   * rows in dataset for purpose of testing DISTINCT functionality
   */
  private void createTestData() {
    int pos = 0;
    Object[] columnValues = new Object[_schema.size()];
    for (int rowIndex = 0; rowIndex < NUM_ROWS; rowIndex++) {
      GenericRow row = new GenericRow();
      double addition;
      double subtraction;
      int col = 0;
      boolean duplicate = false;
      for (FieldSpec.DataType dataType : _dataTypes) {
        // generate each column for the row
        Object value = null;
        if (rowIndex == 0) {
          switch (dataType) {
            case INT:
              value = INT_BASE_VALUE;
              row.putField(M1, value);
              break;
            case LONG:
              value = LONG_BASE_VALUE;
              row.putField(M2, value);
              break;
            case STRING:
              value = RandomStringUtils.randomAlphabetic(10);
              if (col == 0) {
                row.putField(D1, value);
              } else {
                row.putField(D2, value);
              }
              break;
          }
        } else {
          if (rowIndex == pos + (TUPLE_REPEAT_INTERVAL * PER_TUPLE_REPEAT_FREQUENCY)) {
            pos = rowIndex;
          }
          if (rowIndex < pos + TUPLE_REPEAT_INTERVAL) {
            // generate unique row
            switch (dataType) {
              case INT:
                value = (Integer) _rows.get(rowIndex - 1).getValue(M1) + INT_INCREMENT;
                row.putField(M1, value);
                break;
              case LONG:
                value = (Long) _rows.get(rowIndex - 1).getValue(M2) + LONG_INCREMENT;
                row.putField(M2, value);
                break;
              case STRING:
                value = RandomStringUtils.randomAlphabetic(10);
                if (col == 0) {
                  row.putField(D1, value);
                } else {
                  row.putField(D2, value);
                }
                break;
            }
          } else {
            // generate duplicate row
            duplicate = true;
            switch (dataType) {
              case INT:
                value = _rows.get(rowIndex - TUPLE_REPEAT_INTERVAL).getValue(M1);
                row.putField(M1, value);
                break;
              case LONG:
                value = _rows.get(rowIndex - TUPLE_REPEAT_INTERVAL).getValue(M2);
                row.putField(M2, value);
                break;
              case STRING:
                if (col == 0) {
                  row.putField(D1, _rows.get(rowIndex - TUPLE_REPEAT_INTERVAL).getValue(D1));
                } else {
                  row.putField(D2, _rows.get(rowIndex - TUPLE_REPEAT_INTERVAL).getValue(D2));
                }
                break;
            }
          }
        }

        columnValues[col++] = value;
      }

      // add the generated row
      _rows.add(row);

      // compute expected result for add and sub transform function
      addition = ((Integer) columnValues[2]) + ((Long) columnValues[3]);
      subtraction = ((Long) columnValues[3]) - ((Integer) columnValues[2]);

      // compute expected result for multi column distinct
      if (!duplicate) {
        Record record = new Record(new Object[]{columnValues[0], columnValues[1], columnValues[2], columnValues[3]});
        _expectedResults.add(record);
      }

      _expectedAddTransformResults.add(new Record(new Object[]{addition}));
      _expectedSubTransformResults.add(new Record(new Object[]{subtraction}));
      _expectedAddSubTransformResults.add(new Record(new Object[]{addition, subtraction}));
    }
  }

  @Override
  protected String getFilter() {
    return "";
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _indexSegments.get(0);
  }

  @Override
  protected List<SegmentDataManager> getSegmentDataManagers() {
    return _segmentDataManagers;
  }

  private void createSegment(Schema schema, RecordReader recordReader, String segmentName, String tableName)
      throws Exception {
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(schema);
    segmentGeneratorConfig.setTableName(tableName);
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getAbsolutePath());
    segmentGeneratorConfig.setSegmentName(segmentName);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, recordReader);
    driver.build();

    File segmentIndexDir = new File(INDEX_DIR.getAbsolutePath(), segmentName);
    if (!segmentIndexDir.exists()) {
      throw new IllegalStateException("Segment generation failed");
    }
  }

  private ImmutableSegment loadSegment(String segmentName)
      throws Exception {
    return ImmutableSegmentLoader.load(new File(INDEX_DIR, segmentName), ReadMode.heap);
  }

  /**
   * Test DISTINCT query with multiple columns on generated data set.
   * All the generated dataset is put into a single segment
   * and we directly run the {@link AggregationOperator} to
   * get segment level execution results.
   * The results are then compared to the expected result table
   * that was build during data generation
   * @throws Exception
   */
  @Test
  public void testDistinctInnerSegment()
      throws Exception {
    try {
      // put all the generated dataset in a single segment
      try (RecordReader recordReader = new GenericRowRecordReader(_rows, _schema)) {
        createSegment(_schema, recordReader, SEGMENT_NAME_1, TABLE_NAME);
        final ImmutableSegment immutableSegment = loadSegment(SEGMENT_NAME_1);
        _indexSegments.add(immutableSegment);

        // All 200k unique rows should be returned
        String query =
            "SELECT DISTINCT(STRING_COL1, STRING_COL2, INT_COL, LONG_COL) FROM DistinctTestTable LIMIT 1000000";
        innerSegmentTestHelper(query, NUM_UNIQUE_TUPLES);

        // All 200k unique rows should be returned
        query = "SELECT DISTINCT(STRING_COL1, STRING_COL2, INT_COL, LONG_COL) FROM DistinctTestTable LIMIT 200000";
        innerSegmentTestHelper(query, NUM_UNIQUE_TUPLES);

        // 100k rows should be returned
        query = "SELECT DISTINCT(STRING_COL1, STRING_COL2, INT_COL, LONG_COL) FROM DistinctTestTable LIMIT 100000";
        innerSegmentTestHelper(query, 100000);

        // default: 10 unique rows should be returned
        query = "SELECT DISTINCT(STRING_COL1, STRING_COL2, INT_COL, LONG_COL) FROM DistinctTestTable";
        innerSegmentTestHelper(query, 10);

        // default: 10 unique rows should be returned
        query = "SELECT DISTINCT(add(INT_COL,LONG_COL)) FROM DistinctTestTable";
        innerSegmentTransformQueryTestHelper(query, 10, 1, new String[]{"add(INT_COL,LONG_COL)"},
            new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE});

        // default: 10 unique rows should be returned
        query = "SELECT DISTINCT(sub(LONG_COL,INT_COL)) FROM DistinctTestTable";
        innerSegmentTransformQueryTestHelper(query, 10, 2, new String[]{"sub(LONG_COL,INT_COL)"},
            new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE});

        // 100k unique rows should be returned
        query = "SELECT DISTINCT(add(INT_COL,LONG_COL),sub(LONG_COL,INT_COL)) FROM DistinctTestTable LIMIT 100000 ";
        innerSegmentTransformQueryTestHelper(query, 100000, 3,
            new String[]{"add(INT_COL,LONG_COL)", "sub(LONG_COL,INT_COL)"},
            new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE});
      }
    } finally {
      destroySegments();
    }
  }

  /**
   * Helper for inner segment query tests
   * @param query query to run
   * @param expectedSize expected result size
   */
  private void innerSegmentTestHelper(final String query, final int expectedSize) {
    // compile to broker request and directly run the operator
    AggregationOperator aggregationOperator = getOperatorForQuery(query);
    IntermediateResultsBlock resultsBlock = aggregationOperator.nextBlock();
    List<Object> operatorResult = resultsBlock.getAggregationResult();

    // verify resultset
    Assert.assertNotNull(operatorResult);
    Assert.assertEquals(operatorResult.size(), 1);
    Assert.assertTrue(operatorResult.get(0) instanceof DistinctTable);

    DistinctTable distinctTable = (DistinctTable) operatorResult.get(0);
    Assert.assertEquals(_expectedResults.size(), NUM_UNIQUE_TUPLES);
    Assert.assertEquals(distinctTable.size(), expectedSize);

    DataSchema dataSchema = distinctTable.getDataSchema();
    Assert.assertEquals(dataSchema.getColumnNames(), new String[]{D1, D2, M1, M2});
    Assert.assertEquals(dataSchema.getColumnDataTypes(),
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.LONG});

    Iterator<Record> iterator = distinctTable.iterator();
    while (iterator.hasNext()) {
      Record record = iterator.next();
      Assert.assertEquals(record.getValues().length, 4);
      Assert.assertTrue(_expectedResults.contains(record));
    }
  }

  /**
   * Helper for inner segment transform query tests
   * @param query query to run
   * @param expectedSize expected result size
   */
  private void innerSegmentTransformQueryTestHelper(final String query, final int expectedSize, final int op,
      final String[] columnNames, final DataSchema.ColumnDataType[] columnTypes) {
    // compile to broker request and directly run the operator
    AggregationOperator aggregationOperator = getOperatorForQuery(query);
    IntermediateResultsBlock resultsBlock = aggregationOperator.nextBlock();
    final List<Object> operatorResult = resultsBlock.getAggregationResult();

    // verify resultset
    Assert.assertNotNull(operatorResult);
    Assert.assertEquals(operatorResult.size(), 1);
    Assert.assertTrue(operatorResult.get(0) instanceof DistinctTable);

    final DistinctTable distinctTable = (DistinctTable) operatorResult.get(0);
    Assert.assertEquals(distinctTable.size(), expectedSize);

    DataSchema dataSchema = distinctTable.getDataSchema();
    Assert.assertEquals(dataSchema.getColumnNames(), columnNames);
    Assert.assertEquals(dataSchema.getColumnDataTypes(), columnTypes);

    Iterator<Record> iterator = distinctTable.iterator();
    while (iterator.hasNext()) {
     Record record = iterator.next();
      Assert.assertEquals(record.getValues().length, columnNames.length);
      if (op == 1) {
        Assert.assertTrue(_expectedAddTransformResults.contains(record));
      } else if (op == 2) {
        Assert.assertTrue(_expectedSubTransformResults.contains(record));
      } else {
        Assert.assertTrue(_expectedAddSubTransformResults.contains(record));
      }
    }
  }

  /**
   * Test DISTINCT query with multiple columns on generated data set.
   * The generated dataset is divided into two segments.
   * We exercise the entire execution from broker ->
   * server -> segment. The server combines the results
   * from segments and sends the data table to broker.
   *
   * Currently the base class mimics the broker level
   * execution by duplicating the data table to mimic
   * two servers and then doing the merge
   *
   * The results are then compared to the expected result table
   * that was build during data generation
   * @throws Exception
   */
  @Test(dependsOnMethods = {"testDistinctInnerSegment"})
  public void testDistinctInterSegmentInterServer()
      throws Exception {
    try {
      // divide the generated dataset into 2 parts and create 2 segments
      final List<GenericRow> randomRows = new ArrayList<>();
      final List<GenericRow> copiedRows = new ArrayList<>(_rows);
      final int size = copiedRows.size();
      for (int row = size - 1; row >= size / 2; row--) {
        randomRows.add(copiedRows.remove(row));
      }

      try (RecordReader recordReader1 = new GenericRowRecordReader(copiedRows, _schema);
          RecordReader recordReader2 = new GenericRowRecordReader(randomRows, _schema)) {
        createSegment(_schema, recordReader1, SEGMENT_NAME_1, TABLE_NAME);
        createSegment(_schema, recordReader2, SEGMENT_NAME_2, TABLE_NAME);
        final ImmutableSegment segment1 = loadSegment(SEGMENT_NAME_1);
        final ImmutableSegment segment2 = loadSegment(SEGMENT_NAME_2);

        _indexSegments.add(segment1);
        _indexSegments.add(segment2);
        _segmentDataManagers =
            Arrays.asList(new ImmutableSegmentDataManager(segment1), new ImmutableSegmentDataManager(segment2));

        // All 200k unique rows should be returned
        String query =
            "SELECT DISTINCT(STRING_COL1, STRING_COL2, INT_COL, LONG_COL) FROM DistinctTestTable LIMIT 1000000";
        interSegmentInterServerTestHelper(query, NUM_UNIQUE_TUPLES);

        // All 200k unique unique 1 million rows should be returned
        query = "SELECT DISTINCT(STRING_COL1, STRING_COL2, INT_COL, LONG_COL) FROM DistinctTestTable LIMIT 200000";
        interSegmentInterServerTestHelper(query, NUM_UNIQUE_TUPLES);

        // 100k unique rows should be returned
        query = "SELECT DISTINCT(STRING_COL1, STRING_COL2, INT_COL, LONG_COL) FROM DistinctTestTable LIMIT 100000";
        interSegmentInterServerTestHelper(query, 100000);

        // Default: 10 unique rows should be returned
        query = "SELECT DISTINCT(STRING_COL1, STRING_COL2, INT_COL, LONG_COL) FROM DistinctTestTable";
        interSegmentInterServerTestHelper(query, 10);
      }
    } finally {
      destroySegments();
    }
  }

  /**
   * Helper for inter segment, inter server query tests
   * @param query query to run
   * @param expectedSize expected result size
   */
  private void interSegmentInterServerTestHelper(String query, int expectedSize) {
    BrokerResponseNative brokerResponse = getBrokerResponseForQuery(query);
    final SelectionResults selectionResults = brokerResponse.getSelectionResults();

    Assert.assertEquals(selectionResults.getColumns().size(), 4);

    Assert.assertEquals(selectionResults.getColumns().get(0), D1);
    Assert.assertEquals(selectionResults.getColumns().get(1), D2);
    Assert.assertEquals(selectionResults.getColumns().get(2), M1);
    Assert.assertEquals(selectionResults.getColumns().get(3), M2);

    Assert.assertEquals(_expectedResults.size(), NUM_UNIQUE_TUPLES);
    Assert.assertEquals(selectionResults.getRows().size(), expectedSize);

    for (Serializable[] row : selectionResults.getRows()) {
      Assert.assertEquals(row.length, 4);
      Record record = new Record(row);
      Assert.assertTrue(_expectedResults.contains(record));
    }
  }

  /**
   * Test DISTINCT queries on multiple columns with FILTER.
   * A simple hand-written data set of 10 rows in a single segment
   * is used for FILTER based queries as opposed to generated data set.
   * The results are compared to expected table.
   *
   * Runs 4 different queries with predicates.
   * @throws Exception
   */
  @Test(dependsOnMethods = {"testDistinctInterSegmentInterServer"})
  public void testDistinctWithFilter()
      throws Exception {
    try {
      String tableName = TABLE_NAME + "WithFilter";

      Schema schema = new Schema.SchemaBuilder().setSchemaName(tableName)
          .addSingleValueDimension("State", FieldSpec.DataType.STRING)
          .addSingleValueDimension("City", FieldSpec.DataType.STRING).addMetric("SaleAmount", FieldSpec.DataType.INT)
          .build();

      String query1 = "SELECT DISTINCT(State, City) FROM " + tableName + " WHERE SaleAmount >= 200000";
      String query2 = "SELECT DISTINCT(State, City) FROM " + tableName + " WHERE SaleAmount >= 400000";
      String query3 = "SELECT DISTINCT(State, City, SaleAmount) FROM " + tableName + " WHERE SaleAmount >= 200000";
      String query4 = "SELECT DISTINCT(State, City, SaleAmount) FROM " + tableName + " WHERE SaleAmount >= 400000";

      Set<Record> q1ExpectedResults = new HashSet<>();
      Set<Record> q2ExpectedResults = new HashSet<>();
      Set<Record> q3ExpectedResults = new HashSet<>();
      Set<Record> q4ExpectedResults = new HashSet<>();

      List<GenericRow> rows =
          createSimpleTable(q1ExpectedResults, q2ExpectedResults, q3ExpectedResults, q4ExpectedResults);

      try (RecordReader recordReader = new GenericRowRecordReader(rows, schema)) {
        createSegment(schema, recordReader, SEGMENT_NAME_1, tableName);
        final ImmutableSegment segment = loadSegment(SEGMENT_NAME_1);
        _indexSegments.add(segment);
        _segmentDataManagers = Arrays.asList(new ImmutableSegmentDataManager(segment), new ImmutableSegmentDataManager(segment));

        // without ORDER BY
        runFilterQueryInnerSegment(q1ExpectedResults, query1, new String[]{"State", "City"},
            new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.STRING});
        runFilterQueryInnerSegment(q2ExpectedResults, query2, new String[]{"State", "City"},
            new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.STRING});
        runFilterQueryInnerSegment(q3ExpectedResults, query3, new String[]{"State", "City", "SaleAmount"},
            new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT});
        runFilterQueryInnerSegment(q4ExpectedResults, query4, new String[]{"State", "City", "SaleAmount"},
            new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT});

        // with ORDER BY ASC/DESC
        String orderByQuery = query1 + " ORDER BY State, City LIMIT 100";
        List<Record> sortedResults = new ArrayList<>();
        sortedResults.add(new Record(new Object[]{"California", "Mountain View"}));
        sortedResults.add(new Record(new Object[]{"California", "San Mateo"}));
        sortedResults.add(new Record(new Object[]{"California", "Sunnyvale"}));
        runQueryInterSegmentWithOrderBy(orderByQuery, sortedResults, new String[]{"State", "City"});

        orderByQuery = query1 + " ORDER BY State, City LIMIT 2";
        sortedResults = new ArrayList<>();
        sortedResults.add(new Record(new Object[]{"California", "Mountain View"}));
        sortedResults.add(new Record(new Object[]{"California", "San Mateo"}));
        runQueryInterSegmentWithOrderBy(orderByQuery, sortedResults, new String[]{"State", "City"});

        orderByQuery = query1 + " ORDER BY State, City DESC LIMIT 100";
        sortedResults = new ArrayList<>();
        sortedResults.add(new Record(new Object[]{"California", "Sunnyvale"}));
        sortedResults.add(new Record(new Object[]{"California", "San Mateo"}));
        sortedResults.add(new Record(new Object[]{"California", "Mountain View"}));
        runQueryInterSegmentWithOrderBy(orderByQuery, sortedResults, new String[]{"State", "City"});

        orderByQuery = query1 + " ORDER BY State, City DESC LIMIT 2";
        sortedResults = new ArrayList<>();
        sortedResults.add(new Record(new Object[]{"California", "Sunnyvale"}));
        sortedResults.add(new Record(new Object[]{"California", "San Mateo"}));
        runQueryInterSegmentWithOrderBy(orderByQuery, sortedResults, new String[]{"State", "City"});

        orderByQuery = query2 + " ORDER BY State, City LIMIT 100";
        sortedResults = new ArrayList<>();
        sortedResults.add(new Record(new Object[]{"California", "Mountain View"}));
        sortedResults.add(new Record(new Object[]{"California", "San Mateo"}));
        runQueryInterSegmentWithOrderBy(orderByQuery, sortedResults, new String[]{"State", "City"});

        orderByQuery = query2 + " ORDER BY State, City LIMIT 1";
        sortedResults = new ArrayList<>();
        sortedResults.add(new Record(new Object[]{"California", "Mountain View"}));
        runQueryInterSegmentWithOrderBy(orderByQuery, sortedResults, new String[]{"State", "City"});

        orderByQuery = query2 + " ORDER BY State, City DESC LIMIT 100";
        sortedResults = new ArrayList<>();
        sortedResults.add(new Record(new Object[]{"California", "San Mateo"}));
        sortedResults.add(new Record(new Object[]{"California", "Mountain View"}));
        runQueryInterSegmentWithOrderBy(orderByQuery, sortedResults, new String[]{"State", "City"});

        orderByQuery = query2 + " ORDER BY State, City DESC LIMIT 1";
        sortedResults = new ArrayList<>();
        sortedResults.add(new Record(new Object[]{"California", "San Mateo"}));
        runQueryInterSegmentWithOrderBy(orderByQuery, sortedResults, new String[]{"State", "City"});

        orderByQuery = query3 + " ORDER BY State, City, SaleAmount LIMIT 100";
        sortedResults = new ArrayList<>();
        sortedResults.add(new Record(new Object[]{"California", "Mountain View", 200000}));
        sortedResults.add(new Record(new Object[]{"California", "Mountain View", 700000}));
        sortedResults.add(new Record(new Object[]{"California", "San Mateo", 400000}));
        sortedResults.add(new Record(new Object[]{"California", "San Mateo", 500000}));
        sortedResults.add(new Record(new Object[]{"California", "Sunnyvale", 300000}));
        runQueryInterSegmentWithOrderBy(orderByQuery, sortedResults, new String[]{"State", "City", "SaleAmount"});

        orderByQuery = query3 + " ORDER BY State, City, SaleAmount LIMIT 3";
        sortedResults = new ArrayList<>();
        sortedResults.add(new Record(new Object[]{"California", "Mountain View", 200000}));
        sortedResults.add(new Record(new Object[]{"California", "Mountain View", 700000}));
        sortedResults.add(new Record(new Object[]{"California", "San Mateo", 400000}));
        runQueryInterSegmentWithOrderBy(orderByQuery, sortedResults, new String[]{"State", "City", "SaleAmount"});

        orderByQuery = query3 + " ORDER BY State, City, SaleAmount DESC LIMIT 100";
        sortedResults = new ArrayList<>();
        sortedResults.add(new Record(new Object[]{"California", "Mountain View", 700000}));
        sortedResults.add(new Record(new Object[]{"California", "Mountain View", 200000}));
        sortedResults.add(new Record(new Object[]{"California", "San Mateo", 500000}));
        sortedResults.add(new Record(new Object[]{"California", "San Mateo", 400000}));
        sortedResults.add(new Record(new Object[]{"California", "Sunnyvale", 300000}));
        runQueryInterSegmentWithOrderBy(orderByQuery, sortedResults, new String[]{"State", "City", "SaleAmount"});

        orderByQuery = query3 + " ORDER BY State, City, SaleAmount DESC LIMIT 3";
        sortedResults = new ArrayList<>();
        sortedResults.add(new Record(new Object[]{"California", "Mountain View", 700000}));
        sortedResults.add(new Record(new Object[]{"California", "Mountain View", 200000}));
        sortedResults.add(new Record(new Object[]{"California", "San Mateo", 500000}));
        runQueryInterSegmentWithOrderBy(orderByQuery, sortedResults, new String[]{"State", "City", "SaleAmount"});

        orderByQuery = query4 + " ORDER BY State, City, SaleAmount LIMIT 100";
        sortedResults = new ArrayList<>();
        sortedResults.add(new Record(new Object[]{"California", "Mountain View", 700000}));
        sortedResults.add(new Record(new Object[]{"California", "San Mateo", 400000}));
        sortedResults.add(new Record(new Object[]{"California", "San Mateo", 500000}));
        runQueryInterSegmentWithOrderBy(orderByQuery, sortedResults, new String[]{"State", "City", "SaleAmount"});

        orderByQuery = query4 + " ORDER BY State, City, SaleAmount LIMIT 2";
        sortedResults = new ArrayList<>();
        sortedResults.add(new Record(new Object[]{"California", "Mountain View", 700000}));
        sortedResults.add(new Record(new Object[]{"California", "San Mateo", 400000}));
        runQueryInterSegmentWithOrderBy(orderByQuery, sortedResults, new String[]{"State", "City", "SaleAmount"});

        orderByQuery = query4 + " ORDER BY State, City, SaleAmount DESC LIMIT 100";
        sortedResults = new ArrayList<>();
        sortedResults.add(new Record(new Object[]{"California", "Mountain View", 700000}));
        sortedResults.add(new Record(new Object[]{"California", "San Mateo", 500000}));
        sortedResults.add(new Record(new Object[]{"California", "San Mateo", 400000}));
        runQueryInterSegmentWithOrderBy(orderByQuery, sortedResults, new String[]{"State", "City", "SaleAmount"});

        orderByQuery = query4 + " ORDER BY State, City, SaleAmount DESC LIMIT 2";
        sortedResults = new ArrayList<>();
        sortedResults.add(new Record(new Object[]{"California", "Mountain View", 700000}));
        sortedResults.add(new Record(new Object[]{"California", "San Mateo", 500000}));
        runQueryInterSegmentWithOrderBy(orderByQuery, sortedResults, new String[]{"State", "City", "SaleAmount"});

        orderByQuery = "SELECT DISTINCT(State, City, SaleAmount) FROM " + tableName + " ORDER BY State, City, SaleAmount LIMIT 100";
        sortedResults = new ArrayList<>();
        sortedResults.add(new Record(new Object[]{"California", "Mountain View", 200000}));
        sortedResults.add(new Record(new Object[]{"California", "Mountain View", 700000}));
        sortedResults.add(new Record(new Object[]{"California", "San Mateo", 400000}));
        sortedResults.add(new Record(new Object[]{"California", "San Mateo", 500000}));
        sortedResults.add(new Record(new Object[]{"California", "Sunnyvale", 300000}));
        sortedResults.add(new Record(new Object[]{"Oregon", "Portland", 50000}));
        sortedResults.add(new Record(new Object[]{"Oregon", "Portland", 100000}));
        sortedResults.add(new Record(new Object[]{"Washington", "Bellevue", 100000}));
        sortedResults.add(new Record(new Object[]{"Washington", "Bellevue", 150000}));
        sortedResults.add(new Record(new Object[]{"Washington", "Seattle", 100000}));
        runQueryInterSegmentWithOrderBy(orderByQuery, sortedResults, new String[]{"State", "City", "SaleAmount"});

        orderByQuery = "SELECT DISTINCT(State, City, SaleAmount) FROM " + tableName + " ORDER BY State, City, SaleAmount LIMIT 10";
        runQueryInterSegmentWithOrderBy(orderByQuery, sortedResults, new String[]{"State", "City", "SaleAmount"});

        orderByQuery = "SELECT DISTINCT(State, City, SaleAmount) FROM " + tableName + " ORDER BY State, City, SaleAmount LIMIT 5";
        sortedResults = sortedResults.subList(0, 5);
        runQueryInterSegmentWithOrderBy(orderByQuery, sortedResults, new String[]{"State", "City", "SaleAmount"});

        orderByQuery = "SELECT DISTINCT(State, City, SaleAmount) FROM " + tableName + " ORDER BY State, City, SaleAmount DESC LIMIT 100";
        sortedResults = new ArrayList<>();
        sortedResults.add(new Record(new Object[]{"California", "Mountain View", 700000}));
        sortedResults.add(new Record(new Object[]{"California", "Mountain View", 200000}));
        sortedResults.add(new Record(new Object[]{"California", "San Mateo", 500000}));
        sortedResults.add(new Record(new Object[]{"California", "San Mateo", 400000}));
        sortedResults.add(new Record(new Object[]{"California", "Sunnyvale", 300000}));
        sortedResults.add(new Record(new Object[]{"Oregon", "Portland", 100000}));
        sortedResults.add(new Record(new Object[]{"Oregon", "Portland", 50000}));
        sortedResults.add(new Record(new Object[]{"Washington", "Bellevue", 150000}));
        sortedResults.add(new Record(new Object[]{"Washington", "Bellevue", 100000}));
        sortedResults.add(new Record(new Object[]{"Washington", "Seattle", 100000}));
        runQueryInterSegmentWithOrderBy(orderByQuery, sortedResults, new String[]{"State", "City", "SaleAmount"});

        orderByQuery = "SELECT DISTINCT(State, City, SaleAmount) FROM " + tableName + " ORDER BY State, City, SaleAmount DESC LIMIT 10";
        runQueryInterSegmentWithOrderBy(orderByQuery, sortedResults, new String[]{"State", "City", "SaleAmount"});

        orderByQuery = "SELECT DISTINCT(State, City, SaleAmount) FROM " + tableName + " ORDER BY State, City, SaleAmount DESC LIMIT 5";
        sortedResults = sortedResults.subList(0, 5);
        runQueryInterSegmentWithOrderBy(orderByQuery, sortedResults, new String[]{"State", "City", "SaleAmount"});

        orderByQuery = "SELECT DISTINCT(State, City, SaleAmount) FROM " + tableName + " ORDER BY State DESC, City, SaleAmount DESC LIMIT 100";
        sortedResults = new ArrayList<>();
        sortedResults.add(new Record(new Object[]{"Washington", "Bellevue", 150000}));
        sortedResults.add(new Record(new Object[]{"Washington", "Bellevue", 100000}));
        sortedResults.add(new Record(new Object[]{"Washington", "Seattle", 100000}));
        sortedResults.add(new Record(new Object[]{"Oregon", "Portland", 100000}));
        sortedResults.add(new Record(new Object[]{"Oregon", "Portland", 50000}));
        sortedResults.add(new Record(new Object[]{"California", "Mountain View", 700000}));
        sortedResults.add(new Record(new Object[]{"California", "Mountain View", 200000}));
        sortedResults.add(new Record(new Object[]{"California", "San Mateo", 500000}));
        sortedResults.add(new Record(new Object[]{"California", "San Mateo", 400000}));
        sortedResults.add(new Record(new Object[]{"California", "Sunnyvale", 300000}));
        runQueryInterSegmentWithOrderBy(orderByQuery, sortedResults, new String[]{"State", "City", "SaleAmount"});
      }
    } finally {
      destroySegments();
    }
  }

  /**
   * Helper for testing filter queries
   * @param expectedTable expected result set
   * @param query query to run
   * @param columnNames name of columns
   * @param types data types
   */
  private void runFilterQueryInnerSegment(final Set<Record> expectedTable, final String query, String[] columnNames,
      DataSchema.ColumnDataType[] types) {
    AggregationOperator aggregationOperator = getOperatorForQuery(query);
    IntermediateResultsBlock resultsBlock = aggregationOperator.nextBlock();
    List<Object> operatorResult = resultsBlock.getAggregationResult();

    Assert.assertNotNull(operatorResult);
    Assert.assertEquals(operatorResult.size(), 1);
    Assert.assertTrue(operatorResult.get(0) instanceof DistinctTable);

    DistinctTable distinctTable = (DistinctTable) operatorResult.get(0);
    Assert.assertEquals(distinctTable.size(), expectedTable.size());

    DataSchema dataSchema = distinctTable.getDataSchema();
    Assert.assertEquals(dataSchema.getColumnNames(), columnNames);
    Assert.assertEquals(dataSchema.getColumnDataTypes(), types);

    Iterator<Record> iterator = distinctTable.iterator();
    while (iterator.hasNext()) {
      Record record = iterator.next();
      Assert.assertEquals(record.getValues().length, columnNames.length);
      Assert.assertTrue(expectedTable.contains(record));
    }
  }

  private void runQueryInterSegmentWithOrderBy(String query, List<Record> orderedResults, String[] columnNames) {
    BrokerResponseNative brokerResponseNative = getBrokerResponseForQuery(query);
    final SelectionResults selectionResults = brokerResponseNative.getSelectionResults();
    Assert.assertEquals(selectionResults.getColumns(), Lists.newArrayList(columnNames));
    List<Serializable[]> rows = selectionResults.getRows();
    Assert.assertEquals(rows.size(), orderedResults.size());
    int counter = 0;
    for (Serializable[] row : rows) {
      Assert.assertEquals(row.length, columnNames.length);
      Record actualRecord = new Record(row);
      Assert.assertEquals(actualRecord, orderedResults.get(counter++));
    }
  }

  /**
   * Create a segment with simple table of (State, City, SaleAmount, Time)
   * @param q1ExpectedResults expected results of filter query 1
   * @param q2ExpectedResults expected results of filter query 1
   * @param q3ExpectedResults expected results of filter query 1
   * @param q4ExpectedResults expected results of filter query 1
   * @return list of generic rows
   */
  private List<GenericRow> createSimpleTable(final Set<Record> q1ExpectedResults, final Set<Record> q2ExpectedResults,
      final Set<Record> q3ExpectedResults, final Set<Record> q4ExpectedResults) {
    int numRows = 10;
    List<GenericRow> rows = new ArrayList<>(numRows);
    Object[] columns;

    // ROW 1
    GenericRow row = new GenericRow();
    columns = new Object[]{"California", "San Mateo", 500000};
    row.putField("State", columns[0]);
    row.putField("City", columns[1]);
    row.putField("SaleAmount", columns[2]);
    rows.add(row);

    Record record = new Record(new Object[]{columns[0], columns[1]});
    q1ExpectedResults.add(record);
    q2ExpectedResults.add(record);
    record = new Record(new Object[]{columns[0], columns[1], columns[2]});
    q3ExpectedResults.add(record);
    q4ExpectedResults.add(record);

    // ROW 2
    row = new GenericRow();
    columns = new Object[]{"California", "San Mateo", 400000};
    row.putField("State", columns[0]);
    row.putField("City", columns[1]);
    row.putField("SaleAmount", columns[2]);
    rows.add(row);

    record = new Record(new Object[]{columns[0], columns[1], columns[2]});
    q3ExpectedResults.add(record);
    q4ExpectedResults.add(record);

    // ROW 3
    row = new GenericRow();
    columns = new Object[]{"California", "Sunnyvale", 300000};
    row.putField("State", columns[0]);
    row.putField("City", columns[1]);
    row.putField("SaleAmount", columns[2]);
    rows.add(row);

    record = new Record(new Object[]{columns[0], columns[1]});
    q1ExpectedResults.add(record);
    record = new Record(new Object[]{columns[0], columns[1], columns[2]});
    q3ExpectedResults.add(record);

    // ROW 4
    row = new GenericRow();
    columns = new Object[]{"California", "Sunnyvale", 300000};
    row.putField("State", columns[0]);
    row.putField("City", columns[1]);
    row.putField("SaleAmount", columns[2]);
    rows.add(row);

    // ROW 5
    row = new GenericRow();
    columns = new Object[]{"California", "Mountain View", 700000};
    row.putField("State", columns[0]);
    row.putField("City", columns[1]);
    row.putField("SaleAmount", columns[2]);
    rows.add(row);

    record = new Record(new Object[]{columns[0], columns[1]});
    q1ExpectedResults.add(record);
    q2ExpectedResults.add(record);
    record = new Record(new Object[]{columns[0], columns[1], columns[2]});
    q3ExpectedResults.add(record);
    q4ExpectedResults.add(record);

    // ROW 6
    row = new GenericRow();
    columns = new Object[]{"California", "Mountain View", 700000};
    row.putField("State", columns[0]);
    row.putField("City", columns[1]);
    row.putField("SaleAmount", columns[2]);
    rows.add(row);

    // ROW 7
    row = new GenericRow();
    columns = new Object[]{"California", "Mountain View", 200000};
    row.putField("State", columns[0]);
    row.putField("City", columns[1]);
    row.putField("SaleAmount", columns[2]);
    rows.add(row);

    record = new Record(new Object[]{columns[0], columns[1], columns[2]});
    q3ExpectedResults.add(record);

    // ROW 8
    row = new GenericRow();
    columns = new Object[]{"Washington", "Seattle", 100000};
    row.putField("State", columns[0]);
    row.putField("City", columns[1]);
    row.putField("SaleAmount", columns[2]);
    rows.add(row);

    // ROW 9
    row = new GenericRow();
    columns = new Object[]{"Washington", "Bellevue", 100000};
    row.putField("State", columns[0]);
    row.putField("City", columns[1]);
    row.putField("SaleAmount", columns[2]);
    rows.add(row);

    // ROW 10
    row = new GenericRow();
    columns = new Object[]{"Oregon", "Portland", 50000};
    row.putField("State", columns[0]);
    row.putField("City", columns[1]);
    row.putField("SaleAmount", columns[2]);
    rows.add(row);

    // ROW 11
    row = new GenericRow();
    columns = new Object[]{"Washington", "Bellevue", 150000};
    row.putField("State", columns[0]);
    row.putField("City", columns[1]);
    row.putField("SaleAmount", columns[2]);
    rows.add(row);

    // ROW 12
    row = new GenericRow();
    columns = new Object[]{"Oregon", "Portland", 100000};
    row.putField("State", columns[0]);
    row.putField("City", columns[1]);
    row.putField("SaleAmount", columns[2]);
    rows.add(row);

    return rows;
  }

  private void destroySegments() {
    for (IndexSegment indexSegment : _indexSegments) {
      if (indexSegment != null) {
        indexSegment.destroy();
      }
    }
    _indexSegments.clear();
  }
}