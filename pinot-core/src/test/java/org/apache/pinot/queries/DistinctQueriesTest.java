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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.response.broker.SelectionResults;
import org.apache.pinot.common.segment.ReadMode;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.core.data.manager.SegmentDataManager;
import org.apache.pinot.core.data.manager.offline.ImmutableSegmentDataManager;
import org.apache.pinot.core.data.readers.GenericRowRecordReader;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegment;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.core.operator.query.AggregationOperator;
import org.apache.pinot.core.query.aggregation.DistinctTable;
import org.apache.pinot.core.query.reduce.BrokerReduceService;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Queries test for DISTINCT queries.
 */
public class DistinctQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "DistinctQueryTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME_PREFIX = "testSegment_";

  private static final int NUM_RECORDS_PER_SEGMENT = 10000;
  private static final int NUM_UNIQUE_RECORDS_PER_SEGMENT = 100;

  private static final String INT_COLUMN = "intColumn";
  private static final String LONG_COLUMN = "longColumn";
  private static final String FLOAT_COLUMN = "floatColumn";
  private static final String DOUBLE_COLUMN = "doubleColumn";
  private static final String STRING_COLUMN = "stringColumn";
  private static final String BYTES_COLUMN = "bytesColumn";
  private static final Schema SCHEMA = new Schema.SchemaBuilder().addSingleValueDimension(INT_COLUMN, DataType.INT)
      .addSingleValueDimension(LONG_COLUMN, DataType.LONG).addSingleValueDimension(FLOAT_COLUMN, DataType.FLOAT)
      .addSingleValueDimension(DOUBLE_COLUMN, DataType.DOUBLE).addSingleValueDimension(STRING_COLUMN, DataType.STRING)
      .addSingleValueDimension(BYTES_COLUMN, DataType.BYTES).build();
  private static final TableConfig TABLE = new TableConfigBuilder(TableType.OFFLINE).setTableName("test").build();

  private IndexSegment _indexSegment;
  private List<SegmentDataManager> _segmentDataManagers;

  @Override
  protected String getFilter() {
    throw new UnsupportedOperationException();
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  @Override
  protected List<SegmentDataManager> getSegmentDataManagers() {
    return _segmentDataManagers;
  }

  @BeforeClass
  public void setUp() {
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  /**
   * Helper method to generate records based on the given base value.
   *
   * All columns will have the same value but different data types (BYTES values are encoded STRING values).
   * For the {i}th unique record, the value will be {baseValue + i}.
   */
  private List<GenericRow> generateRecords(int baseValue) {
    List<GenericRow> uniqueRecords = new ArrayList<>(NUM_UNIQUE_RECORDS_PER_SEGMENT);
    for (int i = 0; i < NUM_UNIQUE_RECORDS_PER_SEGMENT; i++) {
      int value = baseValue + i;
      GenericRow record = new GenericRow();
      record.putValue(INT_COLUMN, value);
      record.putValue(LONG_COLUMN, (long) value);
      record.putValue(FLOAT_COLUMN, (float) value);
      record.putValue(DOUBLE_COLUMN, (double) value);
      String stringValue = Integer.toString(value);
      record.putValue(STRING_COLUMN, stringValue);
      byte[] bytesValue = StringUtil.encodeUtf8(stringValue);
      record.putValue(BYTES_COLUMN, bytesValue);
      uniqueRecords.add(record);
    }

    List<GenericRow> records = new ArrayList<>(NUM_RECORDS_PER_SEGMENT);
    for (int i = 0; i < NUM_RECORDS_PER_SEGMENT; i += NUM_UNIQUE_RECORDS_PER_SEGMENT) {
      records.addAll(uniqueRecords);
    }
    return records;
  }

  private ImmutableSegment createSegment(int index, List<GenericRow> records)
      throws Exception {
    String segmentName = SEGMENT_NAME_PREFIX + index;

    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(TABLE, SCHEMA);
    segmentGeneratorConfig.setTableName(RAW_TABLE_NAME);
    segmentGeneratorConfig.setSegmentName(segmentName);
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getAbsolutePath());

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(records));
    driver.build();

    return ImmutableSegmentLoader.load(new File(INDEX_DIR, segmentName), ReadMode.mmap);
  }

  /**
   * Test DISTINCT query within a single segment.
   * <p>The following query types are tested:
   * <ul>
   *   <li>Selecting all columns</li>
   *   <li>Selecting some columns with filter</li>
   *   <li>Selecting some columns order by BYTES column</li>
   *   <li>Selecting some columns transform, filter, order-by and limit</li>
   *   <li>Selecting some columns with filter that does not match any record</li>
   * </ul>
   */
  @Test
  public void testDistinctInnerSegment()
      throws Exception {
    _indexSegment = createSegment(0, generateRecords(0));
    try {
      {
        // Test selecting all columns
        String query =
            "SELECT DISTINCT(intColumn, longColumn, floatColumn, doubleColumn, stringColumn, bytesColumn) FROM testTable LIMIT 10000";

        // Check data schema
        DistinctTable distinctTable = getDistinctTableInnerSegment(query);
        DataSchema dataSchema = distinctTable.getDataSchema();
        assertEquals(dataSchema.getColumnNames(),
            new String[]{"intColumn", "longColumn", "floatColumn", "doubleColumn", "stringColumn", "bytesColumn"});
        assertEquals(dataSchema.getColumnDataTypes(),
            new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.FLOAT, ColumnDataType.DOUBLE, ColumnDataType.STRING, ColumnDataType.BYTES});

        // Check values, where all 100 unique values should be returned
        assertEquals(distinctTable.size(), NUM_UNIQUE_RECORDS_PER_SEGMENT);
        Set<Integer> expectedValues = new HashSet<>();
        for (int i = 0; i < NUM_UNIQUE_RECORDS_PER_SEGMENT; i++) {
          expectedValues.add(i);
        }
        Set<Integer> actualValues = new HashSet<>();
        Iterator<Record> iterator = distinctTable.iterator();
        while (iterator.hasNext()) {
          Record record = iterator.next();
          Object[] values = record.getValues();
          int intValue = (int) values[0];
          assertEquals(((Long) values[1]).intValue(), intValue);
          assertEquals(((Float) values[2]).intValue(), intValue);
          assertEquals(((Double) values[3]).intValue(), intValue);
          assertEquals(Integer.parseInt((String) values[4]), intValue);
          assertEquals(StringUtil.decodeUtf8(((ByteArray) values[5]).getBytes()), values[4]);
          actualValues.add(intValue);
        }
        assertEquals(actualValues, expectedValues);
      }
      {
        // Test selecting some columns with filter
        String query =
            "SELECT DISTINCT(stringColumn, bytesColumn, floatColumn) FROM testTable WHERE intColumn >= 60 LIMIT 10000";

        // Check data schema
        DistinctTable distinctTable = getDistinctTableInnerSegment(query);
        DataSchema dataSchema = distinctTable.getDataSchema();
        assertEquals(dataSchema.getColumnNames(), new String[]{"stringColumn", "bytesColumn", "floatColumn"});
        assertEquals(dataSchema.getColumnDataTypes(),
            new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.BYTES, ColumnDataType.FLOAT});

        // Check values, where 40 matched values should be returned
        assertEquals(distinctTable.size(), NUM_UNIQUE_RECORDS_PER_SEGMENT - 60);
        Set<Integer> expectedValues = new HashSet<>();
        for (int i = 60; i < NUM_UNIQUE_RECORDS_PER_SEGMENT; i++) {
          expectedValues.add(i);
        }
        Set<Integer> actualValues = new HashSet<>();
        Iterator<Record> iterator = distinctTable.iterator();
        while (iterator.hasNext()) {
          Record record = iterator.next();
          Object[] values = record.getValues();
          int intValue = Integer.parseInt((String) values[0]);
          assertEquals(StringUtil.decodeUtf8(((ByteArray) values[1]).getBytes()), values[0]);
          assertEquals(((Float) values[2]).intValue(), intValue);
          actualValues.add(intValue);
        }
        assertEquals(actualValues, expectedValues);
      }
      {
        // Test selecting some columns order by BYTES column
        String query = "SELECT DISTINCT(intColumn, bytesColumn) FROM testTable ORDER BY bytesColumn LIMIT 5";

        // Check data schema
        DistinctTable distinctTable = getDistinctTableInnerSegment(query);
        DataSchema dataSchema = distinctTable.getDataSchema();
        assertEquals(dataSchema.getColumnNames(), new String[]{"intColumn", "bytesColumn"});
        assertEquals(dataSchema.getColumnDataTypes(), new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.BYTES});

        // Check values, where all 100 unique values should be returned (limit won't take effect on server side)
        // TODO: After optimizing the DistinctTable (only keep the limit number of records), only 5 values should be
        //       returned
        assertEquals(distinctTable.size(), NUM_UNIQUE_RECORDS_PER_SEGMENT);
        Set<Integer> expectedValues = new HashSet<>();
        for (int i = 0; i < NUM_UNIQUE_RECORDS_PER_SEGMENT; i++) {
          expectedValues.add(i);
        }
        Set<Integer> actualValues = new HashSet<>();
        Iterator<Record> iterator = distinctTable.iterator();
        while (iterator.hasNext()) {
          Record record = iterator.next();
          Object[] values = record.getValues();
          int intValue = (int) values[0];
          assertEquals(StringUtil.decodeUtf8(((ByteArray) values[1]).getBytes()), Integer.toString(intValue));
          actualValues.add(intValue);
        }
        assertEquals(actualValues, expectedValues);
      }
      {
        // Test selecting some columns with transform, filter, order-by and limit. Spaces in 'add' are intentional
        // to ensure that AggregationFunction arguments are standardized (to remove spaces).
        String query =
            "SELECT DISTINCT(ADD ( intColumn,  floatColumn  ), stringColumn) FROM testTable WHERE longColumn < 60 ORDER BY stringColumn DESC, ADD(intColumn, floatColumn) ASC LIMIT 10";

        // Check data schema
        DistinctTable distinctTable = getDistinctTableInnerSegment(query);
        DataSchema dataSchema = distinctTable.getDataSchema();
        assertEquals(dataSchema.getColumnNames(), new String[]{"add(intColumn,floatColumn)", "stringColumn"});
        assertEquals(dataSchema.getColumnDataTypes(),
            new ColumnDataType[]{ColumnDataType.DOUBLE, ColumnDataType.STRING});

        // Check values, where 60 matched values should be returned (limit won't take effect on server side)
        // TODO: After optimizing the DistinctTable (only keep the limit number of records), only 10 values should be
        //       returned
        assertEquals(distinctTable.size(), 60);
        Set<Integer> expectedValues = new HashSet<>();
        for (int i = 0; i < 60; i++) {
          expectedValues.add(i);
        }
        Set<Integer> actualValues = new HashSet<>();
        Iterator<Record> iterator = distinctTable.iterator();
        while (iterator.hasNext()) {
          Record record = iterator.next();
          Object[] values = record.getValues();
          int intValue = ((Double) values[0]).intValue() / 2;
          assertEquals(Integer.parseInt((String) values[1]), intValue);
          actualValues.add(intValue);
        }
        assertEquals(actualValues, expectedValues);
      }
      {
        // Test selecting some columns with filter that does not match any record
        String query =
            "SELECT DISTINCT(floatColumn, longColumn) FROM testTable WHERE stringColumn = 'a' ORDER BY longColumn LIMIT 10";

        // Check data schema, where data type should be STRING for all columns
        DistinctTable distinctTable = getDistinctTableInnerSegment(query);
        DataSchema dataSchema = distinctTable.getDataSchema();
        assertEquals(dataSchema.getColumnNames(), new String[]{"floatColumn", "longColumn"});
        assertEquals(dataSchema.getColumnDataTypes(),
            new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.STRING});

        // Check values, where no record should be returned
        assertEquals(distinctTable.size(), 0);
      }
    } finally {
      _indexSegment.destroy();
    }
  }

  /**
   * Helper method to get the DistinctTable result for one single segment for the given query.
   */
  private DistinctTable getDistinctTableInnerSegment(String query) {
    AggregationOperator aggregationOperator = getOperatorForQuery(query);
    List<Object> aggregationResult = aggregationOperator.nextBlock().getAggregationResult();
    assertNotNull(aggregationResult);
    assertEquals(aggregationResult.size(), 1);
    assertTrue(aggregationResult.get(0) instanceof DistinctTable);
    return (DistinctTable) aggregationResult.get(0);
  }

  /**
   * Test DISTINCT query across multiple segments and servers (2 servers, each with 2 segments).
   * <p>Both PQL and SQL format are tested.
   * <p>The following query types are tested:
   * <ul>
   *   <li>Selecting all columns</li>
   *   <li>Selecting some columns with filter</li>
   *   <li>Selecting some columns order by BYTES column</li>
   *   <li>Selecting some columns transform, filter, order-by and limit</li>
   *   <li>Selecting some columns with filter that does not match any record</li>
   *   <li>
   *     Selecting some columns with filter that does not match any record in one segment but matches some records in
   *     the other segment
   *   </li>
   *   <li>
   *     Selecting some columns with filter that does not match any record in one server but matches some records in the
   *     other server
   *   </li>
   * </ul>
   */
  @Test
  public void testDistinctInterSegment()
      throws Exception {
    ImmutableSegment segment0 = createSegment(0, generateRecords(0));
    ImmutableSegment segment1 = createSegment(1, generateRecords(1000));
    _segmentDataManagers =
        Arrays.asList(new ImmutableSegmentDataManager(segment0), new ImmutableSegmentDataManager(segment1));
    try {
      {
        // Test selecting all columns
        String pqlQuery =
            "SELECT DISTINCT(intColumn, longColumn, floatColumn, doubleColumn, stringColumn, bytesColumn) FROM testTable LIMIT 10000";
        String sqlQuery =
            "SELECT DISTINCT intColumn, longColumn, floatColumn, doubleColumn, stringColumn, bytesColumn FROM testTable LIMIT 10000";

        // Check data schema
        BrokerResponseNative pqlResponse = getBrokerResponseForPqlQuery(pqlQuery);
        SelectionResults selectionResults = pqlResponse.getSelectionResults();
        assertNotNull(selectionResults);
        assertEquals(selectionResults.getColumns(),
            Arrays.asList("intColumn", "longColumn", "floatColumn", "doubleColumn", "stringColumn", "bytesColumn"));
        BrokerResponseNative sqlResponse = getBrokerResponseForSqlQuery(sqlQuery);
        ResultTable resultTable = sqlResponse.getResultTable();
        assertNotNull(resultTable);
        DataSchema dataSchema = resultTable.getDataSchema();
        assertEquals(dataSchema.getColumnNames(),
            new String[]{"intColumn", "longColumn", "floatColumn", "doubleColumn", "stringColumn", "bytesColumn"});
        assertEquals(dataSchema.getColumnDataTypes(),
            new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.FLOAT, ColumnDataType.DOUBLE, ColumnDataType.STRING, ColumnDataType.BYTES});

        // Check values, where all 200 unique values should be returned
        List<Serializable[]> pqlRows = selectionResults.getRows();
        assertEquals(pqlRows.size(), 2 * NUM_UNIQUE_RECORDS_PER_SEGMENT);
        List<Object[]> sqlRows = resultTable.getRows();
        assertEquals(sqlRows.size(), 2 * NUM_UNIQUE_RECORDS_PER_SEGMENT);
        Set<Integer> expectedValues = new HashSet<>();
        for (int i = 0; i < NUM_UNIQUE_RECORDS_PER_SEGMENT; i++) {
          expectedValues.add(i);
          expectedValues.add(1000 + i);
        }
        Set<Integer> pqlValues = new HashSet<>();
        for (Serializable[] row : pqlRows) {
          int intValue = (int) row[0];
          assertEquals(((Long) row[1]).intValue(), intValue);
          assertEquals(((Float) row[2]).intValue(), intValue);
          assertEquals(((Double) row[3]).intValue(), intValue);
          assertEquals(Integer.parseInt((String) row[4]), intValue);
          assertEquals(StringUtil.decodeUtf8(BytesUtils.toBytes((String) row[5])), row[4]);
          pqlValues.add(intValue);
        }
        assertEquals(pqlValues, expectedValues);
        Set<Integer> sqlValues = new HashSet<>();
        for (Object[] row : sqlRows) {
          int intValue = (int) row[0];
          assertEquals(((Long) row[1]).intValue(), intValue);
          assertEquals(((Float) row[2]).intValue(), intValue);
          assertEquals(((Double) row[3]).intValue(), intValue);
          assertEquals(Integer.parseInt((String) row[4]), intValue);
          assertEquals(StringUtil.decodeUtf8(BytesUtils.toBytes((String) row[5])), row[4]);
          sqlValues.add(intValue);
        }
        assertEquals(sqlValues, expectedValues);
      }
      {
        // Test selecting some columns with filter
        String pqlQuery =
            "SELECT DISTINCT(stringColumn, bytesColumn, floatColumn) FROM testTable WHERE intColumn >= 60 LIMIT 10000";
        String sqlQuery =
            "SELECT DISTINCT stringColumn, bytesColumn, floatColumn FROM testTable WHERE intColumn >= 60 LIMIT 10000";

        // Check data schema
        BrokerResponseNative pqlResponse = getBrokerResponseForPqlQuery(pqlQuery);
        SelectionResults selectionResults = pqlResponse.getSelectionResults();
        assertNotNull(selectionResults);
        assertEquals(selectionResults.getColumns(), Arrays.asList("stringColumn", "bytesColumn", "floatColumn"));
        BrokerResponseNative sqlResponse = getBrokerResponseForSqlQuery(sqlQuery);
        ResultTable resultTable = sqlResponse.getResultTable();
        assertNotNull(resultTable);
        DataSchema dataSchema = resultTable.getDataSchema();
        assertEquals(dataSchema.getColumnNames(), new String[]{"stringColumn", "bytesColumn", "floatColumn"});
        assertEquals(dataSchema.getColumnDataTypes(),
            new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.BYTES, ColumnDataType.FLOAT});

        // Check values, where 140 matched values should be returned
        List<Serializable[]> pqlRows = selectionResults.getRows();
        assertEquals(pqlRows.size(), 2 * NUM_UNIQUE_RECORDS_PER_SEGMENT - 60);
        List<Object[]> sqlRows = resultTable.getRows();
        assertEquals(sqlRows.size(), 2 * NUM_UNIQUE_RECORDS_PER_SEGMENT - 60);
        Set<Integer> expectedValues = new HashSet<>();
        for (int i = 0; i < NUM_UNIQUE_RECORDS_PER_SEGMENT; i++) {
          if (i >= 60) {
            expectedValues.add(i);
          }
          expectedValues.add(1000 + i);
        }
        Set<Integer> pqlValues = new HashSet<>();
        for (Serializable[] row : pqlRows) {
          int intValue = Integer.parseInt((String) row[0]);
          assertEquals(StringUtil.decodeUtf8(BytesUtils.toBytes((String) row[1])), row[0]);
          assertEquals(((Float) row[2]).intValue(), intValue);
          pqlValues.add(intValue);
        }
        assertEquals(pqlValues, expectedValues);
        Set<Integer> sqlValues = new HashSet<>();
        for (Object[] row : sqlRows) {
          int intValue = Integer.parseInt((String) row[0]);
          assertEquals(StringUtil.decodeUtf8(BytesUtils.toBytes((String) row[1])), row[0]);
          assertEquals(((Float) row[2]).intValue(), intValue);
          sqlValues.add(intValue);
        }
        assertEquals(sqlValues, expectedValues);
      }
      {
        // Test selecting some columns order by BYTES column
        String pqlQuery = "SELECT DISTINCT(intColumn, bytesColumn) FROM testTable ORDER BY bytesColumn LIMIT 5";
        String sqlQuery = "SELECT DISTINCT intColumn, bytesColumn FROM testTable ORDER BY bytesColumn LIMIT 5";

        // Check data schema
        BrokerResponseNative pqlResponse = getBrokerResponseForPqlQuery(pqlQuery);
        SelectionResults selectionResults = pqlResponse.getSelectionResults();
        assertNotNull(selectionResults);
        assertEquals(selectionResults.getColumns(), Arrays.asList("intColumn", "bytesColumn"));
        BrokerResponseNative sqlResponse = getBrokerResponseForSqlQuery(sqlQuery);
        ResultTable resultTable = sqlResponse.getResultTable();
        assertNotNull(resultTable);
        DataSchema dataSchema = resultTable.getDataSchema();
        assertEquals(dataSchema.getColumnNames(), new String[]{"intColumn", "bytesColumn"});
        assertEquals(dataSchema.getColumnDataTypes(), new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.BYTES});

        // Check values, where only 5 top values sorted in ByteArray format ascending order should be returned
        List<Serializable[]> pqlRows = selectionResults.getRows();
        assertEquals(pqlRows.size(), 5);
        List<Object[]> sqlRows = resultTable.getRows();
        assertEquals(sqlRows.size(), 5);
        // ByteArray of "30", "31", "3130", "31303030", "31303031" (same as String order because all digits can be
        // encoded with a single byte)
        int[] expectedValues = new int[]{0, 1, 10, 1000, 1001};
        for (int i = 0; i < 5; i++) {
          Serializable[] row = pqlRows.get(i);
          int intValue = (int) row[0];
          assertEquals(intValue, expectedValues[i]);
          assertEquals(Integer.parseInt(StringUtil.decodeUtf8(BytesUtils.toBytes((String) row[1]))), intValue);
        }
        for (int i = 0; i < 5; i++) {
          Object[] row = sqlRows.get(i);
          int intValue = (int) row[0];
          assertEquals(intValue, expectedValues[i]);
          assertEquals(Integer.parseInt(StringUtil.decodeUtf8(BytesUtils.toBytes((String) row[1]))), intValue);
        }
      }
      {
        // Test selecting some columns with transform, filter, order-by and limit
        String pqlQuery =
            "SELECT DISTINCT(ADD(intColumn, floatColumn), stringColumn) FROM testTable WHERE longColumn < 60 ORDER BY stringColumn DESC, ADD(intColumn, floatColumn) ASC LIMIT 10";
        String sqlQuery =
            "SELECT DISTINCT ADD(intColumn, floatColumn), stringColumn FROM testTable WHERE longColumn < 60 ORDER BY stringColumn DESC, ADD(intColumn, floatColumn) ASC LIMIT 10";

        // Check data schema
        BrokerResponseNative pqlResponse = getBrokerResponseForPqlQuery(pqlQuery);
        SelectionResults selectionResults = pqlResponse.getSelectionResults();
        assertNotNull(selectionResults);
        assertEquals(selectionResults.getColumns(), Arrays.asList("add(intColumn,floatColumn)", "stringColumn"));
        BrokerResponseNative sqlResponse = getBrokerResponseForSqlQuery(sqlQuery);
        ResultTable resultTable = sqlResponse.getResultTable();
        assertNotNull(resultTable);
        DataSchema dataSchema = resultTable.getDataSchema();
        assertEquals(dataSchema.getColumnNames(), new String[]{"add(intColumn,floatColumn)", "stringColumn"});
        assertEquals(dataSchema.getColumnDataTypes(),
            new ColumnDataType[]{ColumnDataType.DOUBLE, ColumnDataType.STRING});

        // Check values, where only 10 top values sorted in string format descending order should be returned
        List<Serializable[]> pqlRows = selectionResults.getRows();
        assertEquals(pqlRows.size(), 10);
        List<Object[]> sqlRows = resultTable.getRows();
        assertEquals(sqlRows.size(), 10);
        int[] expectedValues = new int[]{9, 8, 7, 6, 59, 58, 57, 56, 55, 54};
        for (int i = 0; i < 10; i++) {
          Serializable[] row = pqlRows.get(i);
          int intValue = ((Double) row[0]).intValue() / 2;
          assertEquals(intValue, expectedValues[i]);
          assertEquals(Integer.parseInt((String) row[1]), intValue);
        }
        for (int i = 0; i < 10; i++) {
          Object[] row = sqlRows.get(i);
          int intValue = ((Double) row[0]).intValue() / 2;
          assertEquals(intValue, expectedValues[i]);
          assertEquals(Integer.parseInt((String) row[1]), intValue);
        }
      }
      {
        // Test selecting some columns with filter that does not match any record
        String pqlQuery =
            "SELECT DISTINCT(floatColumn, longColumn) FROM testTable WHERE stringColumn = 'a' ORDER BY longColumn LIMIT 10";
        String sqlQuery =
            "SELECT DISTINCT floatColumn, longColumn FROM testTable WHERE stringColumn = 'a' ORDER BY longColumn LIMIT 10";

        // Check data schema, where data type should be STRING for all columns
        BrokerResponseNative pqlResponse = getBrokerResponseForPqlQuery(pqlQuery);
        SelectionResults selectionResults = pqlResponse.getSelectionResults();
        assertNotNull(selectionResults);
        assertEquals(selectionResults.getColumns(), Arrays.asList("floatColumn", "longColumn"));
        BrokerResponseNative sqlResponse = getBrokerResponseForSqlQuery(sqlQuery);
        ResultTable resultTable = sqlResponse.getResultTable();
        assertNotNull(resultTable);
        DataSchema dataSchema = resultTable.getDataSchema();
        assertEquals(dataSchema.getColumnNames(), new String[]{"floatColumn", "longColumn"});
        assertEquals(dataSchema.getColumnDataTypes(),
            new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.STRING});

        // Check values, where no record should be returned
        List<Serializable[]> pqlRows = selectionResults.getRows();
        assertTrue(pqlRows.isEmpty());
        List<Object[]> sqlRows = resultTable.getRows();
        assertTrue(sqlRows.isEmpty());
      }
      {
        // Test selecting some columns with filter that does not match any record in one segment but matches some
        // records in the other segment
        String pqlQuery =
            "SELECT DISTINCT(intColumn) FROM testTable WHERE floatColumn > 200 ORDER BY intColumn ASC LIMIT 5";
        String sqlQuery =
            "SELECT DISTINCT intColumn FROM testTable WHERE floatColumn > 200 ORDER BY intColumn ASC LIMIT 5";

        // Check data schema
        BrokerResponseNative pqlResponse = getBrokerResponseForPqlQuery(pqlQuery);
        SelectionResults selectionResults = pqlResponse.getSelectionResults();
        assertNotNull(selectionResults);
        assertEquals(selectionResults.getColumns(), Collections.singletonList("intColumn"));
        BrokerResponseNative sqlResponse = getBrokerResponseForSqlQuery(sqlQuery);
        ResultTable resultTable = sqlResponse.getResultTable();
        assertNotNull(resultTable);
        DataSchema dataSchema = resultTable.getDataSchema();
        assertEquals(dataSchema.getColumnNames(), new String[]{"intColumn"});
        assertEquals(dataSchema.getColumnDataTypes(), new ColumnDataType[]{ColumnDataType.INT});

        // Check values, where only 5 top values sorted in int format ascending order should be returned
        List<Serializable[]> pqlRows = selectionResults.getRows();
        assertEquals(pqlRows.size(), 5);
        List<Object[]> sqlRows = resultTable.getRows();
        assertEquals(sqlRows.size(), 5);
        int[] expectedValues = new int[]{1000, 1001, 1002, 1003, 1004};
        for (int i = 0; i < 5; i++) {
          Serializable[] row = pqlRows.get(i);
          assertEquals((int) row[0], expectedValues[i]);
        }
        for (int i = 0; i < 5; i++) {
          Object[] row = sqlRows.get(i);
          assertEquals((int) row[0], expectedValues[i]);
        }
      }
      {
        // Test electing some columns with filter that does not match any record in one server but matches some records
        // in the other server
        String pqlQuery =
            "SELECT DISTINCT(longColumn) FROM testTable WHERE doubleColumn < 200 ORDER BY longColumn DESC LIMIT 5";
        String sqlQuery =
            "SELECT DISTINCT longColumn FROM testTable WHERE doubleColumn < 200 ORDER BY longColumn DESC LIMIT 5";

        BrokerRequest pqlBrokerRequest = PQL_COMPILER.compileToBrokerRequest(pqlQuery);
        BrokerResponseNative pqlResponse = queryServersWithDifferentSegments(pqlBrokerRequest, segment0, segment1);
        BrokerRequest sqlBrokerRequest = SQL_COMPILER.compileToBrokerRequest(sqlQuery);
        sqlBrokerRequest.setQueryOptions(Collections.singletonMap("responseFormat", "sql"));
        BrokerResponseNative sqlResponse = queryServersWithDifferentSegments(sqlBrokerRequest, segment0, segment1);

        // Check data schema
        SelectionResults selectionResults = pqlResponse.getSelectionResults();
        assertNotNull(selectionResults);
        assertEquals(selectionResults.getColumns(), Collections.singletonList("longColumn"));
        ResultTable resultTable = sqlResponse.getResultTable();
        assertNotNull(resultTable);
        DataSchema dataSchema = resultTable.getDataSchema();
        assertEquals(dataSchema.getColumnNames(), new String[]{"longColumn"});
        assertEquals(dataSchema.getColumnDataTypes(), new ColumnDataType[]{ColumnDataType.LONG});

        // Check values, where only 5 top values sorted in long format descending order should be returned
        List<Serializable[]> pqlRows = selectionResults.getRows();
        assertEquals(pqlRows.size(), 5);
        List<Object[]> sqlRows = resultTable.getRows();
        assertEquals(sqlRows.size(), 5);
        int[] expectedValues = new int[]{99, 98, 97, 96, 95};
        for (int i = 0; i < 5; i++) {
          Serializable[] row = pqlRows.get(i);
          assertEquals(((Long) row[0]).intValue(), expectedValues[i]);
        }
        for (int i = 0; i < 5; i++) {
          Object[] row = sqlRows.get(i);
          assertEquals(((Long) row[0]).intValue(), expectedValues[i]);
        }
      }
    } finally {
      for (SegmentDataManager segmentDataManager : _segmentDataManagers) {
        segmentDataManager.destroy();
      }
    }
  }

  /**
   * Helper method to query 2 servers with different segments. Server0 will have 2 copies of segment0; Server1 will have
   * 2 copies of segment1.
   */
  private BrokerResponseNative queryServersWithDifferentSegments(BrokerRequest brokerRequest, ImmutableSegment segment0,
      ImmutableSegment segment1) {
    List<SegmentDataManager> segmentDataManagers0 =
        Arrays.asList(new ImmutableSegmentDataManager(segment0), new ImmutableSegmentDataManager(segment0));
    List<SegmentDataManager> segmentDataManagers1 =
        Arrays.asList(new ImmutableSegmentDataManager(segment1), new ImmutableSegmentDataManager(segment1));

    // Server side
    DataTable instanceResponse0 = PLAN_MAKER.makeInterSegmentPlan(segmentDataManagers0, brokerRequest, EXECUTOR_SERVICE,
        CommonConstants.Server.DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS).execute();
    DataTable instanceResponse1 = PLAN_MAKER.makeInterSegmentPlan(segmentDataManagers1, brokerRequest, EXECUTOR_SERVICE,
        CommonConstants.Server.DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS).execute();

    // Broker side
    BrokerReduceService brokerReduceService = new BrokerReduceService();
    Map<ServerRoutingInstance, DataTable> dataTableMap = new HashMap<>();
    dataTableMap.put(new ServerRoutingInstance("localhost", 1234, TableType.OFFLINE), instanceResponse0);
    dataTableMap.put(new ServerRoutingInstance("localhost", 1234, TableType.REALTIME), instanceResponse1);
    return brokerReduceService.reduceOnDataTable(brokerRequest, dataTableMap, null);
  }
}
