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
import java.nio.ByteBuffer;
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
import org.apache.commons.lang.StringUtils;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.SelectionResults;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.query.distinct.DistinctTable;
import org.apache.pinot.core.query.reduce.BrokerReduceService;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DataSchema;
import org.apache.pinot.spi.data.DataSchema.ColumnDataType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.ResultTable;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
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
  private static final String RAW_INT_COLUMN = "rawIntColumn";
  private static final String RAW_LONG_COLUMN = "rawLongColumn";
  private static final String RAW_FLOAT_COLUMN = "rawFloatColumn";
  private static final String RAW_DOUBLE_COLUMN = "rawDoubleColumn";
  private static final String RAW_STRING_COLUMN = "rawStringColumn";
  private static final String RAW_BYTES_COLUMN = "rawBytesColumn";
  private static final Schema SCHEMA = new Schema.SchemaBuilder().addSingleValueDimension(INT_COLUMN, DataType.INT)
      .addSingleValueDimension(LONG_COLUMN, DataType.LONG).addSingleValueDimension(FLOAT_COLUMN, DataType.FLOAT)
      .addSingleValueDimension(DOUBLE_COLUMN, DataType.DOUBLE).addSingleValueDimension(STRING_COLUMN, DataType.STRING)
      .addSingleValueDimension(BYTES_COLUMN, DataType.BYTES).addSingleValueDimension(RAW_INT_COLUMN, DataType.INT)
      .addSingleValueDimension(RAW_LONG_COLUMN, DataType.LONG).addSingleValueDimension(RAW_FLOAT_COLUMN, DataType.FLOAT)
      .addSingleValueDimension(RAW_DOUBLE_COLUMN, DataType.DOUBLE)
      .addSingleValueDimension(RAW_STRING_COLUMN, DataType.STRING)
      .addSingleValueDimension(RAW_BYTES_COLUMN, DataType.BYTES).build();
  private static final TableConfig TABLE = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
      .setNoDictionaryColumns(Arrays
          .asList(RAW_INT_COLUMN, RAW_LONG_COLUMN, RAW_FLOAT_COLUMN, RAW_DOUBLE_COLUMN, RAW_STRING_COLUMN,
              RAW_BYTES_COLUMN)).build();

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

    ImmutableSegment segment0 = createSegment(0, generateRecords(0));
    ImmutableSegment segment1 = createSegment(1, generateRecords(1000));
    _indexSegment = segment0;
    _indexSegments = Arrays.asList(segment0, segment1);
  }

  @AfterClass
  public void tearDown() {
    for (IndexSegment indexSegment : _indexSegments) {
      indexSegment.destroy();
    }

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
      record.putValue(BYTES_COLUMN, StringUtils.leftPad(stringValue, 4).getBytes(UTF_8));
      record.putValue(RAW_INT_COLUMN, value);
      record.putValue(RAW_LONG_COLUMN, (long) value);
      record.putValue(RAW_FLOAT_COLUMN, (float) value);
      record.putValue(RAW_DOUBLE_COLUMN, (double) value);
      record.putValue(RAW_STRING_COLUMN, stringValue);
      record.putValue(RAW_BYTES_COLUMN, stringValue.getBytes(UTF_8));
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

  @Test
  public void testSingleColumnDistinctOnlyInnerSegment()
      throws Exception {
    {
      // Numeric columns
      //@formatter:off
      List<String> queries = Arrays
          .asList("SELECT DISTINCT(intColumn) FROM testTable", "SELECT DISTINCT(longColumn) FROM testTable",
              "SELECT DISTINCT(floatColumn) FROM testTable", "SELECT DISTINCT(doubleColumn) FROM testTable",
              "SELECT DISTINCT(rawIntColumn) FROM testTable", "SELECT DISTINCT(rawLongColumn) FROM testTable",
              "SELECT DISTINCT(rawFloatColumn) FROM testTable", "SELECT DISTINCT(rawDoubleColumn) FROM testTable",
              "SELECT DISTINCT(intColumn) FROM testTable ORDER BY intColumn",
              "SELECT DISTINCT(longColumn) FROM testTable ORDER BY longColumn",
              "SELECT DISTINCT(floatColumn) FROM testTable ORDER BY floatColumn",
              "SELECT DISTINCT(doubleColumn) FROM testTable ORDER BY doubleColumn",
              "SELECT DISTINCT(rawIntColumn) FROM testTable ORDER BY rawIntColumn",
              "SELECT DISTINCT(rawLongColumn) FROM testTable ORDER BY rawLongColumn",
              "SELECT DISTINCT(rawFloatColumn) FROM testTable ORDER BY rawFloatColumn",
              "SELECT DISTINCT(rawDoubleColumn) FROM testTable ORDER BY rawDoubleColumn");
      //@formatter:on
      Set<Integer> expectedValues = new HashSet<>();
      for (int i = 0; i < 10; i++) {
        expectedValues.add(i);
      }
      for (String query : queries) {
        DistinctTable pqlDistinctTable = getDistinctTableInnerSegment(query, true);
        DistinctTable pqlDistinctTable2 = DistinctTable.fromByteBuffer(ByteBuffer.wrap(pqlDistinctTable.toBytes()));
        DistinctTable sqlDistinctTable = getDistinctTableInnerSegment(query, false);
        DistinctTable sqlDistinctTable2 = DistinctTable.fromByteBuffer(ByteBuffer.wrap(sqlDistinctTable.toBytes()));
        for (DistinctTable distinctTable : Arrays
            .asList(pqlDistinctTable, pqlDistinctTable2, sqlDistinctTable, sqlDistinctTable2)) {
          assertEquals(distinctTable.size(), 10);
          Set<Integer> actualValues = new HashSet<>();
          for (Record record : distinctTable.getRecords()) {
            Object[] values = record.getValues();
            assertEquals(values.length, 1);
            assertTrue(values[0] instanceof Number);
            actualValues.add(((Number) values[0]).intValue());
          }
          assertEquals(actualValues, expectedValues);
        }
      }
    }
    {
      // String column
      String query = "SELECT DISTINCT(stringColumn) FROM testTable";
      // We define a specific result set here since the data read from dictionary is in alphabetically sorted order
      Set<Integer> expectedValues = new HashSet<>(Arrays.asList(0, 1, 10, 11, 12, 13, 14, 15, 16, 17));
      DistinctTable pqlDistinctTable = getDistinctTableInnerSegment(query, true);
      DistinctTable pqlDistinctTable2 = DistinctTable.fromByteBuffer(ByteBuffer.wrap(pqlDistinctTable.toBytes()));
      DistinctTable sqlDistinctTable = getDistinctTableInnerSegment(query, false);
      DistinctTable sqlDistinctTable2 = DistinctTable.fromByteBuffer(ByteBuffer.wrap(sqlDistinctTable.toBytes()));
      for (DistinctTable distinctTable : Arrays
          .asList(pqlDistinctTable, pqlDistinctTable2, sqlDistinctTable, sqlDistinctTable2)) {
        assertEquals(distinctTable.size(), 10);
        Set<Integer> actualValues = new HashSet<>();
        for (Record record : distinctTable.getRecords()) {
          Object[] values = record.getValues();
          assertEquals(values.length, 1);
          assertTrue(values[0] instanceof String);
          actualValues.add(Integer.parseInt((String) values[0]));
        }
        assertEquals(actualValues, expectedValues);
      }
    }
    {
      // Raw String column
      String query = "SELECT DISTINCT(rawStringColumn) FROM testTable";
      Set<Integer> expectedValues = new HashSet<>();
      for (int i = 0; i < 10; i++) {
        expectedValues.add(i);
      }

      DistinctTable pqlDistinctTable = getDistinctTableInnerSegment(query, true);
      DistinctTable pqlDistinctTable2 = DistinctTable.fromByteBuffer(ByteBuffer.wrap(pqlDistinctTable.toBytes()));
      DistinctTable sqlDistinctTable = getDistinctTableInnerSegment(query, false);
      DistinctTable sqlDistinctTable2 = DistinctTable.fromByteBuffer(ByteBuffer.wrap(sqlDistinctTable.toBytes()));
      for (DistinctTable distinctTable : Arrays
          .asList(pqlDistinctTable, pqlDistinctTable2, sqlDistinctTable, sqlDistinctTable2)) {
        assertEquals(distinctTable.size(), 10);
        Set<Integer> actualValues = new HashSet<>();
        for (Record record : distinctTable.getRecords()) {
          Object[] values = record.getValues();
          assertEquals(values.length, 1);
          assertTrue(values[0] instanceof String);
          actualValues.add(Integer.parseInt((String) values[0]));
        }
        assertEquals(actualValues, expectedValues);
      }
    }
    {
      // Bytes columns
      //@formatter:off
      List<String> queries = Arrays
          .asList("SELECT DISTINCT(bytesColumn) FROM testTable", "SELECT DISTINCT(rawBytesColumn) FROM testTable");
      //@formatter:on
      Set<Integer> expectedValues = new HashSet<>();
      for (int i = 0; i < 10; i++) {
        expectedValues.add(i);
      }
      for (String query : queries) {
        DistinctTable pqlDistinctTable = getDistinctTableInnerSegment(query, true);
        DistinctTable pqlDistinctTable2 = DistinctTable.fromByteBuffer(ByteBuffer.wrap(pqlDistinctTable.toBytes()));
        DistinctTable sqlDistinctTable = getDistinctTableInnerSegment(query, false);
        DistinctTable sqlDistinctTable2 = DistinctTable.fromByteBuffer(ByteBuffer.wrap(sqlDistinctTable.toBytes()));
        for (DistinctTable distinctTable : Arrays
            .asList(pqlDistinctTable, pqlDistinctTable2, sqlDistinctTable, sqlDistinctTable2)) {
          assertEquals(distinctTable.size(), 10);
          Set<Integer> actualValues = new HashSet<>();
          for (Record record : distinctTable.getRecords()) {
            Object[] values = record.getValues();
            assertEquals(values.length, 1);
            assertTrue(values[0] instanceof ByteArray);
            actualValues.add(Integer.parseInt(
                new String(((ByteArray) values[0]).getBytes(), UTF_8).trim()));
          }
          assertEquals(actualValues, expectedValues);
        }
      }
    }
  }

  @Test
  public void testSingleColumnDistinctOrderByInnerSegment()
      throws Exception {
    {
      // Numeric columns
      //@formatter:off
      List<String> queries = Arrays.asList("SELECT DISTINCT(intColumn) FROM testTable ORDER BY intColumn DESC",
          "SELECT DISTINCT(longColumn) FROM testTable ORDER BY longColumn DESC",
          "SELECT DISTINCT(floatColumn) FROM testTable ORDER BY floatColumn DESC",
          "SELECT DISTINCT(doubleColumn) FROM testTable ORDER BY doubleColumn DESC",
          "SELECT DISTINCT(rawIntColumn) FROM testTable ORDER BY rawIntColumn DESC",
          "SELECT DISTINCT(rawLongColumn) FROM testTable ORDER BY rawLongColumn DESC",
          "SELECT DISTINCT(rawFloatColumn) FROM testTable ORDER BY rawFloatColumn DESC",
          "SELECT DISTINCT(rawDoubleColumn) FROM testTable ORDER BY rawDoubleColumn DESC");
      //@formatter:on
      Set<Integer> expectedValues = new HashSet<>();
      for (int i = NUM_UNIQUE_RECORDS_PER_SEGMENT - 10; i < NUM_UNIQUE_RECORDS_PER_SEGMENT; i++) {
        expectedValues.add(i);
      }
      for (String query : queries) {
        DistinctTable pqlDistinctTable = getDistinctTableInnerSegment(query, true);
        DistinctTable pqlDistinctTable2 = DistinctTable.fromByteBuffer(ByteBuffer.wrap(pqlDistinctTable.toBytes()));
        DistinctTable sqlDistinctTable = getDistinctTableInnerSegment(query, false);
        DistinctTable sqlDistinctTable2 = DistinctTable.fromByteBuffer(ByteBuffer.wrap(sqlDistinctTable.toBytes()));
        for (DistinctTable distinctTable : Arrays
            .asList(pqlDistinctTable, pqlDistinctTable2, sqlDistinctTable, sqlDistinctTable2)) {
          assertEquals(distinctTable.size(), 10);
          Set<Integer> actualValues = new HashSet<>();
          for (Record record : distinctTable.getRecords()) {
            Object[] values = record.getValues();
            assertEquals(values.length, 1);
            assertTrue(values[0] instanceof Number);
            actualValues.add(((Number) values[0]).intValue());
          }
          assertEquals(actualValues, expectedValues);
        }
      }
    }
    {
      // String columns
      //@formatter:off
      List<String> queries = Arrays.asList("SELECT DISTINCT(stringColumn) FROM testTable ORDER BY stringColumn",
          "SELECT DISTINCT(rawStringColumn) FROM testTable ORDER BY rawStringColumn");
      //@formatter:on
      Set<String> expectedValues =
          new HashSet<>(Arrays.asList("0", "1", "10", "11", "12", "13", "14", "15", "16", "17"));
      for (String query : queries) {
        DistinctTable pqlDistinctTable = getDistinctTableInnerSegment(query, true);
        DistinctTable pqlDistinctTable2 = DistinctTable.fromByteBuffer(ByteBuffer.wrap(pqlDistinctTable.toBytes()));
        DistinctTable sqlDistinctTable = getDistinctTableInnerSegment(query, false);
        DistinctTable sqlDistinctTable2 = DistinctTable.fromByteBuffer(ByteBuffer.wrap(sqlDistinctTable.toBytes()));
        for (DistinctTable distinctTable : Arrays
            .asList(pqlDistinctTable, pqlDistinctTable2, sqlDistinctTable, sqlDistinctTable2)) {
          assertEquals(distinctTable.size(), 10);
          Set<String> actualValues = new HashSet<>();
          for (Record record : distinctTable.getRecords()) {
            Object[] values = record.getValues();
            assertEquals(values.length, 1);
            assertTrue(values[0] instanceof String);
            actualValues.add((String) values[0]);
          }
          assertEquals(actualValues, expectedValues);
        }
      }
    }
    {
      // Dictionary-encoded bytes column (values are left-padded to the same length)
      String query = "SELECT DISTINCT(bytesColumn) FROM testTable ORDER BY bytesColumn";
      Set<Integer> expectedValues = new HashSet<>();
      for (int i = 0; i < 10; i++) {
        expectedValues.add(i);
      }
      DistinctTable pqlDistinctTable = getDistinctTableInnerSegment(query, true);
      DistinctTable pqlDistinctTable2 = DistinctTable.fromByteBuffer(ByteBuffer.wrap(pqlDistinctTable.toBytes()));
      DistinctTable sqlDistinctTable = getDistinctTableInnerSegment(query, false);
      DistinctTable sqlDistinctTable2 = DistinctTable.fromByteBuffer(ByteBuffer.wrap(sqlDistinctTable.toBytes()));
      for (DistinctTable distinctTable : Arrays
          .asList(pqlDistinctTable, pqlDistinctTable2, sqlDistinctTable, sqlDistinctTable2)) {
        assertEquals(distinctTable.size(), 10);
        Set<Integer> actualValues = new HashSet<>();
        for (Record record : distinctTable.getRecords()) {
          Object[] values = record.getValues();
          assertEquals(values.length, 1);
          assertTrue(values[0] instanceof ByteArray);
          actualValues.add(Integer
              .parseInt(new String(((ByteArray) values[0]).getBytes(), UTF_8).trim()));
        }
        assertEquals(actualValues, expectedValues);
      }
    }
    {
      // Raw bytes column
      String query = "SELECT DISTINCT(rawBytesColumn) FROM testTable ORDER BY rawBytesColumn";
      Set<String> expectedValues =
          new HashSet<>(Arrays.asList("0", "1", "10", "11", "12", "13", "14", "15", "16", "17"));
      DistinctTable pqlDistinctTable = getDistinctTableInnerSegment(query, true);
      DistinctTable pqlDistinctTable2 = DistinctTable.fromByteBuffer(ByteBuffer.wrap(pqlDistinctTable.toBytes()));
      DistinctTable sqlDistinctTable = getDistinctTableInnerSegment(query, false);
      DistinctTable sqlDistinctTable2 = DistinctTable.fromByteBuffer(ByteBuffer.wrap(sqlDistinctTable.toBytes()));
      for (DistinctTable distinctTable : Arrays
          .asList(pqlDistinctTable, pqlDistinctTable2, sqlDistinctTable, sqlDistinctTable2)) {
        assertEquals(distinctTable.size(), 10);
        Set<String> actualValues = new HashSet<>();
        for (Record record : distinctTable.getRecords()) {
          Object[] values = record.getValues();
          assertEquals(values.length, 1);
          assertTrue(values[0] instanceof ByteArray);
          actualValues.add(new String(((ByteArray) values[0]).getBytes(), UTF_8));
        }
        assertEquals(actualValues, expectedValues);
      }
    }
  }

  /**
   * Test DISTINCT query within a single segment.
   * <p>The following query types are tested:
   * <ul>
   *   <li>Selecting all dictionary-encoded columns</li>
   *   <li>Selecting some columns with filter</li>
   *   <li>Selecting some columns order by raw BYTES column</li>
   *   <li>Selecting some columns transform, filter, order-by and limit</li>
   *   <li>Selecting some columns with filter that does not match any record</li>
   * </ul>
   */
  private void testDistinctInnerSegmentHelper(String[] queries, boolean isPql) {
    {
      // Test selecting all dictionary-encoded columns

      // Check data schema
      DistinctTable distinctTable = getDistinctTableInnerSegment(queries[0], isPql);
      DataSchema dataSchema = distinctTable.getDataSchema();
      assertEquals(dataSchema.getColumnNames(),
          new String[]{"intColumn", "longColumn", "floatColumn", "doubleColumn", "stringColumn", "bytesColumn"});
      assertEquals(dataSchema.getColumnDataTypes(), new ColumnDataType[]{
          ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.FLOAT, ColumnDataType.DOUBLE, ColumnDataType.STRING,
          ColumnDataType.BYTES
      });

      // Check values, where all 100 unique values should be returned
      assertEquals(distinctTable.size(), NUM_UNIQUE_RECORDS_PER_SEGMENT);
      assertFalse(distinctTable.isMainTable());
      Set<Integer> expectedValues = new HashSet<>();
      for (int i = 0; i < NUM_UNIQUE_RECORDS_PER_SEGMENT; i++) {
        expectedValues.add(i);
      }
      Set<Integer> actualValues = new HashSet<>();
      for (Record record : distinctTable.getRecords()) {
        Object[] values = record.getValues();
        int intValue = (int) values[0];
        assertEquals(((Long) values[1]).intValue(), intValue);
        assertEquals(((Float) values[2]).intValue(), intValue);
        assertEquals(((Double) values[3]).intValue(), intValue);
        assertEquals(Integer.parseInt((String) values[4]), intValue);
        assertEquals(new String(((ByteArray) values[5]).getBytes(), UTF_8).trim(), values[4]);
        actualValues.add(intValue);
      }
      assertEquals(actualValues, expectedValues);
    }
    {
      // Test selecting some columns with filter

      // Check data schema
      DistinctTable distinctTable = getDistinctTableInnerSegment(queries[1], isPql);
      DataSchema dataSchema = distinctTable.getDataSchema();
      assertEquals(dataSchema.getColumnNames(), new String[]{"stringColumn", "bytesColumn", "floatColumn"});
      assertEquals(dataSchema.getColumnDataTypes(),
          new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.BYTES, ColumnDataType.FLOAT});

      // Check values, where 40 matched values should be returned
      assertEquals(distinctTable.size(), NUM_UNIQUE_RECORDS_PER_SEGMENT - 60);
      assertFalse(distinctTable.isMainTable());
      Set<Integer> expectedValues = new HashSet<>();
      for (int i = 60; i < NUM_UNIQUE_RECORDS_PER_SEGMENT; i++) {
        expectedValues.add(i);
      }
      Set<Integer> actualValues = new HashSet<>();
      for (Record record : distinctTable.getRecords()) {
        Object[] values = record.getValues();
        int intValue = Integer.parseInt((String) values[0]);
        assertEquals(new String(((ByteArray) values[1]).getBytes(), UTF_8).trim(), values[0]);
        assertEquals(((Float) values[2]).intValue(), intValue);
        actualValues.add(intValue);
      }
      assertEquals(actualValues, expectedValues);
    }
    {
      // Test selecting some columns order by BYTES column

      // Check data schema
      DistinctTable distinctTable = getDistinctTableInnerSegment(queries[2], isPql);
      DataSchema dataSchema = distinctTable.getDataSchema();
      assertEquals(dataSchema.getColumnNames(), new String[]{"intColumn", "rawBytesColumn"});
      assertEquals(dataSchema.getColumnDataTypes(), new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.BYTES});

      // Check values, where only 5 top values sorted in ByteArray format ascending order should be returned
      assertEquals(distinctTable.size(), 5);
      assertTrue(distinctTable.isMainTable());
      // ByteArray of "30", "31", "3130", "3131", "3132" (same as String order because all digits can be encoded with
      // a single byte)
      int[] expectedValues = new int[]{0, 1, 10, 11, 12};
      Iterator<Record> iterator = distinctTable.getFinalResult();
      for (int i = 0; i < 5; i++) {
        Object[] values = iterator.next().getValues();
        int intValue = (int) values[0];
        assertEquals(intValue, expectedValues[i]);
        assertEquals(Integer.parseInt(new String(((ByteArray) values[1]).getBytes(), UTF_8)), intValue);
      }
    }
    {
      // Test selecting some columns with transform, filter, order-by and limit. Spaces in 'add' are intentional
      // to ensure that AggregationFunction arguments are standardized (to remove spaces).

      // Check data schema
      DistinctTable distinctTable = getDistinctTableInnerSegment(queries[3], isPql);
      DataSchema dataSchema = distinctTable.getDataSchema();
      assertEquals(dataSchema.getColumnNames(), new String[]{"add(intColumn,floatColumn)", "stringColumn"});
      assertEquals(dataSchema.getColumnDataTypes(), new ColumnDataType[]{ColumnDataType.DOUBLE, ColumnDataType.STRING});

      // Check values, where only 10 top values sorted in string format descending order should be returned
      assertEquals(distinctTable.size(), 10);
      assertTrue(distinctTable.isMainTable());
      int[] expectedValues = new int[]{9, 8, 7, 6, 59, 58, 57, 56, 55, 54};
      Iterator<Record> iterator = distinctTable.getFinalResult();
      for (int i = 0; i < 10; i++) {
        Object[] values = iterator.next().getValues();
        int intValue = ((Double) values[0]).intValue() / 2;
        assertEquals(intValue, expectedValues[i]);
        assertEquals(Integer.parseInt((String) values[1]), intValue);
      }
    }
    {
      // Test selecting some columns with filter that does not match any record

      // Check data schema, where data type should be STRING for all columns
      DistinctTable distinctTable = getDistinctTableInnerSegment(queries[4], isPql);
      DataSchema dataSchema = distinctTable.getDataSchema();
      assertEquals(dataSchema.getColumnNames(), new String[]{"floatColumn", "longColumn"});
      assertEquals(dataSchema.getColumnDataTypes(), new ColumnDataType[]{ColumnDataType.FLOAT, ColumnDataType.LONG});

      // Check values, where no record should be returned
      assertEquals(distinctTable.size(), 0);
      assertFalse(distinctTable.isMainTable());
    }
  }

  /**
   * Test DISTINCT query within a single segment.
   * <p>The following query types are tested:
   * <ul>
   *   <li>Selecting all dictionary-encoded columns</li>
   *   <li>Selecting some columns with filter</li>
   *   <li>Selecting some columns order by raw BYTES column</li>
   *   <li>Selecting some columns transform, filter, order-by and limit</li>
   *   <li>Selecting some columns with filter that does not match any record</li>
   * </ul>
   */
  @Test
  public void testDistinctInnerSegment() {
    testDistinctInnerSegmentHelper(
        new String[]{
            "SELECT DISTINCT(intColumn, longColumn, floatColumn, doubleColumn, stringColumn, bytesColumn) FROM "
                + "testTable LIMIT 10000",
            "SELECT DISTINCT(stringColumn, bytesColumn, floatColumn) FROM testTable WHERE intColumn >= 60 LIMIT 10000",
            "SELECT DISTINCT(intColumn, rawBytesColumn) FROM testTable ORDER BY rawBytesColumn LIMIT 5",
            "SELECT DISTINCT(ADD(intColumn, floatColumn), stringColumn) FROM testTable WHERE longColumn < 60 ORDER BY"
                + " stringColumn DESC, ADD(intColumn, floatColumn) ASC LIMIT 10",
            "SELECT DISTINCT(floatColumn, longColumn) FROM testTable WHERE stringColumn = 'a' ORDER BY longColumn "
                + "LIMIT 10"
        },
        true);
  }

  /**
   * Test Non-Aggregation GroupBy query rewrite to Distinct query within a single segment.
   * <p>The following query types are tested:
   * <ul>
   *   <li>Selecting all dictionary-encoded columns</li>
   *   <li>Selecting some columns with filter</li>
   *   <li>Selecting some columns order by raw BYTES column</li>
   *   <li>Selecting some columns transform, filter, order-by and limit</li>
   *   <li>Selecting some columns with filter that does not match any record</li>
   * </ul>
   */
  @Test
  public void testNonAggGroupByRewriteToDistinctInnerSegment() {
    testDistinctInnerSegmentHelper(
        new String[] {
            "SELECT intColumn, longColumn, floatColumn, doubleColumn, stringColumn, bytesColumn FROM testTable "
                + "GROUP BY intColumn, longColumn, floatColumn, doubleColumn, stringColumn, bytesColumn LIMIT 10000",
            "SELECT stringColumn, bytesColumn, floatColumn FROM testTable WHERE intColumn >= 60 "
                + "GROUP BY stringColumn, bytesColumn, floatColumn LIMIT 10000",
            "SELECT intColumn, rawBytesColumn FROM testTable "
                + "GROUP BY intColumn, rawBytesColumn ORDER BY rawBytesColumn LIMIT 5",
            "SELECT ADD(intColumn, floatColumn), stringColumn FROM testTable WHERE longColumn < 60 "
                + "GROUP BY ADD(intColumn, floatColumn), stringColumn "
                + "ORDER BY stringColumn DESC, ADD(intColumn, floatColumn) ASC LIMIT 10",
            "SELECT floatColumn, longColumn FROM testTable WHERE stringColumn = 'a' "
                + "GROUP BY floatColumn, longColumn ORDER BY longColumn LIMIT 10"
        },
        false);
  }

  /**
   * Helper method to get the DistinctTable result for one single segment for the given query.
   */
  private DistinctTable getDistinctTableInnerSegment(String query, boolean isPql) {
    BaseOperator<IntermediateResultsBlock> distinctOperator =
        isPql ? getOperatorForPqlQuery(query) : getOperatorForSqlQuery(query);
    List<Object> operatorResult = distinctOperator.nextBlock().getAggregationResult();
    assertNotNull(operatorResult);
    assertEquals(operatorResult.size(), 1);
    assertTrue(operatorResult.get(0) instanceof DistinctTable);
    return (DistinctTable) operatorResult.get(0);
  }

  /**
   * Test DISTINCT query across multiple segments and servers (2 servers, each with 2 segments).
   * <p>Both PQL and SQL format are tested.
   * <p>The following query types are tested:
   * <ul>
   *   <li>Selecting all dictionary-encoded columns</li>
   *   <li>Selecting some columns with filter</li>
   *   <li>Selecting some columns order by raw BYTES column</li>
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
  private void testDistinctInterSegmentHelper(String[] pqlQueries, String[] sqlQueries) {
    {
      // Test selecting all columns
      String pqlQuery = pqlQueries[0];
      String sqlQuery = sqlQueries[0];

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
      assertEquals(dataSchema.getColumnDataTypes(), new ColumnDataType[]{
          ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.FLOAT, ColumnDataType.DOUBLE, ColumnDataType.STRING,
          ColumnDataType.BYTES
      });

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
        assertEquals(new String(BytesUtils.toBytes((String) row[5]), UTF_8).trim(), row[4]);
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
        assertEquals(new String(BytesUtils.toBytes((String) row[5]), UTF_8).trim(), row[4]);
        sqlValues.add(intValue);
      }
      assertEquals(sqlValues, expectedValues);
    }
    {
      // Test selecting some columns with filter
      String pqlQuery = pqlQueries[1];
      String sqlQuery = sqlQueries[1];

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
        assertEquals(new String(BytesUtils.toBytes((String) row[1]), UTF_8).trim(), row[0]);
        assertEquals(((Float) row[2]).intValue(), intValue);
        pqlValues.add(intValue);
      }
      assertEquals(pqlValues, expectedValues);
      Set<Integer> sqlValues = new HashSet<>();
      for (Object[] row : sqlRows) {
        int intValue = Integer.parseInt((String) row[0]);
        assertEquals(new String(BytesUtils.toBytes((String) row[1]), UTF_8).trim(), row[0]);
        assertEquals(((Float) row[2]).intValue(), intValue);
        sqlValues.add(intValue);
      }
      assertEquals(sqlValues, expectedValues);
    }
    {
      // Test selecting some columns order by BYTES column
      String pqlQuery = pqlQueries[2];
      String sqlQuery = sqlQueries[2];

      // Check data schema
      BrokerResponseNative pqlResponse = getBrokerResponseForPqlQuery(pqlQuery);
      SelectionResults selectionResults = pqlResponse.getSelectionResults();
      assertNotNull(selectionResults);
      assertEquals(selectionResults.getColumns(), Arrays.asList("intColumn", "rawBytesColumn"));
      BrokerResponseNative sqlResponse = getBrokerResponseForSqlQuery(sqlQuery);
      ResultTable resultTable = sqlResponse.getResultTable();
      assertNotNull(resultTable);
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema.getColumnNames(), new String[]{"intColumn", "rawBytesColumn"});
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
        assertEquals(Integer.parseInt(new String(BytesUtils.toBytes((String) row[1]), UTF_8)), intValue);
      }
      for (int i = 0; i < 5; i++) {
        Object[] row = sqlRows.get(i);
        int intValue = (int) row[0];
        assertEquals(intValue, expectedValues[i]);
        assertEquals(Integer.parseInt(new String(BytesUtils.toBytes((String) row[1]), UTF_8)), intValue);
      }
    }
    {
      // Test selecting some columns with transform, filter, order-by and limit
      String pqlQuery = pqlQueries[3];
      String sqlQuery = sqlQueries[3];

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
      assertEquals(dataSchema.getColumnDataTypes(), new ColumnDataType[]{ColumnDataType.DOUBLE, ColumnDataType.STRING});

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
      String pqlQuery = pqlQueries[4];
      String sqlQuery = sqlQueries[4];

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
      assertEquals(dataSchema.getColumnDataTypes(), new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.STRING});

      // Check values, where no record should be returned
      List<Serializable[]> pqlRows = selectionResults.getRows();
      assertTrue(pqlRows.isEmpty());
      List<Object[]> sqlRows = resultTable.getRows();
      assertTrue(sqlRows.isEmpty());
    }
    {
      // Test selecting some columns with filter that does not match any record in one segment but matches some
      // records in the other segment
      String pqlQuery = pqlQueries[5];
      String sqlQuery = sqlQueries[5];

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
      String pqlQuery = pqlQueries[6];
      String sqlQuery = sqlQueries[6];

      QueryContext pqlQueryContext = QueryContextConverterUtils.getQueryContextFromPQL(pqlQuery);
      BrokerResponseNative pqlResponse = queryServersWithDifferentSegments(pqlQueryContext);
      QueryContext sqlQueryContext =
          QueryContextConverterUtils.getQueryContextFromSQL(sqlQuery + " OPTION(responseFormat=sql)");
      BrokerResponseNative sqlResponse = queryServersWithDifferentSegments(sqlQueryContext);

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
  }

  /**
   * Test DISTINCT query across multiple segments and servers (2 servers, each with 2 segments).
   * <p>Both PQL and SQL format are tested.
   * <p>The following query types are tested:
   * <ul>
   *   <li>Selecting all dictionary-encoded columns</li>
   *   <li>Selecting some columns with filter</li>
   *   <li>Selecting some columns order by raw BYTES column</li>
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
  public void testDistinctInterSegment() {
    //@formatter:off
    String[] pqlQueries = new String[]{
        "SELECT DISTINCT(intColumn, longColumn, floatColumn, doubleColumn, stringColumn, bytesColumn) "
            + "FROM testTable LIMIT 10000",
        "SELECT DISTINCT(stringColumn, bytesColumn, floatColumn) FROM testTable WHERE intColumn >= 60 LIMIT 10000",
        "SELECT DISTINCT(intColumn, rawBytesColumn) FROM testTable ORDER BY rawBytesColumn LIMIT 5",
        "SELECT DISTINCT(ADD(intColumn, floatColumn), stringColumn) FROM testTable WHERE longColumn < 60 "
            + "ORDER BY stringColumn DESC, ADD(intColumn, floatColumn) ASC LIMIT 10",
        "SELECT DISTINCT(floatColumn, longColumn) FROM testTable WHERE stringColumn = 'a' ORDER BY longColumn LIMIT 10",
        "SELECT DISTINCT(intColumn) FROM testTable WHERE floatColumn > 200 ORDER BY intColumn ASC LIMIT 5",
        "SELECT DISTINCT(longColumn) FROM testTable WHERE doubleColumn < 200 ORDER BY longColumn DESC LIMIT 5"
    };
    String[] sqlQueries = new String[]{
        "SELECT DISTINCT intColumn, longColumn, floatColumn, doubleColumn, stringColumn, bytesColumn "
            + "FROM testTable LIMIT 10000",
        "SELECT DISTINCT stringColumn, bytesColumn, floatColumn FROM testTable WHERE intColumn >= 60 LIMIT 10000",
        "SELECT DISTINCT intColumn, rawBytesColumn FROM testTable ORDER BY rawBytesColumn LIMIT 5",
        "SELECT DISTINCT ADD(intColumn, floatColumn), stringColumn FROM testTable WHERE longColumn < 60 "
            + "ORDER BY stringColumn DESC, ADD(intColumn, floatColumn) ASC LIMIT 10",
        "SELECT DISTINCT floatColumn, longColumn FROM testTable WHERE stringColumn = 'a' ORDER BY longColumn LIMIT 10",
        "SELECT DISTINCT intColumn FROM testTable WHERE floatColumn > 200 ORDER BY intColumn ASC LIMIT 5",
        "SELECT DISTINCT longColumn FROM testTable WHERE doubleColumn < 200 ORDER BY longColumn DESC LIMIT 5"
    };
    //@formatter:on
    testDistinctInterSegmentHelper(pqlQueries, sqlQueries);
  }

  /**
   * Test Non-Aggregation GroupBy query rewrite to Distinct query across multiple segments and servers (2 servers,
   * each with 2 segments).
   * <p>Only SQL format are tested.
   * <p>The following query types are tested:
   * <ul>
   *   <li>Selecting all dictionary-encoded columns</li>
   *   <li>Selecting some columns with filter</li>
   *   <li>Selecting some columns order by raw BYTES column</li>
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
  public void testNonAggGroupByRewriteToDistinctInterSegment() {
    //@formatter:off
    String[] pqlQueries = new String[]{
        "SELECT DISTINCT(intColumn, longColumn, floatColumn, doubleColumn, stringColumn, bytesColumn) "
            + "FROM testTable LIMIT 10000",
        "SELECT DISTINCT(stringColumn, bytesColumn, floatColumn) FROM testTable WHERE intColumn >= 60 LIMIT 10000",
        "SELECT DISTINCT(intColumn, rawBytesColumn) FROM testTable ORDER BY rawBytesColumn LIMIT 5",
        "SELECT DISTINCT(ADD(intColumn, floatColumn), stringColumn) FROM testTable WHERE longColumn < 60 "
            + "ORDER BY stringColumn DESC, ADD(intColumn, floatColumn) ASC LIMIT 10",
        "SELECT DISTINCT(floatColumn, longColumn) FROM testTable WHERE stringColumn = 'a' ORDER BY longColumn LIMIT 10",
        "SELECT DISTINCT(intColumn) FROM testTable WHERE floatColumn > 200 ORDER BY intColumn ASC LIMIT 5",
        "SELECT DISTINCT(longColumn) FROM testTable WHERE doubleColumn < 200 ORDER BY longColumn DESC LIMIT 5"
    };
    String[] sqlQueries = new String[]{
        "SELECT intColumn, longColumn, floatColumn, doubleColumn, stringColumn, bytesColumn FROM testTable "
            + "GROUP BY intColumn, longColumn, floatColumn, doubleColumn, stringColumn, bytesColumn LIMIT 10000",
        "SELECT stringColumn, bytesColumn, floatColumn FROM testTable WHERE intColumn >= 60 "
            + "GROUP BY stringColumn, bytesColumn, floatColumn LIMIT 10000",
        "SELECT intColumn, rawBytesColumn FROM testTable GROUP BY intColumn, rawBytesColumn "
            + "ORDER BY rawBytesColumn LIMIT 5",
        "SELECT ADD(intColumn, floatColumn), stringColumn FROM testTable WHERE longColumn < 60 "
            + "GROUP BY ADD(intColumn, floatColumn), stringColumn "
            + "ORDER BY stringColumn DESC, ADD(intColumn, floatColumn) ASC LIMIT 10",
        "SELECT floatColumn, longColumn FROM testTable WHERE stringColumn = 'a' "
            + "GROUP BY floatColumn, longColumn ORDER BY longColumn LIMIT 10",
        "SELECT intColumn FROM testTable WHERE floatColumn > 200 GROUP BY intColumn ORDER BY intColumn ASC LIMIT 5",
        "SELECT longColumn FROM testTable WHERE doubleColumn < 200 GROUP BY longColumn ORDER BY longColumn DESC LIMIT 5"
    };
    //@formatter:on
    testDistinctInterSegmentHelper(pqlQueries, sqlQueries);
  }

  /**
   * Helper method to query 2 servers with different segments. Server0 will have 2 copies of segment0; Server1 will have
   * 2 copies of segment1.
   */
  private BrokerResponseNative queryServersWithDifferentSegments(QueryContext queryContext) {
    IndexSegment segment0 = _indexSegments.get(0);
    IndexSegment segment1 = _indexSegments.get(1);

    // Server side
    queryContext.setEndTimeMs(System.currentTimeMillis() + Server.DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS);
    DataTable instanceResponse0 =
        PLAN_MAKER.makeInstancePlan(Arrays.asList(segment0, segment0), queryContext, EXECUTOR_SERVICE).execute();
    DataTable instanceResponse1 =
        PLAN_MAKER.makeInstancePlan(Arrays.asList(segment1, segment1), queryContext, EXECUTOR_SERVICE).execute();

    // Broker side
    Map<String, Object> properties = new HashMap<>();
    properties.put(CommonConstants.Broker.CONFIG_OF_MAX_REDUCE_THREADS_PER_QUERY, 2); // 2 Threads for 2 data-tables.
    BrokerReduceService brokerReduceService = new BrokerReduceService(new PinotConfiguration(properties));
    Map<ServerRoutingInstance, DataTable> dataTableMap = new HashMap<>();
    dataTableMap.put(new ServerRoutingInstance("localhost", 1234, TableType.OFFLINE), instanceResponse0);
    dataTableMap.put(new ServerRoutingInstance("localhost", 1234, TableType.REALTIME), instanceResponse1);
    BrokerResponseNative brokerResponse = brokerReduceService
        .reduceOnDataTable(queryContext.getBrokerRequest(), dataTableMap,
            CommonConstants.Broker.DEFAULT_BROKER_TIMEOUT_MS, null);
    brokerReduceService.shutDown();
    return brokerResponse;
  }
}
