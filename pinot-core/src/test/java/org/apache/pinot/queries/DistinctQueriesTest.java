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
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.blocks.results.DistinctResultsBlock;
import org.apache.pinot.core.query.distinct.DistinctTable;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.BytesUtils;
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
  private static final String BIG_DECIMAL_COLUMN = "bigDecimalColumn";
  private static final String STRING_COLUMN = "stringColumn";
  private static final String BYTES_COLUMN = "bytesColumn";
  private static final String RAW_INT_COLUMN = "rawIntColumn";
  private static final String RAW_LONG_COLUMN = "rawLongColumn";
  private static final String RAW_FLOAT_COLUMN = "rawFloatColumn";
  private static final String RAW_DOUBLE_COLUMN = "rawDoubleColumn";
  private static final String RAW_BIG_DECIMAL_COLUMN = "rawBigDecimalColumn";
  private static final String RAW_STRING_COLUMN = "rawStringColumn";
  private static final String RAW_BYTES_COLUMN = "rawBytesColumn";
  private static final String INT_MV_COLUMN = "intMVColumn";
  private static final String LONG_MV_COLUMN = "longMVColumn";
  private static final String FLOAT_MV_COLUMN = "floatMVColumn";
  private static final String DOUBLE_MV_COLUMN = "doubleMVColumn";
  private static final String STRING_MV_COLUMN = "stringMVColumn";
  private static final String RAW_INT_MV_COLUMN = "rawIntMVColumn";
  private static final String RAW_LONG_MV_COLUMN = "rawLongMVColumn";
  private static final String RAW_FLOAT_MV_COLUMN = "rawFloatMVColumn";
  private static final String RAW_DOUBLE_MV_COLUMN = "rawDoubleMVColumn";
  private static final String RAW_STRING_MV_COLUMN = "rawStringMVColumn";

  //@formatter:off
  private static final Schema SCHEMA = new Schema.SchemaBuilder()
      .addSingleValueDimension(INT_COLUMN, DataType.INT)
      .addSingleValueDimension(LONG_COLUMN, DataType.LONG)
      .addSingleValueDimension(FLOAT_COLUMN, DataType.FLOAT)
      .addSingleValueDimension(DOUBLE_COLUMN, DataType.DOUBLE)
      .addMetric(BIG_DECIMAL_COLUMN, DataType.BIG_DECIMAL)
      .addSingleValueDimension(STRING_COLUMN, DataType.STRING)
      .addSingleValueDimension(BYTES_COLUMN, DataType.BYTES)
      .addSingleValueDimension(RAW_INT_COLUMN, DataType.INT)
      .addSingleValueDimension(RAW_LONG_COLUMN, DataType.LONG)
      .addSingleValueDimension(RAW_FLOAT_COLUMN, DataType.FLOAT)
      .addSingleValueDimension(RAW_DOUBLE_COLUMN, DataType.DOUBLE)
      .addMetric(RAW_BIG_DECIMAL_COLUMN, DataType.BIG_DECIMAL)
      .addSingleValueDimension(RAW_STRING_COLUMN, DataType.STRING)
      .addSingleValueDimension(RAW_BYTES_COLUMN, DataType.BYTES)
      .addMultiValueDimension(INT_MV_COLUMN, DataType.INT)
      .addMultiValueDimension(LONG_MV_COLUMN, DataType.LONG)
      .addMultiValueDimension(FLOAT_MV_COLUMN, DataType.FLOAT)
      .addMultiValueDimension(DOUBLE_MV_COLUMN, DataType.DOUBLE)
      .addMultiValueDimension(STRING_MV_COLUMN, DataType.STRING)
      .addMultiValueDimension(RAW_INT_MV_COLUMN, DataType.INT)
      .addMultiValueDimension(RAW_LONG_MV_COLUMN, DataType.LONG)
      .addMultiValueDimension(RAW_FLOAT_MV_COLUMN, DataType.FLOAT)
      .addMultiValueDimension(RAW_DOUBLE_MV_COLUMN, DataType.DOUBLE)
      .addMultiValueDimension(RAW_STRING_MV_COLUMN, DataType.STRING)
      .build();
  //@formatter:on

  private static final TableConfig TABLE = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
      .setNoDictionaryColumns(
          Arrays.asList(RAW_INT_COLUMN, RAW_LONG_COLUMN, RAW_FLOAT_COLUMN, RAW_DOUBLE_COLUMN, RAW_BIG_DECIMAL_COLUMN,
              RAW_STRING_COLUMN, RAW_BYTES_COLUMN, RAW_INT_MV_COLUMN, RAW_LONG_MV_COLUMN, RAW_FLOAT_MV_COLUMN,
              RAW_DOUBLE_MV_COLUMN, RAW_STRING_MV_COLUMN)).build();

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
      record.putValue(LONG_COLUMN, value);
      record.putValue(FLOAT_COLUMN, value);
      record.putValue(DOUBLE_COLUMN, value);
      record.putValue(BIG_DECIMAL_COLUMN, value);
      record.putValue(STRING_COLUMN, value);
      record.putValue(BYTES_COLUMN, StringUtils.leftPad(Integer.toString(value), 4).getBytes(UTF_8));
      record.putValue(RAW_INT_COLUMN, value);
      record.putValue(RAW_LONG_COLUMN, value);
      record.putValue(RAW_FLOAT_COLUMN, value);
      record.putValue(RAW_DOUBLE_COLUMN, value);
      record.putValue(RAW_BIG_DECIMAL_COLUMN, value);
      record.putValue(RAW_STRING_COLUMN, value);
      record.putValue(RAW_BYTES_COLUMN, Integer.toString(value).getBytes(UTF_8));
      Integer[] mvValue = new Integer[]{value, value + NUM_UNIQUE_RECORDS_PER_SEGMENT};
      record.putValue(INT_MV_COLUMN, mvValue);
      record.putValue(LONG_MV_COLUMN, mvValue);
      record.putValue(FLOAT_MV_COLUMN, mvValue);
      record.putValue(DOUBLE_MV_COLUMN, mvValue);
      record.putValue(STRING_MV_COLUMN, mvValue);
      record.putValue(RAW_INT_MV_COLUMN, mvValue);
      record.putValue(RAW_LONG_MV_COLUMN, mvValue);
      record.putValue(RAW_FLOAT_MV_COLUMN, mvValue);
      record.putValue(RAW_DOUBLE_MV_COLUMN, mvValue);
      record.putValue(RAW_STRING_MV_COLUMN, mvValue);
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
      List<String> queries = Arrays.asList(
          "SELECT DISTINCT(intColumn) FROM testTable",
          "SELECT DISTINCT(longColumn) FROM testTable",
          "SELECT DISTINCT(floatColumn) FROM testTable",
          "SELECT DISTINCT(doubleColumn) FROM testTable",
          "SELECT DISTINCT(bigDecimalColumn) FROM testTable",
          "SELECT DISTINCT(rawIntColumn) FROM testTable",
          "SELECT DISTINCT(rawLongColumn) FROM testTable",
          "SELECT DISTINCT(rawFloatColumn) FROM testTable",
          "SELECT DISTINCT(rawDoubleColumn) FROM testTable",
          "SELECT DISTINCT(rawBigDecimalColumn) FROM testTable",
          "SELECT DISTINCT(intMVColumn) FROM testTable",
          "SELECT DISTINCT(longMVColumn) FROM testTable",
          "SELECT DISTINCT(floatMVColumn) FROM testTable",
          "SELECT DISTINCT(doubleMVColumn) FROM testTable"
      );
      //@formatter:on
      // Query should be solved with dictionary, so it should return the 10 smallest values
      Set<Integer> expectedValues = new HashSet<>();
      for (int i = 0; i < 10; i++) {
        expectedValues.add(i);
      }
      for (String query : queries) {
        DistinctTable distinctTable1 = getDistinctTableInnerSegment(query);
        DistinctTable distinctTable2 = DistinctTable.fromByteBuffer(ByteBuffer.wrap(distinctTable1.toBytes()));
        for (DistinctTable distinctTable : Arrays.asList(distinctTable1, distinctTable2)) {
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
      // String SV column
      String query = "SELECT DISTINCT(stringColumn) FROM testTable";
      // We define a specific result set here since the data read from dictionary is in alphabetically sorted order
      Set<Integer> expectedValues = new HashSet<>(Arrays.asList(0, 1, 10, 11, 12, 13, 14, 15, 16, 17));
      DistinctTable distinctTable1 = getDistinctTableInnerSegment(query);
      DistinctTable distinctTable2 = DistinctTable.fromByteBuffer(ByteBuffer.wrap(distinctTable1.toBytes()));
      for (DistinctTable distinctTable : Arrays.asList(distinctTable1, distinctTable2)) {
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
      // String MV column
      String query = "SELECT DISTINCT(stringMVColumn) FROM testTable";
      // We define a specific result set here since the data read from dictionary is in alphabetically sorted order
      Set<Integer> expectedValues = new HashSet<>(Arrays.asList(0, 1, 10, 100, 101, 102, 103, 104, 105, 106));
      DistinctTable distinctTable1 = getDistinctTableInnerSegment(query);
      DistinctTable distinctTable2 = DistinctTable.fromByteBuffer(ByteBuffer.wrap(distinctTable1.toBytes()));
      for (DistinctTable distinctTable : Arrays.asList(distinctTable1, distinctTable2)) {
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
      // Raw string SV column
      String query = "SELECT DISTINCT(rawStringColumn) FROM testTable";
      Set<Integer> expectedValues = new HashSet<>();
      for (int i = 0; i < 10; i++) {
        expectedValues.add(i);
      }
      DistinctTable distinctTable1 = getDistinctTableInnerSegment(query);
      DistinctTable distinctTable2 = DistinctTable.fromByteBuffer(ByteBuffer.wrap(distinctTable1.toBytes()));
      for (DistinctTable distinctTable : Arrays.asList(distinctTable1, distinctTable2)) {
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
      List<String> queries = Arrays.asList(
          "SELECT DISTINCT(bytesColumn) FROM testTable",
          "SELECT DISTINCT(rawBytesColumn) FROM testTable"
      );
      //@formatter:on
      Set<Integer> expectedValues = new HashSet<>();
      for (int i = 0; i < 10; i++) {
        expectedValues.add(i);
      }
      for (String query : queries) {
        DistinctTable distinctTable1 = getDistinctTableInnerSegment(query);
        DistinctTable distinctTable2 = DistinctTable.fromByteBuffer(ByteBuffer.wrap(distinctTable1.toBytes()));
        for (DistinctTable distinctTable : Arrays.asList(distinctTable1, distinctTable2)) {
          assertEquals(distinctTable.size(), 10);
          Set<Integer> actualValues = new HashSet<>();
          for (Record record : distinctTable.getRecords()) {
            Object[] values = record.getValues();
            assertEquals(values.length, 1);
            assertTrue(values[0] instanceof ByteArray);
            actualValues.add(Integer.parseInt(new String(((ByteArray) values[0]).getBytes(), UTF_8).trim()));
          }
          assertEquals(actualValues, expectedValues);
        }
      }
    }
    {
      // Raw MV numeric columns
      //@formatter:off
      List<String> queries = Arrays.asList(
          "SELECT DISTINCT(rawIntMVColumn) FROM testTable",
          "SELECT DISTINCT(rawLongMVColumn) FROM testTable",
          "SELECT DISTINCT(rawFloatMVColumn) FROM testTable",
          "SELECT DISTINCT(rawDoubleMVColumn) FROM testTable"
      );
      //@formatter:on
      // We define a specific result set here since the data read from raw is in the order added
      Set<Integer> expectedValues = new HashSet<>(Arrays.asList(0, 1, 2, 3, 4, 100, 101, 102, 103, 104));
      for (String query : queries) {
        DistinctTable distinctTable1 = getDistinctTableInnerSegment(query);
        DistinctTable distinctTable2 = DistinctTable.fromByteBuffer(ByteBuffer.wrap(distinctTable1.toBytes()));
        for (DistinctTable distinctTable : Arrays.asList(distinctTable1, distinctTable2)) {
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
      // Raw MV string column
      //@formatter:off
      String query = "SELECT DISTINCT(rawStringMVColumn) FROM testTable";
      //@formatter:on
      // We define a specific result set here since the data read from raw is in the order added
      Set<Integer> expectedValues = new HashSet<>(Arrays.asList(0, 1, 2, 3, 4, 100, 101, 102, 103, 104));
      DistinctTable distinctTable1 = getDistinctTableInnerSegment(query);
      DistinctTable distinctTable2 = DistinctTable.fromByteBuffer(ByteBuffer.wrap(distinctTable1.toBytes()));
      for (DistinctTable distinctTable : Arrays.asList(distinctTable1, distinctTable2)) {
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
  }

  @Test
  public void testSingleColumnDistinctOrderByInnerSegment()
      throws Exception {
    {
      // Numeric columns ASC
      //@formatter:off
      List<String> queries = Arrays.asList(
          "SELECT DISTINCT(intColumn) FROM testTable ORDER BY intColumn",
          "SELECT DISTINCT(longColumn) FROM testTable ORDER BY longColumn",
          "SELECT DISTINCT(floatColumn) FROM testTable ORDER BY floatColumn",
          "SELECT DISTINCT(doubleColumn) FROM testTable ORDER BY doubleColumn",
          "SELECT DISTINCT(bigDecimalColumn) FROM testTable ORDER BY bigDecimalColumn",
          "SELECT DISTINCT(rawIntColumn) FROM testTable ORDER BY rawIntColumn",
          "SELECT DISTINCT(rawLongColumn) FROM testTable ORDER BY rawLongColumn",
          "SELECT DISTINCT(rawFloatColumn) FROM testTable ORDER BY rawFloatColumn",
          "SELECT DISTINCT(rawDoubleColumn) FROM testTable ORDER BY rawDoubleColumn",
          "SELECT DISTINCT(rawBigDecimalColumn) FROM testTable ORDER BY rawBigDecimalColumn",
          "SELECT DISTINCT(intMVColumn) FROM testTable ORDER BY intMVColumn",
          "SELECT DISTINCT(longMVColumn) FROM testTable ORDER BY longMVColumn",
          "SELECT DISTINCT(floatMVColumn) FROM testTable ORDER BY floatMVColumn",
          "SELECT DISTINCT(doubleMVColumn) FROM testTable ORDER BY doubleMVColumn"
      );
      //@formatter:on
      Set<Integer> expectedValues = new HashSet<>();
      for (int i = 0; i < 10; i++) {
        expectedValues.add(i);
      }
      for (String query : queries) {
        DistinctTable distinctTable1 = getDistinctTableInnerSegment(query);
        DistinctTable distinctTable2 = DistinctTable.fromByteBuffer(ByteBuffer.wrap(distinctTable1.toBytes()));
        for (DistinctTable distinctTable : Arrays.asList(distinctTable1, distinctTable2)) {
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
      // Numeric SV columns DESC
      //@formatter:off
      List<String> queries = Arrays.asList(
          "SELECT DISTINCT(intColumn) FROM testTable ORDER BY intColumn DESC",
          "SELECT DISTINCT(longColumn) FROM testTable ORDER BY longColumn DESC",
          "SELECT DISTINCT(floatColumn) FROM testTable ORDER BY floatColumn DESC",
          "SELECT DISTINCT(doubleColumn) FROM testTable ORDER BY doubleColumn DESC",
          "SELECT DISTINCT(bigDecimalColumn) FROM testTable ORDER BY bigDecimalColumn DESC",
          "SELECT DISTINCT(rawIntColumn) FROM testTable ORDER BY rawIntColumn DESC",
          "SELECT DISTINCT(rawLongColumn) FROM testTable ORDER BY rawLongColumn DESC",
          "SELECT DISTINCT(rawFloatColumn) FROM testTable ORDER BY rawFloatColumn DESC",
          "SELECT DISTINCT(rawDoubleColumn) FROM testTable ORDER BY rawDoubleColumn DESC",
          "SELECT DISTINCT(rawBigDecimalColumn) FROM testTable ORDER BY rawBigDecimalColumn DESC"
      );
      //@formatter:on
      Set<Integer> expectedValues = new HashSet<>();
      for (int i = NUM_UNIQUE_RECORDS_PER_SEGMENT - 10; i < NUM_UNIQUE_RECORDS_PER_SEGMENT; i++) {
        expectedValues.add(i);
      }
      for (String query : queries) {
        DistinctTable distinctTable1 = getDistinctTableInnerSegment(query);
        DistinctTable distinctTable2 = DistinctTable.fromByteBuffer(ByteBuffer.wrap(distinctTable1.toBytes()));
        for (DistinctTable distinctTable : Arrays.asList(distinctTable1, distinctTable2)) {
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
      // Numeric MV columns DESC
      //@formatter:off
      List<String> queries = Arrays.asList(
          "SELECT DISTINCT(intMVColumn) FROM testTable ORDER BY intMVColumn DESC",
          "SELECT DISTINCT(longMVColumn) FROM testTable ORDER BY longMVColumn DESC",
          "SELECT DISTINCT(floatMVColumn) FROM testTable ORDER BY floatMVColumn DESC",
          "SELECT DISTINCT(doubleMVColumn) FROM testTable ORDER BY doubleMVColumn DESC"
      );
      //@formatter:on
      Set<Integer> expectedValues = new HashSet<>();
      for (int i = 2 * NUM_UNIQUE_RECORDS_PER_SEGMENT - 10; i < 2 * NUM_UNIQUE_RECORDS_PER_SEGMENT; i++) {
        expectedValues.add(i);
      }
      for (String query : queries) {
        DistinctTable distinctTable1 = getDistinctTableInnerSegment(query);
        DistinctTable distinctTable2 = DistinctTable.fromByteBuffer(ByteBuffer.wrap(distinctTable1.toBytes()));
        for (DistinctTable distinctTable : Arrays.asList(distinctTable1, distinctTable2)) {
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
      // String SV columns
      //@formatter:off
      List<String> queries = Arrays.asList(
          "SELECT DISTINCT(stringColumn) FROM testTable ORDER BY stringColumn",
          "SELECT DISTINCT(rawStringColumn) FROM testTable ORDER BY rawStringColumn"
      );
      //@formatter:on
      Set<String> expectedValues =
          new HashSet<>(Arrays.asList("0", "1", "10", "11", "12", "13", "14", "15", "16", "17"));
      for (String query : queries) {
        DistinctTable distinctTable1 = getDistinctTableInnerSegment(query);
        DistinctTable distinctTable2 = DistinctTable.fromByteBuffer(ByteBuffer.wrap(distinctTable1.toBytes()));
        for (DistinctTable distinctTable : Arrays.asList(distinctTable1, distinctTable2)) {
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
      // String MV column
      String query = "SELECT DISTINCT(stringMVColumn) FROM testTable ORDER BY stringMVColumn";
      Set<String> expectedValues =
          new HashSet<>(Arrays.asList("0", "1", "10", "100", "101", "102", "103", "104", "105", "106"));
      DistinctTable distinctTable1 = getDistinctTableInnerSegment(query);
      DistinctTable distinctTable2 = DistinctTable.fromByteBuffer(ByteBuffer.wrap(distinctTable1.toBytes()));
      for (DistinctTable distinctTable : Arrays.asList(distinctTable1, distinctTable2)) {
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
    {
      // Dictionary-encoded bytes column (values are left-padded to the same length)
      String query = "SELECT DISTINCT(bytesColumn) FROM testTable ORDER BY bytesColumn";
      Set<Integer> expectedValues = new HashSet<>();
      for (int i = 0; i < 10; i++) {
        expectedValues.add(i);
      }
      DistinctTable distinctTable1 = getDistinctTableInnerSegment(query);
      DistinctTable distinctTable2 = DistinctTable.fromByteBuffer(ByteBuffer.wrap(distinctTable1.toBytes()));
      for (DistinctTable distinctTable : Arrays.asList(distinctTable1, distinctTable2)) {
        assertEquals(distinctTable.size(), 10);
        Set<Integer> actualValues = new HashSet<>();
        for (Record record : distinctTable.getRecords()) {
          Object[] values = record.getValues();
          assertEquals(values.length, 1);
          assertTrue(values[0] instanceof ByteArray);
          actualValues.add(Integer.parseInt(new String(((ByteArray) values[0]).getBytes(), UTF_8).trim()));
        }
        assertEquals(actualValues, expectedValues);
      }
    }
    {
      // Raw bytes column
      String query = "SELECT DISTINCT(rawBytesColumn) FROM testTable ORDER BY rawBytesColumn";
      Set<String> expectedValues =
          new HashSet<>(Arrays.asList("0", "1", "10", "11", "12", "13", "14", "15", "16", "17"));
      DistinctTable distinctTable1 = getDistinctTableInnerSegment(query);
      DistinctTable distinctTable2 = DistinctTable.fromByteBuffer(ByteBuffer.wrap(distinctTable1.toBytes()));
      for (DistinctTable distinctTable : Arrays.asList(distinctTable1, distinctTable2)) {
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
    {
      // Numeric raw MV columns ASC
      //@formatter:off
      List<String> queries = Arrays.asList(
          "SELECT DISTINCT(rawIntMVColumn) FROM testTable ORDER BY rawIntMVColumn",
          "SELECT DISTINCT(rawLongMVColumn) FROM testTable ORDER BY rawLongMVColumn",
          "SELECT DISTINCT(rawFloatMVColumn) FROM testTable ORDER BY rawFloatMVColumn",
          "SELECT DISTINCT(rawDoubleMVColumn) FROM testTable ORDER BY rawDoubleMVColumn"
      );
      //@formatter:on
      Set<Integer> expectedValues = new HashSet<>();
      for (int i = 0; i < 10; i++) {
        expectedValues.add(i);
      }
      for (String query : queries) {
        DistinctTable distinctTable1 = getDistinctTableInnerSegment(query);
        DistinctTable distinctTable2 = DistinctTable.fromByteBuffer(ByteBuffer.wrap(distinctTable1.toBytes()));
        for (DistinctTable distinctTable : Arrays.asList(distinctTable1, distinctTable2)) {
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
      // Numeric raw MV columns DESC
      //@formatter:off
      List<String> queries = Arrays.asList(
          "SELECT DISTINCT(rawIntMVColumn) FROM testTable ORDER BY rawIntMVColumn DESC",
          "SELECT DISTINCT(rawLongMVColumn) FROM testTable ORDER BY rawLongMVColumn DESC",
          "SELECT DISTINCT(rawFloatMVColumn) FROM testTable ORDER BY rawFloatMVColumn DESC",
          "SELECT DISTINCT(rawDoubleMVColumn) FROM testTable ORDER BY rawDoubleMVColumn DESC"
      );
      //@formatter:on
      Set<Integer> expectedValues = new HashSet<>();
      for (int i = 2 * NUM_UNIQUE_RECORDS_PER_SEGMENT - 10; i < 2 * NUM_UNIQUE_RECORDS_PER_SEGMENT; i++) {
        expectedValues.add(i);
      }
      for (String query : queries) {
        DistinctTable distinctTable1 = getDistinctTableInnerSegment(query);
        DistinctTable distinctTable2 = DistinctTable.fromByteBuffer(ByteBuffer.wrap(distinctTable1.toBytes()));
        for (DistinctTable distinctTable : Arrays.asList(distinctTable1, distinctTable2)) {
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
      // String raw MV column
      String query = "SELECT DISTINCT(rawStringMVColumn) FROM testTable ORDER BY rawStringMVColumn";
      Set<String> expectedValues =
          new HashSet<>(Arrays.asList("0", "1", "10", "100", "101", "102", "103", "104", "105", "106"));
      DistinctTable distinctTable1 = getDistinctTableInnerSegment(query);
      DistinctTable distinctTable2 = DistinctTable.fromByteBuffer(ByteBuffer.wrap(distinctTable1.toBytes()));
      for (DistinctTable distinctTable : Arrays.asList(distinctTable1, distinctTable2)) {
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

  /**
   * Test DISTINCT query within a single segment.
   * <p>The following query types are tested:
   * <ul>
   *   <li>Selecting all dictionary-encoded SV columns</li>
   *   <li>Selecting all dictionary-encoded MV columns</li>
   *   <li>Selecting some SV columns (including raw) and some MV columns</li>
   *   <li>Selecting some columns with filter</li>
   *   <li>Selecting some columns order by MV column</li>
   *   <li>Selecting some columns order by raw BYTES column</li>
   *   <li>Selecting some columns transform, filter, order-by and limit</li>
   *   <li>Selecting some columns with filter that does not match any record</li>
   *   <li>Selecting all dictionary-encoded raw MV columns</li>
   *   <li>Selecting some SV columns (including raw) and some raw MV columns</li>
   *   <li>Selecting some columns with filter with raw MV</li>
   *   <li>Selecting some columns order by raw MV column</li>
   *   <li>Selecting some columns with filter that does not match any record with raw MV</li>
   * </ul>
   */
  private void testDistinctInnerSegmentHelper(String[] queries) {
    assertEquals(queries.length, 13);

    // Selecting all dictionary-encoded SV columns
    // SELECT DISTINCT intColumn, longColumn, floatColumn, doubleColumn, bigDecimalColumn, stringColumn, bytesColumn
    // FROM testTable LIMIT 10000
    {
      DistinctTable distinctTable = getDistinctTableInnerSegment(queries[0]);

      // Check data schema
      DataSchema expectedDataSchema = new DataSchema(new String[]{
          "intColumn", "longColumn", "floatColumn", "doubleColumn", "bigDecimalColumn", "stringColumn", "bytesColumn"
      }, new ColumnDataType[]{
          ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.FLOAT, ColumnDataType.DOUBLE,
          ColumnDataType.BIG_DECIMAL, ColumnDataType.STRING, ColumnDataType.BYTES
      });
      assertEquals(distinctTable.getDataSchema(), expectedDataSchema);

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
        int intValue = (Integer) values[0];
        assertEquals(((Long) values[1]).intValue(), intValue);
        assertEquals(((Float) values[2]).intValue(), intValue);
        assertEquals(((Double) values[3]).intValue(), intValue);
        assertEquals(((BigDecimal) values[4]).intValue(), intValue);
        assertEquals(Integer.parseInt((String) values[5]), intValue);
        assertEquals(new String(((ByteArray) values[6]).getBytes(), UTF_8).trim(), values[5]);
        actualValues.add(intValue);
      }
      assertEquals(actualValues, expectedValues);
    }

    // Selecting all dictionary-encoded MV columns
    // SELECT DISTINCT intMVColumn, longMVColumn, floatMVColumn, doubleMVColumn, stringMVColumn
    // FROM testTable LIMIT 10000
    {
      DistinctTable distinctTable = getDistinctTableInnerSegment(queries[1]);

      // Check data schema
      DataSchema expectedDataSchema = new DataSchema(new String[]{
          "intMVColumn", "longMVColumn", "floatMVColumn", "doubleMVColumn", "stringMVColumn"
      }, new ColumnDataType[]{
          ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.FLOAT, ColumnDataType.DOUBLE, ColumnDataType.STRING
      });
      assertEquals(distinctTable.getDataSchema(), expectedDataSchema);

      // Check values, where all 100 * 2^5 unique combinations should be returned
      int numUniqueCombinations = NUM_UNIQUE_RECORDS_PER_SEGMENT * (1 << 5);
      assertEquals(distinctTable.size(), numUniqueCombinations);
      assertFalse(distinctTable.isMainTable());
      Set<List<Integer>> actualValues = new HashSet<>();
      for (Record record : distinctTable.getRecords()) {
        Object[] values = record.getValues();
        int intValue = (Integer) values[0];
        List<Integer> actualValueList =
            Arrays.asList(intValue, ((Long) values[1]).intValue(), ((Float) values[2]).intValue(),
                ((Double) values[3]).intValue(), Integer.parseInt((String) values[4]));
        List<Integer> expectedValues = new ArrayList<>(2);
        expectedValues.add(intValue % NUM_UNIQUE_RECORDS_PER_SEGMENT);
        expectedValues.add(intValue % NUM_UNIQUE_RECORDS_PER_SEGMENT + NUM_UNIQUE_RECORDS_PER_SEGMENT);
        for (Integer actualValue : actualValueList) {
          assertTrue(expectedValues.contains(actualValue));
        }
        actualValues.add(actualValueList);
      }
      assertEquals(actualValues.size(), numUniqueCombinations);
    }

    // Selecting some SV columns (including raw) and some MV columns
    // SELECT DISTINCT longColumn, rawBigDecimalColumn, floatMVColumn, stringMVColumn FROM testTable LIMIT 10000
    {
      DistinctTable distinctTable = getDistinctTableInnerSegment(queries[2]);

      // Check data schema
      DataSchema expectedDataSchema = new DataSchema(new String[]{
          "longColumn", "rawBigDecimalColumn", "floatMVColumn", "stringMVColumn"
      }, new ColumnDataType[]{
          ColumnDataType.LONG, ColumnDataType.BIG_DECIMAL, ColumnDataType.FLOAT, ColumnDataType.STRING
      });
      assertEquals(distinctTable.getDataSchema(), expectedDataSchema);

      // Check values, where all 100 * 2^2 unique combinations should be returned
      int numUniqueCombinations = NUM_UNIQUE_RECORDS_PER_SEGMENT * (1 << 2);
      assertEquals(distinctTable.size(), numUniqueCombinations);
      assertTrue(distinctTable.isMainTable());
      Set<List<Integer>> actualValues = new HashSet<>();
      for (Record record : distinctTable.getRecords()) {
        Object[] values = record.getValues();
        int intValue = ((Long) values[0]).intValue();
        List<Integer> actualValueList =
            Arrays.asList(intValue, ((BigDecimal) values[1]).intValue(), ((Float) values[2]).intValue(),
                Integer.parseInt((String) values[3]));
        assertEquals((int) actualValueList.get(1), intValue);
        List<Integer> expectedMVValues = new ArrayList<>(2);
        expectedMVValues.add(intValue);
        expectedMVValues.add(intValue + NUM_UNIQUE_RECORDS_PER_SEGMENT);
        assertTrue(expectedMVValues.contains(actualValueList.get(2)));
        assertTrue(expectedMVValues.contains(actualValueList.get(3)));
        actualValues.add(actualValueList);
      }
      assertEquals(actualValues.size(), numUniqueCombinations);
    }

    // Selecting some columns with filter
    // SELECT DISTINCT stringColumn, bytesColumn, intMVColumn FROM testTable WHERE intColumn >= 60 LIMIT 10000
    {
      DistinctTable distinctTable = getDistinctTableInnerSegment(queries[3]);

      // Check data schema
      DataSchema expectedDataSchema = new DataSchema(new String[]{"stringColumn", "bytesColumn", "intMVColumn"},
          new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.BYTES, ColumnDataType.INT});
      assertEquals(distinctTable.getDataSchema(), expectedDataSchema);

      // Check values, where 40 * 2 matched combinations should be returned
      int numMatchedCombinations = (NUM_UNIQUE_RECORDS_PER_SEGMENT - 60) * 2;
      assertEquals(distinctTable.size(), numMatchedCombinations);
      assertFalse(distinctTable.isMainTable());
      Set<List<Integer>> actualValues = new HashSet<>();
      for (Record record : distinctTable.getRecords()) {
        Object[] values = record.getValues();
        int intValue = Integer.parseInt((String) values[0]);
        assertTrue(intValue >= 60);
        List<Integer> actualValueList =
            Arrays.asList(intValue, Integer.parseInt(new String(((ByteArray) values[1]).getBytes(), UTF_8).trim()),
                (Integer) values[2]);
        assertEquals((int) actualValueList.get(1), intValue);
        assertTrue((Integer) values[2] == intValue || (Integer) values[2] == intValue + NUM_UNIQUE_RECORDS_PER_SEGMENT);
        actualValues.add(actualValueList);
      }
      assertEquals(actualValues.size(), numMatchedCombinations);
    }

    // Selecting some columns order by MV column
    // SELECT DISTINCT floatColumn, doubleMVColumn FROM testTable ORDER BY doubleMVColumn DESC
    {
      DistinctTable distinctTable = getDistinctTableInnerSegment(queries[4]);

      // Check data schema
      DataSchema expectedDataSchema = new DataSchema(new String[]{"floatColumn", "doubleMVColumn"},
          new ColumnDataType[]{ColumnDataType.FLOAT, ColumnDataType.DOUBLE});
      assertEquals(distinctTable.getDataSchema(), expectedDataSchema);

      // Check values, where only 10 top values should be returned
      assertEquals(distinctTable.size(), 10);
      assertFalse(distinctTable.isMainTable());
      Set<Integer> expectedValues = new HashSet<>();
      for (int i = 0; i < 10; i++) {
        expectedValues.add(NUM_UNIQUE_RECORDS_PER_SEGMENT * 2 - i - 1);
      }
      Set<Integer> actualValues = new HashSet<>();
      for (Record record : distinctTable.getRecords()) {
        Object[] values = record.getValues();
        int actualValue = ((Double) values[1]).intValue();
        assertEquals(((Float) values[0]).intValue(), actualValue - NUM_UNIQUE_RECORDS_PER_SEGMENT);
        actualValues.add(actualValue);
      }
      assertEquals(actualValues, expectedValues);
    }

    // Selecting some columns order by raw BYTES column
    // SELECT DISTINCT intColumn, rawBytesColumn FROM testTable ORDER BY rawBytesColumn LIMIT 5
    {
      DistinctTable distinctTable = getDistinctTableInnerSegment(queries[5]);

      // Check data schema
      DataSchema expectedDataSchema = new DataSchema(new String[]{"intColumn", "rawBytesColumn"},
          new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.BYTES});
      assertEquals(distinctTable.getDataSchema(), expectedDataSchema);

      // Check values, where only 5 top values sorted in ByteArray format ascending order should be returned
      assertEquals(distinctTable.size(), 5);
      assertTrue(distinctTable.isMainTable());
      // ByteArray of "30", "31", "3130", "3131", "3132" (same as String order because all digits can be encoded with
      // a single byte)
      int[] expectedValues = new int[]{0, 1, 10, 11, 12};
      Iterator<Record> iterator = distinctTable.getFinalResult();
      for (int i = 0; i < 5; i++) {
        Object[] values = iterator.next().getValues();
        int intValue = (Integer) values[0];
        assertEquals(intValue, expectedValues[i]);
        assertEquals(Integer.parseInt(new String(((ByteArray) values[1]).getBytes(), UTF_8)), intValue);
      }
    }

    // Selecting some columns transform, filter, order-by and limit
    // SELECT DISTINCT ADD(intColumn, floatColumn), stringColumn FROM testTable WHERE longColumn < 60
    // ORDER BY stringColumn DESC, ADD(intColumn, floatColumn) ASC LIMIT 10
    {
      DistinctTable distinctTable = getDistinctTableInnerSegment(queries[6]);

      // Check data schema
      DataSchema expectedDataSchema = new DataSchema(new String[]{"add(intColumn,floatColumn)", "stringColumn"},
          new ColumnDataType[]{ColumnDataType.DOUBLE, ColumnDataType.STRING});
      assertEquals(distinctTable.getDataSchema(), expectedDataSchema);

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

    // Selecting some columns with filter that does not match any record
    // SELECT DISTINCT floatColumn, longMVColumn FROM testTable WHERE stringColumn = 'a' ORDER BY longMVColumn
    {
      DistinctTable distinctTable = getDistinctTableInnerSegment(queries[7]);

      // Check data schema
      DataSchema expectedDataSchema = new DataSchema(new String[]{"floatColumn", "longMVColumn"},
          new ColumnDataType[]{ColumnDataType.FLOAT, ColumnDataType.LONG});
      assertEquals(distinctTable.getDataSchema(), expectedDataSchema);

      // Check values, where no record should be returned
      assertEquals(distinctTable.size(), 0);
      assertFalse(distinctTable.isMainTable());
    }

    // Selecting all raw MV columns
    // SELECT DISTINCT rawIntMVColumn, rawLongMVColumn, rawFloatMVColumn, rawDoubleMVColumn, rawStringMVColumn
    // FROM testTable LIMIT 10000
    {
      DistinctTable distinctTable = getDistinctTableInnerSegment(queries[8]);

      // Check data schema
      DataSchema expectedDataSchema = new DataSchema(new String[]{
          "rawIntMVColumn", "rawLongMVColumn", "rawFloatMVColumn", "rawDoubleMVColumn", "rawStringMVColumn"
      }, new ColumnDataType[]{
          ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.FLOAT, ColumnDataType.DOUBLE, ColumnDataType.STRING
      });
      assertEquals(distinctTable.getDataSchema(), expectedDataSchema);

      // Check values, where all 100 * 2^5 unique combinations should be returned
      int numUniqueCombinations = NUM_UNIQUE_RECORDS_PER_SEGMENT * (1 << 5);
      assertEquals(distinctTable.size(), numUniqueCombinations);
      assertTrue(distinctTable.isMainTable());
      Set<List<Integer>> actualValues = new HashSet<>();
      for (Record record : distinctTable.getRecords()) {
        Object[] values = record.getValues();
        int intValue = (Integer) values[0];
        List<Integer> actualValueList =
            Arrays.asList(intValue, ((Long) values[1]).intValue(), ((Float) values[2]).intValue(),
                ((Double) values[3]).intValue(), Integer.parseInt((String) values[4]));
        List<Integer> expectedValues = new ArrayList<>(2);
        expectedValues.add(intValue % NUM_UNIQUE_RECORDS_PER_SEGMENT);
        expectedValues.add(intValue % NUM_UNIQUE_RECORDS_PER_SEGMENT + NUM_UNIQUE_RECORDS_PER_SEGMENT);
        for (Integer actualValue : actualValueList) {
          assertTrue(expectedValues.contains(actualValue));
        }
        actualValues.add(actualValueList);
      }
      assertEquals(actualValues.size(), numUniqueCombinations);
    }

    // Selecting some SV columns (including raw) and some raw MV columns
    // SELECT DISTINCT longColumn, rawBigDecimalColumn, rawFloatMVColumn, rawStringMVColumn FROM testTable LIMIT 10000
    {
      DistinctTable distinctTable = getDistinctTableInnerSegment(queries[9]);

      // Check data schema
      DataSchema expectedDataSchema = new DataSchema(new String[]{
          "longColumn", "rawBigDecimalColumn", "rawFloatMVColumn", "rawStringMVColumn"
      }, new ColumnDataType[]{
          ColumnDataType.LONG, ColumnDataType.BIG_DECIMAL, ColumnDataType.FLOAT, ColumnDataType.STRING
      });
      assertEquals(distinctTable.getDataSchema(), expectedDataSchema);

      // Check values, where all 100 * 2^2 unique combinations should be returned
      int numUniqueCombinations = NUM_UNIQUE_RECORDS_PER_SEGMENT * (1 << 2);
      assertEquals(distinctTable.size(), numUniqueCombinations);
      assertTrue(distinctTable.isMainTable());
      Set<List<Integer>> actualValues = new HashSet<>();
      for (Record record : distinctTable.getRecords()) {
        Object[] values = record.getValues();
        int intValue = ((Long) values[0]).intValue();
        List<Integer> actualValueList =
            Arrays.asList(intValue, ((BigDecimal) values[1]).intValue(), ((Float) values[2]).intValue(),
                Integer.parseInt((String) values[3]));
        assertEquals((int) actualValueList.get(1), intValue);
        List<Integer> expectedMVValues = new ArrayList<>(2);
        expectedMVValues.add(intValue);
        expectedMVValues.add(intValue + NUM_UNIQUE_RECORDS_PER_SEGMENT);
        assertTrue(expectedMVValues.contains(actualValueList.get(2)));
        assertTrue(expectedMVValues.contains(actualValueList.get(3)));
        actualValues.add(actualValueList);
      }
      assertEquals(actualValues.size(), numUniqueCombinations);
    }

    // Selecting some columns with filter
    // SELECT DISTINCT stringColumn, bytesColumn, rawIntMVColumn FROM testTable WHERE intColumn >= 60 LIMIT 10000
    {
      DistinctTable distinctTable = getDistinctTableInnerSegment(queries[10]);

      // Check data schema
      DataSchema expectedDataSchema = new DataSchema(new String[]{"stringColumn", "bytesColumn", "rawIntMVColumn"},
          new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.BYTES, ColumnDataType.INT});
      assertEquals(distinctTable.getDataSchema(), expectedDataSchema);

      // Check values, where 40 * 2 matched combinations should be returned
      int numMatchedCombinations = (NUM_UNIQUE_RECORDS_PER_SEGMENT - 60) * 2;
      assertEquals(distinctTable.size(), numMatchedCombinations);
      assertTrue(distinctTable.isMainTable());
      Set<List<Integer>> actualValues = new HashSet<>();
      for (Record record : distinctTable.getRecords()) {
        Object[] values = record.getValues();
        int intValue = Integer.parseInt((String) values[0]);
        assertTrue(intValue >= 60);
        List<Integer> actualValueList =
            Arrays.asList(intValue, Integer.parseInt(new String(((ByteArray) values[1]).getBytes(), UTF_8).trim()),
                (Integer) values[2]);
        assertEquals((int) actualValueList.get(1), intValue);
        assertTrue((Integer) values[2] == intValue || (Integer) values[2] == intValue + NUM_UNIQUE_RECORDS_PER_SEGMENT);
        actualValues.add(actualValueList);
      }
      assertEquals(actualValues.size(), numMatchedCombinations);
    }

    // Selecting some columns order by raw MV column
    // SELECT DISTINCT floatColumn, rawDoubleMVColumn FROM testTable ORDER BY rawDoubleMVColumn DESC
    {
      DistinctTable distinctTable = getDistinctTableInnerSegment(queries[11]);

      // Check data schema
      DataSchema expectedDataSchema = new DataSchema(new String[]{"floatColumn", "rawDoubleMVColumn"},
          new ColumnDataType[]{ColumnDataType.FLOAT, ColumnDataType.DOUBLE});
      assertEquals(distinctTable.getDataSchema(), expectedDataSchema);

      // Check values, where only 10 top values should be returned
      assertEquals(distinctTable.size(), 10);
      assertTrue(distinctTable.isMainTable());
      Set<Integer> expectedValues = new HashSet<>();
      for (int i = 0; i < 10; i++) {
        expectedValues.add(NUM_UNIQUE_RECORDS_PER_SEGMENT * 2 - i - 1);
      }
      Set<Integer> actualValues = new HashSet<>();
      for (Record record : distinctTable.getRecords()) {
        Object[] values = record.getValues();
        int actualValue = ((Double) values[1]).intValue();
        assertEquals(((Float) values[0]).intValue(), actualValue - NUM_UNIQUE_RECORDS_PER_SEGMENT);
        actualValues.add(actualValue);
      }
      assertEquals(actualValues, expectedValues);
    }

    // Selecting some columns with filter that does not match any record
    // SELECT DISTINCT floatColumn, rawLongMVColumn FROM testTable WHERE stringColumn = 'a' ORDER BY rawLongMVColumn
    {
      DistinctTable distinctTable = getDistinctTableInnerSegment(queries[12]);

      // Check data schema
      DataSchema expectedDataSchema = new DataSchema(new String[]{"floatColumn", "rawLongMVColumn"},
          new ColumnDataType[]{ColumnDataType.FLOAT, ColumnDataType.LONG});
      assertEquals(distinctTable.getDataSchema(), expectedDataSchema);

      // Check values, where no record should be returned
      assertEquals(distinctTable.size(), 0);
      assertTrue(distinctTable.isMainTable());
    }
  }

  /**
   * Test DISTINCT query within a single segment.
   * <p>The following query types are tested:
   * <ul>
   *   <li>Selecting all dictionary-encoded SV columns</li>
   *   <li>Selecting all dictionary-encoded MV columns</li>
   *   <li>Selecting some SV columns (including raw) and some MV columns</li>
   *   <li>Selecting some columns with filter</li>
   *   <li>Selecting some columns order by MV column</li>
   *   <li>Selecting some columns order by raw BYTES column</li>
   *   <li>Selecting some columns transform, filter, order-by and limit</li>
   *   <li>Selecting some columns with filter that does not match any record</li>
   *   <li>Selecting all dictionary-encoded raw MV columns</li>
   *   <li>Selecting some SV columns (including raw) and some raw MV columns</li>
   *   <li>Selecting some columns with filter with raw MV</li>
   *   <li>Selecting some columns order by raw MV column</li>
   *   <li>Selecting some columns with filter that does not match any record with raw MV</li>
   * </ul>
   */
  @Test
  public void testDistinctInnerSegment() {
    //@formatter:off
    testDistinctInnerSegmentHelper(new String[]{
        "SELECT DISTINCT intColumn, longColumn, floatColumn, doubleColumn, bigDecimalColumn, stringColumn, bytesColumn "
            + "FROM testTable LIMIT 10000",
        "SELECT DISTINCT intMVColumn, longMVColumn, floatMVColumn, doubleMVColumn, stringMVColumn FROM testTable "
            + "LIMIT 10000",
        "SELECT DISTINCT longColumn, rawBigDecimalColumn, floatMVColumn, stringMVColumn FROM testTable LIMIT 10000",
        "SELECT DISTINCT stringColumn, bytesColumn, intMVColumn FROM testTable WHERE intColumn >= 60 LIMIT 10000",
        "SELECT DISTINCT floatColumn, doubleMVColumn FROM testTable ORDER BY doubleMVColumn DESC",
        "SELECT DISTINCT intColumn, rawBytesColumn FROM testTable ORDER BY rawBytesColumn LIMIT 5",
        "SELECT DISTINCT ADD(intColumn, floatColumn), stringColumn FROM testTable WHERE longColumn < 60 "
            + "ORDER BY stringColumn DESC, ADD(intColumn, floatColumn) ASC LIMIT 10",
        "SELECT DISTINCT floatColumn, longMVColumn FROM testTable WHERE stringColumn = 'a' ORDER BY longMVColumn",
        "SELECT DISTINCT rawIntMVColumn, rawLongMVColumn, rawFloatMVColumn, rawDoubleMVColumn, rawStringMVColumn "
            + "FROM testTable LIMIT 10000",
        "SELECT DISTINCT longColumn, rawBigDecimalColumn, rawFloatMVColumn, rawStringMVColumn FROM testTable "
            + "LIMIT 10000",
        "SELECT DISTINCT stringColumn, bytesColumn, rawIntMVColumn FROM testTable WHERE rawIntColumn >= 60 LIMIT 10000",
        "SELECT DISTINCT floatColumn, rawDoubleMVColumn FROM testTable ORDER BY rawDoubleMVColumn DESC",
        "SELECT DISTINCT floatColumn, rawLongMVColumn FROM testTable WHERE stringColumn = 'a' ORDER BY rawLongMVColumn"
    });
    //@formatter:on
  }

  /**
   * Test Non-Aggregation GroupBy query rewrite to Distinct query within a single segment.
   * <p>The following query types are tested:
   * <ul>
   *   <li>Selecting all dictionary-encoded SV columns</li>
   *   <li>Selecting all dictionary-encoded MV columns</li>
   *   <li>Selecting some SV columns (including raw) and some MV columns</li>
   *   <li>Selecting some columns with filter</li>
   *   <li>Selecting some columns order by MV column</li>
   *   <li>Selecting some columns order by raw BYTES column</li>
   *   <li>Selecting some columns transform, filter, order-by and limit</li>
   *   <li>Selecting some columns with filter that does not match any record</li>
   *   <li>Selecting all dictionary-encoded raw MV columns</li>
   *   <li>Selecting some SV columns (including raw) and some raw MV columns</li>
   *   <li>Selecting some columns with filter with raw MV</li>
   *   <li>Selecting some columns order by raw MV column</li>
   *   <li>Selecting some columns with filter that does not match any record with raw MV</li>
   * </ul>
   */
  @Test
  public void testNonAggGroupByRewriteToDistinctInnerSegment() {
    //@formatter:off
    testDistinctInnerSegmentHelper(new String[]{
        "SELECT intColumn, longColumn, floatColumn, doubleColumn, bigDecimalColumn, stringColumn, bytesColumn "
            + "FROM testTable "
            + "GROUP BY intColumn, longColumn, floatColumn, doubleColumn, bigDecimalColumn, stringColumn, bytesColumn "
            + "LIMIT 10000",
        "SELECT intMVColumn, longMVColumn, floatMVColumn, doubleMVColumn, stringMVColumn FROM testTable "
            + "GROUP BY intMVColumn, longMVColumn, floatMVColumn, doubleMVColumn, stringMVColumn LIMIT 10000",
        "SELECT longColumn, rawBigDecimalColumn, floatMVColumn, stringMVColumn FROM testTable "
            + "GROUP BY longColumn, rawBigDecimalColumn, floatMVColumn, stringMVColumn LIMIT 10000",
        "SELECT stringColumn, bytesColumn, intMVColumn FROM testTable WHERE intColumn >= 60 "
            + "GROUP BY stringColumn, bytesColumn, intMVColumn LIMIT 10000",
        "SELECT floatColumn, doubleMVColumn FROM testTable "
            + "GROUP BY floatColumn, doubleMVColumn ORDER BY doubleMVColumn DESC",
        "SELECT intColumn, rawBytesColumn FROM testTable "
            + "GROUP BY intColumn, rawBytesColumn ORDER BY rawBytesColumn LIMIT 5",
        "SELECT ADD(intColumn, floatColumn), stringColumn FROM testTable WHERE longColumn < 60 "
            + "GROUP BY ADD(intColumn, floatColumn), stringColumn "
            + "ORDER BY stringColumn DESC, ADD(intColumn, floatColumn) ASC LIMIT 10",
        "SELECT floatColumn, longMVColumn FROM testTable WHERE stringColumn = 'a' "
            + "GROUP BY floatColumn, longMVColumn ORDER BY longMVColumn",
        "SELECT rawIntMVColumn, rawLongMVColumn, rawFloatMVColumn, rawDoubleMVColumn, rawStringMVColumn "
            + "FROM testTable GROUP BY rawIntMVColumn, rawLongMVColumn, rawFloatMVColumn, rawDoubleMVColumn, "
            + "rawStringMVColumn LIMIT 10000",
        "SELECT longColumn, rawBigDecimalColumn, rawFloatMVColumn, rawStringMVColumn FROM testTable "
            + "GROUP BY longColumn, rawBigDecimalColumn, rawFloatMVColumn, rawStringMVColumn LIMIT 10000",
        "SELECT stringColumn, bytesColumn, rawIntMVColumn FROM testTable WHERE rawIntColumn >= 60 "
            + "GROUP BY stringColumn, bytesColumn, rawIntMVColumn LIMIT 10000",
        "SELECT floatColumn, rawDoubleMVColumn FROM testTable GROUP BY floatColumn, rawDoubleMVColumn "
            + "ORDER BY rawDoubleMVColumn DESC",
        "SELECT floatColumn, rawLongMVColumn FROM testTable WHERE stringColumn = 'a' GROUP BY floatColumn, "
            + "rawLongMVColumn ORDER BY rawLongMVColumn"
    });
    //@formatter:on
  }

  /**
   * Helper method to get the DistinctTable result for one single segment for the given query.
   */
  private DistinctTable getDistinctTableInnerSegment(String query) {
    BaseOperator<DistinctResultsBlock> distinctOperator = getOperator(query);
    DistinctTable distinctTable = distinctOperator.nextBlock().getDistinctTable();
    assertNotNull(distinctTable);
    return distinctTable;
  }

  /**
   * Test DISTINCT query across multiple segments and servers (2 servers, each with 2 segments).
   * <p>The following query types are tested:
   * <ul>
   *   <li>Selecting all dictionary-encoded SV columns</li>
   *   <li>Selecting all dictionary-encoded MV columns</li>
   *   <li>Selecting some SV columns (including raw) and some MV columns</li>
   *   <li>Selecting some columns with filter</li>
   *   <li>Selecting some columns order by MV column</li>
   *   <li>Selecting some columns order by raw BYTES column</li>
   *   <li>Selecting some columns transform, filter, order-by and limit</li>
   *   <li>Selecting some columns with filter that does not match any record</li>
   *   <li>
   *     Selecting some columns with filter that does not match any record in one segment but matches some records in
   *     the other segment
   *   </li>
   *   <li>Selecting all dictionary-encoded raw MV columns</li>
   *   <li>Selecting some SV columns (including raw) and some raw MV columns</li>
   *   <li>Selecting some columns with filter with raw MV</li>
   *   <li>Selecting some columns order by raw MV column</li>
   *   <li>Selecting some columns with filter that does not match any record with raw MV</li>
   *   TODO: Support alias and add a test for that
   * </ul>
   */
  private void testDistinctInterSegmentHelper(String[] queries) {
    assertEquals(queries.length, 14);

    // Selecting all dictionary-encoded SV columns
    // SELECT DISTINCT intColumn, longColumn, floatColumn, doubleColumn, bigDecimalColumn, stringColumn, bytesColumn
    // FROM testTable LIMIT 10000
    {
      ResultTable resultTable = getBrokerResponse(queries[0]).getResultTable();

      // Check data schema
      DataSchema expectedDataSchema = new DataSchema(new String[]{
          "intColumn", "longColumn", "floatColumn", "doubleColumn", "bigDecimalColumn", "stringColumn", "bytesColumn"
      }, new ColumnDataType[]{
          ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.FLOAT, ColumnDataType.DOUBLE,
          ColumnDataType.BIG_DECIMAL, ColumnDataType.STRING, ColumnDataType.BYTES
      });
      assertEquals(resultTable.getDataSchema(), expectedDataSchema);

      // Check values, where all 200 unique values should be returned
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), 2 * NUM_UNIQUE_RECORDS_PER_SEGMENT);
      Set<Integer> expectedValues = new HashSet<>();
      for (int i = 0; i < NUM_UNIQUE_RECORDS_PER_SEGMENT; i++) {
        expectedValues.add(i);
        expectedValues.add(1000 + i);
      }
      Set<Integer> actualValues = new HashSet<>();
      for (Object[] row : rows) {
        int intValue = (Integer) row[0];
        assertEquals(((Long) row[1]).intValue(), intValue);
        assertEquals(((Float) row[2]).intValue(), intValue);
        assertEquals(((Double) row[3]).intValue(), intValue);
        assertEquals(Integer.parseInt((String) row[4]), intValue);
        assertEquals(Integer.parseInt((String) row[5]), intValue);
        assertEquals(new String(BytesUtils.toBytes((String) row[6]), UTF_8).trim(), row[5]);
        actualValues.add(intValue);
      }
      assertEquals(actualValues, expectedValues);
    }

    // Selecting all dictionary-encoded MV columns
    // SELECT DISTINCT intMVColumn, longMVColumn, floatMVColumn, doubleMVColumn, stringMVColumn FROM testTable
    // LIMIT 10000
    {
      ResultTable resultTable = getBrokerResponse(queries[1]).getResultTable();

      // Check data schema
      DataSchema expectedDataSchema = new DataSchema(new String[]{
          "intMVColumn", "longMVColumn", "floatMVColumn", "doubleMVColumn", "stringMVColumn"
      }, new ColumnDataType[]{
          ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.FLOAT, ColumnDataType.DOUBLE, ColumnDataType.STRING
      });
      assertEquals(resultTable.getDataSchema(), expectedDataSchema);

      // Check values, where all 200 * 2^5 unique values should be returned
      int numUniqueCombinations = 2 * NUM_UNIQUE_RECORDS_PER_SEGMENT * (1 << 5);
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), numUniqueCombinations);
      Set<List<Integer>> actualValues = new HashSet<>();
      for (Object[] row : rows) {
        int intValue = (Integer) row[0];
        List<Integer> actualValueList = Arrays.asList(intValue, ((Long) row[1]).intValue(), ((Float) row[2]).intValue(),
            ((Double) row[3]).intValue(), Integer.parseInt((String) row[4]));
        List<Integer> expectedValues = new ArrayList<>(2);
        if (intValue < 1000) {
          expectedValues.add(intValue % NUM_UNIQUE_RECORDS_PER_SEGMENT);
          expectedValues.add(intValue % NUM_UNIQUE_RECORDS_PER_SEGMENT + NUM_UNIQUE_RECORDS_PER_SEGMENT);
        } else {
          expectedValues.add(intValue % NUM_UNIQUE_RECORDS_PER_SEGMENT + 1000);
          expectedValues.add(intValue % NUM_UNIQUE_RECORDS_PER_SEGMENT + NUM_UNIQUE_RECORDS_PER_SEGMENT + 1000);
        }
        for (Integer actualValue : actualValueList) {
          assertTrue(expectedValues.contains(actualValue));
        }
        actualValues.add(actualValueList);
      }
      assertEquals(actualValues.size(), numUniqueCombinations);
    }

    // Selecting some SV columns (including raw) and some MV columns
    // SELECT DISTINCT longColumn, rawBigDecimalColumn, floatMVColumn, stringMVColumn FROM testTable LIMIT 10000
    {
      ResultTable resultTable = getBrokerResponse(queries[2]).getResultTable();

      // Check data schema
      DataSchema expectedDataSchema = new DataSchema(new String[]{
          "longColumn", "rawBigDecimalColumn", "floatMVColumn", "stringMVColumn"
      }, new ColumnDataType[]{
          ColumnDataType.LONG, ColumnDataType.BIG_DECIMAL, ColumnDataType.FLOAT, ColumnDataType.STRING
      });
      assertEquals(resultTable.getDataSchema(), expectedDataSchema);

      // Check values, where all 200 * 2^2 unique values should be returned
      int numUniqueCombinations = 2 * NUM_UNIQUE_RECORDS_PER_SEGMENT * (1 << 2);
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), numUniqueCombinations);
      Set<List<Integer>> actualValues = new HashSet<>();
      for (Object[] row : rows) {
        int intValue = ((Long) row[0]).intValue();
        List<Integer> actualValueList =
            Arrays.asList(intValue, Integer.parseInt((String) row[1]), ((Float) row[2]).intValue(),
                Integer.parseInt((String) row[3]));
        assertEquals((int) actualValueList.get(1), intValue);
        List<Integer> expectedMVValues = new ArrayList<>(2);
        expectedMVValues.add(intValue);
        expectedMVValues.add(intValue + NUM_UNIQUE_RECORDS_PER_SEGMENT);
        assertTrue(expectedMVValues.contains(actualValueList.get(2)));
        assertTrue(expectedMVValues.contains(actualValueList.get(3)));
        actualValues.add(actualValueList);
      }
      assertEquals(actualValues.size(), numUniqueCombinations);
    }

    // Selecting some columns with filter
    // SELECT DISTINCT stringColumn, bytesColumn, intMVColumn FROM testTable WHERE intColumn >= 60 LIMIT 10000
    {
      ResultTable resultTable = getBrokerResponse(queries[3]).getResultTable();

      // Check data schema
      DataSchema expectedDataSchema = new DataSchema(new String[]{"stringColumn", "bytesColumn", "intMVColumn"},
          new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.BYTES, ColumnDataType.INT});
      assertEquals(resultTable.getDataSchema(), expectedDataSchema);

      // Check values, where 140 * 2 matched values should be returned
      int numMatchedCombinations = (2 * NUM_UNIQUE_RECORDS_PER_SEGMENT - 60) * 2;
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), numMatchedCombinations);
      Set<List<Integer>> actualValues = new HashSet<>();
      for (Object[] row : rows) {
        int intValue = Integer.parseInt((String) row[0]);
        assertTrue(intValue >= 60);
        List<Integer> actualValueList =
            Arrays.asList(intValue, Integer.parseInt(new String(BytesUtils.toBytes((String) row[1]), UTF_8).trim()),
                (Integer) row[2]);
        assertEquals((int) actualValueList.get(1), intValue);
        assertTrue((Integer) row[2] == intValue || (Integer) row[2] == intValue + NUM_UNIQUE_RECORDS_PER_SEGMENT);
        actualValues.add(actualValueList);
      }
      assertEquals(actualValues.size(), numMatchedCombinations);
    }

    // Selecting some columns order by MV column
    // SELECT DISTINCT floatColumn, doubleMVColumn FROM testTable ORDER BY doubleMVColumn DESC
    {
      ResultTable resultTable = getBrokerResponse(queries[4]).getResultTable();

      // Check data schema
      DataSchema expectedDataSchema = new DataSchema(new String[]{"floatColumn", "doubleMVColumn"},
          new ColumnDataType[]{ColumnDataType.FLOAT, ColumnDataType.DOUBLE});
      assertEquals(resultTable.getDataSchema(), expectedDataSchema);

      // Check values, where only 10 top values should be returned
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), 10);
      for (int i = 0; i < 10; i++) {
        int expectedValue = NUM_UNIQUE_RECORDS_PER_SEGMENT * 2 + 1000 - i - 1;
        Object[] row = rows.get(i);
        assertEquals(((Float) row[0]).intValue(), expectedValue - NUM_UNIQUE_RECORDS_PER_SEGMENT);
        assertEquals(((Double) row[1]).intValue(), expectedValue);
      }
    }

    // Selecting some columns order by raw BYTES column
    // SELECT DISTINCT intColumn, rawBytesColumn FROM testTable ORDER BY rawBytesColumn LIMIT 5
    {
      ResultTable resultTable = getBrokerResponse(queries[5]).getResultTable();

      // Check data schema
      DataSchema expectedDataSchema = new DataSchema(new String[]{"intColumn", "rawBytesColumn"},
          new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.BYTES});
      assertEquals(resultTable.getDataSchema(), expectedDataSchema);

      // Check values, where only 5 top values sorted in ByteArray format ascending order should be returned
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), 5);
      // ByteArray of "30", "31", "3130", "31303030", "31303031" (same as String order because all digits can be
      // encoded with a single byte)
      int[] expectedValues = new int[]{0, 1, 10, 1000, 1001};
      for (int i = 0; i < 5; i++) {
        Object[] row = rows.get(i);
        int intValue = (Integer) row[0];
        assertEquals(intValue, expectedValues[i]);
        assertEquals(Integer.parseInt(new String(BytesUtils.toBytes((String) row[1]), UTF_8)), intValue);
      }
    }

    // Selecting some columns transform, filter, order-by and limit
    // SELECT DISTINCT ADD(intColumn, floatColumn), stringColumn FROM testTable WHERE longColumn < 60
    // ORDER BY stringColumn DESC, ADD(intColumn, floatColumn) ASC LIMIT 10
    {
      ResultTable resultTable = getBrokerResponse(queries[6]).getResultTable();

      // Check data schema
      DataSchema expectedDataSchema = new DataSchema(new String[]{"add(intColumn,floatColumn)", "stringColumn"},
          new ColumnDataType[]{ColumnDataType.DOUBLE, ColumnDataType.STRING});
      assertEquals(resultTable.getDataSchema(), expectedDataSchema);

      // Check values, where only 10 top values sorted in string format descending order should be returned
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), 10);
      int[] expectedValues = new int[]{9, 8, 7, 6, 59, 58, 57, 56, 55, 54};
      for (int i = 0; i < 10; i++) {
        Object[] row = rows.get(i);
        int intValue = ((Double) row[0]).intValue() / 2;
        assertEquals(intValue, expectedValues[i]);
        assertEquals(Integer.parseInt((String) row[1]), intValue);
      }
    }

    // Selecting some columns with filter that does not match any record
    // SELECT DISTINCT floatColumn, longMVColumn FROM testTable WHERE stringColumn = 'a' ORDER BY longMVColumn
    {
      ResultTable resultTable = getBrokerResponse(queries[7]).getResultTable();

      // Check data schema
      // NOTE: Segment pruner is not wired up in QueriesTest, and the correct column data types should be returned.
      DataSchema expectedDataSchema = new DataSchema(new String[]{"floatColumn", "longMVColumn"},
          new ColumnDataType[]{ColumnDataType.FLOAT, ColumnDataType.LONG});
      assertEquals(resultTable.getDataSchema(), expectedDataSchema);

      // Check values, where no record should be returned
      assertTrue(resultTable.getRows().isEmpty());
    }

    // Selecting some columns with filter that does not match any record in one segment but matches some records in the
    // other segment
    // SELECT DISTINCT intColumn FROM testTable WHERE floatColumn > 200 ORDER BY intColumn ASC LIMIT 5
    {
      ResultTable resultTable = getBrokerResponse(queries[8]).getResultTable();

      // Check data schema
      DataSchema expectedDataSchema =
          new DataSchema(new String[]{"intColumn"}, new ColumnDataType[]{ColumnDataType.INT});
      assertEquals(resultTable.getDataSchema(), expectedDataSchema);

      // Check values, where only 5 top values sorted in int format ascending order should be returned
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), 5);
      int[] expectedValues = new int[]{1000, 1001, 1002, 1003, 1004};
      for (int i = 0; i < 5; i++) {
        assertEquals((int) rows.get(i)[0], expectedValues[i]);
      }
    }

    // Selecting all dictionary-encoded raw MV columns
    // SELECT DISTINCT rawIntMVColumn, rawLongMVColumn, rawFloatMVColumn, rawDoubleMVColumn, rawStringMVColumn
    // FROM testTable LIMIT 10000
    {
      ResultTable resultTable = getBrokerResponse(queries[9]).getResultTable();

      // Check data schema
      DataSchema expectedDataSchema = new DataSchema(new String[]{
          "rawIntMVColumn", "rawLongMVColumn", "rawFloatMVColumn", "rawDoubleMVColumn", "rawStringMVColumn"
      }, new ColumnDataType[]{
          ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.FLOAT, ColumnDataType.DOUBLE, ColumnDataType.STRING
      });
      assertEquals(resultTable.getDataSchema(), expectedDataSchema);

      // Check values, where all 200 * 2^5 unique values should be returned
      int numUniqueCombinations = 2 * NUM_UNIQUE_RECORDS_PER_SEGMENT * (1 << 5);
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), numUniqueCombinations);
      Set<List<Integer>> actualValues = new HashSet<>();
      for (Object[] row : rows) {
        int intValue = (Integer) row[0];
        List<Integer> actualValueList = Arrays.asList(intValue, ((Long) row[1]).intValue(), ((Float) row[2]).intValue(),
            ((Double) row[3]).intValue(), Integer.parseInt((String) row[4]));
        List<Integer> expectedValues = new ArrayList<>(2);
        if (intValue < 1000) {
          expectedValues.add(intValue % NUM_UNIQUE_RECORDS_PER_SEGMENT);
          expectedValues.add(intValue % NUM_UNIQUE_RECORDS_PER_SEGMENT + NUM_UNIQUE_RECORDS_PER_SEGMENT);
        } else {
          expectedValues.add(intValue % NUM_UNIQUE_RECORDS_PER_SEGMENT + 1000);
          expectedValues.add(intValue % NUM_UNIQUE_RECORDS_PER_SEGMENT + NUM_UNIQUE_RECORDS_PER_SEGMENT + 1000);
        }
        for (Integer actualValue : actualValueList) {
          assertTrue(expectedValues.contains(actualValue));
        }
        actualValues.add(actualValueList);
      }
      assertEquals(actualValues.size(), numUniqueCombinations);
    }

    // Selecting some SV columns (including raw) and some raw MV columns
    // SELECT DISTINCT longColumn, rawBigDecimalColumn, rawFloatMVColumn, rawStringMVColumn FROM testTable LIMIT 10000
    {
      ResultTable resultTable = getBrokerResponse(queries[10]).getResultTable();

      // Check data schema
      DataSchema expectedDataSchema = new DataSchema(new String[]{
          "longColumn", "rawBigDecimalColumn", "rawFloatMVColumn", "rawStringMVColumn"
      }, new ColumnDataType[]{
          ColumnDataType.LONG, ColumnDataType.BIG_DECIMAL, ColumnDataType.FLOAT, ColumnDataType.STRING
      });
      assertEquals(resultTable.getDataSchema(), expectedDataSchema);

      // Check values, where all 200 * 2^2 unique values should be returned
      int numUniqueCombinations = 2 * NUM_UNIQUE_RECORDS_PER_SEGMENT * (1 << 2);
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), numUniqueCombinations);
      Set<List<Integer>> actualValues = new HashSet<>();
      for (Object[] row : rows) {
        int intValue = ((Long) row[0]).intValue();
        List<Integer> actualValueList =
            Arrays.asList(intValue, Integer.parseInt((String) row[1]), ((Float) row[2]).intValue(),
                Integer.parseInt((String) row[3]));
        assertEquals((int) actualValueList.get(1), intValue);
        List<Integer> expectedMVValues = new ArrayList<>(2);
        expectedMVValues.add(intValue);
        expectedMVValues.add(intValue + NUM_UNIQUE_RECORDS_PER_SEGMENT);
        assertTrue(expectedMVValues.contains(actualValueList.get(2)));
        assertTrue(expectedMVValues.contains(actualValueList.get(3)));
        actualValues.add(actualValueList);
      }
      assertEquals(actualValues.size(), numUniqueCombinations);
    }

    // Selecting some columns with filter
    // SELECT DISTINCT stringColumn, bytesColumn, rawIntMVColumn FROM testTable WHERE intColumn >= 60 LIMIT 10000
    {
      ResultTable resultTable = getBrokerResponse(queries[11]).getResultTable();

      // Check data schema
      DataSchema expectedDataSchema = new DataSchema(new String[]{"stringColumn", "bytesColumn", "rawIntMVColumn"},
          new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.BYTES, ColumnDataType.INT});
      assertEquals(resultTable.getDataSchema(), expectedDataSchema);

      // Check values, where 140 * 2 matched values should be returned
      int numMatchedCombinations = (2 * NUM_UNIQUE_RECORDS_PER_SEGMENT - 60) * 2;
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), numMatchedCombinations);
      Set<List<Integer>> actualValues = new HashSet<>();
      for (Object[] row : rows) {
        int intValue = Integer.parseInt((String) row[0]);
        assertTrue(intValue >= 60);
        List<Integer> actualValueList =
            Arrays.asList(intValue, Integer.parseInt(new String(BytesUtils.toBytes((String) row[1]), UTF_8).trim()),
                (Integer) row[2]);
        assertEquals((int) actualValueList.get(1), intValue);
        assertTrue((Integer) row[2] == intValue || (Integer) row[2] == intValue + NUM_UNIQUE_RECORDS_PER_SEGMENT);
        actualValues.add(actualValueList);
      }
      assertEquals(actualValues.size(), numMatchedCombinations);
    }

    // Selecting some columns order by raw MV column
    // SELECT DISTINCT floatColumn, rawDoubleMVColumn FROM testTable ORDER BY rawDoubleMVColumn DESC
    {
      ResultTable resultTable = getBrokerResponse(queries[12]).getResultTable();

      // Check data schema
      DataSchema expectedDataSchema = new DataSchema(new String[]{"floatColumn", "rawDoubleMVColumn"},
          new ColumnDataType[]{ColumnDataType.FLOAT, ColumnDataType.DOUBLE});
      assertEquals(resultTable.getDataSchema(), expectedDataSchema);

      // Check values, where only 10 top values should be returned
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), 10);
      for (int i = 0; i < 10; i++) {
        int expectedValue = NUM_UNIQUE_RECORDS_PER_SEGMENT * 2 + 1000 - i - 1;
        Object[] row = rows.get(i);
        assertEquals(((Float) row[0]).intValue(), expectedValue - NUM_UNIQUE_RECORDS_PER_SEGMENT);
        assertEquals(((Double) row[1]).intValue(), expectedValue);
      }
    }

    // Selecting some columns with filter that does not match any record
    // SELECT DISTINCT floatColumn, rawLongMVColumn FROM testTable WHERE stringColumn = 'a' ORDER BY rawLongMVColumn
    {
      ResultTable resultTable = getBrokerResponse(queries[13]).getResultTable();

      // Check data schema
      // NOTE: Segment pruner is not wired up in QueriesTest, and the correct column data types should be returned.
      DataSchema expectedDataSchema = new DataSchema(new String[]{"floatColumn", "rawLongMVColumn"},
          new ColumnDataType[]{ColumnDataType.FLOAT, ColumnDataType.LONG});
      assertEquals(resultTable.getDataSchema(), expectedDataSchema);

      // Check values, where no record should be returned
      assertTrue(resultTable.getRows().isEmpty());
    }
  }

  /**
   * Test DISTINCT query across multiple segments and servers (2 servers, each with 2 segments).
   * <p>The following query types are tested:
   * <ul>
   *   <li>Selecting all dictionary-encoded SV columns</li>
   *   <li>Selecting all dictionary-encoded MV columns</li>
   *   <li>Selecting some SV columns (including raw) and some MV columns</li>
   *   <li>Selecting some columns with filter</li>
   *   <li>Selecting some columns order by MV column</li>
   *   <li>Selecting some columns order by raw BYTES column</li>
   *   <li>Selecting some columns transform, filter, order-by and limit</li>
   *   <li>Selecting some columns with filter that does not match any record</li>
   *   <li>
   *     Selecting some columns with filter that does not match any record in one segment but matches some records in
   *     the other segment
   *   </li>
   *   <li>Selecting all dictionary-encoded raw MV columns</li>
   *   <li>Selecting some SV columns (including raw) and some raw MV columns</li>
   *   <li>Selecting some columns with filter with raw MV</li>
   *   <li>Selecting some columns order by raw MV column</li>
   *   <li>Selecting some columns with filter that does not match any record with raw MV</li>
   *   TODO: Support alias and add a test for that
   * </ul>
   */
  @Test
  public void testDistinctInterSegment() {
    //@formatter:off
    testDistinctInterSegmentHelper(new String[]{
        "SELECT DISTINCT intColumn, longColumn, floatColumn, doubleColumn, bigDecimalColumn, stringColumn, bytesColumn "
            + "FROM testTable LIMIT 10000",
        "SELECT DISTINCT intMVColumn, longMVColumn, floatMVColumn, doubleMVColumn, stringMVColumn FROM testTable "
            + "LIMIT 10000",
        "SELECT DISTINCT longColumn, rawBigDecimalColumn, floatMVColumn, stringMVColumn FROM testTable LIMIT 10000",
        "SELECT DISTINCT stringColumn, bytesColumn, intMVColumn FROM testTable WHERE intColumn >= 60 LIMIT 10000",
        "SELECT DISTINCT floatColumn, doubleMVColumn FROM testTable ORDER BY doubleMVColumn DESC",
        "SELECT DISTINCT intColumn, rawBytesColumn FROM testTable ORDER BY rawBytesColumn LIMIT 5",
        "SELECT DISTINCT ADD(intColumn, floatColumn), stringColumn FROM testTable WHERE longColumn < 60 "
            + "ORDER BY stringColumn DESC, ADD(intColumn, floatColumn) ASC LIMIT 10",
        "SELECT DISTINCT floatColumn, longMVColumn FROM testTable WHERE stringColumn = 'a' ORDER BY longMVColumn",
        "SELECT DISTINCT intColumn FROM testTable WHERE floatColumn > 200 ORDER BY intColumn ASC LIMIT 5",
        "SELECT DISTINCT rawIntMVColumn, rawLongMVColumn, rawFloatMVColumn, rawDoubleMVColumn, rawStringMVColumn "
            + "FROM testTable LIMIT 10000",
        "SELECT DISTINCT longColumn, rawBigDecimalColumn, rawFloatMVColumn, rawStringMVColumn FROM testTable "
            + "LIMIT 10000",
        "SELECT DISTINCT stringColumn, bytesColumn, rawIntMVColumn FROM testTable WHERE intColumn >= 60 LIMIT 10000",
        "SELECT DISTINCT floatColumn, rawDoubleMVColumn FROM testTable ORDER BY rawDoubleMVColumn DESC",
        "SELECT DISTINCT floatColumn, rawLongMVColumn FROM testTable WHERE stringColumn = 'a' ORDER BY rawLongMVColumn"
    });
    //@formatter:on
  }

  /**
   * Test Non-Aggregation GroupBy query rewrite to Distinct query across multiple segments and servers (2 servers,
   * each with 2 segments).
   * <p>The following query types are tested:
   * <ul>
   *   <li>Selecting all dictionary-encoded SV columns</li>
   *   <li>Selecting all dictionary-encoded MV columns</li>
   *   <li>Selecting some SV columns (including raw) and some MV columns</li>
   *   <li>Selecting some columns with filter</li>
   *   <li>Selecting some columns order by MV column</li>
   *   <li>Selecting some columns order by raw BYTES column</li>
   *   <li>Selecting some columns transform, filter, order-by and limit</li>
   *   <li>Selecting some columns with filter that does not match any record</li>
   *   <li>
   *     Selecting some columns with filter that does not match any record in one segment but matches some records in
   *     the other segment
   *   </li>
   *   <li>Selecting all dictionary-encoded raw MV columns</li>
   *   <li>Selecting some SV columns (including raw) and some raw MV columns</li>
   *   <li>Selecting some columns with filter with raw MV</li>
   *   <li>Selecting some columns order by raw MV column</li>
   *   <li>Selecting some columns with filter that does not match any record with raw MV</li>
   *   TODO: Support alias and add a test for that
   * </ul>
   */
  @Test
  public void testNonAggGroupByRewriteToDistinctInterSegment() {
    //@formatter:off
    testDistinctInterSegmentHelper(new String[]{
        "SELECT intColumn, longColumn, floatColumn, doubleColumn, bigDecimalColumn, stringColumn, bytesColumn "
            + "FROM testTable "
            + "GROUP BY intColumn, longColumn, floatColumn, doubleColumn, bigDecimalColumn, stringColumn, bytesColumn "
            + "LIMIT 10000",
        "SELECT intMVColumn, longMVColumn, floatMVColumn, doubleMVColumn, stringMVColumn FROM testTable "
            + "GROUP BY intMVColumn, longMVColumn, floatMVColumn, doubleMVColumn, stringMVColumn LIMIT 10000",
        "SELECT longColumn, rawBigDecimalColumn, floatMVColumn, stringMVColumn FROM testTable "
            + "GROUP BY longColumn, rawBigDecimalColumn, floatMVColumn, stringMVColumn LIMIT 10000",
        "SELECT stringColumn, bytesColumn, intMVColumn FROM testTable WHERE intColumn >= 60 "
            + "GROUP BY stringColumn, bytesColumn, intMVColumn LIMIT 10000",
        "SELECT floatColumn, doubleMVColumn FROM testTable "
            + "GROUP BY floatColumn, doubleMVColumn ORDER BY doubleMVColumn DESC",
        "SELECT intColumn, rawBytesColumn FROM testTable "
            + "GROUP BY intColumn, rawBytesColumn ORDER BY rawBytesColumn LIMIT 5",
        "SELECT ADD(intColumn, floatColumn), stringColumn FROM testTable WHERE longColumn < 60 "
            + "GROUP BY ADD(intColumn, floatColumn), stringColumn "
            + "ORDER BY stringColumn DESC, ADD(intColumn, floatColumn) ASC LIMIT 10",
        "SELECT floatColumn, longMVColumn FROM testTable WHERE stringColumn = 'a' "
            + "GROUP BY floatColumn, longMVColumn ORDER BY longMVColumn",
        "SELECT intColumn FROM testTable WHERE floatColumn > 200 GROUP BY intColumn ORDER BY intColumn ASC LIMIT 5",
        "SELECT rawIntMVColumn, rawLongMVColumn, rawFloatMVColumn, rawDoubleMVColumn, rawStringMVColumn FROM testTable "
            + "GROUP BY rawIntMVColumn, rawLongMVColumn, rawFloatMVColumn, rawDoubleMVColumn, rawStringMVColumn "
            + "LIMIT 10000",
        "SELECT longColumn, rawBigDecimalColumn, rawFloatMVColumn, rawStringMVColumn FROM testTable "
            + "GROUP BY longColumn, rawBigDecimalColumn, rawFloatMVColumn, rawStringMVColumn LIMIT 10000",
        "SELECT stringColumn, bytesColumn, rawIntMVColumn FROM testTable WHERE intColumn >= 60 GROUP BY "
            + "stringColumn, bytesColumn, rawIntMVColumn LIMIT 10000",
        "SELECT floatColumn, rawDoubleMVColumn FROM testTable GROUP BY floatColumn, rawDoubleMVColumn "
            + "ORDER BY rawDoubleMVColumn DESC",
        "SELECT floatColumn, rawLongMVColumn FROM testTable WHERE stringColumn = 'a' GROUP BY floatColumn, "
            + "rawLongMVColumn ORDER BY rawLongMVColumn"
    });
    //@formatter:on
  }
}
