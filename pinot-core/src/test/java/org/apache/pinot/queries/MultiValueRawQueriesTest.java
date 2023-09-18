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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


public class MultiValueRawQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "MultiValueRawQueriesTest");

  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME_1 = "testSegment1";
  private static final String SEGMENT_NAME_2 = "testSegment2";

  private static final int NUM_UNIQUE_RECORDS_PER_SEGMENT = 10;
  private static final int NUM_DUPLICATES_PER_RECORDS = 2;
  private static final int MV_OFFSET = 100;
  private static final int MV_LENGTH = 2; // Entries in each multivalue array
  private static final int BASE_VALUE_1 = 0;
  private static final int BASE_VALUE_2 = 1000;

  private final static String SV_INT_COL = "svIntCol";
  private final static String MV_INT_COL = "mvIntCol";
  private final static String MV_LONG_COL = "mvLongCol";
  private final static String MV_FLOAT_COL = "mvFloatCol";
  private final static String MV_DOUBLE_COL = "mvDoubleCol";
  private final static String MV_STRING_COL = "mvStringCol";
  private final static String MV_STRING_COL_2 = "mvStringCol2";
  private final static String MV_RAW_INT_COL = "mvRawIntCol";
  private final static String MV_RAW_LONG_COL = "mvRawLongCol";
  private final static String MV_RAW_FLOAT_COL = "mvRawFloatCol";
  private final static String MV_RAW_DOUBLE_COL = "mvRawDoubleCol";
  private final static String MV_RAW_STRING_COL = "mvRawStringCol";
  private final static String MV_RAW_STRING_COL_2 = "mvRawStringCol2";

  private static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
      .addSingleValueDimension(SV_INT_COL, FieldSpec.DataType.INT)
      .addMultiValueDimension(MV_INT_COL, FieldSpec.DataType.INT)
      .addMultiValueDimension(MV_LONG_COL, FieldSpec.DataType.LONG)
      .addMultiValueDimension(MV_FLOAT_COL, FieldSpec.DataType.FLOAT)
      .addMultiValueDimension(MV_DOUBLE_COL, FieldSpec.DataType.DOUBLE)
      .addMultiValueDimension(MV_STRING_COL, FieldSpec.DataType.STRING)
      .addMultiValueDimension(MV_STRING_COL_2, FieldSpec.DataType.STRING)
      .addMultiValueDimension(MV_RAW_INT_COL, FieldSpec.DataType.INT)
      .addMultiValueDimension(MV_RAW_LONG_COL, FieldSpec.DataType.LONG)
      .addMultiValueDimension(MV_RAW_FLOAT_COL, FieldSpec.DataType.FLOAT)
      .addMultiValueDimension(MV_RAW_DOUBLE_COL, FieldSpec.DataType.DOUBLE)
      .addMultiValueDimension(MV_RAW_STRING_COL, FieldSpec.DataType.STRING)
      .addMultiValueDimension(MV_RAW_STRING_COL_2, FieldSpec.DataType.STRING)
      .build();

  private static final DataSchema DATA_SCHEMA = new DataSchema(new String[]{"mvDoubleCol", "mvFloatCol", "mvIntCol",
      "mvLongCol", "mvRawDoubleCol", "mvRawFloatCol", "mvRawIntCol", "mvRawLongCol", "mvRawStringCol",
      "mvRawStringCol2", "mvStringCol", "mvStringCol2", "svIntCol"},
      new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE_ARRAY, DataSchema.ColumnDataType.FLOAT_ARRAY,
          DataSchema.ColumnDataType.INT_ARRAY, DataSchema.ColumnDataType.LONG_ARRAY,
          DataSchema.ColumnDataType.DOUBLE_ARRAY, DataSchema.ColumnDataType.FLOAT_ARRAY,
          DataSchema.ColumnDataType.INT_ARRAY, DataSchema.ColumnDataType.LONG_ARRAY,
          DataSchema.ColumnDataType.STRING_ARRAY, DataSchema.ColumnDataType.STRING_ARRAY,
          DataSchema.ColumnDataType.STRING_ARRAY, DataSchema.ColumnDataType.STRING_ARRAY,
          DataSchema.ColumnDataType.INT});

  private static final TableConfig TABLE = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
      .setNoDictionaryColumns(
          Arrays.asList(MV_RAW_INT_COL, MV_RAW_LONG_COL, MV_RAW_FLOAT_COL, MV_RAW_DOUBLE_COL, MV_RAW_STRING_COL,
              MV_RAW_STRING_COL_2))
      .build();

  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegments;

  private List<String> _sortedStringListOverall;

  private final List<String> _stringList1 = new ArrayList<>();
  private final List<String> _stringList2 = new ArrayList<>();
  private final Set<String> _stringSet = new HashSet<>();
  private final Set<String> _stringSet2 = new HashSet<>();

  private final Random _random = new Random();

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
    FileUtils.deleteQuietly(INDEX_DIR);

    ImmutableSegment segment1 = createSegment(generateRecords(BASE_VALUE_1), SEGMENT_NAME_1);
    ImmutableSegment segment2 = createSegment(generateRecords(BASE_VALUE_2), SEGMENT_NAME_2);
    _indexSegment = segment1;
    _indexSegments = Arrays.asList(segment1, segment2);

    _sortedStringListOverall = new ArrayList<>(_stringSet);
    _sortedStringListOverall.addAll(_stringSet2);
    Collections.sort(_sortedStringListOverall);
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
      record.putValue(SV_INT_COL, value);
      Integer[] mvValue = new Integer[]{value, value + MV_OFFSET};
      record.putValue(MV_INT_COL, mvValue);
      record.putValue(MV_LONG_COL, mvValue);
      record.putValue(MV_FLOAT_COL, mvValue);
      record.putValue(MV_DOUBLE_COL, mvValue);
      record.putValue(MV_STRING_COL, mvValue);
      record.putValue(MV_RAW_INT_COL, mvValue);
      record.putValue(MV_RAW_LONG_COL, mvValue);
      record.putValue(MV_RAW_FLOAT_COL, mvValue);
      record.putValue(MV_RAW_DOUBLE_COL, mvValue);
      record.putValue(MV_RAW_STRING_COL, mvValue);

      String stringVal = RandomStringUtils.randomAlphanumeric(10, 100);
      String stringVal2 = RandomStringUtils.randomAlphanumeric(10, 100);
      record.putValue(MV_STRING_COL_2, Arrays.asList(stringVal, stringVal2));
      record.putValue(MV_RAW_STRING_COL_2, Arrays.asList(stringVal, stringVal2));
      _stringSet.add(stringVal);
      _stringSet2.add(stringVal2);
      _stringList1.add(stringVal);
      _stringList2.add(stringVal2);

      uniqueRecords.add(record);
    }

    List<GenericRow> records = new ArrayList<>(NUM_UNIQUE_RECORDS_PER_SEGMENT * NUM_DUPLICATES_PER_RECORDS);
    for (int i = 0; i < NUM_DUPLICATES_PER_RECORDS; i++) {
      records.addAll(uniqueRecords);
    }
    return records;
  }

  private ImmutableSegment createSegment(List<GenericRow> records, String segmentName)
      throws Exception {
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
  public void testSelectQueries() {
    {
      // Select * query
      String query = "SELECT * from testTable ORDER BY svIntCol LIMIT 40";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();
      assertNotNull(resultTable);
      assertEquals(resultTable.getDataSchema(), DATA_SCHEMA);
      List<Object[]> recordRows = resultTable.getRows();
      assertEquals(recordRows.size(), 40);

      Set<Integer> expectedValuesFirst = new HashSet<>();
      Set<Integer> expectedValuesSecond = new HashSet<>();
      for (int i = 0; i < NUM_UNIQUE_RECORDS_PER_SEGMENT; i++) {
        expectedValuesFirst.add(i);
        expectedValuesSecond.add(i + MV_OFFSET);
      }

      Set<Integer> actualValuesFirst = new HashSet<>();
      Set<Integer> actualValuesSecond = new HashSet<>();
      for (int i = 0; i < 40; i++) {
        Object[] values = recordRows.get(i);
        assertEquals(values.length, 13);
        int svIntValue = (int) values[12];
        int[] intValues = (int[]) values[2];
        assertEquals(intValues[1] - intValues[0], MV_OFFSET);
        assertEquals(svIntValue, intValues[0]);

        int[] intValuesRaw = (int[]) values[6];
        assertEquals(intValues[0], intValuesRaw[0]);
        assertEquals(intValues[1], intValuesRaw[1]);

        long[] longValues = (long[]) values[3];
        long[] longValuesRaw = (long[]) values[7];
        assertEquals(longValues[0], intValues[0]);
        assertEquals(longValues[1], intValues[1]);
        assertEquals(longValues[0], longValuesRaw[0]);
        assertEquals(longValues[1], longValuesRaw[1]);

        float[] floatValues = (float[]) values[1];
        float[] floatValuesRaw = (float[]) values[5];
        assertEquals(floatValues[0], (float) intValues[0]);
        assertEquals(floatValues[1], (float) intValues[1]);
        assertEquals(floatValues[0], floatValuesRaw[0]);
        assertEquals(floatValues[1], floatValuesRaw[1]);

        double[] doubleValues = (double[]) values[0];
        double[] doubleValuesRaw = (double[]) values[4];
        assertEquals(doubleValues[0], (double) intValues[0]);
        assertEquals(doubleValues[1], (double) intValues[1]);
        assertEquals(doubleValues[0], doubleValuesRaw[0]);
        assertEquals(doubleValues[1], doubleValuesRaw[1]);

        String[] stringValues = (String[]) values[8];
        String[] stringValuesRaw = (String[]) values[10];
        assertEquals(Integer.parseInt(stringValues[0]), intValues[0]);
        assertEquals(Integer.parseInt(stringValues[1]), intValues[1]);
        assertEquals(stringValues[0], stringValuesRaw[0]);
        assertEquals(stringValues[1], stringValuesRaw[1]);

        String[] stringValues2 = (String[]) values[9];
        String[] stringValuesRaw2 = (String[]) values[11];
        assertEquals(stringValues2[0], stringValuesRaw2[0]);
        assertEquals(stringValues2[1], stringValuesRaw2[1]);
        assertTrue(_stringSet.contains(stringValuesRaw2[0]));
        assertTrue(_stringSet2.contains(stringValuesRaw2[1]));

        actualValuesFirst.add(intValues[0]);
        actualValuesSecond.add(intValues[1]);
      }
      assertTrue(actualValuesFirst.containsAll(expectedValuesFirst));
      assertTrue(actualValuesSecond.containsAll(expectedValuesSecond));
    }
    {
      // Select some dict based MV and some raw MV columns. Validate that the values match for the corresponding rows
      String query = "SELECT mvIntCol, mvDoubleCol, mvStringCol, mvRawIntCol, mvRawDoubleCol, mvRawStringCol, "
          + "svIntCol, mvStringCol2, mvRawStringCol2 from testTable ORDER BY svIntCol LIMIT 40";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();
      assertNotNull(resultTable);
      DataSchema dataSchema = new DataSchema(new String[]{
          "mvIntCol", "mvDoubleCol", "mvStringCol", "mvRawIntCol", "mvRawDoubleCol", "mvRawStringCol", "svIntCol",
          "mvStringCol2", "mvRawStringCol2"
      }, new DataSchema.ColumnDataType[]{
          DataSchema.ColumnDataType.INT_ARRAY, DataSchema.ColumnDataType.DOUBLE_ARRAY,
          DataSchema.ColumnDataType.STRING_ARRAY, DataSchema.ColumnDataType.INT_ARRAY,
          DataSchema.ColumnDataType.DOUBLE_ARRAY, DataSchema.ColumnDataType.STRING_ARRAY,
          DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING_ARRAY, DataSchema.ColumnDataType.STRING_ARRAY
      });
      assertEquals(resultTable.getDataSchema(), dataSchema);
      List<Object[]> recordRows = resultTable.getRows();
      assertEquals(recordRows.size(), 40);

      Set<Integer> expectedValuesFirst = new HashSet<>();
      Set<Integer> expectedValuesSecond = new HashSet<>();
      for (int i = 0; i < NUM_UNIQUE_RECORDS_PER_SEGMENT; i++) {
        expectedValuesFirst.add(i);
        expectedValuesSecond.add(i + MV_OFFSET);
      }

      Set<Integer> actualValuesFirst = new HashSet<>();
      Set<Integer> actualValuesSecond = new HashSet<>();
      for (int i = 0; i < 40; i++) {
        Object[] values = recordRows.get(i);
        assertEquals(values.length, 9);
        int[] intValues = (int[]) values[0];
        assertEquals(intValues[1] - intValues[0], MV_OFFSET);

        int[] intValuesRaw = (int[]) values[3];
        assertEquals(intValues[0], intValuesRaw[0]);
        assertEquals(intValues[1], intValuesRaw[1]);

        double[] doubleValues = (double[]) values[1];
        double[] doubleValuesRaw = (double[]) values[4];
        assertEquals(doubleValues[0], (double) intValues[0]);
        assertEquals(doubleValues[1], (double) intValues[1]);
        assertEquals(doubleValues[0], doubleValuesRaw[0]);
        assertEquals(doubleValues[1], doubleValuesRaw[1]);

        String[] stringValues = (String[]) values[2];
        String[] stringValuesRaw = (String[]) values[5];
        assertEquals(Integer.parseInt(stringValues[0]), intValues[0]);
        assertEquals(Integer.parseInt(stringValues[1]), intValues[1]);
        assertEquals(stringValues[0], stringValuesRaw[0]);
        assertEquals(stringValues[1], stringValuesRaw[1]);

        String[] stringValues2 = (String[]) values[7];
        String[] stringValuesRaw2 = (String[]) values[8];
        assertEquals(stringValues2[0], stringValuesRaw2[0]);
        assertEquals(stringValues2[1], stringValuesRaw2[1]);
        assertTrue(_stringSet.contains(stringValuesRaw2[0]));
        assertTrue(_stringSet2.contains(stringValuesRaw2[1]));

        assertEquals(intValues[0], (int) values[6]);
        assertEquals(intValuesRaw[0], (int) values[6]);

        actualValuesFirst.add(intValues[0]);
        actualValuesSecond.add(intValues[1]);
      }
      assertTrue(actualValuesFirst.containsAll(expectedValuesFirst));
      assertTrue(actualValuesSecond.containsAll(expectedValuesSecond));
    }
    {
      // Test a select with a ARRAYLENGTH transform function
      String query = "SELECT ARRAYLENGTH(mvRawLongCol), ARRAYLENGTH(mvLongCol) from testTable LIMIT 10";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();
      assertNotNull(resultTable);
      DataSchema dataSchema = new DataSchema(new String[]{"arraylength(mvRawLongCol)", "arraylength(mvLongCol)"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT});
      assertEquals(resultTable.getDataSchema(), dataSchema);
      List<Object[]> recordRows = resultTable.getRows();
      assertEquals(recordRows.size(), 10);

      for (int i = 0; i < 10; i++) {
        Object[] values = recordRows.get(i);
        assertEquals(values.length, 2);
        int intRawVal = (int) values[0];
        int intVal = (int) values[1];
        assertEquals(intRawVal, 2);
        assertEquals(intVal, intRawVal);
      }
    }
  }

  /**
   * Today selection ORDER BY only on MV columns (irrespective of whether it's dictionary based or raw) doesn't work
   * as the semantics of how such queries should behave isn't clear. Such queries should always fail.
   */
  @Test
  public void testSelectionOrderBy() {
    {
      String query = "SELECT mvFloatCol from testTable WHERE mvFloatCol < 5 ORDER BY mvFloatCol LIMIT 10";
      assertThrows(BadQueryRequestException.class, () -> getBrokerResponse(query));

      String query1 = "SELECT mvRawFloatCol from testTable WHERE mvRawFloatCol < 5 ORDER BY mvRawFloatCol LIMIT 10";
      assertThrows(BadQueryRequestException.class, () -> getBrokerResponse(query1));

      String query2 = "SELECT mvIntCol, mvFloatCol from testTable ORDER BY mvIntCol, mvFloatCol LIMIT 10";
      assertThrows(BadQueryRequestException.class, () -> getBrokerResponse(query2));

      String query3 = "SELECT mvRawIntCol, mvRawFloatCol from testTable ORDER BY mvRawIntCol, mvRawFloatCol LIMIT 10";
      assertThrows(BadQueryRequestException.class, () -> getBrokerResponse(query3));
    }
    {
      String query = "SELECT mvFloatCol, svIntCol from testTable WHERE mvFloatCol < 5 ORDER BY mvFloatCol, svIntCol "
          + "LIMIT 10";
      assertThrows(BadQueryRequestException.class, () -> getBrokerResponse(query));

      String query1 = "SELECT mvRawFloatCol, svIntCol from testTable WHERE mvRawFloatCol < 5 ORDER BY mvRawFloatCol, "
          + "svIntCol LIMIT 10";
      assertThrows(BadQueryRequestException.class, () -> getBrokerResponse(query1));
    }
    {
      String query = "SELECT svIntCol, mvFloatCol from testTable WHERE mvRawFloatCol < 5 ORDER BY svIntCol, "
          + "mvFloatCol LIMIT 10";
      assertThrows(BadQueryRequestException.class, () -> getBrokerResponse(query));

      String query1 = "SELECT svIntCol, mvRawFloatCol from testTable WHERE mvRawFloatCol < 5 ORDER BY svIntCol, "
          + "mvRawFloatCol LIMIT 10";
      assertThrows(BadQueryRequestException.class, () -> getBrokerResponse(query1));
    }
    {
      String query = "SELECT VALUEIN(mvIntCol, '0') from testTable WHERE mvIntCol IN (0) ORDER BY "
          + "VALUEIN(mvIntCol, '0') DESC LIMIT 10";
      assertThrows(BadQueryRequestException.class, () -> getBrokerResponse(query));

      String query1 = "SELECT VALUEIN(mvRawIntCol, '0') from testTable WHERE mvRawIntCol IN (0) ORDER BY "
          + "VALUEIN(mvRawIntCol, '0') DESC LIMIT 10";
      assertThrows(BadQueryRequestException.class, () -> getBrokerResponse(query1));
    }
    {
      // Arraylength eventually translates to a SV column, so this should pass
      String query = "SELECT ARRAYLENGTH(mvRawLongCol), ARRAYLENGTH(mvLongCol) from testTable ORDER BY "
          + "ARRAYLENGTH(mvRawLongCol), ARRAYLENGTH(mvLongCol) LIMIT 10";
      BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
      assertTrue(brokerResponseNative.getProcessingExceptions() == null
          || brokerResponseNative.getProcessingExceptions().size() == 0);
      ResultTable resultTable = brokerResponseNative.getResultTable();
      assertEquals(resultTable.getRows().size(), 10);
      List<Object[]> recordRows = resultTable.getRows();
      for (Object[] row : recordRows) {
        assertEquals(row.length, 2);
        assertEquals(row[0], MV_LENGTH);
        assertEquals(row[1], MV_LENGTH);
      }
    }
  }

  @Test
  public void testNonAggregateMVGroupBy() {
    {
      // Test a group by query on some raw MV rows. Order by on SV column added for determinism
      String query = "SELECT svIntCol, mvRawFloatCol, mvRawDoubleCol, mvRawStringCol from testTable GROUP BY "
          + "svIntCol, mvRawFloatCol, mvRawDoubleCol, mvRawStringCol ORDER BY svIntCol, mvRawFloatCol, "
          + "mvRawDoubleCol, mvRawStringCol LIMIT 10";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();
      assertNotNull(resultTable);
      DataSchema dataSchema = new DataSchema(new String[]{
          "svIntCol", "mvRawFloatCol", "mvRawDoubleCol", "mvRawStringCol"
      }, new DataSchema.ColumnDataType[]{
          DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.FLOAT, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.STRING
      });
      assertEquals(resultTable.getDataSchema(), dataSchema);
      List<Object[]> recordRows = resultTable.getRows();
      assertEquals(recordRows.size(), 10);

      int[] expectedSVInts = new int[]{0, 0, 0, 0, 0, 0, 0, 0, 1, 1};
      float[] expecteMVFloats = new float[]{0.0F, 0.0F, 0.0F, 0.0F, 100.0F, 100.0F, 100.0F, 100.0F, 1.0F, 1.0F};
      double[] expectedMVDoubles = new double[]{0.0, 0.0, 100.0, 100.0, 0.0, 0.0, 100.0, 100.0, 1.0, 1.0};
      String[] expectedMVStrings = new String[]{"0", "100", "0", "100", "0", "100", "0", "100", "1", "101"};

      for (int i = 0; i < 10; i++) {
        Object[] values = recordRows.get(i);
        assertEquals(values.length, 4);
        assertEquals((int) values[0], expectedSVInts[i]);
        assertEquals(values[1], expecteMVFloats[i]);
        assertEquals(values[2], expectedMVDoubles[i]);
        assertEquals((String) values[3], expectedMVStrings[i]);
      }
    }
    {
      // Test a group by order by query on some raw MV rows (order by on int, double and string)
      String query = "SELECT mvRawIntCol, mvRawDoubleCol, mvRawStringCol from testTable GROUP BY mvRawIntCol, "
          + "mvRawDoubleCol, mvRawStringCol ORDER BY mvRawIntCol, mvRawDoubleCol, mvRawStringCol LIMIT 20";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();
      assertNotNull(resultTable);
      DataSchema dataSchema = new DataSchema(new String[]{
          "mvRawIntCol", "mvRawDoubleCol", "mvRawStringCol"
      }, new DataSchema.ColumnDataType[]{
          DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.STRING
      });
      assertEquals(resultTable.getDataSchema(), dataSchema);
      List<Object[]> recordRows = resultTable.getRows();
      assertEquals(recordRows.size(), 20);

      int[] expectedIntValues = new int[]{0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4};
      double[] expectedDoubleValues = new double[]{0.0, 0.0, 100.0, 100.0, 1.0, 1.0, 101.0, 101.0, 2.0, 2.0, 102.0,
          102.0, 3.0, 3.0, 103.0, 103.0, 4.0, 4.0, 104.0, 104.0};
      String[] expectedStringValues = new String[]{"0", "100", "0", "100", "1", "101", "1", "101", "102", "2", "102",
          "2", "103", "3", "103", "3", "104", "4", "104", "4"};

      for (int i = 0; i < 20; i++) {
        Object[] values = recordRows.get(i);
        assertEquals(values.length, 3);
        assertEquals((int) values[0], expectedIntValues[i]);
        assertEquals(values[1], expectedDoubleValues[i]);
        assertEquals((String) values[2], expectedStringValues[i]);
      }
    }
    {
      // Test a group by order by query on some raw MV rows (order by on string, int and double)
      String query = "SELECT mvRawIntCol, mvRawDoubleCol, mvRawStringCol from testTable GROUP BY mvRawIntCol, "
          + "mvRawDoubleCol, mvRawStringCol ORDER BY mvRawStringCol, mvRawIntCol, mvRawDoubleCol LIMIT 10";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();
      assertNotNull(resultTable);
      DataSchema dataSchema = new DataSchema(new String[]{
          "mvRawIntCol", "mvRawDoubleCol", "mvRawStringCol"
      }, new DataSchema.ColumnDataType[]{
          DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.STRING
      });
      assertEquals(resultTable.getDataSchema(), dataSchema);
      List<Object[]> recordRows = resultTable.getRows();
      assertEquals(recordRows.size(), 10);

      int[] expectedIntValues = new int[]{0, 0, 100, 100, 1, 1, 101, 101, 0, 0};
      double[] expectedDoubleValues = new double[]{0.0, 100.0, 0.0, 100.0, 1.0, 101.0, 1.0, 101.0, 0.0, 100.0};
      String[] expectedStringValues = new String[]{"0", "0", "0", "0", "1", "1", "1", "1", "100", "100"};

      for (int i = 0; i < 10; i++) {
        Object[] values = recordRows.get(i);
        assertEquals(values.length, 3);
        assertEquals((int) values[0], expectedIntValues[i]);
        assertEquals(values[1], expectedDoubleValues[i]);
        assertEquals((String) values[2], expectedStringValues[i]);
      }
    }
    {
      // Test a group by order by query on variable length string column and compare results with query on dict based
      // variable length string column
      String query1 = "SELECT mvRawStringCol2 from testTable GROUP BY mvRawStringCol2 ORDER BY mvRawStringCol2 "
          + "LIMIT 20";
      ResultTable resultTable1 = getBrokerResponse(query1).getResultTable();
      assertNotNull(resultTable1);
      DataSchema dataSchema1 = new DataSchema(new String[]{
          "mvRawStringCol2"
      }, new DataSchema.ColumnDataType[]{
          DataSchema.ColumnDataType.STRING
      });
      assertEquals(resultTable1.getDataSchema(), dataSchema1);
      List<Object[]> recordRows1 = resultTable1.getRows();
      assertEquals(recordRows1.size(), 20);

      String query2 = "SELECT mvStringCol2 from testTable GROUP BY mvStringCol2 ORDER BY mvStringCol2 LIMIT 20";
      ResultTable resultTable2 = getBrokerResponse(query2).getResultTable();
      assertNotNull(resultTable2);
      DataSchema dataSchema2 = new DataSchema(new String[]{
          "mvStringCol2"
      }, new DataSchema.ColumnDataType[]{
          DataSchema.ColumnDataType.STRING
      });
      assertEquals(resultTable2.getDataSchema(), dataSchema2);
      List<Object[]> recordRows2 = resultTable2.getRows();
      assertEquals(recordRows2.size(), 20);

      for (int i = 0; i < 10; i++) {
        Object[] values1 = recordRows1.get(i);
        Object[] values2 = recordRows2.get(i);
        assertEquals(values1.length, 1);
        assertEquals(values2.length, 1);

        assertEquals(values1[0], values2[0]);
        assertEquals(values1[0], _sortedStringListOverall.get(i));
      }
    }
    {
      // Test a select with a VALUEIN transform function with group by
      String query = "SELECT VALUEIN(mvRawIntCol, '0') from testTable WHERE mvRawIntCol IN (0) GROUP BY "
          + "VALUEIN(mvRawIntCol, '0') LIMIT 10";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();
      assertNotNull(resultTable);
      DataSchema dataSchema = new DataSchema(new String[]{"valuein(mvRawIntCol,'0')"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT});
      assertEquals(resultTable.getDataSchema(), dataSchema);
      List<Object[]> recordRows = resultTable.getRows();
      assertEquals(recordRows.size(), 1);
      Object[] values = recordRows.get(0);
      assertEquals(values.length, 1);
      int intRawVal = (int) values[0];
      assertEquals(intRawVal, 0);
    }
    {
      // Test a select with a VALUEIN transform function with group by order by
      String query = "SELECT VALUEIN(mvRawDoubleCol, '0.0') from testTable WHERE mvRawDoubleCol IN (0.0) GROUP BY "
          + "VALUEIN(mvRawDoubleCol, '0.0') ORDER BY VALUEIN(mvRawDoubleCol, '0.0') LIMIT 10";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();
      assertNotNull(resultTable);
      DataSchema dataSchema = new DataSchema(new String[]{"valuein(mvRawDoubleCol,'0.0')"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE});
      assertEquals(resultTable.getDataSchema(), dataSchema);
      List<Object[]> recordRows = resultTable.getRows();
      assertEquals(recordRows.size(), 1);
      Object[] values = recordRows.get(0);
      assertEquals(values.length, 1);
      double doubleRawVal = (double) values[0];
      assertEquals(doubleRawVal, 0.0);
    }
    {
      // Test a select with a ARRAYLENGTH transform function
      String query = "SELECT ARRAYLENGTH(mvRawLongCol), ARRAYLENGTH(mvLongCol), CARDINALITY(mvRawLongCol), "
          + "CARDINALITY(mvLongCol) from testTable GROUP BY ARRAYLENGTH(mvRawLongCol), ARRAYLENGTH(mvLongCol), "
          + "CARDINALITY(mvRawLongCol), CARDINALITY(mvLongCol) LIMIT 10";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();
      assertNotNull(resultTable);
      DataSchema dataSchema = new DataSchema(new String[]{"arraylength(mvRawLongCol)", "arraylength(mvLongCol)",
          "cardinality(mvRawLongCol)", "cardinality(mvLongCol)"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT,
              DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT});
      assertEquals(resultTable.getDataSchema(), dataSchema);
      List<Object[]> recordRows = resultTable.getRows();
      assertEquals(recordRows.size(), 1);
      Object[] values = recordRows.get(0);
      assertEquals(values.length, 4);
      int intRawVal = (int) values[0];
      int intVal = (int) values[1];
      int intRawVal2 = (int) values[2];
      int intVal2 = (int) values[3];
      assertEquals(intRawVal, 2);
      assertEquals(intVal, intRawVal);
      assertEquals(intRawVal2, intRawVal);
      assertEquals(intVal2, intRawVal);
    }
  }

  @Test
  public void testSelectWithFilterQueries() {
    {
      // Test a select with filter query on a MV raw column identifier
      String query = "SELECT mvRawIntCol, mvRawDoubleCol, mvRawStringCol from testTable where mvRawIntCol < 5 LIMIT 10";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();
      assertNotNull(resultTable);
      DataSchema dataSchema = new DataSchema(new String[]{
          "mvRawIntCol", "mvRawDoubleCol", "mvRawStringCol"
      }, new DataSchema.ColumnDataType[]{
          DataSchema.ColumnDataType.INT_ARRAY, DataSchema.ColumnDataType.DOUBLE_ARRAY,
          DataSchema.ColumnDataType.STRING_ARRAY
      });
      assertEquals(resultTable.getDataSchema(), dataSchema);
      List<Object[]> recordRows = resultTable.getRows();
      assertEquals(recordRows.size(), 10);

      for (int i = 0; i < 10; i++) {
        Object[] values = recordRows.get(i);
        assertEquals(values.length, 3);
        int[] intVal = (int[]) values[0];
        assertEquals(intVal[1] - intVal[0], MV_OFFSET);
        assertEquals(intVal[0], i % 5);
        assertEquals(intVal[1], (i % 5) + MV_OFFSET);

        double[] doubleVal = (double[]) values[1];
        assertEquals(doubleVal[0], (double) i % 5);
        assertEquals(doubleVal[1], (double) (i % 5) + MV_OFFSET);

        String[] stringVal = (String[]) values[2];
        assertEquals(Integer.parseInt(stringVal[0]), i % 5);
        assertEquals(Integer.parseInt(stringVal[1]), (i % 5) + MV_OFFSET);
      }
    }
    {
      // Test a select with filter query (OR) on two MV raw column identifiers (int and double)
      String query = "SELECT mvRawIntCol, mvRawDoubleCol, mvRawStringCol from testTable where mvRawIntCol <= 5 "
          + "OR mvRawDoubleCol > 1104.0 LIMIT 10";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();
      assertNotNull(resultTable);
      DataSchema dataSchema = new DataSchema(new String[]{
          "mvRawIntCol", "mvRawDoubleCol", "mvRawStringCol"
      }, new DataSchema.ColumnDataType[]{
          DataSchema.ColumnDataType.INT_ARRAY, DataSchema.ColumnDataType.DOUBLE_ARRAY,
          DataSchema.ColumnDataType.STRING_ARRAY
      });
      assertEquals(resultTable.getDataSchema(), dataSchema);
      List<Object[]> recordRows = resultTable.getRows();
      assertEquals(recordRows.size(), 10);

      for (int i = 0; i < 10; i++) {
        Object[] values = recordRows.get(i);
        assertEquals(values.length, 3);
        int[] intVal = (int[]) values[0];
        assertEquals(intVal[1] - intVal[0], MV_OFFSET);
        double[] doubleVal = (double[]) values[1];
        assertTrue(intVal[0] <= 5 || intVal[1] <= 5 || doubleVal[0] > 1104.0 || doubleVal[1] > 1104.0);
      }
    }
    {
      // Test a select with filter query on a long MV raw column identifier
      String query = "SELECT mvRawLongCol from testTable where mvRawLongCol >= 1100 LIMIT 10";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();
      assertNotNull(resultTable);
      DataSchema dataSchema = new DataSchema(new String[]{
          "mvRawLongCol"
      }, new DataSchema.ColumnDataType[]{
          DataSchema.ColumnDataType.LONG_ARRAY
      });
      assertEquals(resultTable.getDataSchema(), dataSchema);
      List<Object[]> recordRows = resultTable.getRows();
      assertEquals(recordRows.size(), 10);

      for (int i = 0; i < 10; i++) {
        Object[] values = recordRows.get(i);
        assertEquals(values.length, 1);
        long[] longVal = (long[]) values[0];
        assertEquals(longVal[1] - longVal[0], MV_OFFSET);
        assertTrue(longVal[0] >= 1100 || longVal[1] >= 1100);
      }
    }
    {
      // Test a select with filter = query on a string MV raw column identifier
      String query = "SELECT mvRawStringCol from testTable where mvRawStringCol = '1100' LIMIT 10";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();
      assertNotNull(resultTable);
      DataSchema dataSchema = new DataSchema(new String[]{
          "mvRawStringCol"
      }, new DataSchema.ColumnDataType[]{
          DataSchema.ColumnDataType.STRING_ARRAY
      });
      assertEquals(resultTable.getDataSchema(), dataSchema);
      List<Object[]> recordRows = resultTable.getRows();
      assertEquals(recordRows.size(), 4);

      for (int i = 0; i < 4; i++) {
        Object[] values = recordRows.get(i);
        assertEquals(values.length, 1);
        String[] stringVal = (String[]) values[0];
        assertEquals(Integer.parseInt(stringVal[1]) - Integer.parseInt(stringVal[0]), MV_OFFSET);
        assertTrue(Integer.parseInt(stringVal[0]) == 1100 || Integer.parseInt(stringVal[1]) == 1100);
      }
    }
    {
      // Test a select with filter = query on int, float, long, and double MV raw column identifiers
      String query = "SELECT mvRawIntCol, mvRawFloatCol, mvRawLongCol, mvRawDoubleCol from testTable where "
          + "mvRawIntCol = '1100' AND mvRawFloatCol = '1100.0' AND mvRawLongCol = '1100' AND mvRawDoubleCol = '1100.0' "
          + "LIMIT 10";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();
      assertNotNull(resultTable);
      DataSchema dataSchema = new DataSchema(new String[]{
          "mvRawIntCol", "mvRawFloatCol", "mvRawLongCol", "mvRawDoubleCol"
      }, new DataSchema.ColumnDataType[]{
          DataSchema.ColumnDataType.INT_ARRAY, DataSchema.ColumnDataType.FLOAT_ARRAY,
          DataSchema.ColumnDataType.LONG_ARRAY, DataSchema.ColumnDataType.DOUBLE_ARRAY
      });
      assertEquals(resultTable.getDataSchema(), dataSchema);
      List<Object[]> recordRows = resultTable.getRows();
      assertEquals(recordRows.size(), 4);

      for (int i = 0; i < 4; i++) {
        Object[] values = recordRows.get(i);
        assertEquals(values.length, 4);
        int[] intVal = (int[]) values[0];
        assertEquals(intVal[1] - intVal[0], MV_OFFSET);
        assertTrue(intVal[0] == 1100 || intVal[1] == 1100);

        float[] floatVal = (float[]) values[1];
        assertEquals(floatVal[1] - floatVal[0], (float) MV_OFFSET);
        assertTrue(floatVal[0] == 1100.0F || floatVal[1] == 1100.0F);

        long[] longVal = (long[]) values[2];
        assertEquals(longVal[1] - longVal[0], MV_OFFSET);
        assertTrue(longVal[0] == 1100L || longVal[1] == 1100L);

        double[] doubleVal = (double[]) values[3];
        assertEquals(doubleVal[1] - doubleVal[0], (double) MV_OFFSET);
        assertTrue(doubleVal[0] == 1100.0 || doubleVal[1] == 1100.0);
      }
    }
    {
      // Test a select with filter != query on int, float, long, and double MV raw column identifiers
      String query = "SELECT mvRawIntCol, mvRawFloatCol, mvRawLongCol, mvRawDoubleCol from testTable where "
          + "mvRawIntCol != '1100' AND mvRawFloatCol != '1100.0' AND mvRawLongCol != '1100' AND "
          + "mvRawDoubleCol != '1100.0' LIMIT 10";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();
      assertNotNull(resultTable);
      DataSchema dataSchema = new DataSchema(new String[]{
          "mvRawIntCol", "mvRawFloatCol", "mvRawLongCol", "mvRawDoubleCol"
      }, new DataSchema.ColumnDataType[]{
          DataSchema.ColumnDataType.INT_ARRAY, DataSchema.ColumnDataType.FLOAT_ARRAY,
          DataSchema.ColumnDataType.LONG_ARRAY, DataSchema.ColumnDataType.DOUBLE_ARRAY
      });
      assertEquals(resultTable.getDataSchema(), dataSchema);
      List<Object[]> recordRows = resultTable.getRows();
      assertEquals(recordRows.size(), 10);

      for (int i = 0; i < 10; i++) {
        Object[] values = recordRows.get(i);
        assertEquals(values.length, 4);
        int[] intVal = (int[]) values[0];
        assertEquals(intVal[1] - intVal[0], MV_OFFSET);
        assertTrue(intVal[0] != 1100 && intVal[1] != 1100);

        float[] floatVal = (float[]) values[1];
        assertEquals(floatVal[1] - floatVal[0], (float) MV_OFFSET);
        assertTrue(floatVal[0] != 1100.0F && floatVal[1] != 1100.0F);

        long[] longVal = (long[]) values[2];
        assertEquals(longVal[1] - longVal[0], MV_OFFSET);
        assertTrue(longVal[0] != 1100L && longVal[1] != 1100L);

        double[] doubleVal = (double[]) values[3];
        assertEquals(doubleVal[1] - doubleVal[0], (double) MV_OFFSET);
        assertTrue(doubleVal[0] != 1100.0 && doubleVal[1] != 1100.0);
      }
    }
    {
      // Test a select with filter query (AND) on two MV raw column identifiers (one int and another double) such that
      // the values in the filter are mutually exclusive
      // No match should be found
      String query = "SELECT mvRawIntCol, mvRawDoubleCol, mvRawStringCol from testTable where mvRawIntCol < 5 "
          + "AND mvRawDoubleCol > 1104.0 LIMIT 10";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();
      assertNotNull(resultTable);
      DataSchema dataSchema = new DataSchema(new String[]{
          "mvRawIntCol", "mvRawDoubleCol", "mvRawStringCol"
      }, new DataSchema.ColumnDataType[]{
          DataSchema.ColumnDataType.INT_ARRAY, DataSchema.ColumnDataType.DOUBLE_ARRAY,
          DataSchema.ColumnDataType.STRING_ARRAY
      });
      assertEquals(resultTable.getDataSchema(), dataSchema);
      List<Object[]> recordRows = resultTable.getRows();
      assertEquals(recordRows.size(), 0);
    }
    {
      // Test a select with filter IN query on int, float, long, and double MV raw column identifiers
      String query = "SELECT mvRawIntCol, mvRawFloatCol, mvRawLongCol, mvRawDoubleCol from testTable where "
          + "mvRawIntCol IN (1100, 1101) AND mvRawFloatCol IN (1100.0, 1101.0) AND mvRawLongCol "
          + "IN (1100, 1101) AND mvRawDoubleCol IN (1100.0, 1101.0) LIMIT 10";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();
      assertNotNull(resultTable);
      DataSchema dataSchema = new DataSchema(new String[]{
          "mvRawIntCol", "mvRawFloatCol", "mvRawLongCol", "mvRawDoubleCol"
      }, new DataSchema.ColumnDataType[]{
          DataSchema.ColumnDataType.INT_ARRAY, DataSchema.ColumnDataType.FLOAT_ARRAY,
          DataSchema.ColumnDataType.LONG_ARRAY, DataSchema.ColumnDataType.DOUBLE_ARRAY
      });
      assertEquals(resultTable.getDataSchema(), dataSchema);
      List<Object[]> recordRows = resultTable.getRows();
      assertEquals(recordRows.size(), 8);

      for (int i = 0; i < 8; i++) {
        Object[] values = recordRows.get(i);
        assertEquals(values.length, 4);
        int[] intVal = (int[]) values[0];
        assertEquals(intVal[1] - intVal[0], MV_OFFSET);
        assertTrue(intVal[0] == 1100 || intVal[1] == 1100 || intVal[0] == 1101 || intVal[1] == 1101);

        float[] floatVal = (float[]) values[1];
        assertEquals(floatVal[1] - floatVal[0], (float) MV_OFFSET);
        assertTrue(floatVal[0] == 1100.0F || floatVal[1] == 1100.0F || floatVal[0] == 1101.0F
            || floatVal[1] == 1101.0F);

        long[] longVal = (long[]) values[2];
        assertEquals(longVal[1] - longVal[0], MV_OFFSET);
        assertTrue(longVal[0] == 1100L || longVal[1] == 1100L || longVal[0] == 1101L || longVal[1] == 1101L);

        double[] doubleVal = (double[]) values[3];
        assertEquals(doubleVal[1] - doubleVal[0], (double) MV_OFFSET);
        assertTrue(doubleVal[0] == 1100.0 || doubleVal[1] == 1100.0 || doubleVal[0] == 1101.0
            || doubleVal[1] == 1101.0);
      }
    }
    {
      // Test a select with filter IN query on the string MV raw column identifier
      String query = "SELECT mvRawStringCol from testTable where mvRawStringCol IN ('1100', '1101') LIMIT 10";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();
      assertNotNull(resultTable);
      DataSchema dataSchema = new DataSchema(new String[]{
          "mvRawStringCol"
      }, new DataSchema.ColumnDataType[]{
          DataSchema.ColumnDataType.STRING_ARRAY
      });
      assertEquals(resultTable.getDataSchema(), dataSchema);
      List<Object[]> recordRows = resultTable.getRows();
      assertEquals(recordRows.size(), 8);

      for (int i = 0; i < 8; i++) {
        Object[] values = recordRows.get(i);
        assertEquals(values.length, 1);
        String[] stringVal = (String[]) values[0];
        assertEquals(Integer.parseInt(stringVal[1]) - Integer.parseInt(stringVal[0]), MV_OFFSET);
        assertTrue(Integer.parseInt(stringVal[0]) == 1100 || Integer.parseInt(stringVal[1]) == 1100
            || Integer.parseInt(stringVal[0]) == 1101 || Integer.parseInt(stringVal[1]) == 1101);
      }
    }
    {
      // Test a select with filter IN query on the variable length string MV raw column identifier
      int index1 = _random.nextInt(_stringList1.size());
      int index2 = _random.nextInt(_stringList2.size());
      while (index2 == index1) {
        index2 = _random.nextInt(_stringList2.size());
      }
      String val1 = _stringList1.get(index1);
      String val2 = _stringList2.get(index2);

      String query1 = "SELECT mvRawStringCol2 from testTable where mvRawStringCol2 IN ('" + val1 + "', '" + val2
          + "') LIMIT 10";
      ResultTable resultTable1 = getBrokerResponse(query1).getResultTable();
      assertNotNull(resultTable1);
      DataSchema dataSchema1 = new DataSchema(new String[]{
          "mvRawStringCol2"
      }, new DataSchema.ColumnDataType[]{
          DataSchema.ColumnDataType.STRING_ARRAY
      });
      assertEquals(resultTable1.getDataSchema(), dataSchema1);
      List<Object[]> recordRows1 = resultTable1.getRows();
      assertEquals(recordRows1.size(), 8);

      String query2 = "SELECT mvStringCol2 from testTable where mvStringCol2 IN ('" + val1 + "', '" + val2
          + "') LIMIT 10";
      ResultTable resultTable2 = getBrokerResponse(query2).getResultTable();
      assertNotNull(resultTable2);
      DataSchema dataSchema2 = new DataSchema(new String[]{
          "mvStringCol2"
      }, new DataSchema.ColumnDataType[]{
          DataSchema.ColumnDataType.STRING_ARRAY
      });
      assertEquals(resultTable2.getDataSchema(), dataSchema2);
      List<Object[]> recordRows2 = resultTable2.getRows();
      assertEquals(recordRows2.size(), 8);

      for (int i = 0; i < 8; i++) {
        Object[] values1 = recordRows1.get(i);
        Object[] values2 = recordRows2.get(i);
        assertEquals(values1.length, 1);
        assertEquals(values2.length, 1);
        String[] stringVal1 = (String[]) values1[0];
        String[] stringVal2 = (String[]) values2[0];
        assertTrue(stringVal1[0].equals(val1) || stringVal1[1].equals(val2));
        assertTrue(stringVal2[0].equals(val1) || stringVal2[1].equals(val2));
      }
    }
    {
      // Test a select with filter query on an arraylength transform function
      String query = "SELECT mvRawIntCol, mvRawDoubleCol, mvRawStringCol FROM testTable WHERE "
          + "ARRAYLENGTH(mvRawIntCol) < 5 AND CARDINALITY(mvRawIntCol) < 5 AND CARDINALITY(mvRawIntCol) < 5 "
          + "LIMIT 10";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();
      assertNotNull(resultTable);
      DataSchema dataSchema = new DataSchema(new String[]{
          "mvRawIntCol", "mvRawDoubleCol", "mvRawStringCol"
      }, new DataSchema.ColumnDataType[]{
          DataSchema.ColumnDataType.INT_ARRAY, DataSchema.ColumnDataType.DOUBLE_ARRAY,
          DataSchema.ColumnDataType.STRING_ARRAY
      });
      assertEquals(resultTable.getDataSchema(), dataSchema);
      List<Object[]> recordRows = resultTable.getRows();
      assertEquals(recordRows.size(), 10);

      for (int i = 0; i < 10; i++) {
        Object[] values = recordRows.get(i);
        assertEquals(values.length, 3);
        int[] intVal = (int[]) values[0];
        assertEquals(intVal[1] - intVal[0], MV_OFFSET);

        double[] doubleVal = (double[]) values[1];
        assertEquals(doubleVal[0], (double) intVal[0]);
        assertEquals(doubleVal[1], (double) intVal[1]);

        String[] stringVal = (String[]) values[2];
        assertEquals(Integer.parseInt(stringVal[0]), intVal[0]);
        assertEquals(Integer.parseInt(stringVal[1]), intVal[1]);
      }
    }
    {
      // Test a select with filter = query on an arraylength transform function
      String query = "SELECT mvRawIntCol, mvRawDoubleCol, mvRawStringCol from testTable where "
          + "ARRAYLENGTH(mvRawDoubleCol) = 2 LIMIT 10";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();
      assertNotNull(resultTable);
      DataSchema dataSchema = new DataSchema(new String[]{
          "mvRawIntCol", "mvRawDoubleCol", "mvRawStringCol"
      }, new DataSchema.ColumnDataType[]{
          DataSchema.ColumnDataType.INT_ARRAY, DataSchema.ColumnDataType.DOUBLE_ARRAY,
          DataSchema.ColumnDataType.STRING_ARRAY
      });
      assertEquals(resultTable.getDataSchema(), dataSchema);
      List<Object[]> recordRows = resultTable.getRows();
      assertEquals(recordRows.size(), 10);

      for (int i = 0; i < 10; i++) {
        Object[] values = recordRows.get(i);
        assertEquals(values.length, 3);
        int[] intVal = (int[]) values[0];
        assertEquals(intVal[1] - intVal[0], MV_OFFSET);

        double[] doubleVal = (double[]) values[1];
        assertEquals(doubleVal[0], (double) intVal[0]);
        assertEquals(doubleVal[1], (double) intVal[1]);

        String[] stringVal = (String[]) values[2];
        assertEquals(Integer.parseInt(stringVal[0]), intVal[0]);
        assertEquals(Integer.parseInt(stringVal[1]), intVal[1]);
      }
    }
    {
      // Test a select with filter IN query on an arraylength transform function
      String query = "SELECT svIntCol, mvRawIntCol, mvRawDoubleCol, mvRawStringCol from testTable where "
          + "ARRAYLENGTH(mvRawStringCol) IN (2, 5) ORDER BY svIntCol LIMIT 10";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();
      assertNotNull(resultTable);
      DataSchema dataSchema = new DataSchema(new String[]{
          "svIntCol", "mvRawIntCol", "mvRawDoubleCol", "mvRawStringCol"
      }, new DataSchema.ColumnDataType[]{
          DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT_ARRAY, DataSchema.ColumnDataType.DOUBLE_ARRAY,
          DataSchema.ColumnDataType.STRING_ARRAY
      });
      assertEquals(resultTable.getDataSchema(), dataSchema);
      List<Object[]> recordRows = resultTable.getRows();
      assertEquals(recordRows.size(), 10);

      int[] svExpectedValues = new int[]{0, 0, 0, 0, 1, 1, 1, 1, 2, 2};

      for (int i = 0; i < 10; i++) {
        Object[] values = recordRows.get(i);
        assertEquals(values.length, 4);
        int svIntVal = (int) values[0];
        assertEquals(svIntVal, svExpectedValues[i]);

        int[] intVal = (int[]) values[1];
        assertEquals(svIntVal, intVal[0]);
        assertEquals(intVal[1] - intVal[0], MV_OFFSET);

        double[] doubleVal = (double[]) values[2];
        assertEquals(doubleVal[0], (double) intVal[0]);
        assertEquals(doubleVal[1], (double) intVal[1]);

        String[] stringVal = (String[]) values[3];
        assertEquals(Integer.parseInt(stringVal[0]), intVal[0]);
        assertEquals(Integer.parseInt(stringVal[1]), intVal[1]);
      }
    }
  }

  @Test
  public void testSimpleAggregateQueries() {
    {
      // Aggregation on int columns
      String query = "SELECT COUNTMV(mvIntCol), COUNTMV(mvRawIntCol), SUMMV(mvIntCol), SUMMV(mvRawIntCol), "
          + "MINMV(mvIntCol), MINMV(mvRawIntCol), MAXMV(mvIntCol), MAXMV(mvRawIntCol), AVGMV(mvIntCol), "
          + "AVGMV(mvRawIntCol) from testTable";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();

      DataSchema dataSchema = new DataSchema(new String[]{
          "countmv(mvIntCol)", "countmv(mvRawIntCol)", "summv(mvIntCol)", "summv(mvRawIntCol)", "minmv(mvIntCol)",
          "minmv(mvRawIntCol)", "maxmv(mvIntCol)", "maxmv(mvRawIntCol)", "avgmv(mvIntCol)", "avgmv(mvRawIntCol)"
      }, new DataSchema.ColumnDataType[]{
          DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE
      });
      validateSimpleAggregateQueryResults(resultTable, dataSchema);
    }
    {
      // Aggregation on long columns
      String query = "SELECT COUNTMV(mvLongCol), COUNTMV(mvRawLongCol), SUMMV(mvLongCol), SUMMV(mvRawLongCol), "
          + "MINMV(mvLongCol), MINMV(mvRawLongCol), MAXMV(mvLongCol), MAXMV(mvRawLongCol), AVGMV(mvLongCol), "
          + "AVGMV(mvRawLongCol) from testTable";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();

      DataSchema dataSchema = new DataSchema(new String[]{
          "countmv(mvLongCol)", "countmv(mvRawLongCol)", "summv(mvLongCol)", "summv(mvRawLongCol)", "minmv(mvLongCol)",
          "minmv(mvRawLongCol)", "maxmv(mvLongCol)", "maxmv(mvRawLongCol)", "avgmv(mvLongCol)", "avgmv(mvRawLongCol)"
      }, new DataSchema.ColumnDataType[]{
          DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE
      });
      validateSimpleAggregateQueryResults(resultTable, dataSchema);
    }
    {
      // Aggregation on float columns
      String query = "SELECT COUNTMV(mvFloatCol), COUNTMV(mvRawFloatCol), SUMMV(mvFloatCol), SUMMV(mvRawFloatCol), "
          + "MINMV(mvFloatCol), MINMV(mvRawFloatCol), MAXMV(mvFloatCol), MAXMV(mvRawFloatCol), AVGMV(mvFloatCol), "
          + "AVGMV(mvRawFloatCol) from testTable";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();

      DataSchema dataSchema = new DataSchema(new String[]{
          "countmv(mvFloatCol)", "countmv(mvRawFloatCol)", "summv(mvFloatCol)", "summv(mvRawFloatCol)",
          "minmv(mvFloatCol)", "minmv(mvRawFloatCol)", "maxmv(mvFloatCol)", "maxmv(mvRawFloatCol)",
          "avgmv(mvFloatCol)", "avgmv(mvRawFloatCol)"
      }, new DataSchema.ColumnDataType[]{
          DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE
      });
      validateSimpleAggregateQueryResults(resultTable, dataSchema);
    }
    {
      // Aggregation on double columns
      String query = "SELECT COUNTMV(mvDoubleCol), COUNTMV(mvRawDoubleCol), SUMMV(mvDoubleCol), SUMMV(mvRawDoubleCol), "
          + "MINMV(mvDoubleCol), MINMV(mvRawDoubleCol), MAXMV(mvDoubleCol), MAXMV(mvRawDoubleCol), AVGMV(mvDoubleCol), "
          + "AVGMV(mvRawDoubleCol) from testTable";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();

      DataSchema dataSchema = new DataSchema(new String[]{
          "countmv(mvDoubleCol)", "countmv(mvRawDoubleCol)", "summv(mvDoubleCol)", "summv(mvRawDoubleCol)",
          "minmv(mvDoubleCol)", "minmv(mvRawDoubleCol)", "maxmv(mvDoubleCol)", "maxmv(mvRawDoubleCol)",
          "avgmv(mvDoubleCol)", "avgmv(mvRawDoubleCol)"
      }, new DataSchema.ColumnDataType[]{
          DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE
      });
      validateSimpleAggregateQueryResults(resultTable, dataSchema);
    }
    {
      // Aggregation on string columns
      String query = "SELECT COUNTMV(mvStringCol), COUNTMV(mvRawStringCol), SUMMV(mvStringCol), SUMMV(mvRawStringCol), "
          + "MINMV(mvStringCol), MINMV(mvRawStringCol), MAXMV(mvStringCol), MAXMV(mvRawStringCol), AVGMV(mvStringCol), "
          + "AVGMV(mvRawStringCol) from testTable";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();

      DataSchema dataSchema = new DataSchema(new String[]{
          "countmv(mvStringCol)", "countmv(mvRawStringCol)", "summv(mvStringCol)", "summv(mvRawStringCol)",
          "minmv(mvStringCol)", "minmv(mvRawStringCol)", "maxmv(mvStringCol)", "maxmv(mvRawStringCol)",
          "avgmv(mvStringCol)", "avgmv(mvRawStringCol)"
      }, new DataSchema.ColumnDataType[]{
          DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE
      });
      validateSimpleAggregateQueryResults(resultTable, dataSchema);
    }
    {
      // Aggregation on variable length string columns. Only countmv works for string columns with alpha-numeric
      // characters. Other aggregations work for string columns if the strings are actually numeric strings.
      String query = "SELECT COUNTMV(mvStringCol2), COUNTMV(mvRawStringCol2) from testTable";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();

      DataSchema dataSchema = new DataSchema(new String[]{
          "countmv(mvStringCol2)", "countmv(mvRawStringCol2)"
      }, new DataSchema.ColumnDataType[]{
          DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.LONG
      });
      assertNotNull(resultTable);
      assertEquals(resultTable.getDataSchema(), dataSchema);
      List<Object[]> recordRows = resultTable.getRows();
      assertEquals(recordRows.size(), 1);

      Object[] values = recordRows.get(0);
      assertEquals(values.length, 2);
      long countInt = (long) values[0];
      long countIntRaw = (long) values[1];
      assertEquals(countInt, 160);
      assertEquals(countInt, countIntRaw);
    }
    {
      // Aggregation on variable length string columns. Only countmv works for string columns with alpha-numeric
      // characters. Other aggregations work for string columns if the strings are actually numeric strings.
      String query = "SELECT SUMMV(mvStringCol2), SUMMV(mvRawStringCol2) from testTable";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();
      assertNull(resultTable);

      query = "SELECT AVGMV(mvStringCol2), AVGMV(mvRawStringCol2) from testTable";
      resultTable = getBrokerResponse(query).getResultTable();
      assertNull(resultTable);

      query = "SELECT MINMV(mvStringCol2), MINMV(mvRawStringCol2) from testTable";
      resultTable = getBrokerResponse(query).getResultTable();
      assertNull(resultTable);

      query = "SELECT MAXMV(mvStringCol2), MAXMV(mvRawStringCol2) from testTable";
      resultTable = getBrokerResponse(query).getResultTable();
      assertNull(resultTable);
    }
  }

  private void validateSimpleAggregateQueryResults(ResultTable resultTable, DataSchema expectedDataSchema) {
    assertNotNull(resultTable);
    assertEquals(resultTable.getDataSchema(), expectedDataSchema);
    List<Object[]> recordRows = resultTable.getRows();
    assertEquals(recordRows.size(), 1);

    Object[] values = recordRows.get(0);
    assertEquals(values.length, 10);
    long countInt = (long) values[0];
    long countIntRaw = (long) values[1];
    assertEquals(countInt, 160);
    assertEquals(countInt, countIntRaw);

    double sumInt = (double) values[2];
    double sumIntRaw = (double) values[3];
    assertEquals(sumInt, 88720.0);
    assertEquals(sumInt, sumIntRaw);

    double minInt = (double) values[4];
    double minIntRaw = (double) values[5];
    assertEquals(minInt, 0.0);
    assertEquals(minInt, minIntRaw);

    double maxInt = (double) values[6];
    double maxIntRaw = (double) values[7];
    assertEquals(maxInt, 1109.0);
    assertEquals(maxInt, maxIntRaw);

    double avgInt = (double) values[8];
    double avgIntRaw = (double) values[9];
    assertEquals(avgInt, 554.5);
    assertEquals(avgInt, avgIntRaw);
  }

  @Test
  public void testAggregateWithFilterQueries() {
    {
      // Aggregation on int columns with filter
      String query = "SELECT COUNTMV(mvIntCol), COUNTMV(mvRawIntCol), SUMMV(mvIntCol), SUMMV(mvRawIntCol), "
          + "MINMV(mvIntCol), MINMV(mvRawIntCol), MAXMV(mvIntCol), MAXMV(mvRawIntCol), AVGMV(mvIntCol), "
          + "AVGMV(mvRawIntCol) from testTable WHERE mvRawIntCol > 1000";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();

      DataSchema dataSchema = new DataSchema(new String[]{
          "countmv(mvIntCol)", "countmv(mvRawIntCol)", "summv(mvIntCol)", "summv(mvRawIntCol)", "minmv(mvIntCol)",
          "minmv(mvRawIntCol)", "maxmv(mvIntCol)", "maxmv(mvRawIntCol)", "avgmv(mvIntCol)", "avgmv(mvRawIntCol)"
      }, new DataSchema.ColumnDataType[]{
          DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE
      });
      validateAggregateWithFilterQueryResults(resultTable, dataSchema);
    }
    {
      // Aggregation on double columns with filter
      String query = "SELECT COUNTMV(mvDoubleCol), COUNTMV(mvRawDoubleCol), SUMMV(mvDoubleCol), SUMMV(mvRawDoubleCol), "
          + "MINMV(mvDoubleCol), MINMV(mvRawDoubleCol), MAXMV(mvDoubleCol), MAXMV(mvRawDoubleCol), AVGMV(mvDoubleCol), "
          + "AVGMV(mvRawDoubleCol) from testTable WHERE mvRawDoubleCol > 1000.0";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();

      DataSchema dataSchema = new DataSchema(new String[]{
          "countmv(mvDoubleCol)", "countmv(mvRawDoubleCol)", "summv(mvDoubleCol)", "summv(mvRawDoubleCol)",
          "minmv(mvDoubleCol)", "minmv(mvRawDoubleCol)", "maxmv(mvDoubleCol)", "maxmv(mvRawDoubleCol)",
          "avgmv(mvDoubleCol)", "avgmv(mvRawDoubleCol)"
      }, new DataSchema.ColumnDataType[]{
          DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE
      });
      validateAggregateWithFilterQueryResults(resultTable, dataSchema);
    }
  }

  private void validateAggregateWithFilterQueryResults(ResultTable resultTable, DataSchema expectedDataSchema) {
    assertNotNull(resultTable);
    assertEquals(resultTable.getDataSchema(), expectedDataSchema);
    List<Object[]> recordRows = resultTable.getRows();
    assertEquals(recordRows.size(), 1);

    Object[] values = recordRows.get(0);
    long count = (long) values[0];
    long countRaw = (long) values[1];
    assertEquals(count, 80);
    assertEquals(count, countRaw);

    double sum = (double) values[2];
    double sumRaw = (double) values[3];
    assertEquals(sum, 84360.0);
    assertEquals(sum, sumRaw);

    double min = (double) values[4];
    double minRaw = (double) values[5];
    assertEquals(min, 1000.0);
    assertEquals(min, minRaw);

    double max = (double) values[6];
    double maxRaw = (double) values[7];
    assertEquals(max, 1109.0);
    assertEquals(max, maxRaw);

    double avg = (double) values[8];
    double avgRaw = (double) values[9];
    assertEquals(avg, 1054.5);
    assertEquals(avg, avgRaw);
  }

  @Test
  public void testAggregateWithGroupByQueries() {
    {
      // Aggregation on a single column, group by on a single MV raw column
      String query = "SELECT mvRawIntCol, COUNTMV(mvRawLongCol) from testTable GROUP BY mvRawIntCol ORDER BY "
          + "mvRawIntCol LIMIT 10";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();

      DataSchema dataSchema = new DataSchema(new String[]{
          "mvRawIntCol", "countmv(mvRawLongCol)"
      }, new DataSchema.ColumnDataType[]{
          DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.LONG
      });
      assertNotNull(resultTable);
      assertEquals(resultTable.getDataSchema(), dataSchema);
      List<Object[]> recordRows = resultTable.getRows();
      assertEquals(recordRows.size(), 10);

      for (int i = 0; i < 10; i++) {
        Object[] values = resultTable.getRows().get(i);
        assertEquals(values.length, 2);
        assertEquals((int) values[0], i);
        assertEquals((long) values[1], 8);
      }
    }
    {
      // Aggregation on a single variable length string column, group by on a single MV raw column
      String query = "SELECT mvRawIntCol, COUNTMV(mvRawStringCol2) from testTable GROUP BY mvRawIntCol ORDER BY "
          + "mvRawIntCol LIMIT 10";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();

      DataSchema dataSchema = new DataSchema(new String[]{
          "mvRawIntCol", "countmv(mvRawStringCol2)"
      }, new DataSchema.ColumnDataType[]{
          DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.LONG
      });
      assertNotNull(resultTable);
      assertEquals(resultTable.getDataSchema(), dataSchema);
      List<Object[]> recordRows = resultTable.getRows();
      assertEquals(recordRows.size(), 10);

      for (int i = 0; i < 10; i++) {
        Object[] values = resultTable.getRows().get(i);
        assertEquals(values.length, 2);
        assertEquals((int) values[0], i);
        assertEquals((long) values[1], 8);
      }
    }
    {
      // Aggregation on a single column, group by on 2 MV raw columns
      String query = "SELECT mvRawIntCol, mvRawDoubleCol, COUNTMV(mvRawLongCol) from testTable GROUP BY mvRawIntCol, "
          + "mvRawDoubleCol ORDER BY mvRawIntCol, mvRawDoubleCol LIMIT 10";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();

      DataSchema dataSchema = new DataSchema(new String[]{
          "mvRawIntCol", "mvRawDoubleCol", "countmv(mvRawLongCol)"
      }, new DataSchema.ColumnDataType[]{
          DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.LONG
      });
      assertNotNull(resultTable);
      assertEquals(resultTable.getDataSchema(), dataSchema);
      List<Object[]> recordRows = resultTable.getRows();
      assertEquals(recordRows.size(), 10);

      int[] expectedIntValues = new int[]{0, 0, 1, 1, 2, 2, 3, 3, 4, 4};
      double[] expectedDoubleValues = new double[]{0.0, 100.0, 1.0, 101.0, 2.0, 102.0, 3.0, 103.0, 4.0, 104.0};

      for (int i = 0; i < 10; i++) {
        Object[] values = resultTable.getRows().get(i);
        assertEquals(values.length, 3);
        assertEquals((int) values[0], expectedIntValues[i]);
        assertEquals(values[1], expectedDoubleValues[i]);
        assertEquals((long) values[2], 8);
      }
    }
    {
      // Aggregation on a single column, group by on 2 MV columns, one raw one with dict
      String query = "SELECT mvRawIntCol, mvDoubleCol, COUNTMV(mvRawLongCol) from testTable GROUP BY mvRawIntCol, "
          + "mvDoubleCol ORDER BY mvRawIntCol, mvDoubleCol LIMIT 10";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();

      DataSchema dataSchema = new DataSchema(new String[]{
          "mvRawIntCol", "mvDoubleCol", "countmv(mvRawLongCol)"
      }, new DataSchema.ColumnDataType[]{
          DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.LONG
      });
      assertNotNull(resultTable);
      assertEquals(resultTable.getDataSchema(), dataSchema);
      List<Object[]> recordRows = resultTable.getRows();
      assertEquals(recordRows.size(), 10);

      int[] expectedIntValues = new int[]{0, 0, 1, 1, 2, 2, 3, 3, 4, 4};
      double[] expectedDoubleValues = new double[]{0.0, 100.0, 1.0, 101.0, 2.0, 102.0, 3.0, 103.0, 4.0, 104.0};

      for (int i = 0; i < 10; i++) {
        Object[] values = resultTable.getRows().get(i);
        assertEquals(values.length, 3);
        assertEquals((int) values[0], expectedIntValues[i]);
        assertEquals(values[1], expectedDoubleValues[i]);
        assertEquals((long) values[2], 8);
      }
    }
    {
      // Aggregation on int columns with group by
      String query = "SELECT COUNTMV(mvIntCol), COUNTMV(mvRawIntCol), SUMMV(mvIntCol), SUMMV(mvRawIntCol), "
          + "MINMV(mvIntCol), MINMV(mvRawIntCol), MAXMV(mvIntCol), MAXMV(mvRawIntCol), AVGMV(mvIntCol), "
          + "AVGMV(mvRawIntCol), svIntCol, mvRawLongCol from testTable GROUP BY svIntCol, mvRawLongCol "
          + "ORDER BY svIntCol";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();

      DataSchema dataSchema = new DataSchema(new String[]{
          "countmv(mvIntCol)", "countmv(mvRawIntCol)", "summv(mvIntCol)", "summv(mvRawIntCol)", "minmv(mvIntCol)",
          "minmv(mvRawIntCol)", "maxmv(mvIntCol)", "maxmv(mvRawIntCol)", "avgmv(mvIntCol)", "avgmv(mvRawIntCol)",
          "svIntCol", "mvRawLongCol"
      }, new DataSchema.ColumnDataType[]{
          DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.LONG
      });
      validateAggregateWithGroupByQueryResults(resultTable, dataSchema, false);
    }
    {
      // Aggregation on long columns with group by
      String query = "SELECT COUNTMV(mvLongCol), COUNTMV(mvRawLongCol), SUMMV(mvLongCol), SUMMV(mvRawLongCol), "
          + "MINMV(mvLongCol), MINMV(mvRawLongCol), MAXMV(mvLongCol), MAXMV(mvRawLongCol), AVGMV(mvLongCol), "
          + "AVGMV(mvRawLongCol), svIntCol, mvRawIntCol from testTable GROUP BY svIntCol, mvRawIntCol "
          + "ORDER BY svIntCol";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();

      DataSchema dataSchema = new DataSchema(new String[]{
          "countmv(mvLongCol)", "countmv(mvRawLongCol)", "summv(mvLongCol)", "summv(mvRawLongCol)", "minmv(mvLongCol)",
          "minmv(mvRawLongCol)", "maxmv(mvLongCol)", "maxmv(mvRawLongCol)", "avgmv(mvLongCol)", "avgmv(mvRawLongCol)",
          "svIntCol", "mvRawIntCol"
      }, new DataSchema.ColumnDataType[]{
          DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT
      });
      validateAggregateWithGroupByQueryResults(resultTable, dataSchema, false);
    }
    {
      // Aggregation on float columns with group by
      String query = "SELECT COUNTMV(mvFloatCol), COUNTMV(mvRawFloatCol), SUMMV(mvFloatCol), SUMMV(mvRawFloatCol), "
          + "MINMV(mvFloatCol), MINMV(mvRawFloatCol), MAXMV(mvFloatCol), MAXMV(mvRawFloatCol), AVGMV(mvFloatCol), "
          + "AVGMV(mvRawFloatCol), svIntCol, mvRawIntCol from testTable GROUP BY svIntCol, mvRawIntCol "
          + "ORDER BY svIntCol";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();

      DataSchema dataSchema = new DataSchema(new String[]{
          "countmv(mvFloatCol)", "countmv(mvRawFloatCol)", "summv(mvFloatCol)", "summv(mvRawFloatCol)",
          "minmv(mvFloatCol)", "minmv(mvRawFloatCol)", "maxmv(mvFloatCol)", "maxmv(mvRawFloatCol)",
          "avgmv(mvFloatCol)", "avgmv(mvRawFloatCol)", "svIntCol", "mvRawIntCol"
      }, new DataSchema.ColumnDataType[]{
          DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT
      });
      validateAggregateWithGroupByQueryResults(resultTable, dataSchema, false);
    }
    {
      // Aggregation on double columns with group by
      String query = "SELECT COUNTMV(mvDoubleCol), COUNTMV(mvRawDoubleCol), SUMMV(mvDoubleCol), SUMMV(mvRawDoubleCol), "
          + "MINMV(mvDoubleCol), MINMV(mvRawDoubleCol), MAXMV(mvDoubleCol), MAXMV(mvRawDoubleCol), AVGMV(mvDoubleCol), "
          + "AVGMV(mvRawDoubleCol), svIntCol, mvRawIntCol from testTable GROUP BY svIntCol, mvRawIntCol "
          + "ORDER BY svIntCol";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();

      DataSchema dataSchema = new DataSchema(new String[]{
          "countmv(mvDoubleCol)", "countmv(mvRawDoubleCol)", "summv(mvDoubleCol)", "summv(mvRawDoubleCol)",
          "minmv(mvDoubleCol)", "minmv(mvRawDoubleCol)", "maxmv(mvDoubleCol)", "maxmv(mvRawDoubleCol)",
          "avgmv(mvDoubleCol)", "avgmv(mvRawDoubleCol)", "svIntCol", "mvRawIntCol"
      }, new DataSchema.ColumnDataType[]{
          DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT
      });
      validateAggregateWithGroupByQueryResults(resultTable, dataSchema, false);
    }
    {
      // Aggregation on string columns with group by
      String query = "SELECT COUNTMV(mvStringCol), COUNTMV(mvRawStringCol), SUMMV(mvStringCol), SUMMV(mvRawStringCol), "
          + "MINMV(mvStringCol), MINMV(mvRawStringCol), MAXMV(mvStringCol), MAXMV(mvRawStringCol), AVGMV(mvStringCol), "
          + "AVGMV(mvRawStringCol), svIntCol, mvRawIntCol from testTable GROUP BY svIntCol, mvRawIntCol "
          + "ORDER BY svIntCol";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();

      DataSchema dataSchema = new DataSchema(new String[]{
          "countmv(mvStringCol)", "countmv(mvRawStringCol)", "summv(mvStringCol)", "summv(mvRawStringCol)",
          "minmv(mvStringCol)", "minmv(mvRawStringCol)", "maxmv(mvStringCol)", "maxmv(mvRawStringCol)",
          "avgmv(mvStringCol)", "avgmv(mvRawStringCol)", "svIntCol", "mvRawIntCol"
      }, new DataSchema.ColumnDataType[]{
          DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT
      });
      validateAggregateWithGroupByQueryResults(resultTable, dataSchema, false);
    }
    {
      // Aggregation on variable length string columns with group by. Only count aggregations should work
      String query = "SELECT COUNTMV(mvStringCol2), COUNTMV(mvRawStringCol2), svIntCol, mvRawIntCol from testTable "
          + "GROUP BY svIntCol, mvRawIntCol ORDER BY svIntCol";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();

      DataSchema dataSchema = new DataSchema(new String[]{
          "countmv(mvStringCol2)", "countmv(mvRawStringCol2)", "svIntCol", "mvRawIntCol"
      }, new DataSchema.ColumnDataType[]{
          DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.INT,
          DataSchema.ColumnDataType.INT
      });

      assertNotNull(resultTable);
      assertEquals(resultTable.getDataSchema(), dataSchema);
      List<Object[]> recordRows = resultTable.getRows();
      assertEquals(recordRows.size(), 10);

      for (int i = 0; i < 10; i++) {
        Object[] values = recordRows.get(i);
        assertEquals(values.length, 4);
        assertEquals(values[0], 8L);
        assertEquals(values[1], 8L);
        assertEquals(values[2], i / 2);
        assertTrue((((int) values[3] - (int) values[2]) == 0) || (((int) values[3] - (int) values[2]) == 100));
      }
    }
    {
      // Aggregation on int columns with group by on 3 columns
      String query = "SELECT COUNTMV(mvIntCol), COUNTMV(mvRawIntCol), SUMMV(mvIntCol), SUMMV(mvRawIntCol), "
          + "MINMV(mvIntCol), MINMV(mvRawIntCol), MAXMV(mvIntCol), MAXMV(mvRawIntCol), AVGMV(mvIntCol), "
          + "AVGMV(mvRawIntCol), svIntCol, mvLongCol, mvRawLongCol from testTable GROUP BY svIntCol, mvLongCol, "
          + "mvRawLongCol ORDER BY svIntCol";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();

      DataSchema dataSchema = new DataSchema(new String[]{
          "countmv(mvIntCol)", "countmv(mvRawIntCol)", "summv(mvIntCol)", "summv(mvRawIntCol)", "minmv(mvIntCol)",
          "minmv(mvRawIntCol)", "maxmv(mvIntCol)", "maxmv(mvRawIntCol)", "avgmv(mvIntCol)", "avgmv(mvRawIntCol)",
          "svIntCol", "mvLongCol", "mvRawLongCol"
      }, new DataSchema.ColumnDataType[]{
          DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.LONG,
          DataSchema.ColumnDataType.LONG
      });
      validateAggregateWithGroupByQueryResults(resultTable, dataSchema, true);
    }
    {
      // Aggregation on long columns with group by on 3 columns
      String query = "SELECT COUNTMV(mvLongCol), COUNTMV(mvRawLongCol), SUMMV(mvLongCol), SUMMV(mvRawLongCol), "
          + "MINMV(mvLongCol), MINMV(mvRawLongCol), MAXMV(mvLongCol), MAXMV(mvRawLongCol), AVGMV(mvLongCol), "
          + "AVGMV(mvRawLongCol), svIntCol, mvIntCol, mvRawIntCol from testTable GROUP BY svIntCol, mvIntCol, "
          + "mvRawIntCol ORDER BY svIntCol";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();

      DataSchema dataSchema = new DataSchema(new String[]{
          "countmv(mvLongCol)", "countmv(mvRawLongCol)", "summv(mvLongCol)", "summv(mvRawLongCol)", "minmv(mvLongCol)",
          "minmv(mvRawLongCol)", "maxmv(mvLongCol)", "maxmv(mvRawLongCol)", "avgmv(mvLongCol)", "avgmv(mvRawLongCol)",
          "svIntCol", "mvIntCol", "mvRawIntCol"
      }, new DataSchema.ColumnDataType[]{
          DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT,
          DataSchema.ColumnDataType.INT
      });
      validateAggregateWithGroupByQueryResults(resultTable, dataSchema, true);
    }
    {
      // Aggregation on float columns with group by on 3 columns
      String query = "SELECT COUNTMV(mvFloatCol), COUNTMV(mvRawFloatCol), SUMMV(mvFloatCol), SUMMV(mvRawFloatCol), "
          + "MINMV(mvFloatCol), MINMV(mvRawFloatCol), MAXMV(mvFloatCol), MAXMV(mvRawFloatCol), AVGMV(mvFloatCol), "
          + "AVGMV(mvRawFloatCol), svIntCol, mvIntCol, mvRawIntCol  from testTable GROUP BY svIntCol, mvIntCol, "
          + "mvRawIntCol ORDER BY svIntCol";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();

      DataSchema dataSchema = new DataSchema(new String[]{
          "countmv(mvFloatCol)", "countmv(mvRawFloatCol)", "summv(mvFloatCol)", "summv(mvRawFloatCol)",
          "minmv(mvFloatCol)", "minmv(mvRawFloatCol)", "maxmv(mvFloatCol)", "maxmv(mvRawFloatCol)",
          "avgmv(mvFloatCol)", "avgmv(mvRawFloatCol)", "svIntCol", "mvIntCol", "mvRawIntCol"
      }, new DataSchema.ColumnDataType[]{
          DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT,
          DataSchema.ColumnDataType.INT
      });
      validateAggregateWithGroupByQueryResults(resultTable, dataSchema, true);
    }
    {
      // Aggregation on double columns with group by on 3 columns
      String query = "SELECT COUNTMV(mvDoubleCol), COUNTMV(mvRawDoubleCol), SUMMV(mvDoubleCol), SUMMV(mvRawDoubleCol), "
          + "MINMV(mvDoubleCol), MINMV(mvRawDoubleCol), MAXMV(mvDoubleCol), MAXMV(mvRawDoubleCol), AVGMV(mvDoubleCol), "
          + "AVGMV(mvRawDoubleCol), svIntCol, mvIntCol, mvRawIntCol from testTable GROUP BY svIntCol, mvIntCol, "
          + "mvRawIntCol ORDER BY svIntCol";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();

      DataSchema dataSchema = new DataSchema(new String[]{
          "countmv(mvDoubleCol)", "countmv(mvRawDoubleCol)", "summv(mvDoubleCol)", "summv(mvRawDoubleCol)",
          "minmv(mvDoubleCol)", "minmv(mvRawDoubleCol)", "maxmv(mvDoubleCol)", "maxmv(mvRawDoubleCol)",
          "avgmv(mvDoubleCol)", "avgmv(mvRawDoubleCol)", "svIntCol", "mvIntCol", "mvRawIntCol"
      }, new DataSchema.ColumnDataType[]{
          DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT,
          DataSchema.ColumnDataType.INT
      });
      validateAggregateWithGroupByQueryResults(resultTable, dataSchema, true);
    }
    {
      // Aggregation on string columns with group by on 3 columns
      String query = "SELECT COUNTMV(mvStringCol), COUNTMV(mvRawStringCol), SUMMV(mvStringCol), SUMMV(mvRawStringCol), "
          + "MINMV(mvStringCol), MINMV(mvRawStringCol), MAXMV(mvStringCol), MAXMV(mvRawStringCol), AVGMV(mvStringCol), "
          + "AVGMV(mvRawStringCol), svIntCol, mvIntCol, mvRawIntCol from testTable GROUP BY svIntCol, mvIntCol, "
          + "mvRawIntCol ORDER BY svIntCol";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();

      DataSchema dataSchema = new DataSchema(new String[]{
          "countmv(mvStringCol)", "countmv(mvRawStringCol)", "summv(mvStringCol)", "summv(mvRawStringCol)",
          "minmv(mvStringCol)", "minmv(mvRawStringCol)", "maxmv(mvStringCol)", "maxmv(mvRawStringCol)",
          "avgmv(mvStringCol)", "avgmv(mvRawStringCol)", "svIntCol", "mvIntCol", "mvRawIntCol"
      }, new DataSchema.ColumnDataType[]{
          DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT,
          DataSchema.ColumnDataType.INT
      });
      validateAggregateWithGroupByQueryResults(resultTable, dataSchema, true);
    }
    {
      // Aggregation on variable length string columns with group by on 3 columns. Only count aggregation should work.
      String query = "SELECT COUNTMV(mvStringCol2), COUNTMV(mvRawStringCol2), svIntCol, mvIntCol, mvRawIntCol "
          + "from testTable GROUP BY svIntCol, mvIntCol, mvRawIntCol ORDER BY svIntCol";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();

      DataSchema dataSchema = new DataSchema(new String[]{
          "countmv(mvStringCol2)", "countmv(mvRawStringCol2)", "svIntCol", "mvIntCol", "mvRawIntCol"
      }, new DataSchema.ColumnDataType[]{
          DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.INT,
          DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT
      });

      assertNotNull(resultTable);
      assertEquals(resultTable.getDataSchema(), dataSchema);
      List<Object[]> recordRows = resultTable.getRows();
      assertEquals(recordRows.size(), 10);

      int[] expectedSvIntValues = new int[]{0, 0, 0, 0, 1, 1, 1, 1, 2, 2};

      for (int i = 0; i < 10; i++) {
        Object[] values = recordRows.get(i);
        assertEquals(values.length, 5);
        assertEquals(values[0], 8L);
        assertEquals(values[1], 8L);
        assertEquals(values[2], expectedSvIntValues[i]);
        assertTrue((((int) values[3] - (int) values[2]) == 0) || (((int) values[3] - (int) values[2]) == 100));
        assertTrue((((int) values[4] - (int) values[2]) == 0) || (((int) values[4] - (int) values[2]) == 100));
      }
    }
    {
      // Aggregation on int columns with group by on 3 columns, two of them RAW
      String query = "SELECT COUNTMV(mvIntCol), COUNTMV(mvRawIntCol), SUMMV(mvIntCol), SUMMV(mvRawIntCol), "
          + "MINMV(mvIntCol), MINMV(mvRawIntCol), MAXMV(mvIntCol), MAXMV(mvRawIntCol), AVGMV(mvIntCol), "
          + "AVGMV(mvRawIntCol), svIntCol, mvRawLongCol, mvRawFloatCol from testTable GROUP BY svIntCol, mvRawLongCol, "
          + "mvRawFloatCol ORDER BY svIntCol";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();

      DataSchema dataSchema = new DataSchema(new String[]{
          "countmv(mvIntCol)", "countmv(mvRawIntCol)", "summv(mvIntCol)", "summv(mvRawIntCol)", "minmv(mvIntCol)",
          "minmv(mvRawIntCol)", "maxmv(mvIntCol)", "maxmv(mvRawIntCol)", "avgmv(mvIntCol)", "avgmv(mvRawIntCol)",
          "svIntCol", "mvRawLongCol", "mvRawFloatCol"
      }, new DataSchema.ColumnDataType[]{
          DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.LONG,
          DataSchema.ColumnDataType.FLOAT
      });
      assertNotNull(resultTable);
      assertEquals(resultTable.getDataSchema(), dataSchema);
      List<Object[]> recordRows = resultTable.getRows();
      assertEquals(recordRows.size(), 10);

      int[] expectedSVIntValues;
      expectedSVIntValues = new int[]{0, 0, 0, 0, 1, 1, 1, 1, 2, 2};

      for (int i = 0; i < 10; i++) {
        Object[] values = recordRows.get(i);
        assertEquals(values.length, 13);

        long count = (long) values[0];
        long countRaw = (long) values[1];
        assertEquals(count, 8);
        assertEquals(count, countRaw);

        double sum = (double) values[2];
        double sumRaw = (double) values[3];
        assertEquals(sum, sumRaw);

        double min = (double) values[4];
        double minRaw = (double) values[5];
        assertEquals(min, minRaw);

        double max = (double) values[6];
        double maxRaw = (double) values[7];
        assertEquals(max, maxRaw);

        assertEquals(max - min, (double) MV_OFFSET);

        double avg = (double) values[8];
        double avgRaw = (double) values[9];
        assertEquals(avg, avgRaw);

        assertEquals((int) values[10], expectedSVIntValues[i]);

        assertTrue((long) values[11] == expectedSVIntValues[i]
            || (long) values[11] == expectedSVIntValues[i] + MV_OFFSET);

        assertTrue((float) values[12] == (float) expectedSVIntValues[i]
            || (float) values[12] == (float) (expectedSVIntValues[i] + MV_OFFSET));
      }
    }
  }

  private void validateAggregateWithGroupByQueryResults(ResultTable resultTable, DataSchema expectedDataSchema,
      boolean isThreeColumnGroupBy) {
    assertNotNull(resultTable);
    assertEquals(resultTable.getDataSchema(), expectedDataSchema);
    List<Object[]> recordRows = resultTable.getRows();
    assertEquals(recordRows.size(), 10);

    int[] expectedSVIntValues;

    if (isThreeColumnGroupBy) {
      expectedSVIntValues = new int[]{0, 0, 0, 0, 1, 1, 1, 1, 2, 2};
    } else {
      expectedSVIntValues = new int[]{0, 0, 1, 1, 2, 2, 3, 3, 4, 4};
    }

    for (int i = 0; i < 10; i++) {
      Object[] values = recordRows.get(i);
      if (isThreeColumnGroupBy) {
        assertEquals(values.length, 13);
      } else {
        assertEquals(values.length, 12);
      }

      long count = (long) values[0];
      long countRaw = (long) values[1];
      assertEquals(count, 8);
      assertEquals(count, countRaw);

      double sum = (double) values[2];
      double sumRaw = (double) values[3];
      assertEquals(sum, sumRaw);

      double min = (double) values[4];
      double minRaw = (double) values[5];
      assertEquals(min, minRaw);

      double max = (double) values[6];
      double maxRaw = (double) values[7];
      assertEquals(max, maxRaw);

      assertEquals(max - min, (double) MV_OFFSET);

      double avg = (double) values[8];
      double avgRaw = (double) values[9];
      assertEquals(avg, avgRaw);

      assertEquals((int) values[10], expectedSVIntValues[i]);

      if (expectedDataSchema.getColumnDataType(11) == DataSchema.ColumnDataType.LONG) {
        assertTrue((long) values[11] == expectedSVIntValues[i]
            || (long) values[11] == expectedSVIntValues[i] + MV_OFFSET);
      } else {
        assertTrue((int) values[11] == expectedSVIntValues[i]
            || (int) values[11] == expectedSVIntValues[i] + MV_OFFSET);
      }

      if (isThreeColumnGroupBy) {
        if (expectedDataSchema.getColumnDataType(12) == DataSchema.ColumnDataType.LONG) {
          assertTrue((long) values[12] == expectedSVIntValues[i]
              || (long) values[12] == expectedSVIntValues[i] + MV_OFFSET);
        } else {
          assertTrue((int) values[12] == expectedSVIntValues[i]
              || (int) values[12] == expectedSVIntValues[i] + MV_OFFSET);
        }
      }
    }
  }

  @Test
  public void testAggregateWithGroupByOrderByQueries() {
    {
      // Aggregation on int columns with group by order by
      String query = "SELECT COUNTMV(mvIntCol), COUNTMV(mvRawIntCol), SUMMV(mvIntCol), SUMMV(mvRawIntCol), "
          + "MINMV(mvIntCol), MINMV(mvRawIntCol), MAXMV(mvIntCol), MAXMV(mvRawIntCol), AVGMV(mvIntCol), "
          + "AVGMV(mvRawIntCol), mvRawLongCol from testTable GROUP BY mvRawLongCol ORDER BY mvRawLongCol";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();

      DataSchema dataSchema = new DataSchema(new String[]{
          "countmv(mvIntCol)", "countmv(mvRawIntCol)", "summv(mvIntCol)", "summv(mvRawIntCol)", "minmv(mvIntCol)",
          "minmv(mvRawIntCol)", "maxmv(mvIntCol)", "maxmv(mvRawIntCol)", "avgmv(mvIntCol)", "avgmv(mvRawIntCol)",
          "mvRawLongCol"
      }, new DataSchema.ColumnDataType[]{
          DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.LONG
      });
      validateAggregateWithGroupByOrderByQueryResults(resultTable, dataSchema);
    }
    {
      // Aggregation on long columns with order by (same results as simple aggregation)
      String query = "SELECT COUNTMV(mvLongCol), COUNTMV(mvRawLongCol), SUMMV(mvLongCol), SUMMV(mvRawLongCol), "
          + "MINMV(mvLongCol), MINMV(mvRawLongCol), MAXMV(mvLongCol), MAXMV(mvRawLongCol), AVGMV(mvLongCol), "
          + "AVGMV(mvRawLongCol), mvRawIntCol from testTable GROUP BY mvRawIntCol ORDER BY mvRawIntCol";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();

      DataSchema dataSchema = new DataSchema(new String[]{
          "countmv(mvLongCol)", "countmv(mvRawLongCol)", "summv(mvLongCol)", "summv(mvRawLongCol)", "minmv(mvLongCol)",
          "minmv(mvRawLongCol)", "maxmv(mvLongCol)", "maxmv(mvRawLongCol)", "avgmv(mvLongCol)", "avgmv(mvRawLongCol)",
          "mvRawIntCol"
      }, new DataSchema.ColumnDataType[]{
          DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.INT
      });
      validateAggregateWithGroupByOrderByQueryResults(resultTable, dataSchema);
    }
    {
      // Aggregation on float columns with order by (same results as simple aggregation)
      String query = "SELECT COUNTMV(mvFloatCol), COUNTMV(mvRawFloatCol), SUMMV(mvFloatCol), SUMMV(mvRawFloatCol), "
          + "MINMV(mvFloatCol), MINMV(mvRawFloatCol), MAXMV(mvFloatCol), MAXMV(mvRawFloatCol), AVGMV(mvFloatCol), "
          + "AVGMV(mvRawFloatCol), mvRawIntCol from testTable GROUP BY mvRawIntCol ORDER BY mvRawIntCol";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();

      DataSchema dataSchema = new DataSchema(new String[]{
          "countmv(mvFloatCol)", "countmv(mvRawFloatCol)", "summv(mvFloatCol)", "summv(mvRawFloatCol)",
          "minmv(mvFloatCol)", "minmv(mvRawFloatCol)", "maxmv(mvFloatCol)", "maxmv(mvRawFloatCol)",
          "avgmv(mvFloatCol)", "avgmv(mvRawFloatCol)", "mvRawIntCol"
      }, new DataSchema.ColumnDataType[]{
          DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.INT
      });
      validateAggregateWithGroupByOrderByQueryResults(resultTable, dataSchema);
    }
    {
      // Aggregation on double columns with order by (same results as simple aggregation)
      String query = "SELECT COUNTMV(mvDoubleCol), COUNTMV(mvRawDoubleCol), SUMMV(mvDoubleCol), SUMMV(mvRawDoubleCol), "
          + "MINMV(mvDoubleCol), MINMV(mvRawDoubleCol), MAXMV(mvDoubleCol), MAXMV(mvRawDoubleCol), AVGMV(mvDoubleCol), "
          + "AVGMV(mvRawDoubleCol), mvRawIntCol from testTable GROUP BY mvRawIntCol ORDER BY mvRawIntCol";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();

      DataSchema dataSchema = new DataSchema(new String[]{
          "countmv(mvDoubleCol)", "countmv(mvRawDoubleCol)", "summv(mvDoubleCol)", "summv(mvRawDoubleCol)",
          "minmv(mvDoubleCol)", "minmv(mvRawDoubleCol)", "maxmv(mvDoubleCol)", "maxmv(mvRawDoubleCol)",
          "avgmv(mvDoubleCol)", "avgmv(mvRawDoubleCol)", "mvRawIntCol"
      }, new DataSchema.ColumnDataType[]{
          DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.INT
      });
      validateAggregateWithGroupByOrderByQueryResults(resultTable, dataSchema);
    }
    {
      // Aggregation on string columns with order by (same results as simple aggregation)
      String query = "SELECT COUNTMV(mvStringCol), COUNTMV(mvRawStringCol), SUMMV(mvStringCol), SUMMV(mvRawStringCol), "
          + "MINMV(mvStringCol), MINMV(mvRawStringCol), MAXMV(mvStringCol), MAXMV(mvRawStringCol), AVGMV(mvStringCol), "
          + "AVGMV(mvRawStringCol), mvRawIntCol from testTable GROUP BY mvRawIntCol ORDER BY mvRawIntCol";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();

      DataSchema dataSchema = new DataSchema(new String[]{
          "countmv(mvStringCol)", "countmv(mvRawStringCol)", "summv(mvStringCol)", "summv(mvRawStringCol)",
          "minmv(mvStringCol)", "minmv(mvRawStringCol)", "maxmv(mvStringCol)", "maxmv(mvRawStringCol)",
          "avgmv(mvStringCol)", "avgmv(mvRawStringCol)", "mvRawIntCol"
      }, new DataSchema.ColumnDataType[]{
          DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.INT
      });
      validateAggregateWithGroupByOrderByQueryResults(resultTable, dataSchema);
    }
    {
      // Aggregation on variable length string columns with order by (same results as simple aggregation)
      String query = "SELECT COUNTMV(mvStringCol2), COUNTMV(mvRawStringCol2), mvRawIntCol from testTable GROUP BY "
          + "mvRawIntCol ORDER BY mvRawIntCol";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();

      DataSchema dataSchema = new DataSchema(new String[]{
          "countmv(mvStringCol2)", "countmv(mvRawStringCol2)", "mvRawIntCol"
      }, new DataSchema.ColumnDataType[]{
          DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.INT
      });

      assertNotNull(resultTable);
      assertEquals(resultTable.getDataSchema(), dataSchema);
      List<Object[]> recordRows = resultTable.getRows();
      assertEquals(recordRows.size(), 10);

      for (int i = 0; i < 10; i++) {
        Object[] values = recordRows.get(i);
        assertEquals(values.length, 3);
        assertEquals(values[0], 8L);
        assertEquals(values[1], 8L);
        assertEquals(values[2], i);
      }
    }
    {
      // Aggregation on int columns with group by order by with order by agg
      String query = "SELECT COUNTMV(mvIntCol), COUNTMV(mvRawIntCol), SUMMV(mvIntCol), SUMMV(mvRawIntCol), "
          + "MINMV(mvIntCol), MINMV(mvRawIntCol), MAXMV(mvIntCol), MAXMV(mvRawIntCol), AVGMV(mvIntCol), "
          + "AVGMV(mvRawIntCol), mvRawIntCol from testTable GROUP BY mvRawIntCol ORDER BY mvRawIntCol DESC, "
          + "SUMMV(mvRawIntCol) DESC";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();

      DataSchema dataSchema = new DataSchema(new String[]{
          "countmv(mvIntCol)", "countmv(mvRawIntCol)", "summv(mvIntCol)", "summv(mvRawIntCol)", "minmv(mvIntCol)",
          "minmv(mvRawIntCol)", "maxmv(mvIntCol)", "maxmv(mvRawIntCol)", "avgmv(mvIntCol)", "avgmv(mvRawIntCol)",
          "mvRawIntCol"
      }, new DataSchema.ColumnDataType[]{
          DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
          DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.INT
      });
      assertNotNull(resultTable);
      assertEquals(resultTable.getDataSchema(), dataSchema);
      List<Object[]> recordRows = resultTable.getRows();
      assertEquals(recordRows.size(), 10);

      double[] expectedSumValues = new double[]{8472.0, 8464.0, 8456.0, 8448.0, 8440.0, 8432.0, 8424.0, 8416.0, 8408.0,
          8400.0};
      double[] expectedMinValues = new double[]{1009.0, 1008.0, 1007.0, 1006.0, 1005.0, 1004.0, 1003.0, 1002.0, 1001.0,
          1000.0};
      double[] expectedMaxValues = new double[]{1109.0, 1108.0, 1107.0, 1106.0, 1105.0, 1104.0, 1103.0, 1102.0, 1101.0,
          1100.0};
      double[] expectedAvgValues = new double[]{1059.0, 1058.0, 1057.0, 1056.0, 1055.0, 1054.0, 1053.0, 1052.0, 1051.0,
          1050.0};

      for (int i = 0; i < 10; i++) {
        Object[] values = recordRows.get(i);
        assertEquals(values.length, 11);
        long count = (long) values[0];
        long countRaw = (long) values[1];
        assertEquals(count, 8);
        assertEquals(count, countRaw);

        double sum = (double) values[2];
        double sumRaw = (double) values[3];
        assertEquals(sum, sumRaw);
        assertEquals(sum, expectedSumValues[i]);

        double min = (double) values[4];
        double minRaw = (double) values[5];
        assertEquals(min, minRaw);
        assertEquals(min, expectedMinValues[i]);

        double max = (double) values[6];
        double maxRaw = (double) values[7];
        assertEquals(max, maxRaw);
        assertEquals(max, expectedMaxValues[i]);

        double avg = (double) values[8];
        double avgRaw = (double) values[9];
        assertEquals(avg, avgRaw);
        assertEquals(avg, expectedAvgValues[i]);

        assertEquals((int) values[10], (int) max);
      }
    }
  }

  private void validateAggregateWithGroupByOrderByQueryResults(ResultTable resultTable, DataSchema expectedDataSchema) {
    assertNotNull(resultTable);
    assertEquals(resultTable.getDataSchema(), expectedDataSchema);
    List<Object[]> recordRows = resultTable.getRows();
    assertEquals(recordRows.size(), 10);

    double[] expectedSumValues = new double[]{400.0, 408.0, 416.0, 424.0, 432.0, 440.0, 448.0, 456.0, 464.0, 472.0};
    double[] expectedMinValues = new double[]{0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0};
    double[] expectedMaxValues = new double[]{100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0, 108.0, 109.0};
    double[] expectedAvgValues = new double[]{50.0, 51.0, 52.0, 53.0, 54.0, 55.0, 56.0, 57.0, 58.0, 59.0};

    for (int i = 0; i < 10; i++) {
      Object[] values = recordRows.get(i);
      assertEquals(values.length, 11);
      long count = (long) values[0];
      long countRaw = (long) values[1];
      assertEquals(count, 8);
      assertEquals(count, countRaw);

      double sum = (double) values[2];
      double sumRaw = (double) values[3];
      assertEquals(sum, sumRaw);
      assertEquals(sum, expectedSumValues[i]);

      double min = (double) values[4];
      double minRaw = (double) values[5];
      assertEquals(min, minRaw);
      assertEquals(min, expectedMinValues[i]);

      double max = (double) values[6];
      double maxRaw = (double) values[7];
      assertEquals(max, maxRaw);
      assertEquals(max, expectedMaxValues[i]);

      double avg = (double) values[8];
      double avgRaw = (double) values[9];
      assertEquals(avg, avgRaw);
      assertEquals(avg, expectedAvgValues[i]);

      if (expectedDataSchema.getColumnDataType(10) == DataSchema.ColumnDataType.LONG) {
        assertEquals((long) values[10], i);
      } else {
        assertEquals((int) values[10], i);
      }
    }
  }

  @Test
  public void testTransformInsideAggregateQueries() {
    {
      // Transform within aggregation for raw int MV
      String query = "SELECT SUMMV(VALUEIN(mvRawIntCol, '0', '5')) from testTable WHERE mvRawIntCol IN (0, 5)";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();

      DataSchema dataSchema = new DataSchema(new String[]{"summv(valuein(mvRawIntCol,'0','5'))"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE});
      assertEquals(resultTable.getDataSchema(), dataSchema);

      assertEquals(resultTable.getRows().size(), 1);
      Object[] value = resultTable.getRows().get(0);
      assertEquals(value[0], 20.0);
    }
    {
      // Transform within aggregation for raw long MV
      String query = "SELECT SUMMV(VALUEIN(mvRawLongCol, '0', '5')) from testTable WHERE mvRawLongCol IN (0, 5)";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();

      DataSchema dataSchema = new DataSchema(new String[]{"summv(valuein(mvRawLongCol,'0','5'))"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE});
      assertEquals(resultTable.getDataSchema(), dataSchema);

      assertEquals(resultTable.getRows().size(), 1);
      Object[] value = resultTable.getRows().get(0);
      assertEquals(value[0], 20.0);
    }
    {
      // Transform within aggregation for raw float MV
      String query = "SELECT SUMMV(VALUEIN(mvRawFloatCol, '0', '5')) from testTable WHERE mvRawFloatCol IN (0, 5)";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();

      DataSchema dataSchema = new DataSchema(new String[]{"summv(valuein(mvRawFloatCol,'0','5'))"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE});
      assertEquals(resultTable.getDataSchema(), dataSchema);

      assertEquals(resultTable.getRows().size(), 1);
      Object[] value = resultTable.getRows().get(0);
      assertEquals(value[0], 20.0);
    }
    {
      // Transform within aggregation for raw double MV
      String query = "SELECT SUMMV(VALUEIN(mvRawDoubleCol, '0', '5')) from testTable WHERE mvRawDoubleCol IN (0, 5)";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();

      DataSchema dataSchema = new DataSchema(new String[]{"summv(valuein(mvRawDoubleCol,'0','5'))"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE});
      assertEquals(resultTable.getDataSchema(), dataSchema);

      assertEquals(resultTable.getRows().size(), 1);
      Object[] value = resultTable.getRows().get(0);
      assertEquals(value[0], 20.0);
    }
    {
      // Transform within aggregation for raw String MV
      String query = "SELECT SUMMV(VALUEIN(mvRawStringCol, '0', '5')) from testTable WHERE mvRawStringCol "
          + "IN ('0', '5')";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();

      DataSchema dataSchema = new DataSchema(new String[]{"summv(valuein(mvRawStringCol,'0','5'))"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE});
      assertEquals(resultTable.getDataSchema(), dataSchema);

      assertEquals(resultTable.getRows().size(), 1);
      Object[] value = resultTable.getRows().get(0);
      assertEquals(value[0], 20.0);
    }
    {
      // Transform within aggregation for variable length raw String MV
      int index1 = _random.nextInt(_stringList1.size());
      int index2 = _random.nextInt(_stringList2.size());
      while (index2 == index1) {
        index2 = _random.nextInt(_stringList2.size());
      }
      String val1 = _stringList1.get(index1);
      String val2 = _stringList2.get(index2);
      String query = "SELECT COUNTMV(VALUEIN(mvRawStringCol2, '" + val1 + "', '" + val2 + "')) from testTable "
          + "WHERE mvRawStringCol2 IN ('" + val1 + "', '" + val2 + "')";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();

      DataSchema dataSchema = new DataSchema(new String[]{"countmv(valuein(mvRawStringCol2,'" + val1 + "','" + val2
          + "'))"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.LONG});
      assertEquals(resultTable.getDataSchema(), dataSchema);

      assertEquals(resultTable.getRows().size(), 1);
      Object[] value = resultTable.getRows().get(0);
      assertEquals(value[0], 8L);
    }
  }

  private boolean validateMvArrayLength(String query, int expectedLength, int expectedRecords) {
    ResultTable resultTable = getBrokerResponse(query).getResultTable();
    assertNotNull(resultTable);
    List<Object[]> recordRows = resultTable.getRows();
    assertNotNull(recordRows);
    assertEquals(recordRows.size(), expectedRecords);
    for (int i = 0; i < recordRows.size(); i++) {
      Object[] values = recordRows.get(i);
      assertNotNull(values);
      int len = (int) values[0];
      if (len != expectedLength) {
        return false;
      }
    }
    return true;
  }

  private boolean validateQueryOutputSize(String query, int expectedRecords) {
    ResultTable resultTable = getBrokerResponse(query).getResultTable();
    assertNotNull(resultTable);
    List<Object[]> recordRows = resultTable.getRows();
    assertNotNull(recordRows);
    assertEquals(recordRows.size(), expectedRecords);
    return true;
  }

  private String getConcatQueryString(String concatFunction, String col1, String col2, String concatCol, String table,
      int limit) {
    return String.format("SELECT %s(%s, %s) AS %s FROM %s LIMIT %d", concatFunction, col1, col2, concatCol, table,
        limit);
  }

  private String getConcatNestingQueryString(String concatFunction, String col1, String col2, String concatCol,
      String table, int limit) {
    return String.format("SELECT arraylength(%s(%s, %s)) AS %s FROM %s LIMIT %d", concatFunction, col1, col2, concatCol,
        table, limit);
  }

  private String getConcatUseInWhereQueryString(String concatFunction, String col1, String col2, String concatCol,
      String table, String compareOperator, int mvPerArray, int limit) {
    String function = String.format("%s(%s, %s)", concatFunction, col1, col2);
    return String.format("SELECT %s AS %s FROM %s WHERE arraylength(%s) %s %d LIMIT %d", function, concatCol, table,
        function, compareOperator, mvPerArray, limit);
  }

  private String getConcatGroupByQueryString(String concatFunction, String col1, String col2, String concatCol,
      String table, String compareOperator, int mvPerArray, int limit) {
    String function = String.format("%s(%s, %s)", concatFunction, col1, col2);
    return String.format("SELECT %s AS %s, sum(svIntCol) FROM %s WHERE arraylength(%s) %s %d GROUP BY %s LIMIT %d",
        function, concatCol, table, function, compareOperator, mvPerArray, concatCol, limit);
  }

  @Test
  public void testArrayConcatInt() {
    {
      // Test basic arrayConcatInt() functionality
      String query = getConcatQueryString("arrayConcatInt", "mvIntCol", "mvRawIntCol", "concatInts", "testTable",
          NUM_UNIQUE_RECORDS_PER_SEGMENT);
      ResultTable resultTable = getBrokerResponse(query).getResultTable();
      assertNotNull(resultTable);
      List<Object[]> recordRows = resultTable.getRows();
      assertNotNull(recordRows);
      assertEquals(recordRows.size(), NUM_UNIQUE_RECORDS_PER_SEGMENT);
      for (int i = 0; i < recordRows.size(); i++) {
        Object[] values = recordRows.get(i);
        assertNotNull(values);
        assertTrue(values.length >= 1);
        int[] intValues = (int[]) values[0];
        int baseValue = intValues[0];
        int[] expectedConcatInt = new int[]{baseValue, baseValue + MV_OFFSET, baseValue, baseValue + MV_OFFSET};
        assertTrue(Arrays.equals(intValues, expectedConcatInt));
      }
    }
    {
      // Test nesting arrayConcatInt()
      String query =
          getConcatNestingQueryString("arrayConcatInt", "mvIntCol", "mvRawIntCol", "concatIntsLen", "testTable",
              NUM_UNIQUE_RECORDS_PER_SEGMENT);
      // After concatenation, make sure each multivalue entry has double the entries
      assertTrue(validateMvArrayLength(query, 2 * MV_LENGTH, NUM_UNIQUE_RECORDS_PER_SEGMENT));
    }
    {
      // Test use of the output in WHERE expression
      String query =
          getConcatUseInWhereQueryString("arrayConcatInt", "mvIntCol", "mvRawIntCol", "concatInts", "testTable", "<=",
              2 * MV_LENGTH, NUM_UNIQUE_RECORDS_PER_SEGMENT);
      assertTrue(validateQueryOutputSize(query, NUM_UNIQUE_RECORDS_PER_SEGMENT));
      query =
          getConcatUseInWhereQueryString("arrayConcatInt", "mvIntCol", "mvRawIntCol", "concatInts", "testTable", ">",
              2 * MV_LENGTH, NUM_UNIQUE_RECORDS_PER_SEGMENT);
      assertTrue(validateQueryOutputSize(query, 0));
    }
    {
      // Test use of output in GROUP BY
      String query =
          getConcatGroupByQueryString("arrayConcatInt", "mvIntCol", "mvRawIntCol", "concatInts", "testTable", "<=",
              2 * MV_LENGTH, NUM_UNIQUE_RECORDS_PER_SEGMENT);
      assertTrue(validateQueryOutputSize(query, NUM_UNIQUE_RECORDS_PER_SEGMENT));
    }
  }

  @Test
  public void testArrayConcatLong() {
    {
      // Test basic arrayConcatLong() functionality
      String query = getConcatQueryString("arrayConcatLong", "mvLongCol", "mvRawLongCol", "concatLongs", "testTable",
          NUM_UNIQUE_RECORDS_PER_SEGMENT);
      ResultTable resultTable = getBrokerResponse(query).getResultTable();
      assertNotNull(resultTable);
      List<Object[]> recordRows = resultTable.getRows();
      assertNotNull(recordRows);
      assertEquals(recordRows.size(), NUM_UNIQUE_RECORDS_PER_SEGMENT);
      for (int i = 0; i < recordRows.size(); i++) {
        Object[] values = recordRows.get(i);
        assertNotNull(values);
        assertTrue(values.length >= 1);
        long[] intValues = (long[]) values[0];
        long baseValue = intValues[0];
        long[] expectedConcatInt = new long[]{baseValue, baseValue + MV_OFFSET, baseValue, baseValue + MV_OFFSET};
        assertTrue(Arrays.equals(intValues, expectedConcatInt));
      }
    }
    {
      // Test nesting arrayConcatLong()
      String query =
          getConcatNestingQueryString("arrayConcatLong", "mvLongCol", "mvRawLongCol", "concatLongsLen", "testTable",
              NUM_UNIQUE_RECORDS_PER_SEGMENT);
      // After concatenation, make sure each multivalue entry has double the entries
      assertTrue(validateMvArrayLength(query, 2 * MV_LENGTH, NUM_UNIQUE_RECORDS_PER_SEGMENT));
    }
    {
      // Test use of the output in WHERE expression
      String query =
          getConcatUseInWhereQueryString("arrayConcatLong", "mvLongCol", "mvRawLongCol", "concatLongs", "testTable",
              "<=", 2 * MV_LENGTH, NUM_UNIQUE_RECORDS_PER_SEGMENT);
      assertTrue(validateQueryOutputSize(query, NUM_UNIQUE_RECORDS_PER_SEGMENT));
      query = getConcatUseInWhereQueryString("arrayConcatLong", "mvLongCol", "mvRawLongCol", "concatLongs", "testTable",
          ">", 2 * MV_LENGTH, NUM_UNIQUE_RECORDS_PER_SEGMENT);
      assertTrue(validateQueryOutputSize(query, 0));
    }
    {
      // Test use of output in GROUP BY
      String query =
          getConcatGroupByQueryString("arrayConcatLong", "mvLongCol", "mvRawLongCol", "concatLongs", "testTable", "<=",
              2 * MV_LENGTH, NUM_UNIQUE_RECORDS_PER_SEGMENT);
      assertTrue(validateQueryOutputSize(query, NUM_UNIQUE_RECORDS_PER_SEGMENT));
    }
  }

  @Test
  public void testArrayConcatFloat() {
    {
      // Test basic arrayConcatFloat() functionality
      String query =
          getConcatQueryString("arrayConcatFloat", "mvFloatCol", "mvRawFloatCol", "concatFloats", "testTable",
              NUM_UNIQUE_RECORDS_PER_SEGMENT);
      ResultTable resultTable = getBrokerResponse(query).getResultTable();
      assertNotNull(resultTable);
      List<Object[]> recordRows = resultTable.getRows();
      assertNotNull(recordRows);
      assertEquals(recordRows.size(), NUM_UNIQUE_RECORDS_PER_SEGMENT);
      for (int i = 0; i < recordRows.size(); i++) {
        Object[] values = recordRows.get(i);
        assertNotNull(values);
        assertTrue(values.length >= 1);
        float[] intValues = (float[]) values[0];
        float baseValue = intValues[0];
        float[] expectedConcatInt = new float[]{baseValue, baseValue + MV_OFFSET, baseValue, baseValue + MV_OFFSET};
        assertTrue(Arrays.equals(intValues, expectedConcatInt));
      }
    }
    {
      // Test nesting arrayConcatFloat()
      String query =
          getConcatNestingQueryString("arrayConcatFloat", "mvFloatCol", "mvRawFloatCol", "concatFloatsLen", "testTable",
              NUM_UNIQUE_RECORDS_PER_SEGMENT);
      // After concatenation, make sure each multivalue entry has double the entries
      assertTrue(validateMvArrayLength(query, 2 * MV_LENGTH, NUM_UNIQUE_RECORDS_PER_SEGMENT));
    }
    {
      // Test use of the output in WHERE expression
      String query =
          getConcatUseInWhereQueryString("arrayConcatFloat", "mvFloatCol", "mvRawFloatCol", "concatFloats", "testTable",
              "<=", 2 * MV_LENGTH, NUM_UNIQUE_RECORDS_PER_SEGMENT);
      assertTrue(validateQueryOutputSize(query, NUM_UNIQUE_RECORDS_PER_SEGMENT));
      query =
          getConcatUseInWhereQueryString("arrayConcatFloat", "mvFloatCol", "mvRawFloatCol", "concatFloats", "testTable",
              ">", 2 * MV_LENGTH, NUM_UNIQUE_RECORDS_PER_SEGMENT);
      assertTrue(validateQueryOutputSize(query, 0));
    }
    {
      // Test use of output in GROUP BY
      String query =
          getConcatGroupByQueryString("arrayConcatFloat", "mvFloatCol", "mvRawFloatCol", "concatFloats", "testTable",
              "<=", 2 * MV_LENGTH, NUM_UNIQUE_RECORDS_PER_SEGMENT);
      assertTrue(validateQueryOutputSize(query, NUM_UNIQUE_RECORDS_PER_SEGMENT));
    }
  }

  @Test
  public void testArrayConcatDouble() {
    {
      // Test basic arrayConcatDouble() functionality
      String query =
          getConcatQueryString("arrayConcatDouble", "mvDoubleCol", "mvRawDoubleCol", "concatDoubles", "testTable",
              NUM_UNIQUE_RECORDS_PER_SEGMENT);
      ResultTable resultTable = getBrokerResponse(query).getResultTable();
      assertNotNull(resultTable);
      List<Object[]> recordRows = resultTable.getRows();
      assertNotNull(recordRows);
      assertEquals(recordRows.size(), NUM_UNIQUE_RECORDS_PER_SEGMENT);
      for (int i = 0; i < recordRows.size(); i++) {
        Object[] values = recordRows.get(i);
        assertNotNull(values);
        assertTrue(values.length >= 1);
        double[] intValues = (double[]) values[0];
        double baseValue = intValues[0];
        double[] expectedConcatInt = new double[]{baseValue, baseValue + MV_OFFSET, baseValue, baseValue + MV_OFFSET};
        assertTrue(Arrays.equals(intValues, expectedConcatInt));
      }
    }
    {
      // Test nesting arrayConcatDouble()
      String query =
          getConcatNestingQueryString("arrayConcatDouble", "mvDoubleCol", "mvRawDoubleCol", "concatDoublesLen",
              "testTable", NUM_UNIQUE_RECORDS_PER_SEGMENT);
      // After concatenation, make sure each multivalue entry has double the entries
      assertTrue(validateMvArrayLength(query, 2 * MV_LENGTH, NUM_UNIQUE_RECORDS_PER_SEGMENT));
    }
    {
      // Test use of the output in WHERE expression
      String query =
          getConcatUseInWhereQueryString("arrayConcatDouble", "mvDoubleCol", "mvRawDoubleCol", "concatDoubles",
              "testTable", "<=", 2 * MV_LENGTH, NUM_UNIQUE_RECORDS_PER_SEGMENT);
      assertTrue(validateQueryOutputSize(query, NUM_UNIQUE_RECORDS_PER_SEGMENT));
      query = getConcatUseInWhereQueryString("arrayConcatDouble", "mvDoubleCol", "mvRawDoubleCol", "concatDoubles",
          "testTable", ">", 2 * MV_LENGTH, NUM_UNIQUE_RECORDS_PER_SEGMENT);
      assertTrue(validateQueryOutputSize(query, 0));
    }
    {
      // Test use of output in GROUP BY
      String query = getConcatGroupByQueryString("arrayConcatDouble", "mvDoubleCol", "mvRawDoubleCol", "concatDoubles",
          "testTable", "<=", 2 * MV_LENGTH, NUM_UNIQUE_RECORDS_PER_SEGMENT);
      assertTrue(validateQueryOutputSize(query, NUM_UNIQUE_RECORDS_PER_SEGMENT));
    }
  }

  @Test
  public void testArrayConcatString() {
    {
      // Test basic arrayConcatDouble() functionality
      String query =
          getConcatQueryString("arrayConcatString", "mvStringCol", "mvRawStringCol", "concatStrings", "testTable",
              NUM_UNIQUE_RECORDS_PER_SEGMENT);
      ResultTable resultTable = getBrokerResponse(query).getResultTable();
      assertNotNull(resultTable);
      List<Object[]> recordRows = resultTable.getRows();
      assertNotNull(recordRows);
      assertEquals(recordRows.size(), NUM_UNIQUE_RECORDS_PER_SEGMENT);
      for (int i = 0; i < recordRows.size(); i++) {
        Object[] values = recordRows.get(i);
        assertNotNull(values);
        assertTrue(values.length >= 1);
        String[] stringValues = (String[]) values[0];
        assertTrue(stringValues.length == 2 * MV_LENGTH);
        String[] expectedConcatStrings = new String[2 * MV_LENGTH];
        for (int j = 0, k = MV_LENGTH; j < MV_LENGTH; j++) {
          expectedConcatStrings[j] = stringValues[j];
          expectedConcatStrings[k++] = stringValues[j];
        }
        assertTrue(Arrays.equals(stringValues, expectedConcatStrings));
      }
    }
    {
      // Test nesting arrayConcatString()
      String query =
          getConcatNestingQueryString("arrayConcatString", "mvStringCol", "mvRawStringCol", "concatStringLen",
              "testTable", NUM_UNIQUE_RECORDS_PER_SEGMENT);
      // After concatenation, make sure each multivalue entry has double the entries
      assertTrue(validateMvArrayLength(query, 2 * MV_LENGTH, NUM_UNIQUE_RECORDS_PER_SEGMENT));
    }
    {
      // Test use of the output in WHERE expression
      String query =
          getConcatUseInWhereQueryString("arrayConcatString", "mvStringCol", "mvRawStringCol", "concatStrings",
              "testTable", "<=", 2 * MV_LENGTH, NUM_UNIQUE_RECORDS_PER_SEGMENT);
      assertTrue(validateQueryOutputSize(query, NUM_UNIQUE_RECORDS_PER_SEGMENT));
      query = getConcatUseInWhereQueryString("arrayConcatString", "mvStringCol", "mvRawStringCol", "concatStrings",
          "testTable", ">", 2 * MV_LENGTH, NUM_UNIQUE_RECORDS_PER_SEGMENT);
      assertTrue(validateQueryOutputSize(query, 0));
    }
    {
      // Test use of output in GROUP BY
      String query = getConcatGroupByQueryString("arrayConcatString", "mvStringCol", "mvRawStringCol", "concatStrings",
          "testTable", "<=", 2 * MV_LENGTH, NUM_UNIQUE_RECORDS_PER_SEGMENT);
      assertTrue(validateQueryOutputSize(query, NUM_UNIQUE_RECORDS_PER_SEGMENT));
    }
  }
}
