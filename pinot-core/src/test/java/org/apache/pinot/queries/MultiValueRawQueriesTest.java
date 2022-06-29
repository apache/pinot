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
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


// TODO: Add tests for more query patterns when additional fixes for MV raw columns are made
public class MultiValueRawQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "MultiValueRawQueriesTest");

  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME_1 = "testSegment1";
  private static final String SEGMENT_NAME_2 = "testSegment2";

  private static final int NUM_UNIQUE_RECORDS_PER_SEGMENT = 10;
  private static final int NUM_DUPLICATES_PER_RECORDS = 2;
  private static final int MV_OFFSET = 100;
  private static final int BASE_VALUE_1 = 0;
  private static final int BASE_VALUE_2 = 1000;

  private final static String SV_INT_COL = "svIntCol";
  private final static String MV_INT_COL = "mvIntCol";
  private final static String MV_LONG_COL = "mvLongCol";
  private final static String MV_FLOAT_COL = "mvFloatCol";
  private final static String MV_DOUBLE_COL = "mvDoubleCol";
  private final static String MV_STRING_COL = "mvStringCol";
  private final static String MV_RAW_INT_COL = "mvRawIntCol";
  private final static String MV_RAW_LONG_COL = "mvRawLongCol";
  private final static String MV_RAW_FLOAT_COL = "mvRawFloatCol";
  private final static String MV_RAW_DOUBLE_COL = "mvRawDoubleCol";
  private final static String MV_RAW_STRING_COL = "mvRawStringCol";

  private static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
      .addSingleValueDimension(SV_INT_COL, FieldSpec.DataType.INT)
      .addMultiValueDimension(MV_INT_COL, FieldSpec.DataType.INT)
      .addMultiValueDimension(MV_LONG_COL, FieldSpec.DataType.LONG)
      .addMultiValueDimension(MV_FLOAT_COL, FieldSpec.DataType.FLOAT)
      .addMultiValueDimension(MV_DOUBLE_COL, FieldSpec.DataType.DOUBLE)
      .addMultiValueDimension(MV_STRING_COL, FieldSpec.DataType.STRING)
      .addMultiValueDimension(MV_RAW_INT_COL, FieldSpec.DataType.INT)
      .addMultiValueDimension(MV_RAW_LONG_COL, FieldSpec.DataType.LONG)
      .addMultiValueDimension(MV_RAW_FLOAT_COL, FieldSpec.DataType.FLOAT)
      .addMultiValueDimension(MV_RAW_DOUBLE_COL, FieldSpec.DataType.DOUBLE)
      .addMultiValueDimension(MV_RAW_STRING_COL, FieldSpec.DataType.STRING)
      .build();

  private static final DataSchema DATA_SCHEMA = new DataSchema(new String[]{"mvDoubleCol", "mvFloatCol", "mvIntCol",
      "mvLongCol", "mvRawDoubleCol", "mvRawFloatCol", "mvRawIntCol", "mvRawLongCol", "mvRawStringCol", "mvStringCol",
      "svIntCol"},
      new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE_ARRAY, DataSchema.ColumnDataType.FLOAT_ARRAY,
          DataSchema.ColumnDataType.INT_ARRAY, DataSchema.ColumnDataType.LONG_ARRAY,
          DataSchema.ColumnDataType.DOUBLE_ARRAY, DataSchema.ColumnDataType.FLOAT_ARRAY,
          DataSchema.ColumnDataType.INT_ARRAY, DataSchema.ColumnDataType.LONG_ARRAY,
          DataSchema.ColumnDataType.STRING_ARRAY, DataSchema.ColumnDataType.STRING_ARRAY,
          DataSchema.ColumnDataType.INT});

  private static final TableConfig TABLE = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
      .setNoDictionaryColumns(
          Arrays.asList(MV_RAW_INT_COL, MV_RAW_LONG_COL, MV_RAW_FLOAT_COL, MV_RAW_DOUBLE_COL, MV_RAW_STRING_COL))
      .build();

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
    FileUtils.deleteQuietly(INDEX_DIR);

    ImmutableSegment segment1 = createSegment(generateRecords(BASE_VALUE_1), SEGMENT_NAME_1);
    ImmutableSegment segment2 = createSegment(generateRecords(BASE_VALUE_2), SEGMENT_NAME_2);
    _indexSegment = segment1;
    _indexSegments = Arrays.asList(segment1, segment2);
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
        assertEquals(values.length, 11);
        int svIntValue = (int) values[10];
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
        String[] stringValuesRaw = (String[]) values[9];
        assertEquals(Integer.parseInt(stringValues[0]), intValues[0]);
        assertEquals(Integer.parseInt(stringValues[1]), intValues[1]);
        assertEquals(stringValues[0], stringValuesRaw[0]);
        assertEquals(stringValues[1], stringValuesRaw[1]);

        actualValuesFirst.add(intValues[0]);
        actualValuesSecond.add(intValues[1]);
      }
      assertTrue(actualValuesFirst.containsAll(expectedValuesFirst));
      assertTrue(actualValuesSecond.containsAll(expectedValuesSecond));
    }
    {
      // Select some dict based MV and some raw MV columns. Validate that the values match for the corresponding rows
      String query = "SELECT mvIntCol, mvDoubleCol, mvStringCol, mvRawIntCol, mvRawDoubleCol, mvRawStringCol, svIntCol "
          + "from testTable ORDER BY svIntCol LIMIT 40";
      ResultTable resultTable = getBrokerResponse(query).getResultTable();
      assertNotNull(resultTable);
      DataSchema dataSchema = new DataSchema(new String[]{
          "mvIntCol", "mvDoubleCol", "mvStringCol", "mvRawIntCol", "mvRawDoubleCol", "mvRawStringCol", "svIntCol"
      }, new DataSchema.ColumnDataType[]{
          DataSchema.ColumnDataType.INT_ARRAY, DataSchema.ColumnDataType.DOUBLE_ARRAY,
          DataSchema.ColumnDataType.STRING_ARRAY, DataSchema.ColumnDataType.INT_ARRAY,
          DataSchema.ColumnDataType.DOUBLE_ARRAY, DataSchema.ColumnDataType.STRING_ARRAY,
          DataSchema.ColumnDataType.INT
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
        assertEquals(values.length, 7);
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
        assertEquals(intVal, intRawVal);
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
  }

  private void validateSimpleAggregateQueryResults(ResultTable resultTable, DataSchema expectedDataSchema) {
    assertNotNull(resultTable);
    assertEquals(resultTable.getDataSchema(), expectedDataSchema);
    List<Object[]> recordRows = resultTable.getRows();
    assertEquals(recordRows.size(), 1);

    Object[] values = recordRows.get(0);
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
  public void testAggregateWithGroupByQueries() {
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
          + "AVGMV(mvRawStringCol), svIntCol, mvIntCol, mvRawIntCol from testTable GROUP BY svIntCol, mvIntCol,"
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
}
