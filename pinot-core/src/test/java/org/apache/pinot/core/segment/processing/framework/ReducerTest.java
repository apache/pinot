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
package org.apache.pinot.core.segment.processing.framework;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.core.segment.processing.genericrow.GenericRowFileManager;
import org.apache.pinot.core.segment.processing.genericrow.GenericRowFileReader;
import org.apache.pinot.core.segment.processing.genericrow.GenericRowFileRecordReader;
import org.apache.pinot.core.segment.processing.genericrow.GenericRowFileWriter;
import org.apache.pinot.core.segment.processing.reducer.Reducer;
import org.apache.pinot.core.segment.processing.reducer.ReducerFactory;
import org.apache.pinot.core.segment.processing.timehandler.TimeHandler;
import org.apache.pinot.core.segment.processing.utils.SegmentProcessorUtils;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;


/**
 * Tests for {@link Reducer}
 */
public class ReducerTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "ReducerTest");
  private static final File FILE_MANAGER_OUTPUT_DIR = new File(TEMP_DIR, "fileManagerOutput");
  private static final File REDUCER_OUTPUT_DIR = new File(TEMP_DIR, "reducerOutput");
  private static final Random RANDOM = new Random();

  @BeforeMethod
  public void setUp()
      throws IOException {
    FileUtils.deleteQuietly(TEMP_DIR);
    FileUtils.forceMkdir(FILE_MANAGER_OUTPUT_DIR);
    FileUtils.forceMkdir(REDUCER_OUTPUT_DIR);
  }

  @AfterMethod
  public void tearDown() {
    FileUtils.deleteQuietly(TEMP_DIR);
  }

  @Test
  public void testConcat()
      throws Exception {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    Schema schema =
        new Schema.SchemaBuilder().setSchemaName("testTable").addSingleValueDimension("d", DataType.INT).build();
    Pair<List<FieldSpec>, Integer> result = SegmentProcessorUtils.getFieldSpecs(schema, MergeType.CONCAT, null);
    GenericRowFileManager fileManager =
        new GenericRowFileManager(FILE_MANAGER_OUTPUT_DIR, result.getLeft(), false, result.getRight());

    GenericRowFileWriter fileWriter = fileManager.getFileWriter();
    int numRecords = 100;
    int[] expectedValues = new int[numRecords];
    GenericRow row = new GenericRow();
    for (int i = 0; i < numRecords; i++) {
      row.clear();
      int value = RANDOM.nextInt();
      row.putValue("d", value);
      fileWriter.write(row);
      expectedValues[i] = value;
    }
    fileManager.closeFileWriter();

    SegmentProcessorConfig config = new SegmentProcessorConfig.Builder().setTableConfig(tableConfig).setSchema(schema)
        .setMergeType(MergeType.CONCAT).build();
    Reducer reducer = ReducerFactory.getReducer("0", fileManager, config, REDUCER_OUTPUT_DIR);
    GenericRowFileManager reducedFileManager = reducer.reduce();
    GenericRowFileReader fileReader = reducedFileManager.getFileReader();
    GenericRowFileRecordReader recordReader = fileReader.getRecordReader();
    for (int i = 0; i < numRecords; i++) {
      row.clear();
      recordReader.read(i, row);
      Map<String, Object> fieldToValueMap = row.getFieldToValueMap();
      assertEquals(fieldToValueMap.size(), 1);
      assertEquals(fieldToValueMap.get("d"), expectedValues[i]);
    }
    reducedFileManager.cleanUp();
  }

  @Test
  public void testConcatWithNull()
      throws Exception {
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").setNullHandlingEnabled(true).build();
    Schema schema =
        new Schema.SchemaBuilder().setSchemaName("testTable").addSingleValueDimension("d", DataType.INT).build();
    Pair<List<FieldSpec>, Integer> result = SegmentProcessorUtils.getFieldSpecs(schema, MergeType.CONCAT, null);
    GenericRowFileManager fileManager =
        new GenericRowFileManager(FILE_MANAGER_OUTPUT_DIR, result.getLeft(), true, result.getRight());

    GenericRowFileWriter fileWriter = fileManager.getFileWriter();
    int numRecords = 100;
    int[] expectedValues = new int[numRecords];
    boolean[] expectedIsNulls = new boolean[numRecords];
    GenericRow row = new GenericRow();
    for (int i = 0; i < numRecords; i++) {
      row.clear();
      int value;
      boolean isNull = RANDOM.nextBoolean();
      if (isNull) {
        value = Integer.MIN_VALUE;
        row.putDefaultNullValue("d", value);
      } else {
        value = RANDOM.nextInt();
        row.putValue("d", value);
      }
      expectedValues[i] = value;
      expectedIsNulls[i] = isNull;
      fileWriter.write(row);
    }
    fileManager.closeFileWriter();

    SegmentProcessorConfig config = new SegmentProcessorConfig.Builder().setTableConfig(tableConfig).setSchema(schema)
        .setMergeType(MergeType.CONCAT).build();
    Reducer reducer = ReducerFactory.getReducer("0", fileManager, config, REDUCER_OUTPUT_DIR);
    GenericRowFileManager reducedFileManager = reducer.reduce();
    GenericRowFileReader fileReader = reducedFileManager.getFileReader();
    GenericRowFileRecordReader recordReader = fileReader.getRecordReader();
    for (int i = 0; i < numRecords; i++) {
      row.clear();
      recordReader.read(i, row);
      Map<String, Object> fieldToValueMap = row.getFieldToValueMap();
      assertEquals(fieldToValueMap.size(), 1);
      assertEquals(fieldToValueMap.get("d"), expectedValues[i]);
      assertEquals(row.isNullValue("d"), expectedIsNulls[i]);
    }
    reducedFileManager.cleanUp();
  }

  @Test
  public void testConcatWithSort()
      throws Exception {
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").setSortedColumn("d").build();
    Schema schema =
        new Schema.SchemaBuilder().setSchemaName("testTable").addSingleValueDimension("d", DataType.INT).build();
    Pair<List<FieldSpec>, Integer> result =
        SegmentProcessorUtils.getFieldSpecs(schema, MergeType.CONCAT, Collections.singletonList("d"));
    GenericRowFileManager fileManager =
        new GenericRowFileManager(FILE_MANAGER_OUTPUT_DIR, result.getLeft(), false, result.getRight());

    GenericRowFileWriter fileWriter = fileManager.getFileWriter();
    int numRecords = 100;
    int[] expectedValues = new int[numRecords];
    GenericRow row = new GenericRow();
    for (int i = 0; i < numRecords; i++) {
      row.clear();
      int value = RANDOM.nextInt();
      row.putValue("d", value);
      fileWriter.write(row);
      expectedValues[i] = value;
    }
    fileManager.closeFileWriter();

    Arrays.sort(expectedValues);
    SegmentProcessorConfig config = new SegmentProcessorConfig.Builder().setTableConfig(tableConfig).setSchema(schema)
        .setMergeType(MergeType.CONCAT).build();
    Reducer reducer = ReducerFactory.getReducer("0", fileManager, config, REDUCER_OUTPUT_DIR);
    GenericRowFileManager reducedFileManager = reducer.reduce();
    GenericRowFileReader fileReader = reducedFileManager.getFileReader();
    GenericRowFileRecordReader recordReader = fileReader.getRecordReader();
    for (int i = 0; i < numRecords; i++) {
      row.clear();
      recordReader.read(i, row);
      Map<String, Object> fieldToValueMap = row.getFieldToValueMap();
      assertEquals(fieldToValueMap.size(), 1);
      assertEquals(fieldToValueMap.get("d"), expectedValues[i]);
    }
    reducedFileManager.cleanUp();
  }

  @Test
  public void testRollup()
      throws Exception {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testTable").addSingleValueDimension("d", DataType.INT)
        .addMetric("m1", DataType.INT).addMetric("m2", DataType.LONG).addMetric("m3", DataType.FLOAT).build();
    Pair<List<FieldSpec>, Integer> result = SegmentProcessorUtils.getFieldSpecs(schema, MergeType.ROLLUP, null);
    GenericRowFileManager fileManager =
        new GenericRowFileManager(FILE_MANAGER_OUTPUT_DIR, result.getLeft(), false, result.getRight());

    GenericRowFileWriter fileWriter = fileManager.getFileWriter();
    int numRecords = 100;
    // NOTE: Use TreeMap so that the entries are sorted
    Map<Integer, Object[]> expectedValues = new TreeMap<>();
    GenericRow row = new GenericRow();
    for (int i = 0; i < numRecords; i++) {
      row.clear();
      int d = RANDOM.nextInt(10);
      int m1 = RANDOM.nextInt();
      long m2 = RANDOM.nextLong();
      float m3 = RANDOM.nextFloat();
      row.putValue("d", d);
      row.putValue("m1", m1);
      row.putValue("m2", m2);
      row.putValue("m3", m3);
      fileWriter.write(row);
      Object[] metrics = expectedValues.get(d);
      if (metrics == null) {
        expectedValues.put(d, new Object[]{m1, m2, m3});
      } else {
        metrics[0] = (int) metrics[0] + m1;
        metrics[1] = Math.min((long) metrics[1], m2);
        metrics[2] = Math.max((float) metrics[2], m3);
      }
    }
    fileManager.closeFileWriter();

    Map<String, AggregationFunctionType> aggregationTypes = new HashMap<>();
    aggregationTypes.put("m1", AggregationFunctionType.SUM);
    aggregationTypes.put("m2", AggregationFunctionType.MIN);
    aggregationTypes.put("m3", AggregationFunctionType.MAX);
    SegmentProcessorConfig config = new SegmentProcessorConfig.Builder().setTableConfig(tableConfig).setSchema(schema)
        .setMergeType(MergeType.ROLLUP).setAggregationTypes(aggregationTypes).build();
    Reducer reducer = ReducerFactory.getReducer("0", fileManager, config, REDUCER_OUTPUT_DIR);
    GenericRowFileManager reducedFileManager = reducer.reduce();
    GenericRowFileReader fileReader = reducedFileManager.getFileReader();
    assertEquals(fileReader.getNumRows(), expectedValues.size());
    GenericRowFileRecordReader recordReader = fileReader.getRecordReader();
    int rowId = 0;
    for (Map.Entry<Integer, Object[]> entry : expectedValues.entrySet()) {
      row.clear();
      recordReader.read(rowId++, row);
      Map<String, Object> fieldToValueMap = row.getFieldToValueMap();
      assertEquals(fieldToValueMap.size(), 4);
      assertEquals(fieldToValueMap.get("d"), entry.getKey());
      Object[] expectedMetrics = entry.getValue();
      assertEquals(fieldToValueMap.get("m1"), expectedMetrics[0]);
      assertEquals(fieldToValueMap.get("m2"), expectedMetrics[1]);
      assertEquals(fieldToValueMap.get("m3"), expectedMetrics[2]);
    }
  }

  @Test
  public void testRollupWithNull()
      throws Exception {
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").setNullHandlingEnabled(true).build();
    // NOTE: Intentionally put non-zero default value for metric to test that null values are skipped
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testTable").addSingleValueDimension("d", DataType.INT)
        .addMetric("m", DataType.INT, 1).build();
    Pair<List<FieldSpec>, Integer> result = SegmentProcessorUtils.getFieldSpecs(schema, MergeType.ROLLUP, null);
    GenericRowFileManager fileManager =
        new GenericRowFileManager(FILE_MANAGER_OUTPUT_DIR, result.getLeft(), true, result.getRight());

    GenericRowFileWriter fileWriter = fileManager.getFileWriter();
    int numRecords = 100;
    // NOTE: Use TreeMap so that the entries are sorted
    Map<Integer, Integer> expectedValues = new TreeMap<>();
    GenericRow row = new GenericRow();
    for (int i = 0; i < numRecords; i++) {
      row.clear();
      int d = RANDOM.nextInt(10);
      row.putValue("d", d);
      if (RANDOM.nextBoolean()) {
        row.putDefaultNullValue("m", 1);
        // Put 0 as the expected value because null value should be skipped
        expectedValues.putIfAbsent(d, 0);
      } else {
        int m = RANDOM.nextInt();
        row.putValue("m", m);
        expectedValues.merge(d, m, Integer::sum);
      }
      fileWriter.write(row);
    }
    fileManager.closeFileWriter();

    SegmentProcessorConfig config = new SegmentProcessorConfig.Builder().setTableConfig(tableConfig).setSchema(schema)
        .setMergeType(MergeType.ROLLUP).build();
    Reducer reducer = ReducerFactory.getReducer("0", fileManager, config, REDUCER_OUTPUT_DIR);
    GenericRowFileManager reducedFileManager = reducer.reduce();
    GenericRowFileReader fileReader = reducedFileManager.getFileReader();
    assertEquals(fileReader.getNumRows(), expectedValues.size());
    GenericRowFileRecordReader recordReader = fileReader.getRecordReader();
    int rowId = 0;
    for (Map.Entry<Integer, Integer> entry : expectedValues.entrySet()) {
      row.clear();
      recordReader.read(rowId++, row);
      Map<String, Object> fieldToValueMap = row.getFieldToValueMap();
      assertEquals(fieldToValueMap.size(), 2);
      assertEquals(fieldToValueMap.get("d"), entry.getKey());
      int m = (int) fieldToValueMap.get("m");
      if (row.isNullValue("m")) {
        assertEquals(m, 1);
      } else {
        assertEquals(m, (int) entry.getValue());
      }
    }
  }

  @Test
  public void testRollupWithMV()
      throws Exception {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testTable").addMultiValueDimension("d", DataType.INT)
        .addMetric("m", DataType.INT).build();
    Pair<List<FieldSpec>, Integer> result = SegmentProcessorUtils.getFieldSpecs(schema, MergeType.ROLLUP, null);
    GenericRowFileManager fileManager =
        new GenericRowFileManager(FILE_MANAGER_OUTPUT_DIR, result.getLeft(), false, result.getRight());

    GenericRowFileWriter fileWriter = fileManager.getFileWriter();
    int numRecords = 100;
    // NOTE: dValues are sorted
    Object[][] dValues = new Object[][]{
        new Object[]{0}, new Object[]{1}, new Object[]{2}, new Object[]{0, 1}, new Object[]{0, 2}, new Object[]{1, 0},
        new Object[]{1, 2}, new Object[]{2, 0}, new Object[]{2, 1}, new Object[]{0, 1, 2}
    };
    // NOTE: Use TreeMap so that the entries are sorted
    Map<Integer, Integer> expectedValues = new TreeMap<>();
    GenericRow row = new GenericRow();
    for (int i = 0; i < numRecords; i++) {
      row.clear();
      int dIndex = RANDOM.nextInt(10);
      int m = RANDOM.nextInt();
      row.putValue("d", dValues[dIndex]);
      row.putValue("m", m);
      fileWriter.write(row);
      expectedValues.merge(dIndex, m, Integer::sum);
    }
    fileManager.closeFileWriter();

    SegmentProcessorConfig config = new SegmentProcessorConfig.Builder().setTableConfig(tableConfig).setSchema(schema)
        .setMergeType(MergeType.ROLLUP).build();
    Reducer reducer = ReducerFactory.getReducer("0", fileManager, config, REDUCER_OUTPUT_DIR);
    GenericRowFileManager reducedFileManager = reducer.reduce();
    GenericRowFileReader fileReader = reducedFileManager.getFileReader();
    assertEquals(fileReader.getNumRows(), expectedValues.size());
    GenericRowFileRecordReader recordReader = fileReader.getRecordReader();
    int rowId = 0;
    for (Map.Entry<Integer, Integer> entry : expectedValues.entrySet()) {
      row.clear();
      recordReader.read(rowId++, row);
      Map<String, Object> fieldToValueMap = row.getFieldToValueMap();
      assertEquals(fieldToValueMap.size(), 2);
      assertEquals(fieldToValueMap.get("d"), dValues[entry.getKey()]);
      assertEquals(fieldToValueMap.get("m"), entry.getValue());
    }
  }

  @Test
  public void testRollupWithSort()
      throws Exception {
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").setSortedColumn("d2").build();
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testTable").addSingleValueDimension("d1", DataType.INT)
        .addSingleValueDimension("d2", DataType.INT).addMetric("m", DataType.INT).build();
    Pair<List<FieldSpec>, Integer> result =
        SegmentProcessorUtils.getFieldSpecs(schema, MergeType.ROLLUP, Collections.singletonList("d2"));
    GenericRowFileManager fileManager =
        new GenericRowFileManager(FILE_MANAGER_OUTPUT_DIR, result.getLeft(), false, result.getRight());

    GenericRowFileWriter fileWriter = fileManager.getFileWriter();
    int numRecords = 100;
    // NOTE: dValues are sorted with the second value (d2), secondary sort with the first value (d1)
    int[][] dValues = new int[][]{
        new int[]{1, 0}, new int[]{5, 0}, new int[]{10, 0}, new int[]{3, 2}, new int[]{0, 5}, new int[]{5, 5},
        new int[]{8, 5}, new int[]{2, 6}, new int[]{4, 6}, new int[]{1, 10}
    };
    // NOTE: Use TreeMap so that the entries are sorted
    Map<Integer, Integer> expectedValues = new TreeMap<>();
    GenericRow row = new GenericRow();
    for (int i = 0; i < numRecords; i++) {
      row.clear();
      int dIndex = RANDOM.nextInt(10);
      int m = RANDOM.nextInt();
      row.putValue("d1", dValues[dIndex][0]);
      row.putValue("d2", dValues[dIndex][1]);
      row.putValue("m", m);
      fileWriter.write(row);
      expectedValues.merge(dIndex, m, Integer::sum);
    }
    fileManager.closeFileWriter();

    SegmentProcessorConfig config = new SegmentProcessorConfig.Builder().setTableConfig(tableConfig).setSchema(schema)
        .setMergeType(MergeType.ROLLUP).build();
    Reducer reducer = ReducerFactory.getReducer("0", fileManager, config, REDUCER_OUTPUT_DIR);
    GenericRowFileManager reducedFileManager = reducer.reduce();
    GenericRowFileReader fileReader = reducedFileManager.getFileReader();
    assertEquals(fileReader.getNumRows(), expectedValues.size());
    GenericRowFileRecordReader recordReader = fileReader.getRecordReader();
    int rowId = 0;
    for (Map.Entry<Integer, Integer> entry : expectedValues.entrySet()) {
      row.clear();
      recordReader.read(rowId++, row);
      Map<String, Object> fieldToValueMap = row.getFieldToValueMap();
      assertEquals(fieldToValueMap.size(), 3);
      int expectedDIndex = entry.getKey();
      assertEquals(fieldToValueMap.get("d1"), dValues[expectedDIndex][0]);
      assertEquals(fieldToValueMap.get("d2"), dValues[expectedDIndex][1]);
      assertEquals(fieldToValueMap.get("m"), entry.getValue());
    }
  }

  @Test
  public void testRollupWithFirstLast()
      throws Exception {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testTable").addSingleValueDimension("d", DataType.INT)
        .addMetric("m1", DataType.LONG).addMetric("m2", DataType.LONG).build();
    Pair<List<FieldSpec>, Integer> result = SegmentProcessorUtils.getFieldSpecs(schema, MergeType.ROLLUP, null, true);
    List<FieldSpec> fieldSpecs = result.getLeft();
    int numSortFields = result.getRight();
    // The hidden original time column should be appended as the last sort field
    assertEquals(numSortFields, 2);
    assertEquals(fieldSpecs.get(numSortFields - 1).getName(), TimeHandler.ORIGINAL_TIME_MS_COLUMN);
    GenericRowFileManager fileManager =
        new GenericRowFileManager(FILE_MANAGER_OUTPUT_DIR, fieldSpecs, false, numSortFields, true);

    GenericRowFileWriter fileWriter = fileManager.getFileWriter();
    int numDimensionValues = 10;
    int numRecordsPerDimensionValue = 10;
    List<Long> times = new ArrayList<>();
    for (long timeMs = 0; timeMs < numRecordsPerDimensionValue; timeMs++) {
      times.add(timeMs);
    }
    GenericRow row = new GenericRow();
    for (int d = 0; d < numDimensionValues; d++) {
      // Write the rows in random time order to verify that the values are picked based on the original time order
      Collections.shuffle(times, RANDOM);
      for (long timeMs : times) {
        row.clear();
        row.putValue("d", d);
        row.putValue(TimeHandler.ORIGINAL_TIME_MS_COLUMN, timeMs);
        // Metric values encode the original time so that the expected first/last values can be derived
        row.putValue("m1", timeMs * 100 + d);
        row.putValue("m2", timeMs * 100 + d);
        fileWriter.write(row);
      }
    }
    fileManager.closeFileWriter();

    Map<String, AggregationFunctionType> aggregationTypes = new HashMap<>();
    aggregationTypes.put("m1", AggregationFunctionType.LASTWITHTIME);
    aggregationTypes.put("m2", AggregationFunctionType.FIRSTWITHTIME);
    SegmentProcessorConfig config = new SegmentProcessorConfig.Builder().setTableConfig(tableConfig).setSchema(schema)
        .setMergeType(MergeType.ROLLUP).setAggregationTypes(aggregationTypes).build();
    Reducer reducer = ReducerFactory.getReducer("0", fileManager, config, REDUCER_OUTPUT_DIR);
    GenericRowFileManager reducedFileManager = reducer.reduce();
    GenericRowFileReader fileReader = reducedFileManager.getFileReader();
    assertEquals(fileReader.getNumRows(), numDimensionValues);
    GenericRowFileRecordReader recordReader = fileReader.getRecordReader();
    for (int d = 0; d < numDimensionValues; d++) {
      row.clear();
      recordReader.read(d, row);
      Map<String, Object> fieldToValueMap = row.getFieldToValueMap();
      // The hidden original time column should be stripped from the output
      assertEquals(fieldToValueMap.size(), 3);
      assertEquals(fieldToValueMap.get("d"), d);
      assertEquals(fieldToValueMap.get("m1"), (numRecordsPerDimensionValue - 1) * 100L + d);
      assertEquals(fieldToValueMap.get("m2"), (long) d);
    }
    reducedFileManager.cleanUp();
  }

  @Test
  public void testRollupWithFirstLastAndNull()
      throws Exception {
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").setNullHandlingEnabled(true).build();
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testTable").addSingleValueDimension("d", DataType.INT)
        .addMetric("m1", DataType.LONG, -1L).addMetric("m2", DataType.LONG, -1L).build();
    Pair<List<FieldSpec>, Integer> result = SegmentProcessorUtils.getFieldSpecs(schema, MergeType.ROLLUP, null, true);
    GenericRowFileManager fileManager =
        new GenericRowFileManager(FILE_MANAGER_OUTPUT_DIR, result.getLeft(), true, result.getRight(), true);

    GenericRowFileWriter fileWriter = fileManager.getFileWriter();
    GenericRow row = new GenericRow();
    // Null values should be skipped:
    // m1 (last): null at time > 2, so the last non-null value (at time 2) should be picked
    // m2 (first): null at time < 2, so the first non-null value (at time 2) should be picked
    long[] times = new long[]{4, 1, 3, 0, 2};
    for (long timeMs : times) {
      row.clear();
      row.putValue("d", 0);
      row.putValue(TimeHandler.ORIGINAL_TIME_MS_COLUMN, timeMs);
      if (timeMs > 2) {
        row.putDefaultNullValue("m1", -1L);
      } else {
        row.putValue("m1", timeMs * 100);
      }
      if (timeMs < 2) {
        row.putDefaultNullValue("m2", -1L);
      } else {
        row.putValue("m2", timeMs * 100);
      }
      fileWriter.write(row);
    }
    fileManager.closeFileWriter();

    Map<String, AggregationFunctionType> aggregationTypes = new HashMap<>();
    aggregationTypes.put("m1", AggregationFunctionType.LASTWITHTIME);
    aggregationTypes.put("m2", AggregationFunctionType.FIRSTWITHTIME);
    SegmentProcessorConfig config = new SegmentProcessorConfig.Builder().setTableConfig(tableConfig).setSchema(schema)
        .setMergeType(MergeType.ROLLUP).setAggregationTypes(aggregationTypes).build();
    Reducer reducer = ReducerFactory.getReducer("0", fileManager, config, REDUCER_OUTPUT_DIR);
    GenericRowFileManager reducedFileManager = reducer.reduce();
    GenericRowFileReader fileReader = reducedFileManager.getFileReader();
    assertEquals(fileReader.getNumRows(), 1);
    GenericRowFileRecordReader recordReader = fileReader.getRecordReader();
    row.clear();
    recordReader.read(0, row);
    Map<String, Object> fieldToValueMap = row.getFieldToValueMap();
    assertEquals(fieldToValueMap.size(), 3);
    assertEquals(fieldToValueMap.get("d"), 0);
    assertEquals(fieldToValueMap.get("m1"), 200L);
    assertEquals(fieldToValueMap.get("m2"), 200L);
    reducedFileManager.cleanUp();
  }

  @Test
  public void testRollupFirstLastWithoutOriginalTimeField()
      throws Exception {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testTable").addSingleValueDimension("d", DataType.INT)
        .addMetric("m", DataType.LONG).build();
    Pair<List<FieldSpec>, Integer> result = SegmentProcessorUtils.getFieldSpecs(schema, MergeType.ROLLUP, null);
    GenericRowFileManager fileManager =
        new GenericRowFileManager(FILE_MANAGER_OUTPUT_DIR, result.getLeft(), false, result.getRight());

    GenericRowFileWriter fileWriter = fileManager.getFileWriter();
    GenericRow row = new GenericRow();
    row.putValue("d", 0);
    row.putValue("m", 1L);
    fileWriter.write(row);
    fileManager.closeFileWriter();

    Map<String, AggregationFunctionType> aggregationTypes = new HashMap<>();
    aggregationTypes.put("m", AggregationFunctionType.LASTWITHTIME);
    SegmentProcessorConfig config = new SegmentProcessorConfig.Builder().setTableConfig(tableConfig).setSchema(schema)
        .setMergeType(MergeType.ROLLUP).setAggregationTypes(aggregationTypes).build();
    Reducer reducer = ReducerFactory.getReducer("0", fileManager, config, REDUCER_OUTPUT_DIR);
    // Without the hidden original time column, order sensitive aggregation should fail
    assertThrows(IllegalStateException.class, reducer::reduce);
    fileManager.cleanUp();
  }

  @Test
  public void testRollupWithColumnNamedLikeOriginalTimeField()
      throws Exception {
    // A schema column which happens to share the hidden column name must be treated as a regular group field when
    // order sensitive aggregation is not configured: not stripped from the output, and part of the group key
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testTable")
        .addSingleValueDimension(TimeHandler.ORIGINAL_TIME_MS_COLUMN, DataType.LONG)
        .addMetric("m", DataType.INT).build();
    Pair<List<FieldSpec>, Integer> result = SegmentProcessorUtils.getFieldSpecs(schema, MergeType.ROLLUP, null);
    // The real column is the only (and last) sort field, mimicking the hidden column layout
    assertEquals((int) result.getRight(), 1);
    assertEquals(result.getLeft().get(0).getName(), TimeHandler.ORIGINAL_TIME_MS_COLUMN);
    GenericRowFileManager fileManager =
        new GenericRowFileManager(FILE_MANAGER_OUTPUT_DIR, result.getLeft(), false, result.getRight());

    GenericRowFileWriter fileWriter = fileManager.getFileWriter();
    GenericRow row = new GenericRow();
    long[] dValues = new long[]{1, 0, 2, 0, 1};
    for (int i = 0; i < dValues.length; i++) {
      row.clear();
      row.putValue(TimeHandler.ORIGINAL_TIME_MS_COLUMN, dValues[i]);
      row.putValue("m", i + 1);
      fileWriter.write(row);
    }
    fileManager.closeFileWriter();

    SegmentProcessorConfig config = new SegmentProcessorConfig.Builder().setTableConfig(tableConfig).setSchema(schema)
        .setMergeType(MergeType.ROLLUP).build();
    Reducer reducer = ReducerFactory.getReducer("0", fileManager, config, REDUCER_OUTPUT_DIR);
    GenericRowFileManager reducedFileManager = reducer.reduce();
    GenericRowFileReader fileReader = reducedFileManager.getFileReader();
    assertEquals(fileReader.getNumRows(), 3);
    GenericRowFileRecordReader recordReader = fileReader.getRecordReader();
    long[] expectedDValues = new long[]{0, 1, 2};
    int[] expectedMValues = new int[]{2 + 4, 1 + 5, 3};
    for (int i = 0; i < 3; i++) {
      row.clear();
      recordReader.read(i, row);
      Map<String, Object> fieldToValueMap = row.getFieldToValueMap();
      assertEquals(fieldToValueMap.size(), 2);
      assertEquals(fieldToValueMap.get(TimeHandler.ORIGINAL_TIME_MS_COLUMN), expectedDValues[i]);
      assertEquals(fieldToValueMap.get("m"), expectedMValues[i]);
    }
    reducedFileManager.cleanUp();
  }

  @Test
  public void testConcatIgnoresAggregationTypes()
      throws Exception {
    // For CONCAT merge type, aggregation type configs (including order sensitive ones) are ignored: rows pass
    // through unchanged and no hidden time column is involved
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testTable").addSingleValueDimension("d", DataType.INT)
        .addMetric("m", DataType.LONG).build();
    Pair<List<FieldSpec>, Integer> result = SegmentProcessorUtils.getFieldSpecs(schema, MergeType.CONCAT, null);
    GenericRowFileManager fileManager =
        new GenericRowFileManager(FILE_MANAGER_OUTPUT_DIR, result.getLeft(), false, result.getRight());

    GenericRowFileWriter fileWriter = fileManager.getFileWriter();
    GenericRow row = new GenericRow();
    int numRecords = 5;
    for (int i = 0; i < numRecords; i++) {
      row.clear();
      row.putValue("d", 0);
      row.putValue("m", (long) i);
      fileWriter.write(row);
    }
    fileManager.closeFileWriter();

    Map<String, AggregationFunctionType> aggregationTypes = new HashMap<>();
    aggregationTypes.put("m", AggregationFunctionType.LASTWITHTIME);
    SegmentProcessorConfig config = new SegmentProcessorConfig.Builder().setTableConfig(tableConfig).setSchema(schema)
        .setMergeType(MergeType.CONCAT).setAggregationTypes(aggregationTypes).build();
    Reducer reducer = ReducerFactory.getReducer("0", fileManager, config, REDUCER_OUTPUT_DIR);
    GenericRowFileManager reducedFileManager = reducer.reduce();
    GenericRowFileReader fileReader = reducedFileManager.getFileReader();
    assertEquals(fileReader.getNumRows(), numRecords);
    GenericRowFileRecordReader recordReader = fileReader.getRecordReader();
    for (int i = 0; i < numRecords; i++) {
      row.clear();
      recordReader.read(i, row);
      Map<String, Object> fieldToValueMap = row.getFieldToValueMap();
      assertEquals(fieldToValueMap.size(), 2);
      assertEquals(fieldToValueMap.get("m"), (long) i);
    }
    reducedFileManager.cleanUp();
  }

  @Test
  public void testHasOriginalTimeFieldRequiresHiddenColumn() {
    // The explicit flag must match the field layout: the last sort field has to be the hidden original time column
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testTable").addSingleValueDimension("d", DataType.INT)
        .addMetric("m", DataType.INT).build();
    Pair<List<FieldSpec>, Integer> result = SegmentProcessorUtils.getFieldSpecs(schema, MergeType.ROLLUP, null);
    assertThrows(IllegalArgumentException.class,
        () -> new GenericRowFileManager(FILE_MANAGER_OUTPUT_DIR, result.getLeft(), false, result.getRight(), true));
  }

  @Test
  public void testDedup()
      throws Exception {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testTable").addSingleValueDimension("d", DataType.INT)
        .addMetric("m", DataType.INT).build();
    Pair<List<FieldSpec>, Integer> result = SegmentProcessorUtils.getFieldSpecs(schema, MergeType.DEDUP, null);
    GenericRowFileManager fileManager =
        new GenericRowFileManager(FILE_MANAGER_OUTPUT_DIR, result.getLeft(), false, result.getRight());

    GenericRowFileWriter fileWriter = fileManager.getFileWriter();
    int numRecords = 100;
    // NOTE: Use TreeSet so that the entries are sorted
    Set<Integer> expectedValues = new TreeSet<>();
    GenericRow row = new GenericRow();
    for (int i = 0; i < numRecords; i++) {
      row.clear();
      int d = RANDOM.nextInt(5);
      int m = RANDOM.nextInt(5);
      row.putValue("d", d);
      row.putValue("m", m);
      fileWriter.write(row);
      expectedValues.add(d * 5 + m);
    }
    fileManager.closeFileWriter();

    SegmentProcessorConfig config =
        new SegmentProcessorConfig.Builder().setTableConfig(tableConfig).setSchema(schema).setMergeType(MergeType.DEDUP)
            .build();
    Reducer reducer = ReducerFactory.getReducer("0", fileManager, config, REDUCER_OUTPUT_DIR);
    GenericRowFileManager reducedFileManager = reducer.reduce();
    GenericRowFileReader fileReader = reducedFileManager.getFileReader();
    assertEquals(fileReader.getNumRows(), expectedValues.size());
    GenericRowFileRecordReader recordReader = fileReader.getRecordReader();
    int rowId = 0;
    for (int expectedValue : expectedValues) {
      row.clear();
      recordReader.read(rowId++, row);
      Map<String, Object> fieldToValueMap = row.getFieldToValueMap();
      assertEquals(fieldToValueMap.size(), 2);
      assertEquals(fieldToValueMap.get("d"), expectedValue / 5);
      assertEquals(fieldToValueMap.get("m"), expectedValue % 5);
    }
  }

  @Test
  public void testDedupWithMV()
      throws Exception {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    Schema schema =
        new Schema.SchemaBuilder().setSchemaName("testTable").addMultiValueDimension("d", DataType.INT).build();
    Pair<List<FieldSpec>, Integer> result = SegmentProcessorUtils.getFieldSpecs(schema, MergeType.DEDUP, null);
    GenericRowFileManager fileManager =
        new GenericRowFileManager(FILE_MANAGER_OUTPUT_DIR, result.getLeft(), false, result.getRight());

    GenericRowFileWriter fileWriter = fileManager.getFileWriter();
    int numRecords = 100;
    // NOTE: dValues are sorted
    Object[][] dValues = new Object[][]{
        new Object[]{0}, new Object[]{1}, new Object[]{2}, new Object[]{0, 1}, new Object[]{0, 2}, new Object[]{1, 0},
        new Object[]{1, 2}, new Object[]{2, 0}, new Object[]{2, 1}, new Object[]{0, 1, 2}
    };
    // NOTE: Use TreeSet so that the entries are sorted
    Set<Integer> expectedValues = new TreeSet<>();
    GenericRow row = new GenericRow();
    for (int i = 0; i < numRecords; i++) {
      row.clear();
      int dIndex = RANDOM.nextInt(10);
      row.putValue("d", dValues[dIndex]);
      fileWriter.write(row);
      expectedValues.add(dIndex);
    }
    fileManager.closeFileWriter();

    SegmentProcessorConfig config = new SegmentProcessorConfig.Builder().setTableConfig(tableConfig).setSchema(schema)
        .setMergeType(MergeType.ROLLUP).build();
    Reducer reducer = ReducerFactory.getReducer("0", fileManager, config, REDUCER_OUTPUT_DIR);
    GenericRowFileManager reducedFileManager = reducer.reduce();
    GenericRowFileReader fileReader = reducedFileManager.getFileReader();
    assertEquals(fileReader.getNumRows(), expectedValues.size());
    GenericRowFileRecordReader recordReader = fileReader.getRecordReader();
    int rowId = 0;
    for (int expectedValue : expectedValues) {
      row.clear();
      recordReader.read(rowId++, row);
      Map<String, Object> fieldToValueMap = row.getFieldToValueMap();
      assertEquals(fieldToValueMap.size(), 1);
      assertEquals(fieldToValueMap.get("d"), dValues[expectedValue]);
    }
  }

  @Test
  public void testDedupWithSort()
      throws Exception {
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").setSortedColumn("d2").build();
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testTable").addSingleValueDimension("d1", DataType.INT)
        .addSingleValueDimension("d2", DataType.INT).build();
    Pair<List<FieldSpec>, Integer> result =
        SegmentProcessorUtils.getFieldSpecs(schema, MergeType.DEDUP, Collections.singletonList("d2"));
    GenericRowFileManager fileManager =
        new GenericRowFileManager(FILE_MANAGER_OUTPUT_DIR, result.getLeft(), false, result.getRight());

    GenericRowFileWriter fileWriter = fileManager.getFileWriter();
    int numRecords = 100;
    // NOTE: dValues are sorted with the second value (d2), secondary sort with the first value (d1)
    int[][] dValues = new int[][]{
        new int[]{1, 0}, new int[]{5, 0}, new int[]{10, 0}, new int[]{3, 2}, new int[]{0, 5}, new int[]{5, 5},
        new int[]{8, 5}, new int[]{2, 6}, new int[]{4, 6}, new int[]{1, 10}
    };
    // NOTE: Use TreeSet so that the entries are sorted
    Set<Integer> expectedValues = new TreeSet<>();
    GenericRow row = new GenericRow();
    for (int i = 0; i < numRecords; i++) {
      row.clear();
      int dIndex = RANDOM.nextInt(10);
      int m = RANDOM.nextInt();
      row.putValue("d1", dValues[dIndex][0]);
      row.putValue("d2", dValues[dIndex][1]);
      row.putValue("m", m);
      fileWriter.write(row);
      expectedValues.add(dIndex);
    }
    fileManager.closeFileWriter();

    SegmentProcessorConfig config = new SegmentProcessorConfig.Builder().setTableConfig(tableConfig).setSchema(schema)
        .setMergeType(MergeType.ROLLUP).build();
    Reducer reducer = ReducerFactory.getReducer("0", fileManager, config, REDUCER_OUTPUT_DIR);
    GenericRowFileManager reducedFileManager = reducer.reduce();
    GenericRowFileReader fileReader = reducedFileManager.getFileReader();
    assertEquals(fileReader.getNumRows(), expectedValues.size());
    GenericRowFileRecordReader recordReader = fileReader.getRecordReader();
    int rowId = 0;
    for (int expectedDIndex : expectedValues) {
      row.clear();
      recordReader.read(rowId++, row);
      Map<String, Object> fieldToValueMap = row.getFieldToValueMap();
      assertEquals(fieldToValueMap.size(), 2);
      assertEquals(fieldToValueMap.get("d1"), dValues[expectedDIndex][0]);
      assertEquals(fieldToValueMap.get("d2"), dValues[expectedDIndex][1]);
    }
  }
}
