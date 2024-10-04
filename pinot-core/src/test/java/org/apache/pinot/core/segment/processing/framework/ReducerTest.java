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
import org.apache.pinot.core.segment.processing.genericrow.GenericRowFileWriter;
import org.apache.pinot.core.segment.processing.mapper.GenericRowMapperOutputRecordReader;
import org.apache.pinot.core.segment.processing.mapper.MapperOutputReader;
import org.apache.pinot.core.segment.processing.reducer.Reducer;
import org.apache.pinot.core.segment.processing.reducer.ReducerFactory;
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
    MapperOutputReader fileReader = reducedFileManager.getFileReader();
    GenericRowMapperOutputRecordReader recordReader = fileReader.getRecordReader();
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
    MapperOutputReader fileReader = reducedFileManager.getFileReader();
    GenericRowMapperOutputRecordReader recordReader = fileReader.getRecordReader();
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
    MapperOutputReader fileReader = reducedFileManager.getFileReader();
    GenericRowMapperOutputRecordReader recordReader = fileReader.getRecordReader();
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
    MapperOutputReader fileReader = reducedFileManager.getFileReader();
    assertEquals(fileReader.getNumRows(), expectedValues.size());
    GenericRowMapperOutputRecordReader recordReader = fileReader.getRecordReader();
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
    MapperOutputReader fileReader = reducedFileManager.getFileReader();
    assertEquals(fileReader.getNumRows(), expectedValues.size());
    GenericRowMapperOutputRecordReader recordReader = fileReader.getRecordReader();
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
    MapperOutputReader fileReader = reducedFileManager.getFileReader();
    assertEquals(fileReader.getNumRows(), expectedValues.size());
    GenericRowMapperOutputRecordReader recordReader = fileReader.getRecordReader();
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
    MapperOutputReader fileReader = reducedFileManager.getFileReader();
    assertEquals(fileReader.getNumRows(), expectedValues.size());
    GenericRowMapperOutputRecordReader recordReader = fileReader.getRecordReader();
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
    MapperOutputReader fileReader = reducedFileManager.getFileReader();
    assertEquals(fileReader.getNumRows(), expectedValues.size());
    GenericRowMapperOutputRecordReader recordReader = fileReader.getRecordReader();
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
    MapperOutputReader fileReader = reducedFileManager.getFileReader();
    assertEquals(fileReader.getNumRows(), expectedValues.size());
    GenericRowMapperOutputRecordReader recordReader = fileReader.getRecordReader();
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
    MapperOutputReader fileReader = reducedFileManager.getFileReader();
    assertEquals(fileReader.getNumRows(), expectedValues.size());
    GenericRowMapperOutputRecordReader recordReader = fileReader.getRecordReader();
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
