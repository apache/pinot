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
package org.apache.pinot.core.common;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static java.nio.charset.StandardCharsets.UTF_8;


public class DataFetcherTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "DataFetcherTest");
  private static final long RANDOM_SEED = System.currentTimeMillis();
  private static final Random RANDOM = new Random(RANDOM_SEED);
  private static final String ERROR_MESSAGE = "Random seed: " + RANDOM_SEED;
  private static final int NUM_ROWS = 1000;
  private static final int MAX_VALUE = 1000000;
  private static final String INT_COLUMN = "int";
  private static final String LONG_COLUMN = "long";
  private static final String FLOAT_COLUMN = "float";
  private static final String DOUBLE_COLUMN = "double";
  private static final String STRING_COLUMN = "string";
  private static final String BYTES_COLUMN = "bytes";
  private static final String HEX_STRING_COLUMN = "hex_string";
  private static final String NO_DICT_INT_COLUMN = "no_dict_int";
  private static final String NO_DICT_LONG_COLUMN = "no_dict_long";
  private static final String NO_DICT_FLOAT_COLUMN = "no_dict_float";
  private static final String NO_DICT_DOUBLE_COLUMN = "no_dict_double";
  private static final String NO_DICT_STRING_COLUMN = "no_dict_string";
  private static final String NO_DICT_BYTES_COLUMN = "no_dict_bytes";
  private static final String NO_DICT_HEX_STRING_COLUMN = "no_dict_hex_string";
  private static final int MAX_STEP_LENGTH = 5;

  private final int[] _values = new int[NUM_ROWS];
  private IndexSegment _indexSegment;
  private DataFetcher _dataFetcher;

  @BeforeClass
  private void setup()
      throws Exception {
    FileUtils.deleteDirectory(TEMP_DIR);

    // Generate random values
    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      int value = RANDOM.nextInt(MAX_VALUE);
      _values[i] = value;
      GenericRow row = new GenericRow();
      row.putValue(INT_COLUMN, value);
      row.putValue(NO_DICT_INT_COLUMN, value);
      row.putValue(LONG_COLUMN, (long) value);
      row.putValue(NO_DICT_LONG_COLUMN, (long) value);
      row.putValue(FLOAT_COLUMN, (float) value);
      row.putValue(NO_DICT_FLOAT_COLUMN, (float) value);
      row.putValue(DOUBLE_COLUMN, (double) value);
      row.putValue(NO_DICT_DOUBLE_COLUMN, (double) value);
      String stringValue = Integer.toString(value);
      row.putValue(STRING_COLUMN, stringValue);
      row.putValue(NO_DICT_STRING_COLUMN, stringValue);
      byte[] bytesValue = stringValue.getBytes(UTF_8);
      row.putValue(BYTES_COLUMN, bytesValue);
      row.putValue(NO_DICT_BYTES_COLUMN, bytesValue);
      String hexStringValue = BytesUtils.toHexString(bytesValue);
      row.putValue(HEX_STRING_COLUMN, hexStringValue);
      row.putValue(NO_DICT_HEX_STRING_COLUMN, hexStringValue);
      rows.add(row);
    }

    // Create the segment
    String tableName = "testTable";
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(tableName).setNoDictionaryColumns(
        Arrays.asList(NO_DICT_INT_COLUMN, NO_DICT_LONG_COLUMN, NO_DICT_FLOAT_COLUMN, NO_DICT_DOUBLE_COLUMN,
            NO_DICT_STRING_COLUMN, NO_DICT_BYTES_COLUMN, NO_DICT_HEX_STRING_COLUMN)).build();
    Schema schema =
        new Schema.SchemaBuilder().setSchemaName(tableName).addSingleValueDimension(INT_COLUMN, DataType.INT)
            .addSingleValueDimension(LONG_COLUMN, DataType.LONG).addSingleValueDimension(FLOAT_COLUMN, DataType.FLOAT)
            .addSingleValueDimension(DOUBLE_COLUMN, DataType.DOUBLE)
            .addSingleValueDimension(STRING_COLUMN, DataType.STRING)
            .addSingleValueDimension(BYTES_COLUMN, DataType.BYTES)
            .addSingleValueDimension(HEX_STRING_COLUMN, DataType.STRING)
            .addSingleValueDimension(NO_DICT_INT_COLUMN, DataType.INT)
            .addSingleValueDimension(NO_DICT_LONG_COLUMN, DataType.LONG)
            .addSingleValueDimension(NO_DICT_FLOAT_COLUMN, DataType.FLOAT)
            .addSingleValueDimension(NO_DICT_DOUBLE_COLUMN, DataType.DOUBLE)
            .addSingleValueDimension(NO_DICT_STRING_COLUMN, DataType.STRING)
            .addSingleValueDimension(NO_DICT_BYTES_COLUMN, DataType.BYTES)
            .addSingleValueDimension(NO_DICT_HEX_STRING_COLUMN, DataType.STRING).build();
    String segmentName = "testSegment";
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(TEMP_DIR.getPath());
    config.setSegmentName(segmentName);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();

    _indexSegment = ImmutableSegmentLoader.load(new File(TEMP_DIR, segmentName), ReadMode.mmap);
    Map<String, DataSource> dataSourceMap = new HashMap<>();
    for (String column : _indexSegment.getPhysicalColumnNames()) {
      dataSourceMap.put(column, _indexSegment.getDataSource(column));
    }
    _dataFetcher = new DataFetcher(dataSourceMap);
  }

  @Test
  public void testFetchIntValues() {
    testFetchIntValues(INT_COLUMN);
    testFetchIntValues(LONG_COLUMN);
    testFetchIntValues(FLOAT_COLUMN);
    testFetchIntValues(DOUBLE_COLUMN);
    testFetchIntValues(STRING_COLUMN);
    testFetchIntValues(NO_DICT_INT_COLUMN);
    testFetchIntValues(NO_DICT_LONG_COLUMN);
    testFetchIntValues(NO_DICT_FLOAT_COLUMN);
    testFetchIntValues(NO_DICT_DOUBLE_COLUMN);
    testFetchIntValues(NO_DICT_STRING_COLUMN);
  }

  @Test
  public void testFetchLongValues() {
    testFetchLongValues(INT_COLUMN);
    testFetchLongValues(LONG_COLUMN);
    testFetchLongValues(FLOAT_COLUMN);
    testFetchLongValues(DOUBLE_COLUMN);
    testFetchLongValues(STRING_COLUMN);
    testFetchLongValues(NO_DICT_INT_COLUMN);
    testFetchLongValues(NO_DICT_LONG_COLUMN);
    testFetchLongValues(NO_DICT_FLOAT_COLUMN);
    testFetchLongValues(NO_DICT_DOUBLE_COLUMN);
    testFetchLongValues(NO_DICT_STRING_COLUMN);
  }

  @Test
  public void testFetchFloatValues() {
    testFetchFloatValues(INT_COLUMN);
    testFetchFloatValues(LONG_COLUMN);
    testFetchFloatValues(FLOAT_COLUMN);
    testFetchFloatValues(DOUBLE_COLUMN);
    testFetchFloatValues(STRING_COLUMN);
    testFetchFloatValues(NO_DICT_INT_COLUMN);
    testFetchFloatValues(NO_DICT_LONG_COLUMN);
    testFetchFloatValues(NO_DICT_FLOAT_COLUMN);
    testFetchFloatValues(NO_DICT_DOUBLE_COLUMN);
    testFetchFloatValues(NO_DICT_STRING_COLUMN);
  }

  @Test
  public void testFetchDoubleValues() {
    testFetchDoubleValues(INT_COLUMN);
    testFetchDoubleValues(LONG_COLUMN);
    testFetchDoubleValues(FLOAT_COLUMN);
    testFetchDoubleValues(DOUBLE_COLUMN);
    testFetchDoubleValues(STRING_COLUMN);
    testFetchDoubleValues(NO_DICT_INT_COLUMN);
    testFetchDoubleValues(NO_DICT_LONG_COLUMN);
    testFetchDoubleValues(NO_DICT_FLOAT_COLUMN);
    testFetchDoubleValues(NO_DICT_DOUBLE_COLUMN);
    testFetchDoubleValues(NO_DICT_STRING_COLUMN);
  }

  @Test
  public void testFetchStringValues() {
    testFetchStringValues(INT_COLUMN);
    testFetchStringValues(LONG_COLUMN);
    testFetchStringValues(FLOAT_COLUMN);
    testFetchStringValues(DOUBLE_COLUMN);
    testFetchStringValues(STRING_COLUMN);
    testFetchStringValues(NO_DICT_INT_COLUMN);
    testFetchStringValues(NO_DICT_LONG_COLUMN);
    testFetchStringValues(NO_DICT_FLOAT_COLUMN);
    testFetchStringValues(NO_DICT_DOUBLE_COLUMN);
    testFetchStringValues(NO_DICT_STRING_COLUMN);
  }

  @Test
  public void testFetchBytesValues() {
    testFetchBytesValues(BYTES_COLUMN);
    testFetchBytesValues(HEX_STRING_COLUMN);
    testFetchBytesValues(NO_DICT_BYTES_COLUMN);
    testFetchBytesValues(NO_DICT_HEX_STRING_COLUMN);
  }

  @Test
  public void testFetchHexStringValues() {
    testFetchHexStringValues(BYTES_COLUMN);
    testFetchHexStringValues(HEX_STRING_COLUMN);
    testFetchBytesValues(NO_DICT_BYTES_COLUMN);
    testFetchBytesValues(NO_DICT_HEX_STRING_COLUMN);
  }

  public void testFetchIntValues(String column) {
    int[] docIds = new int[NUM_ROWS];
    int length = 0;
    for (int i = RANDOM.nextInt(MAX_STEP_LENGTH); i < NUM_ROWS; i += RANDOM.nextInt(MAX_STEP_LENGTH) + 1) {
      docIds[length++] = i;
    }

    int[] intValues = new int[length];
    _dataFetcher.fetchIntValues(column, docIds, length, intValues);

    for (int i = 0; i < length; i++) {
      Assert.assertEquals(intValues[i], _values[docIds[i]], ERROR_MESSAGE);
    }
  }

  public void testFetchLongValues(String column) {
    int[] docIds = new int[NUM_ROWS];
    int length = 0;
    for (int i = RANDOM.nextInt(MAX_STEP_LENGTH); i < NUM_ROWS; i += RANDOM.nextInt(MAX_STEP_LENGTH) + 1) {
      docIds[length++] = i;
    }

    long[] longValues = new long[length];
    _dataFetcher.fetchLongValues(column, docIds, length, longValues);

    for (int i = 0; i < length; i++) {
      Assert.assertEquals((int) longValues[i], _values[docIds[i]], ERROR_MESSAGE);
    }
  }

  public void testFetchFloatValues(String column) {
    int[] docIds = new int[NUM_ROWS];
    int length = 0;
    for (int i = RANDOM.nextInt(MAX_STEP_LENGTH); i < NUM_ROWS; i += RANDOM.nextInt(MAX_STEP_LENGTH) + 1) {
      docIds[length++] = i;
    }

    float[] floatValues = new float[length];
    _dataFetcher.fetchFloatValues(column, docIds, length, floatValues);

    for (int i = 0; i < length; i++) {
      Assert.assertEquals((int) floatValues[i], _values[docIds[i]], ERROR_MESSAGE);
    }
  }

  public void testFetchDoubleValues(String column) {
    int[] docIds = new int[NUM_ROWS];
    int length = 0;
    for (int i = RANDOM.nextInt(MAX_STEP_LENGTH); i < NUM_ROWS; i += RANDOM.nextInt(MAX_STEP_LENGTH) + 1) {
      docIds[length++] = i;
    }

    double[] doubleValues = new double[length];
    _dataFetcher.fetchDoubleValues(column, docIds, length, doubleValues);

    for (int i = 0; i < length; i++) {
      Assert.assertEquals((int) doubleValues[i], _values[docIds[i]], ERROR_MESSAGE);
    }
  }

  public void testFetchStringValues(String column) {
    int[] docIds = new int[NUM_ROWS];
    int length = 0;
    for (int i = RANDOM.nextInt(MAX_STEP_LENGTH); i < NUM_ROWS; i += RANDOM.nextInt(MAX_STEP_LENGTH) + 1) {
      docIds[length++] = i;
    }

    String[] stringValues = new String[length];
    _dataFetcher.fetchStringValues(column, docIds, length, stringValues);

    for (int i = 0; i < length; i++) {
      Assert.assertEquals((int) Double.parseDouble(stringValues[i]), _values[docIds[i]], ERROR_MESSAGE);
    }
  }

  public void testFetchBytesValues(String column) {
    int[] docIds = new int[NUM_ROWS];
    int length = 0;
    for (int i = RANDOM.nextInt(MAX_STEP_LENGTH); i < NUM_ROWS; i += RANDOM.nextInt(MAX_STEP_LENGTH) + 1) {
      docIds[length++] = i;
    }

    byte[][] bytesValues = new byte[length][];
    _dataFetcher.fetchBytesValues(column, docIds, length, bytesValues);

    for (int i = 0; i < length; i++) {
      Assert.assertEquals(new String(bytesValues[i], UTF_8), Integer.toString(_values[docIds[i]]), ERROR_MESSAGE);
    }
  }

  public void testFetchHexStringValues(String column) {
    int[] docIds = new int[NUM_ROWS];
    int length = 0;
    for (int i = RANDOM.nextInt(MAX_STEP_LENGTH); i < NUM_ROWS; i += RANDOM.nextInt(MAX_STEP_LENGTH) + 1) {
      docIds[length++] = i;
    }

    String[] hexStringValues = new String[length];
    _dataFetcher.fetchStringValues(column, docIds, length, hexStringValues);

    for (int i = 0; i < length; i++) {
      Assert.assertEquals(new String(BytesUtils.toBytes(hexStringValues[i]), UTF_8),
          Integer.toString(_values[docIds[i]]), ERROR_MESSAGE);
    }
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    _indexSegment.destroy();
    FileUtils.deleteDirectory(TEMP_DIR);
  }
}
