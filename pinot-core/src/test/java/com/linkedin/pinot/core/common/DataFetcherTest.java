/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.common;

import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.GenericRowRecordReader;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.operator.BaseOperator;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import com.linkedin.pinot.core.segment.index.loader.Loaders;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class DataFetcherTest {
  private static final String SEGMENT_NAME = "dataFetcherTestSegment";
  private static final String INDEX_DIR_PATH = FileUtils.getTempDirectoryPath() + File.separator + SEGMENT_NAME;
  private static final int NUM_ROWS = 1000;
  private static final String DIMENSION_NAME = "dimension";
  private static final String INT_METRIC_NAME = "int_metric";
  private static final String LONG_METRIC_NAME = "long_metric";
  private static final String FLOAT_METRIC_NAME = "float_metric";
  private static final String DOUBLE_METRIC_NAME = "double_metric";
  private static final String NO_DICT_INT_METRIC_NAME = "no_dict_int_metric";
  private static final String NO_DICT_LONG_METRIC_NAME = "no_dict_long_metric";
  private static final String NO_DICT_FLOAT_METRIC_NAME = "no_dict_float_metric";
  private static final String NO_DICT_DOUBLE_METRIC_NAME = "no_dict_double_metric";
  private static final int MAX_STEP_LENGTH = 5;

  private final long _randomSeed = System.currentTimeMillis();
  private final Random _random = new Random(_randomSeed);
  private final String _errorMessage = "Random seed is: " + _randomSeed;
  private final String[] _dimensionValues = new String[NUM_ROWS];
  private final int[] _intMetricValues = new int[NUM_ROWS];
  private final long[] _longMetricValues = new long[NUM_ROWS];
  private final float[] _floatMetricValues = new float[NUM_ROWS];
  private final double[] _doubleMetricValues = new double[NUM_ROWS];
  private DataFetcher _dataFetcher;

  @BeforeClass
  private void setup() throws Exception {
    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);

    // Generate random dimension and metric values.
    for (int i = 0; i < NUM_ROWS; i++) {
      double randomDouble = _random.nextDouble();
      String randomDoubleString = String.valueOf(randomDouble);
      _dimensionValues[i] = randomDoubleString;
      _intMetricValues[i] = (int) randomDouble;
      _longMetricValues[i] = (long) randomDouble;
      _floatMetricValues[i] = (float) randomDouble;
      _doubleMetricValues[i] = randomDouble;
      HashMap<String, Object> map = new HashMap<>();
      map.put(DIMENSION_NAME, _dimensionValues[i]);
      map.put(INT_METRIC_NAME, _intMetricValues[i]);
      map.put(LONG_METRIC_NAME, _longMetricValues[i]);
      map.put(FLOAT_METRIC_NAME, _floatMetricValues[i]);
      map.put(DOUBLE_METRIC_NAME, _doubleMetricValues[i]);
      map.put(NO_DICT_INT_METRIC_NAME, _intMetricValues[i]);
      map.put(NO_DICT_LONG_METRIC_NAME, _longMetricValues[i]);
      map.put(NO_DICT_FLOAT_METRIC_NAME, _floatMetricValues[i]);
      map.put(NO_DICT_DOUBLE_METRIC_NAME, _doubleMetricValues[i]);
      GenericRow genericRow = new GenericRow();
      genericRow.init(map);
      rows.add(genericRow);
    }

    // Create an index segment with the random dimension and metric values.
    final Schema schema = new Schema();
    schema.addField(new DimensionFieldSpec(DIMENSION_NAME, FieldSpec.DataType.STRING, true));
    schema.addField(new MetricFieldSpec(INT_METRIC_NAME, FieldSpec.DataType.INT));
    schema.addField(new MetricFieldSpec(LONG_METRIC_NAME, FieldSpec.DataType.LONG));
    schema.addField(new MetricFieldSpec(FLOAT_METRIC_NAME, FieldSpec.DataType.FLOAT));
    schema.addField(new MetricFieldSpec(DOUBLE_METRIC_NAME, FieldSpec.DataType.DOUBLE));

    schema.addField(new MetricFieldSpec(NO_DICT_INT_METRIC_NAME, FieldSpec.DataType.INT));
    schema.addField(new MetricFieldSpec(NO_DICT_LONG_METRIC_NAME, FieldSpec.DataType.LONG));
    schema.addField(new MetricFieldSpec(NO_DICT_FLOAT_METRIC_NAME, FieldSpec.DataType.FLOAT));
    schema.addField(new MetricFieldSpec(NO_DICT_DOUBLE_METRIC_NAME, FieldSpec.DataType.DOUBLE));

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(schema);
    FileUtils.deleteQuietly(new File(INDEX_DIR_PATH));
    config.setOutDir(INDEX_DIR_PATH);
    config.setSegmentName(SEGMENT_NAME);
    config.setRawIndexCreationColumns(
        Arrays.asList(NO_DICT_INT_METRIC_NAME, NO_DICT_LONG_METRIC_NAME, NO_DICT_FLOAT_METRIC_NAME,
            NO_DICT_DOUBLE_METRIC_NAME));

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows, schema));
    driver.build();

    IndexSegment indexSegment = Loaders.IndexSegment.load(new File(INDEX_DIR_PATH, SEGMENT_NAME), ReadMode.heap);

    Map<String, BaseOperator> dataSourceMap = new HashMap<>();
    for (String column : indexSegment.getColumnNames()) {
      dataSourceMap.put(column, indexSegment.getDataSource(column));
    }
    // Get a data fetcher for the index segment.
    _dataFetcher = new DataFetcher(dataSourceMap);
  }

  @Test
  public void testFetchSingleIntValues() {
    testFetchSingleIntValues(INT_METRIC_NAME);
    testFetchSingleIntValues(NO_DICT_INT_METRIC_NAME);
  }

  @Test
  public void testFetchSingleLongValues() {
    testFetchSingleLongValues(LONG_METRIC_NAME);
    testFetchSingleLongValues(NO_DICT_LONG_METRIC_NAME);
  }

  @Test
  public void testFetchSingleFloatValues() {
    testFetchSingleFloatValues(FLOAT_METRIC_NAME);
    testFetchSingleFloatValues(NO_DICT_FLOAT_METRIC_NAME);
  }

  @Test
  public void testFetchSingleDoubleValues() {
    testFetchSingleDoubleValues(DOUBLE_METRIC_NAME);
    testFetchSingleDoubleValues(NO_DICT_DOUBLE_METRIC_NAME);
  }

  public void testFetchSingleIntValues(String column) {
    int[] docIds = new int[NUM_ROWS];
    int length = 0;
    for (int i = _random.nextInt(MAX_STEP_LENGTH); i < NUM_ROWS; i += _random.nextInt(MAX_STEP_LENGTH) + 1) {
      docIds[length++] = i;
    }

    int[] intValues = new int[length];
    _dataFetcher.fetchIntValues(column, docIds, 0, length, intValues, 0);

    for (int i = 0; i < length; i++) {
      Assert.assertEquals(intValues[i], _intMetricValues[docIds[i]], _errorMessage);
    }
  }

  public void testFetchSingleLongValues(String column) {
    int[] docIds = new int[NUM_ROWS];
    int length = 0;
    for (int i = _random.nextInt(MAX_STEP_LENGTH); i < NUM_ROWS; i += _random.nextInt(MAX_STEP_LENGTH) + 1) {
      docIds[length++] = i;
    }

    long[] longValues = new long[length];
    _dataFetcher.fetchLongValues(column, docIds, 0, length, longValues, 0);

    for (int i = 0; i < length; i++) {
      Assert.assertEquals(longValues[i], _longMetricValues[docIds[i]], _errorMessage);
    }
  }

  public void testFetchSingleFloatValues(String column) {
    int[] docIds = new int[NUM_ROWS];
    int length = 0;
    for (int i = _random.nextInt(MAX_STEP_LENGTH); i < NUM_ROWS; i += _random.nextInt(MAX_STEP_LENGTH) + 1) {
      docIds[length++] = i;
    }

    float[] floatValues = new float[length];
    _dataFetcher.fetchFloatValues(column, docIds, 0, length, floatValues, 0);

    for (int i = 0; i < length; i++) {
      Assert.assertEquals(floatValues[i], _floatMetricValues[docIds[i]], _errorMessage);
    }
  }

  public void testFetchSingleDoubleValues(String column) {
    int[] docIds = new int[NUM_ROWS];
    int length = 0;
    for (int i = _random.nextInt(MAX_STEP_LENGTH); i < NUM_ROWS; i += _random.nextInt(MAX_STEP_LENGTH) + 1) {
      docIds[length++] = i;
    }

    double[] doubleValues = new double[length];
    _dataFetcher.fetchDoubleValues(column, docIds, 0, length, doubleValues, 0);

    for (int i = 0; i < length; i++) {
      Assert.assertEquals(doubleValues[i], _doubleMetricValues[docIds[i]], _errorMessage);
    }
  }

  @Test
  public void testFetchSingleStringValues() {
    int[] docIds = new int[NUM_ROWS];
    int length = 0;
    for (int i = _random.nextInt(MAX_STEP_LENGTH); i < NUM_ROWS; i += _random.nextInt(MAX_STEP_LENGTH) + 1) {
      docIds[length++] = i;
    }

    String[] stringValues = new String[length];
    _dataFetcher.fetchStringValues(DIMENSION_NAME, docIds, 0, length, stringValues, 0);

    for (int i = 0; i < length; i++) {
      Assert.assertEquals(stringValues[i], _dimensionValues[docIds[i]], _errorMessage);
    }
  }

  @AfterClass
  public void cleanUp() {
    FileUtils.deleteQuietly(new File(INDEX_DIR_PATH));
  }
}
