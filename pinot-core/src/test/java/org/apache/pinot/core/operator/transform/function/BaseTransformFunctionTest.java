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
package org.apache.pinot.core.operator.transform.function;

import java.io.File;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pinot.core.operator.DocIdSetOperator;
import org.apache.pinot.core.operator.ProjectionOperator;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.filter.MatchAllFilterOperator;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;


public abstract class BaseTransformFunctionTest {
  private static final String SEGMENT_NAME = "testSegment";
  private static final String INDEX_DIR_PATH = FileUtils.getTempDirectoryPath() + File.separator + SEGMENT_NAME;
  private static final Random RANDOM = new Random();

  protected static final int NUM_ROWS = 1000;
  protected static final int MAX_NUM_MULTI_VALUES = 5;
  protected static final int MAX_MULTI_VALUE = 10;
  protected static final String INT_SV_COLUMN = "intSV";
  protected static final String LONG_SV_COLUMN = "longSV";
  protected static final String FLOAT_SV_COLUMN = "floatSV";
  protected static final String DOUBLE_SV_COLUMN = "doubleSV";
  protected static final String STRING_SV_COLUMN = "stringSV";
  protected static final String BYTES_SV_COLUMN = "bytesSV";
  protected static final String BIGDECIMAL_SV_COLUMN = "bigDecimalSV";
  protected static final String STRING_ALPHANUM_SV_COLUMN = "stringAlphaNumSV";
  protected static final String INT_MV_COLUMN = "intMV";
  protected static final String LONG_MV_COLUMN = "longMV";
  protected static final String FLOAT_MV_COLUMN = "floatMV";
  protected static final String DOUBLE_MV_COLUMN = "doubleMV";
  protected static final String STRING_MV_COLUMN = "stringMV";
  protected static final String STRING_ALPHANUM_MV_COLUMN = "stringAlphaNumMV";
  protected static final String TIME_COLUMN = "timeColumn";
  protected static final String JSON_COLUMN = "json";
  protected final int[] _intSVValues = new int[NUM_ROWS];
  protected final long[] _longSVValues = new long[NUM_ROWS];
  protected final float[] _floatSVValues = new float[NUM_ROWS];
  protected final double[] _doubleSVValues = new double[NUM_ROWS];
  protected final String[] _stringSVValues = new String[NUM_ROWS];
  protected final String[] _stringAlphaNumericSVValues = new String[NUM_ROWS];
  protected final byte[][] _bytesSVValues = new byte[NUM_ROWS][];
  protected final BigDecimal[] _bigDecimalSVValues = new BigDecimal[NUM_ROWS];
  protected final int[][] _intMVValues = new int[NUM_ROWS][];
  protected final long[][] _longMVValues = new long[NUM_ROWS][];
  protected final float[][] _floatMVValues = new float[NUM_ROWS][];
  protected final double[][] _doubleMVValues = new double[NUM_ROWS][];
  protected final String[][] _stringMVValues = new String[NUM_ROWS][];
  protected final String[][] _stringAlphaNumericMVValues = new String[NUM_ROWS][];
  protected final long[] _timeValues = new long[NUM_ROWS];
  protected final String[] _jsonValues = new String[NUM_ROWS];

  protected Map<String, DataSource> _dataSourceMap;
  protected ProjectionBlock _projectionBlock;

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(new File(INDEX_DIR_PATH));
    DecimalFormat df = new DecimalFormat("0", DecimalFormatSymbols.getInstance(Locale.ENGLISH));
    df.setMaximumFractionDigits(340); // 340 = DecimalFormat.DOUBLE_FRACTION_DIGITS
    long currentTimeMs = System.currentTimeMillis();
    for (int i = 0; i < NUM_ROWS; i++) {
      _intSVValues[i] = RANDOM.nextInt();
      _longSVValues[i] = RANDOM.nextLong();
      _floatSVValues[i] = _intSVValues[i] * RANDOM.nextFloat();
      _doubleSVValues[i] = _intSVValues[i] * RANDOM.nextDouble();
      _stringSVValues[i] = df.format(_intSVValues[i] * RANDOM.nextDouble());
      _stringAlphaNumericSVValues[i] = RandomStringUtils.randomAlphanumeric(26);
      _bytesSVValues[i] = RandomStringUtils.randomAlphanumeric(26).getBytes();
      _bigDecimalSVValues[i] = BigDecimal.valueOf(_intSVValues[i] * RANDOM.nextDouble());

      int numValues = 1 + RANDOM.nextInt(MAX_NUM_MULTI_VALUES);
      _intMVValues[i] = new int[numValues];
      _longMVValues[i] = new long[numValues];
      _floatMVValues[i] = new float[numValues];
      _doubleMVValues[i] = new double[numValues];
      _stringMVValues[i] = new String[numValues];
      _stringAlphaNumericMVValues[i] = new String[numValues];

      for (int j = 0; j < numValues; j++) {
        _intMVValues[i][j] = 1 + RANDOM.nextInt(MAX_MULTI_VALUE);
        _longMVValues[i][j] = 1 + RANDOM.nextLong();
        _floatMVValues[i][j] = 1 + RANDOM.nextFloat();
        _doubleMVValues[i][j] = 1 + RANDOM.nextDouble();
        _stringMVValues[i][j] = df.format(_intSVValues[i] * RANDOM.nextDouble());
        _stringAlphaNumericMVValues[i][j] = RandomStringUtils.randomAlphanumeric(26);
      }

      // Time in the past year
      _timeValues[i] = currentTimeMs - RANDOM.nextInt(365 * 24 * 3600) * 1000L;
    }

    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      Map<String, Object> map = new HashMap<>();
      map.put(INT_SV_COLUMN, _intSVValues[i]);
      map.put(LONG_SV_COLUMN, _longSVValues[i]);
      map.put(FLOAT_SV_COLUMN, _floatSVValues[i]);
      map.put(DOUBLE_SV_COLUMN, _doubleSVValues[i]);
      map.put(STRING_SV_COLUMN, _stringSVValues[i]);
      map.put(STRING_ALPHANUM_SV_COLUMN, _stringAlphaNumericSVValues[i]);
      map.put(BYTES_SV_COLUMN, _bytesSVValues[i]);
      map.put(BIGDECIMAL_SV_COLUMN, _bigDecimalSVValues[i]);
      map.put(INT_MV_COLUMN, ArrayUtils.toObject(_intMVValues[i]));
      map.put(LONG_MV_COLUMN, ArrayUtils.toObject(_longMVValues[i]));
      map.put(FLOAT_MV_COLUMN, ArrayUtils.toObject(_floatMVValues[i]));
      map.put(DOUBLE_MV_COLUMN, ArrayUtils.toObject(_doubleMVValues[i]));
      map.put(STRING_MV_COLUMN, _stringMVValues[i]);
      map.put(STRING_ALPHANUM_MV_COLUMN, _stringAlphaNumericMVValues[i]);
      map.put(TIME_COLUMN, _timeValues[i]);
      _jsonValues[i] = JsonUtils.objectToJsonNode(map).toString();
      map.put(JSON_COLUMN, _jsonValues[i]);
      GenericRow row = new GenericRow();
      row.init(map);
      rows.add(row);
    }

    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension(INT_SV_COLUMN, FieldSpec.DataType.INT)
        .addSingleValueDimension(LONG_SV_COLUMN, FieldSpec.DataType.LONG)
        .addSingleValueDimension(FLOAT_SV_COLUMN, FieldSpec.DataType.FLOAT)
        .addSingleValueDimension(DOUBLE_SV_COLUMN, FieldSpec.DataType.DOUBLE)
        .addSingleValueDimension(STRING_SV_COLUMN, FieldSpec.DataType.STRING)
        .addSingleValueDimension(STRING_ALPHANUM_SV_COLUMN, FieldSpec.DataType.STRING)
        .addSingleValueDimension(BYTES_SV_COLUMN, FieldSpec.DataType.BYTES)
        .addSingleValueDimension(BIGDECIMAL_SV_COLUMN, FieldSpec.DataType.BIGDECIMAL)
        .addSingleValueDimension(JSON_COLUMN, FieldSpec.DataType.STRING, Integer.MAX_VALUE, null)
        .addMultiValueDimension(INT_MV_COLUMN, FieldSpec.DataType.INT)
        .addMultiValueDimension(LONG_MV_COLUMN, FieldSpec.DataType.LONG)
        .addMultiValueDimension(FLOAT_MV_COLUMN, FieldSpec.DataType.FLOAT)
        .addMultiValueDimension(DOUBLE_MV_COLUMN, FieldSpec.DataType.DOUBLE)
        .addMultiValueDimension(STRING_MV_COLUMN, FieldSpec.DataType.STRING)
        .addMultiValueDimension(STRING_ALPHANUM_MV_COLUMN, FieldSpec.DataType.STRING)
        .addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, TIME_COLUMN), null).build();
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("test").setTimeColumnName(TIME_COLUMN)
            .setVarLengthDictionaryColumns(Collections.singletonList(BIGDECIMAL_SV_COLUMN)).build();

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(INDEX_DIR_PATH);
    config.setSegmentName(SEGMENT_NAME);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();

    IndexSegment indexSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR_PATH, SEGMENT_NAME), ReadMode.heap);
    Set<String> columnNames = indexSegment.getPhysicalColumnNames();
    _dataSourceMap = new HashMap<>(columnNames.size());
    for (String columnName : columnNames) {
      _dataSourceMap.put(columnName, indexSegment.getDataSource(columnName));
    }

    _projectionBlock = new ProjectionOperator(_dataSourceMap,
        new DocIdSetOperator(new MatchAllFilterOperator(NUM_ROWS), DocIdSetPlanNode.MAX_DOC_PER_CALL)).nextBlock();
  }

  protected void testTransformFunction(TransformFunction transformFunction, int[] expectedValues) {
    int[] intValues = transformFunction.transformToIntValuesSV(_projectionBlock);
    long[] longValues = transformFunction.transformToLongValuesSV(_projectionBlock);
    float[] floatValues = transformFunction.transformToFloatValuesSV(_projectionBlock);
    double[] doubleValues = transformFunction.transformToDoubleValuesSV(_projectionBlock);
    String[] stringValues = transformFunction.transformToStringValuesSV(_projectionBlock);
    BigDecimal[] bigDecimalValues = transformFunction.transformToBigDecimalValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      Assert.assertEquals(intValues[i], expectedValues[i]);
      Assert.assertEquals(longValues[i], expectedValues[i]);
      Assert.assertEquals(floatValues[i], (float) expectedValues[i]);
      Assert.assertEquals(doubleValues[i], (double) expectedValues[i]);
      Assert.assertEquals(stringValues[i], Integer.toString(expectedValues[i]));
      Assert.assertEquals(bigDecimalValues[i], BigDecimal.valueOf(expectedValues[i]));
    }
  }

  protected void testTransformFunction(TransformFunction transformFunction, long[] expectedValues) {
    int[] intValues = transformFunction.transformToIntValuesSV(_projectionBlock);
    long[] longValues = transformFunction.transformToLongValuesSV(_projectionBlock);
    float[] floatValues = transformFunction.transformToFloatValuesSV(_projectionBlock);
    double[] doubleValues = transformFunction.transformToDoubleValuesSV(_projectionBlock);
    String[] stringValues = transformFunction.transformToStringValuesSV(_projectionBlock);
    BigDecimal[] bigDecimalValues = transformFunction.transformToBigDecimalValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      Assert.assertEquals(intValues[i], (int) expectedValues[i]);
      Assert.assertEquals(longValues[i], expectedValues[i]);
      Assert.assertEquals(floatValues[i], (float) expectedValues[i]);
      Assert.assertEquals(doubleValues[i], (double) expectedValues[i]);
      Assert.assertEquals(stringValues[i], Long.toString(expectedValues[i]));
      Assert.assertEquals(bigDecimalValues[i], BigDecimal.valueOf(expectedValues[i]));
    }
  }

  protected void testTransformFunction(TransformFunction transformFunction, float[] expectedValues) {
    int[] intValues = transformFunction.transformToIntValuesSV(_projectionBlock);
    long[] longValues = transformFunction.transformToLongValuesSV(_projectionBlock);
    float[] floatValues = transformFunction.transformToFloatValuesSV(_projectionBlock);
    double[] doubleValues = transformFunction.transformToDoubleValuesSV(_projectionBlock);
    String[] stringValues = transformFunction.transformToStringValuesSV(_projectionBlock);
    BigDecimal[] bigDecimalValues = transformFunction.transformToBigDecimalValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      Assert.assertEquals(intValues[i], (int) expectedValues[i]);
      Assert.assertEquals(longValues[i], (long) expectedValues[i]);
      Assert.assertEquals(floatValues[i], expectedValues[i]);
      Assert.assertEquals(doubleValues[i], (double) expectedValues[i]);
      Assert.assertEquals(stringValues[i], Float.toString(expectedValues[i]));
      Assert.assertEquals(bigDecimalValues[i], BigDecimal.valueOf(expectedValues[i]));
    }
  }

  protected void testTransformFunction(TransformFunction transformFunction, double[] expectedValues) {
    int[] intValues = transformFunction.transformToIntValuesSV(_projectionBlock);
    long[] longValues = transformFunction.transformToLongValuesSV(_projectionBlock);
    float[] floatValues = transformFunction.transformToFloatValuesSV(_projectionBlock);
    double[] doubleValues = transformFunction.transformToDoubleValuesSV(_projectionBlock);
    String[] stringValues = transformFunction.transformToStringValuesSV(_projectionBlock);
    BigDecimal[] bigDecimalValues = transformFunction.transformToBigDecimalValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      Assert.assertEquals(intValues[i], (int) expectedValues[i]);
      Assert.assertEquals(longValues[i], (long) expectedValues[i]);
      Assert.assertEquals(floatValues[i], (float) expectedValues[i]);
      Assert.assertEquals(doubleValues[i], expectedValues[i]);
      Assert.assertEquals(stringValues[i], Double.toString(expectedValues[i]));
      Assert.assertEquals(bigDecimalValues[i], BigDecimal.valueOf(expectedValues[i]));
    }
  }

  protected void testTransformFunction(TransformFunction transformFunction, String[] expectedValues) {
    String[] stringValues = transformFunction.transformToStringValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      Assert.assertEquals(stringValues[i], expectedValues[i]);
    }
  }

  protected void testTransformFunction(TransformFunction transformFunction, byte[][] expectedValues) {
    byte[][] bytesValues = transformFunction.transformToBytesValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      Assert.assertEquals(bytesValues[i], expectedValues[i]);
    }
  }

  protected void testTransformFunction(TransformFunction transformFunction, BigDecimal[] expectedValues) {
    BigDecimal[] bigDecimalValues = transformFunction.transformToBigDecimalValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      Assert.assertEquals(bigDecimalValues[i], expectedValues[i]);
    }
  }

  protected void testTransformFunctionMV(TransformFunction transformFunction, int[][] expectedValues) {
    int[][] intMVValues = transformFunction.transformToIntValuesMV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      Assert.assertEquals(intMVValues[i], expectedValues[i]);
    }
  }

  protected void testTransformFunctionMV(TransformFunction transformFunction, long[][] expectedValues) {
    long[][] longMVValues = transformFunction.transformToLongValuesMV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      Assert.assertEquals(longMVValues[i], expectedValues[i]);
    }
  }

  protected void testTransformFunctionMV(TransformFunction transformFunction, float[][] expectedValues) {
    float[][] floatMVValues = transformFunction.transformToFloatValuesMV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      Assert.assertEquals(floatMVValues[i], expectedValues[i]);
    }
  }

  protected void testTransformFunctionMV(TransformFunction transformFunction, double[][] expectedValues) {
    double[][] doubleMVValues = transformFunction.transformToDoubleValuesMV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      Assert.assertEquals(doubleMVValues[i], expectedValues[i]);
    }
  }

  protected void testTransformFunctionMV(TransformFunction transformFunction, String[][] expectedValues) {
    String[][] stringMVValues = transformFunction.transformToStringValuesMV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      Assert.assertEquals(stringMVValues[i], expectedValues[i]);
    }
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(new File(INDEX_DIR_PATH));
  }
}
