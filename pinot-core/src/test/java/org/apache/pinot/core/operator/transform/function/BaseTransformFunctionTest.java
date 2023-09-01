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
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
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
import org.apache.commons.lang3.RandomUtils;
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
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import static org.testng.Assert.assertEquals;


public abstract class BaseTransformFunctionTest {
  private static final String SEGMENT_NAME = "testSegment";
  private static final String INDEX_DIR_PATH = FileUtils.getTempDirectoryPath() + File.separator + SEGMENT_NAME;
  private static final Random RANDOM = new Random();

  protected static final int NUM_ROWS = 1000;
  protected static final int MAX_NUM_MULTI_VALUES = 5;
  protected static final int MAX_MULTI_VALUE = 10;
  protected static final int VECTOR_DIM_SIZE = 512;
  protected static final String INT_SV_COLUMN = "intSV";
  // INT_SV_NULL_COLUMN's even row equals to INT_SV_COLUMN. odd row is null.
  protected static final String INT_SV_NULL_COLUMN = "intSVNull";
  protected static final String LONG_SV_COLUMN = "longSV";
  protected static final String FLOAT_SV_COLUMN = "floatSV";
  protected static final String DOUBLE_SV_COLUMN = "doubleSV";
  protected static final String BIG_DECIMAL_SV_COLUMN = "bigDecimalSV";
  protected static final String STRING_SV_COLUMN = "stringSV";
  protected static final String STRING_SV_NULL_COLUMN = "stringSVNull";

  protected static final String BYTES_SV_COLUMN = "bytesSV";

  protected static final String VECTOR_1_COLUMN = "vector1";
  protected static final String VECTOR_2_COLUMN = "vector2";
  protected static final String ZERO_VECTOR_COLUMN = "zeroVector";

  protected static final String STRING_ALPHANUM_SV_COLUMN = "stringAlphaNumSV";

  protected static final String STRING_ALPHANUM_NULL_SV_COLUMN = "stringAlphaNumSVNull";

  protected static final String INT_MV_COLUMN = "intMV";
  protected static final String INT_MV_NULL_COLUMN = "intMVNull";

  protected static final String LONG_MV_COLUMN = "longMV";
  protected static final String FLOAT_MV_COLUMN = "floatMV";
  protected static final String DOUBLE_MV_COLUMN = "doubleMV";
  protected static final String STRING_MV_COLUMN = "stringMV";
  protected static final String STRING_ALPHANUM_MV_COLUMN = "stringAlphaNumMV";
  protected static final String STRING_LONG_MV_COLUMN = "stringLongMV";
  protected static final String TIME_COLUMN = "timeColumn";
  protected static final String TIMESTAMP_COLUMN = "timestampColumn";
  protected static final String TIMESTAMP_COLUMN_NULL = "timestampColumnNull";

  protected static final String JSON_COLUMN = "json";
  protected static final String DEFAULT_JSON_COLUMN = "defaultJson";
  protected final int[] _intSVValues = new int[NUM_ROWS];
  protected final long[] _longSVValues = new long[NUM_ROWS];
  protected final float[] _floatSVValues = new float[NUM_ROWS];
  protected final double[] _doubleSVValues = new double[NUM_ROWS];
  protected final BigDecimal[] _bigDecimalSVValues = new BigDecimal[NUM_ROWS];
  protected final String[] _stringSVValues = new String[NUM_ROWS];
  protected final String[] _stringAlphaNumericSVValues = new String[NUM_ROWS];
  protected final byte[][] _bytesSVValues = new byte[NUM_ROWS][];
  protected final int[][] _intMVValues = new int[NUM_ROWS][];
  protected final long[][] _longMVValues = new long[NUM_ROWS][];
  protected final float[][] _floatMVValues = new float[NUM_ROWS][];
  protected final double[][] _doubleMVValues = new double[NUM_ROWS][];
  protected final String[][] _stringMVValues = new String[NUM_ROWS][];
  protected final String[][] _stringAlphaNumericMVValues = new String[NUM_ROWS][];
  protected final String[][] _stringLongFormatMVValues = new String[NUM_ROWS][];
  protected final long[] _timeValues = new long[NUM_ROWS];
  protected final String[] _jsonValues = new String[NUM_ROWS];
  protected final float[][] _vector1Values = new float[NUM_ROWS][];
  protected final float[][] _vector2Values = new float[NUM_ROWS][];

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
      _bigDecimalSVValues[i] = BigDecimal.valueOf(RANDOM.nextDouble()).multiply(BigDecimal.valueOf(_intSVValues[i]));
      _stringSVValues[i] = df.format(_intSVValues[i] * RANDOM.nextDouble());
      _stringAlphaNumericSVValues[i] = RandomStringUtils.randomAlphanumeric(26);
      _bytesSVValues[i] = RandomStringUtils.randomAlphanumeric(26).getBytes();

      int numValues = 1 + RANDOM.nextInt(MAX_NUM_MULTI_VALUES);
      _intMVValues[i] = new int[numValues];
      _longMVValues[i] = new long[numValues];
      _floatMVValues[i] = new float[numValues];
      _doubleMVValues[i] = new double[numValues];
      _stringMVValues[i] = new String[numValues];
      _stringAlphaNumericMVValues[i] = new String[numValues];
      _stringLongFormatMVValues[i] = new String[numValues];
      _vector1Values[i] = new float[VECTOR_DIM_SIZE];
      _vector2Values[i] = new float[VECTOR_DIM_SIZE];

      for (int j = 0; j < numValues; j++) {
        _intMVValues[i][j] = 1 + RANDOM.nextInt(MAX_MULTI_VALUE);
        _longMVValues[i][j] = 1 + RANDOM.nextLong();
        _floatMVValues[i][j] = 1 + RANDOM.nextFloat();
        _doubleMVValues[i][j] = 1 + RANDOM.nextDouble();
        _stringMVValues[i][j] = df.format(_intSVValues[i] * RANDOM.nextDouble());
        _stringAlphaNumericMVValues[i][j] = RandomStringUtils.randomAlphanumeric(26);
        _stringLongFormatMVValues[i][j] = df.format(_intSVValues[i] * RANDOM.nextLong());
      }

      for (int j = 0; j < VECTOR_DIM_SIZE; j++) {
        _vector1Values[i][j] = Math.abs(RandomUtils.nextFloat(0.0f, 1.0f));
        _vector2Values[i][j] = Math.abs(RandomUtils.nextFloat(0.0f, 1.0f));
      }

      // Time in the past year
      _timeValues[i] = currentTimeMs - RANDOM.nextInt(365 * 24 * 3600) * 1000L;
    }

    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      Map<String, Object> map = new HashMap<>();
      map.put(INT_SV_COLUMN, _intSVValues[i]);
      if (isNullRow(i)) {
        map.put(INT_SV_NULL_COLUMN, null);
      } else {
        map.put(INT_SV_NULL_COLUMN, _intSVValues[i]);
      }
      map.put(LONG_SV_COLUMN, _longSVValues[i]);
      map.put(FLOAT_SV_COLUMN, _floatSVValues[i]);
      map.put(DOUBLE_SV_COLUMN, _doubleSVValues[i]);
      map.put(BIG_DECIMAL_SV_COLUMN, _bigDecimalSVValues[i]);
      map.put(STRING_SV_COLUMN, _stringSVValues[i]);
      if (isNullRow(i)) {
        map.put(STRING_SV_NULL_COLUMN, null);
      } else {
        map.put(STRING_SV_NULL_COLUMN, _stringSVValues[i]);
      }
      map.put(STRING_ALPHANUM_SV_COLUMN, _stringAlphaNumericSVValues[i]);
      if (isNullRow(i)) {
        map.put(STRING_ALPHANUM_NULL_SV_COLUMN, null);
      } else {
        map.put(STRING_ALPHANUM_NULL_SV_COLUMN, _stringAlphaNumericSVValues[i]);
      }
      map.put(BYTES_SV_COLUMN, _bytesSVValues[i]);

      map.put(INT_MV_COLUMN, ArrayUtils.toObject(_intMVValues[i]));
      if (isNullRow(i)) {
        map.put(INT_MV_NULL_COLUMN, null);
      } else {
        map.put(INT_MV_NULL_COLUMN, ArrayUtils.toObject(_intMVValues[i]));
      }
      map.put(LONG_MV_COLUMN, ArrayUtils.toObject(_longMVValues[i]));
      map.put(FLOAT_MV_COLUMN, ArrayUtils.toObject(_floatMVValues[i]));
      map.put(VECTOR_1_COLUMN, ArrayUtils.toObject(_vector1Values[i]));
      map.put(VECTOR_2_COLUMN, ArrayUtils.toObject(_vector2Values[i]));
      map.put(ZERO_VECTOR_COLUMN, ArrayUtils.toObject(new float[VECTOR_DIM_SIZE]));
      map.put(DOUBLE_MV_COLUMN, ArrayUtils.toObject(_doubleMVValues[i]));
      map.put(STRING_MV_COLUMN, _stringMVValues[i]);
      map.put(STRING_ALPHANUM_MV_COLUMN, _stringAlphaNumericMVValues[i]);
      map.put(STRING_LONG_MV_COLUMN, _stringLongFormatMVValues[i]);
      map.put(TIMESTAMP_COLUMN, _timeValues[i]);
      if (isNullRow(i)) {
        map.put(TIMESTAMP_COLUMN_NULL, null);
      } else {
        map.put(TIMESTAMP_COLUMN_NULL, _timeValues[i]);
      }
      map.put(TIME_COLUMN, _timeValues[i]);
      _jsonValues[i] = JsonUtils.objectToJsonNode(map).toString();
      map.put(JSON_COLUMN, _jsonValues[i]);
      GenericRow row = new GenericRow();
      row.init(map);
      rows.add(row);
    }

    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension(INT_SV_COLUMN, FieldSpec.DataType.INT)
        .addSingleValueDimension(INT_SV_NULL_COLUMN, FieldSpec.DataType.INT)
        .addSingleValueDimension(LONG_SV_COLUMN, FieldSpec.DataType.LONG)
        .addSingleValueDimension(FLOAT_SV_COLUMN, FieldSpec.DataType.FLOAT)
        .addSingleValueDimension(DOUBLE_SV_COLUMN, FieldSpec.DataType.DOUBLE)
        .addMetric(BIG_DECIMAL_SV_COLUMN, FieldSpec.DataType.BIG_DECIMAL)
        .addSingleValueDimension(STRING_SV_COLUMN, FieldSpec.DataType.STRING)
        .addSingleValueDimension(STRING_SV_NULL_COLUMN, FieldSpec.DataType.STRING)
        .addSingleValueDimension(STRING_ALPHANUM_SV_COLUMN, FieldSpec.DataType.STRING)
        .addSingleValueDimension(STRING_ALPHANUM_NULL_SV_COLUMN, FieldSpec.DataType.STRING)
        .addSingleValueDimension(BYTES_SV_COLUMN, FieldSpec.DataType.BYTES)
        .addSingleValueDimension(JSON_COLUMN, FieldSpec.DataType.JSON)
        .addSingleValueDimension(DEFAULT_JSON_COLUMN, FieldSpec.DataType.JSON)
        .addMultiValueDimension(INT_MV_COLUMN, FieldSpec.DataType.INT)
        .addMultiValueDimension(INT_MV_NULL_COLUMN, FieldSpec.DataType.INT)
        .addMultiValueDimension(LONG_MV_COLUMN, FieldSpec.DataType.LONG)
        .addMultiValueDimension(FLOAT_MV_COLUMN, FieldSpec.DataType.FLOAT)
        .addMultiValueDimension(DOUBLE_MV_COLUMN, FieldSpec.DataType.DOUBLE)
        .addMultiValueDimension(STRING_MV_COLUMN, FieldSpec.DataType.STRING)
        .addMultiValueDimension(STRING_ALPHANUM_MV_COLUMN, FieldSpec.DataType.STRING)
        .addMultiValueDimension(STRING_LONG_MV_COLUMN, FieldSpec.DataType.STRING)
        .addMultiValueDimension(VECTOR_1_COLUMN, FieldSpec.DataType.FLOAT)
        .addMultiValueDimension(VECTOR_2_COLUMN, FieldSpec.DataType.FLOAT)
        .addMultiValueDimension(ZERO_VECTOR_COLUMN, FieldSpec.DataType.FLOAT)
        .addDateTime(TIMESTAMP_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .addDateTime(TIMESTAMP_COLUMN_NULL, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, TIME_COLUMN), null).build();
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("test").setTimeColumnName(TIME_COLUMN)
            .setNullHandlingEnabled(true).build();

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

  protected boolean isNullRow(int i) {
    return i % 2 != 0;
  }

  protected void testNullBitmap(TransformFunction transformFunction, RoaringBitmap expectedNull) {
    RoaringBitmap nullBitmap = transformFunction.getNullBitmap(_projectionBlock);
    assertEquals(nullBitmap, expectedNull);
  }

  protected void testTransformFunction(TransformFunction transformFunction, int[] expectedValues) {
    int[] intValues = transformFunction.transformToIntValuesSV(_projectionBlock);
    long[] longValues = transformFunction.transformToLongValuesSV(_projectionBlock);
    float[] floatValues = transformFunction.transformToFloatValuesSV(_projectionBlock);
    double[] doubleValues = transformFunction.transformToDoubleValuesSV(_projectionBlock);
    BigDecimal[] bigDecimalValues = transformFunction.transformToBigDecimalValuesSV(_projectionBlock);
    String[] stringValues = transformFunction.transformToStringValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      assertEquals(intValues[i], expectedValues[i]);
      assertEquals(longValues[i], expectedValues[i]);
      assertEquals(floatValues[i], (float) expectedValues[i]);
      assertEquals(doubleValues[i], (double) expectedValues[i]);
      assertEquals(bigDecimalValues[i].intValue(), expectedValues[i]);
      assertEquals(stringValues[i], Integer.toString(expectedValues[i]));
    }
    testNullBitmap(transformFunction, null);
  }

  protected void testTransformFunctionWithNull(TransformFunction transformFunction, int[] expectedValues,
      RoaringBitmap expectedNull) {
    int[] intValues = transformFunction.transformToIntValuesSV(_projectionBlock);
    long[] longValues = transformFunction.transformToLongValuesSV(_projectionBlock);
    float[] floatValues = transformFunction.transformToFloatValuesSV(_projectionBlock);
    double[] doubleValues = transformFunction.transformToDoubleValuesSV(_projectionBlock);
    BigDecimal[] bigDecimalValues =
        transformFunction.transformToBigDecimalValuesSV(_projectionBlock);
    String[] stringValues = transformFunction.transformToStringValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      if (expectedNull.contains(i)) {
        continue;
      }
      // only compare the rows that are not null.
      assertEquals(intValues[i], expectedValues[i]);
      assertEquals(longValues[i], expectedValues[i]);
      assertEquals(floatValues[i], (float) expectedValues[i]);
      assertEquals(doubleValues[i], (double) expectedValues[i]);
      assertEquals(bigDecimalValues[i].intValue(), expectedValues[i]);
      assertEquals(stringValues[i], Integer.toString(expectedValues[i]));
    }
    testNullBitmap(transformFunction, expectedNull);
  }

  protected void testTransformFunction(TransformFunction transformFunction, long[] expectedValues) {
    int[] intValues = transformFunction.transformToIntValuesSV(_projectionBlock);
    long[] longValues = transformFunction.transformToLongValuesSV(_projectionBlock);
    float[] floatValues = transformFunction.transformToFloatValuesSV(_projectionBlock);
    double[] doubleValues = transformFunction.transformToDoubleValuesSV(_projectionBlock);
    BigDecimal[] bigDecimalValues = transformFunction.transformToBigDecimalValuesSV(_projectionBlock);
    String[] stringValues = transformFunction.transformToStringValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      assertEquals(intValues[i], (int) expectedValues[i]);
      assertEquals(longValues[i], expectedValues[i]);
      assertEquals(floatValues[i], (float) expectedValues[i]);
      assertEquals(doubleValues[i], (double) expectedValues[i]);
      assertEquals(bigDecimalValues[i].longValue(), expectedValues[i]);
      assertEquals(stringValues[i], Long.toString(expectedValues[i]));
    }
    testNullBitmap(transformFunction, null);
  }

  protected void testTransformFunctionWithNull(TransformFunction transformFunction, long[] expectedValues,
      RoaringBitmap expectedNull) {
    int[] intValues = transformFunction.transformToIntValuesSV(_projectionBlock);
    long[] longValues = transformFunction.transformToLongValuesSV(_projectionBlock);
    float[] floatValues = transformFunction.transformToFloatValuesSV(_projectionBlock);
    double[] doubleValues = transformFunction.transformToDoubleValuesSV(_projectionBlock);
    BigDecimal[] bigDecimalValues =
        transformFunction.transformToBigDecimalValuesSV(_projectionBlock);
    String[] stringValues = transformFunction.transformToStringValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      if (expectedNull.contains(i)) {
        continue;
      }
      // only compare the rows that are not null.
      assertEquals(intValues[i], (int) expectedValues[i]);
      assertEquals(longValues[i], expectedValues[i]);
      assertEquals(floatValues[i], (float) expectedValues[i]);
      assertEquals(doubleValues[i], (double) expectedValues[i]);
      assertEquals(bigDecimalValues[i].longValue(), expectedValues[i]);
      assertEquals(stringValues[i], Long.toString(expectedValues[i]));
    }
    testNullBitmap(transformFunction, expectedNull);
  }

  protected void testTransformFunction(TransformFunction transformFunction, float[] expectedValues) {
    int[] intValues = transformFunction.transformToIntValuesSV(_projectionBlock);
    long[] longValues = transformFunction.transformToLongValuesSV(_projectionBlock);
    float[] floatValues = transformFunction.transformToFloatValuesSV(_projectionBlock);
    double[] doubleValues = transformFunction.transformToDoubleValuesSV(_projectionBlock);
    BigDecimal[] bigDecimalValues = transformFunction.transformToBigDecimalValuesSV(_projectionBlock);
    String[] stringValues = transformFunction.transformToStringValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      assertEquals(intValues[i], (int) expectedValues[i]);
      assertEquals(longValues[i], (long) expectedValues[i]);
      assertEquals(floatValues[i], expectedValues[i]);
      assertEquals(doubleValues[i], Double.parseDouble(Float.valueOf(expectedValues[i]).toString()));
      assertEquals(bigDecimalValues[i].floatValue(), expectedValues[i]);
      assertEquals(stringValues[i], Float.toString(expectedValues[i]));
    }
    testNullBitmap(transformFunction, null);
  }

  protected void testTransformFunction(TransformFunction transformFunction, double[] expectedValues) {
    int[] intValues = transformFunction.transformToIntValuesSV(_projectionBlock);
    long[] longValues = transformFunction.transformToLongValuesSV(_projectionBlock);
    float[] floatValues = transformFunction.transformToFloatValuesSV(_projectionBlock);
    double[] doubleValues = transformFunction.transformToDoubleValuesSV(_projectionBlock);
    BigDecimal[] bigDecimalValues = null;
    try {
      // 1- Some transform functions cannot work with BigDecimal (e.g. exp, ln, and sqrt).
      // 2- NumberFormatException is thrown when converting double.NaN, Double.POSITIVE_INFINITY,
      // or Double.NEGATIVE_INFINITY.
      bigDecimalValues = transformFunction.transformToBigDecimalValuesSV(_projectionBlock);
    } catch (UnsupportedOperationException | NumberFormatException ignored) {
    }
    String[] stringValues = transformFunction.transformToStringValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      assertEquals(intValues[i], (int) expectedValues[i]);
      assertEquals(longValues[i], (long) expectedValues[i]);
      assertEquals(floatValues[i], (float) expectedValues[i]);
      assertEquals(doubleValues[i], expectedValues[i]);
      if (bigDecimalValues != null) {
        assertEquals(bigDecimalValues[i].doubleValue(), expectedValues[i]);
      }
      assertEquals(stringValues[i], Double.toString(expectedValues[i]));
    }
    testNullBitmap(transformFunction, null);
  }

  protected void testTransformFunctionWithNull(TransformFunction transformFunction, double[] expectedValues,
      RoaringBitmap expectedNull) {
    int[] intValues = transformFunction.transformToIntValuesSV(_projectionBlock);
    long[] longValues = transformFunction.transformToLongValuesSV(_projectionBlock);
    float[] floatValues = transformFunction.transformToFloatValuesSV(_projectionBlock);
    double[] doubleValues = transformFunction.transformToDoubleValuesSV(_projectionBlock);
    BigDecimal[] bigDecimalValues = null;
    try {
      // 1- Some transform functions cannot work with BigDecimal (e.g. exp, ln, and sqrt).
      // 2- NumberFormatException is thrown when converting double.NaN, Double.POSITIVE_INFINITY,
      // or Double.NEGATIVE_INFINITY.
      bigDecimalValues = transformFunction.transformToBigDecimalValuesSV(_projectionBlock);
    } catch (UnsupportedOperationException | NumberFormatException ignored) {
    }
    String[] stringValues = transformFunction.transformToStringValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      // only compare the results for non-null rows.
      if (expectedNull.contains(i)) {
        continue;
      }
      assertEquals(intValues[i], (int) expectedValues[i]);
      assertEquals(longValues[i], (long) expectedValues[i]);
      assertEquals(floatValues[i], (float) expectedValues[i]);
      assertEquals(doubleValues[i], expectedValues[i]);
      if (bigDecimalValues != null) {
        assertEquals(bigDecimalValues[i].doubleValue(), expectedValues[i]);
      }
      assertEquals(stringValues[i], Double.toString(expectedValues[i]));
    }
    testNullBitmap(transformFunction, expectedNull);
  }

  protected void testTransformFunction(TransformFunction transformFunction, BigDecimal[] expectedValues) {
    int[] intValues = transformFunction.transformToIntValuesSV(_projectionBlock);
    long[] longValues = transformFunction.transformToLongValuesSV(_projectionBlock);
    float[] floatValues = transformFunction.transformToFloatValuesSV(_projectionBlock);
    double[] doubleValues = transformFunction.transformToDoubleValuesSV(_projectionBlock);
    BigDecimal[] bigDecimalValues = transformFunction.transformToBigDecimalValuesSV(_projectionBlock);
    String[] stringValues = transformFunction.transformToStringValuesSV(_projectionBlock);
    byte[][] bytesValues = transformFunction.transformToBytesValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      assertEquals(intValues[i], expectedValues[i].intValue());
      assertEquals(longValues[i], expectedValues[i].longValue());
      assertEquals(floatValues[i], expectedValues[i].floatValue());
      assertEquals(doubleValues[i], expectedValues[i].doubleValue());
      assertEquals(bigDecimalValues[i].compareTo(expectedValues[i]), 0);
      assertEquals((new BigDecimal(stringValues[i])).compareTo(expectedValues[i]), 0);
      assertEquals(BigDecimalUtils.deserialize(bytesValues[i]).compareTo(expectedValues[i]), 0);
    }
    testNullBitmap(transformFunction, null);
  }

  protected void testTransformFunction(TransformFunction transformFunction, boolean[] expectedValues) {
    int[] intValues = transformFunction.transformToIntValuesSV(_projectionBlock);
    long[] longValues = transformFunction.transformToLongValuesSV(_projectionBlock);
    float[] floatValues = transformFunction.transformToFloatValuesSV(_projectionBlock);
    double[] doubleValues = transformFunction.transformToDoubleValuesSV(_projectionBlock);
    BigDecimal[] bigDecimalValues = transformFunction.transformToBigDecimalValuesSV(_projectionBlock);
    // TODO: Support implicit cast from BOOLEAN to STRING
    for (int i = 0; i < NUM_ROWS; i++) {
      assertEquals(intValues[i] == 1, expectedValues[i]);
      assertEquals(longValues[i] == 1, expectedValues[i]);
      assertEquals(floatValues[i] == 1, expectedValues[i]);
      assertEquals(doubleValues[i] == 1, expectedValues[i]);
      assertEquals(bigDecimalValues[i].intValue() == 1, expectedValues[i]);
    }
    testNullBitmap(transformFunction, null);
  }

  protected void testTransformFunctionWithNull(TransformFunction transformFunction, boolean[] expectedValues,
      RoaringBitmap expectedNulls) {
    int[] intValues = transformFunction.transformToIntValuesSV(_projectionBlock);
    long[] longValues = transformFunction.transformToLongValuesSV(_projectionBlock);
    float[] floatValues = transformFunction.transformToFloatValuesSV(_projectionBlock);
    double[] doubleValues = transformFunction.transformToDoubleValuesSV(_projectionBlock);
    BigDecimal[] bigDecimalValues =
        transformFunction.transformToBigDecimalValuesSV(_projectionBlock);

    for (int i = 0; i < NUM_ROWS; i++) {
      if (expectedNulls.contains(i)) {
        continue;
      }
      assertEquals(intValues[i] == 1, expectedValues[i]);
      assertEquals(longValues[i] == 1, expectedValues[i]);
      assertEquals(floatValues[i] == 1, expectedValues[i]);
      assertEquals(doubleValues[i] == 1, expectedValues[i]);
      assertEquals(bigDecimalValues[i].intValue() == 1, expectedValues[i]);
    }
    testNullBitmap(transformFunction, expectedNulls);
  }

  protected void testTransformFunction(TransformFunction transformFunction, Timestamp[] expectedValues) {
    int[] intValues = transformFunction.transformToIntValuesSV(_projectionBlock);
    long[] longValues = transformFunction.transformToLongValuesSV(_projectionBlock);
    float[] floatValues = transformFunction.transformToFloatValuesSV(_projectionBlock);
    double[] doubleValues = transformFunction.transformToDoubleValuesSV(_projectionBlock);
    BigDecimal[] bigDecimalValues = transformFunction.transformToBigDecimalValuesSV(_projectionBlock);
    // TODO: Support implicit cast from TIMESTAMP to STRING
//    String[] stringValues = transformFunction.transformToStringValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      assertEquals(intValues[i], (int) expectedValues[i].getTime());
      assertEquals(longValues[i], expectedValues[i].getTime());
      assertEquals(floatValues[i], (float) expectedValues[i].getTime());
      assertEquals(doubleValues[i], (double) expectedValues[i].getTime());
      assertEquals(bigDecimalValues[i], BigDecimal.valueOf(expectedValues[i].getTime()));
//      assertEquals(stringValues[i], expectedValues[i].toString());
    }
    testNullBitmap(transformFunction, null);
  }

  protected void testTransformFunction(TransformFunction transformFunction, String[] expectedValues) {
    String[] stringValues = transformFunction.transformToStringValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      assertEquals(stringValues[i], expectedValues[i]);
    }
    testNullBitmap(transformFunction, null);
  }

  protected void testTransformFunctionWithNull(TransformFunction transformFunction, String[] expectedValues,
      RoaringBitmap expectedNulls) {
    String[] stringValues = transformFunction.transformToStringValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      if (expectedNulls.contains(i)) {
        continue;
      }
      assertEquals(stringValues[i], expectedValues[i]);
    }
    testNullBitmap(transformFunction, expectedNulls);
  }

  protected void testTransformFunction(TransformFunction transformFunction, byte[][] expectedValues) {
    String[] stringValues = transformFunction.transformToStringValuesSV(_projectionBlock);
    byte[][] bytesValues = transformFunction.transformToBytesValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      assertEquals(bytesValues[i], BytesUtils.toBytes(stringValues[i]));
      assertEquals(bytesValues[i], expectedValues[i]);
    }
    testNullBitmap(transformFunction, null);
  }

  protected void testTransformFunctionMV(TransformFunction transformFunction, int[][] expectedValues) {
    int[][] intValuesMV = transformFunction.transformToIntValuesMV(_projectionBlock);
    long[][] longValuesMV = transformFunction.transformToLongValuesMV(_projectionBlock);
    float[][] floatValuesMV = transformFunction.transformToFloatValuesMV(_projectionBlock);
    double[][] doubleValuesMV = transformFunction.transformToDoubleValuesMV(_projectionBlock);
    String[][] stringValuesMV = transformFunction.transformToStringValuesMV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      int[] expectedValueMV = expectedValues[i];
      int numValues = expectedValueMV.length;
      assertEquals(intValuesMV[i].length, numValues);
      assertEquals(longValuesMV[i].length, numValues);
      assertEquals(floatValuesMV[i].length, numValues);
      assertEquals(doubleValuesMV[i].length, numValues);
      assertEquals(stringValuesMV[i].length, numValues);
      for (int j = 0; j < numValues; j++) {
        assertEquals(intValuesMV[i][j], expectedValues[i][j]);
        assertEquals(longValuesMV[i][j], expectedValues[i][j]);
        assertEquals(floatValuesMV[i][j], (float) expectedValues[i][j]);
        assertEquals(doubleValuesMV[i][j], (double) expectedValues[i][j]);
        assertEquals(stringValuesMV[i][j], Integer.toString(expectedValues[i][j]));
      }
    }
    testNullBitmap(transformFunction, null);
  }

  protected void testTransformFunctionMV(TransformFunction transformFunction, long[][] expectedValues) {
    int[][] intValuesMV = transformFunction.transformToIntValuesMV(_projectionBlock);
    long[][] longValuesMV = transformFunction.transformToLongValuesMV(_projectionBlock);
    float[][] floatValuesMV = transformFunction.transformToFloatValuesMV(_projectionBlock);
    double[][] doubleValuesMV = transformFunction.transformToDoubleValuesMV(_projectionBlock);
    String[][] stringValuesMV = transformFunction.transformToStringValuesMV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      long[] expectedValueMV = expectedValues[i];
      int numValues = expectedValueMV.length;
      assertEquals(intValuesMV[i].length, numValues);
      assertEquals(longValuesMV[i].length, numValues);
      assertEquals(floatValuesMV[i].length, numValues);
      assertEquals(doubleValuesMV[i].length, numValues);
      assertEquals(stringValuesMV[i].length, numValues);
      for (int j = 0; j < numValues; j++) {
        assertEquals(intValuesMV[i][j], (int) expectedValues[i][j]);
        assertEquals(longValuesMV[i][j], expectedValues[i][j]);
        assertEquals(floatValuesMV[i][j], (float) expectedValues[i][j]);
        assertEquals(doubleValuesMV[i][j], (double) expectedValues[i][j]);
        assertEquals(stringValuesMV[i][j], Long.toString(expectedValues[i][j]));
      }
    }
    testNullBitmap(transformFunction, null);
  }

  protected void testTransformFunctionMV(TransformFunction transformFunction, float[][] expectedValues) {
    int[][] intValuesMV = transformFunction.transformToIntValuesMV(_projectionBlock);
    long[][] longValuesMV = transformFunction.transformToLongValuesMV(_projectionBlock);
    float[][] floatValuesMV = transformFunction.transformToFloatValuesMV(_projectionBlock);
    double[][] doubleValuesMV = transformFunction.transformToDoubleValuesMV(_projectionBlock);
    String[][] stringValuesMV = transformFunction.transformToStringValuesMV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      float[] expectedValueMV = expectedValues[i];
      int numValues = expectedValueMV.length;
      assertEquals(intValuesMV[i].length, numValues);
      assertEquals(longValuesMV[i].length, numValues);
      assertEquals(floatValuesMV[i].length, numValues);
      assertEquals(doubleValuesMV[i].length, numValues);
      assertEquals(stringValuesMV[i].length, numValues);
      for (int j = 0; j < numValues; j++) {
        assertEquals(intValuesMV[i][j], (int) expectedValues[i][j]);
        assertEquals(longValuesMV[i][j], (long) expectedValues[i][j]);
        assertEquals(floatValuesMV[i][j], expectedValues[i][j]);
        assertEquals(doubleValuesMV[i][j], Double.parseDouble(Float.valueOf(expectedValues[i][j]).toString()));
        assertEquals(stringValuesMV[i][j], Float.toString(expectedValues[i][j]));
      }
    }
    testNullBitmap(transformFunction, null);
  }

  protected void testTransformFunctionMV(TransformFunction transformFunction, double[][] expectedValues) {
    int[][] intValuesMV = transformFunction.transformToIntValuesMV(_projectionBlock);
    long[][] longValuesMV = transformFunction.transformToLongValuesMV(_projectionBlock);
    float[][] floatValuesMV = transformFunction.transformToFloatValuesMV(_projectionBlock);
    double[][] doubleValuesMV = transformFunction.transformToDoubleValuesMV(_projectionBlock);
    String[][] stringValuesMV = transformFunction.transformToStringValuesMV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      double[] expectedValueMV = expectedValues[i];
      int numValues = expectedValueMV.length;
      assertEquals(intValuesMV[i].length, numValues);
      assertEquals(longValuesMV[i].length, numValues);
      assertEquals(floatValuesMV[i].length, numValues);
      assertEquals(doubleValuesMV[i].length, numValues);
      assertEquals(stringValuesMV[i].length, numValues);
      for (int j = 0; j < numValues; j++) {
        assertEquals(intValuesMV[i][j], (int) expectedValues[i][j]);
        assertEquals(longValuesMV[i][j], (long) expectedValues[i][j]);
        assertEquals(floatValuesMV[i][j], (float) expectedValues[i][j]);
        assertEquals(doubleValuesMV[i][j], expectedValues[i][j]);
        assertEquals(stringValuesMV[i][j], Double.toString(expectedValues[i][j]));
      }
    }
    testNullBitmap(transformFunction, null);
  }

  protected void testTransformFunctionMV(TransformFunction transformFunction, String[][] expectedValues) {
    String[][] stringValuesMV = transformFunction.transformToStringValuesMV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      assertEquals(stringValuesMV[i], expectedValues[i]);
    }
    testNullBitmap(transformFunction, null);
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(new File(INDEX_DIR_PATH));
  }
}
