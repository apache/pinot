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
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
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
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class NullHandlingTransformFunctionTest {
  private static final String SEGMENT_NAME = "testSegmentWithNulls";
  private static final String INDEX_DIR_PATH = FileUtils.getTempDirectoryPath() + File.separator + SEGMENT_NAME;
  private static final Random RANDOM = new Random();

  protected static final int NUM_ROWS = 1000;
  protected static final String INT_SV_COLUMN = "intSV";
  protected static final String LONG_SV_COLUMN = "longSV";
  protected static final String FLOAT_SV_COLUMN = "floatSV";
  protected static final String DOUBLE_SV_COLUMN = "doubleSV";
  protected static final String STRING_SV_COLUMN = "stringSV";
  protected static final String BYTES_SV_COLUMN = "bytesSV";

  protected static final String TIMESTAMP_COLUMN = "timestampColumn";
  protected static final String TIME_COLUMN = "timeColumn";
  protected final long[] _timeValues = new long[NUM_ROWS];

  protected final int[] _intSVValues = new int[NUM_ROWS];
  protected final long[] _longSVValues = new long[NUM_ROWS];
  protected final float[] _floatSVValues = new float[NUM_ROWS];
  protected final double[] _doubleSVValues = new double[NUM_ROWS];
  protected final String[] _stringSVValues = new String[NUM_ROWS];
  protected final byte[][] _bytesSVValues = new byte[NUM_ROWS][];

  protected Map<String, DataSource> _dataSourceMap;
  protected ProjectionBlock _projectionBlock;
  protected static final int NULL_VALUE_MOD = 10;

  @BeforeClass
  public void setup()
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
      _bytesSVValues[i] = RandomStringUtils.randomAlphanumeric(26).getBytes();

      _timeValues[i] = currentTimeMs - RANDOM.nextInt(365 * 24 * 3600) * 1000L;
    }

    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      Map<String, Object> map = new HashMap<>();
      if (i % NULL_VALUE_MOD != 0) {
        map.put(INT_SV_COLUMN, _intSVValues[i]);
        map.put(LONG_SV_COLUMN, _longSVValues[i]);
        map.put(FLOAT_SV_COLUMN, _floatSVValues[i]);
        map.put(DOUBLE_SV_COLUMN, _doubleSVValues[i]);
        map.put(STRING_SV_COLUMN, _stringSVValues[i]);
        map.put(BYTES_SV_COLUMN, _bytesSVValues[i]);
      } else {
        map.put(INT_SV_COLUMN, null);
        map.put(LONG_SV_COLUMN, null);
        map.put(FLOAT_SV_COLUMN, null);
        map.put(DOUBLE_SV_COLUMN, null);
        map.put(STRING_SV_COLUMN, null);
        map.put(BYTES_SV_COLUMN, null);
      }
      map.put(TIMESTAMP_COLUMN, _timeValues[i]);
      map.put(TIME_COLUMN, _timeValues[i]);
      GenericRow row = new GenericRow();
      row.init(map);
      rows.add(row);
    }

    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension(INT_SV_COLUMN, FieldSpec.DataType.INT)
        .addSingleValueDimension(LONG_SV_COLUMN, FieldSpec.DataType.LONG)
        .addSingleValueDimension(FLOAT_SV_COLUMN, FieldSpec.DataType.FLOAT)
        .addSingleValueDimension(DOUBLE_SV_COLUMN, FieldSpec.DataType.DOUBLE)
        .addSingleValueDimension(STRING_SV_COLUMN, FieldSpec.DataType.STRING)
        .addSingleValueDimension(BYTES_SV_COLUMN, FieldSpec.DataType.BYTES)
        .addDateTime(TIMESTAMP_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, TIME_COLUMN), null).build();
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("testWithNulls").setNullHandlingEnabled(true)
            .setTimeColumnName(TIME_COLUMN).build();

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

  @Test
  public void testIsNullTransformFunction()
      throws Exception {
    testIsNullTransformFunction(INT_SV_COLUMN);
    testIsNullTransformFunction(LONG_SV_COLUMN);
    testIsNullTransformFunction(FLOAT_SV_COLUMN);
    testIsNullTransformFunction(DOUBLE_SV_COLUMN);
    testIsNullTransformFunction(STRING_SV_COLUMN);
    testIsNullTransformFunction(BYTES_SV_COLUMN);
  }

  public void testIsNullTransformFunction(String columnName)
      throws Exception {
    ExpressionContext expression = RequestContextUtils.getExpression(String.format("%s IS NULL", columnName));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof IsNullTransformFunction);
    Assert.assertEquals(transformFunction.getName(), TransformFunctionType.IS_NULL.getName());
    int[] expectedValues = new int[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      if (i % NULL_VALUE_MOD == 0) {
        expectedValues[i] = 1;
      }
    }
    testTransformFunction(expression, expectedValues);
  }

  @Test
  public void testIsNotNullTransformFunction()
      throws Exception {
    testIsNotNullTransformFunction(INT_SV_COLUMN);
    testIsNotNullTransformFunction(LONG_SV_COLUMN);
    testIsNotNullTransformFunction(FLOAT_SV_COLUMN);
    testIsNotNullTransformFunction(DOUBLE_SV_COLUMN);
    testIsNotNullTransformFunction(STRING_SV_COLUMN);
    testIsNotNullTransformFunction(BYTES_SV_COLUMN);
  }

  public void testIsNotNullTransformFunction(String columnName)
      throws Exception {
    ExpressionContext expression =
        RequestContextUtils.getExpression(String.format("%s IS NOT NULL", columnName));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof IsNotNullTransformFunction);
    Assert.assertEquals(transformFunction.getName(), TransformFunctionType.IS_NOT_NULL.getName());
    int[] expectedValues = new int[NUM_ROWS];
    Arrays.fill(expectedValues, 1);
    for (int i = 0; i < NUM_ROWS; i++) {
      if (i % NULL_VALUE_MOD == 0) {
        expectedValues[i] = 0;
      }
    }
    testTransformFunction(expression, expectedValues);
  }

  protected void testTransformFunction(ExpressionContext expression, int[] expectedValues) throws Exception {
    int[] intValues = getTransformFunctionInstance(expression).transformToIntValuesSV(_projectionBlock);
    long[] longValues = getTransformFunctionInstance(expression).transformToLongValuesSV(_projectionBlock);
    float[] floatValues = getTransformFunctionInstance(expression).transformToFloatValuesSV(_projectionBlock);
    double[] doubleValues = getTransformFunctionInstance(expression).transformToDoubleValuesSV(_projectionBlock);
    String[] stringValues = getTransformFunctionInstance(expression).transformToStringValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      Assert.assertEquals(intValues[i], expectedValues[i]);
      Assert.assertEquals(longValues[i], expectedValues[i]);
      Assert.assertEquals(floatValues[i], (float) expectedValues[i]);
      Assert.assertEquals(doubleValues[i], (double) expectedValues[i]);
      Assert.assertEquals(stringValues[i], Integer.toString(expectedValues[i]));
    }
  }

  private TransformFunction getTransformFunctionInstance(ExpressionContext expression) {
    return TransformFunctionFactory.get(expression, _dataSourceMap);
  }
}
