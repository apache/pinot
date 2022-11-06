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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.commons.io.FileUtils;
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
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class CoalesceTransformFunctionTest extends BaseTransformFunctionTest {
  private static final String ENABLE_NULL_SEGMENT_NAME = "testSegment1";
  private static final String DISABLE_NULL_SEGMENT_NAME = "testSegment2";
  private static final Random RANDOM = new Random();

  private static final int NUM_ROWS = 1000;
  private static final String INT_SV_COLUMN1 = "intSV1";
  private static final String INT_SV_COLUMN2 = "intSV2";
  private static final String STRING_SV_COLUMN1 = "StringSV1";
  private static final String STRING_SV_COLUMN2 = "StringSV2";
  private static final String BIG_DECIMAL_SV_COLUMN1 = "BigDecimalSV1";
  private static final String BIG_DECIMAL_SV_COLUMN2 = "BigDecimalSV2";
  private static final String LONG_SV_COLUMN1 = "LongSV1";
  private static final String LONG_SV_COLUMN2 = "LongSV2";
  private static final String DOUBLE_SV_COLUMN1 = "DoubleSV1";
  private static final String DOUBLE_SV_COLUMN2 = "DoubleSV2";

  private static final String FLOAT_SV_COLUMN1 = "FloatSV1";
  private static final String FLOAT_SV_COLUMN2 = "FLoatSV2";
  private final int[] _intSVValues = new int[NUM_ROWS];
  private final double[] _doubleValues = new double[NUM_ROWS];
  private final float[] _floatValues = new float[NUM_ROWS];
  private final String[] _stringSVValues = new String[NUM_ROWS];
  private Map<String, DataSource> _enableNullDataSourceMap;
  private Map<String, DataSource> _disableNullDataSourceMap;
  private ProjectionBlock _enableNullProjectionBlock;
  private ProjectionBlock _disableNullProjectionBlock;
  // Mod decides whether the first column of the same type should be null.
  private static final int NULL_MOD1 = 3;
  // Mod decides whether the second column of the same type should be null.
  private static final int NULL_MOD2 = 5;
  // Difference between two same type numeric columns.
  private static final int INT_VALUE_SHIFT = 2;
  private static final double DOUBLE_VALUE_SHIFT = 0.1;
  private static final float FLOAT_VALUE_SHIFT = 0.1f;

  // Suffix for second string column.
  private static final String SUFFIX = "column2";

  private static String getIndexDirPath(String segmentName) {
    return FileUtils.getTempDirectoryPath() + File.separator + segmentName;
  }

  private static Map<String, DataSource> getDataSourceMap(Schema schema, List<GenericRow> rows, String segmentName)
      throws Exception {
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(segmentName).setNullHandlingEnabled(true).build();
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(getIndexDirPath(segmentName));
    config.setSegmentName(segmentName);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();
    IndexSegment indexSegment =
        ImmutableSegmentLoader.load(new File(getIndexDirPath(segmentName), segmentName), ReadMode.heap);
    Set<String> columnNames = indexSegment.getPhysicalColumnNames();
    Map<String, DataSource> enableNullDataSourceMap = new HashMap<>(columnNames.size());
    for (String columnName : columnNames) {
      enableNullDataSourceMap.put(columnName, indexSegment.getDataSource(columnName));
    }
    return enableNullDataSourceMap;
  }

  private static ProjectionBlock getProjectionBlock(Map<String, DataSource> dataSourceMap) {
    return new ProjectionOperator(dataSourceMap,
        new DocIdSetOperator(new MatchAllFilterOperator(NUM_ROWS), DocIdSetPlanNode.MAX_DOC_PER_CALL)).nextBlock();
  }

  private static boolean isColumn1Null(int i) {
    return i % NULL_MOD1 == 0;
  }

  private static boolean isColumn2Null(int i) {
    return i % NULL_MOD2 == 0;
  }

  @BeforeClass
  public void setup()
      throws Exception {
    // Set up two tables: one with null option enable, the other with null option disable.
    // Each table one string column, and one int column with some rows set to null.
    FileUtils.deleteQuietly(new File(getIndexDirPath(DISABLE_NULL_SEGMENT_NAME)));
    FileUtils.deleteQuietly(new File(getIndexDirPath(ENABLE_NULL_SEGMENT_NAME)));
    for (int i = 0; i < NUM_ROWS; i++) {
      _intSVValues[i] = RANDOM.nextInt();
      _doubleValues[i] = RANDOM.nextDouble();
      _floatValues[i] = RANDOM.nextFloat();
      _stringSVValues[i] = "a" + RANDOM.nextInt();
    }
    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      Map<String, Object> map = new HashMap<>();
      map.put(INT_SV_COLUMN1, _intSVValues[i]);
      map.put(INT_SV_COLUMN2, _intSVValues[i] + INT_VALUE_SHIFT);
      map.put(DOUBLE_SV_COLUMN1, _doubleValues[i]);
      map.put(DOUBLE_SV_COLUMN2, _doubleValues[i] + DOUBLE_VALUE_SHIFT);
      map.put(FLOAT_SV_COLUMN1, _floatValues[i]);
      map.put(FLOAT_SV_COLUMN2, _floatValues[i] + FLOAT_VALUE_SHIFT);
      map.put(STRING_SV_COLUMN1, _stringSVValues[i]);
      map.put(STRING_SV_COLUMN2, _stringSVValues[i] + SUFFIX);
      map.put(BIG_DECIMAL_SV_COLUMN1, BigDecimal.valueOf(_intSVValues[i]));
      map.put(BIG_DECIMAL_SV_COLUMN2, BigDecimal.valueOf(_intSVValues[i] + INT_VALUE_SHIFT));
      map.put(LONG_SV_COLUMN1, _intSVValues[i]);
      map.put(LONG_SV_COLUMN2, _intSVValues[i] + INT_VALUE_SHIFT);

      if (isColumn1Null(i)) {
        map.put(INT_SV_COLUMN1, null);
        map.put(STRING_SV_COLUMN1, null);
        map.put(BIG_DECIMAL_SV_COLUMN1, null);
        map.put(LONG_SV_COLUMN1, null);
        map.put(DOUBLE_SV_COLUMN1, null);
        map.put(FLOAT_SV_COLUMN1, null);
      }
      if (isColumn2Null(i)) {
        map.put(INT_SV_COLUMN2, null);
        map.put(STRING_SV_COLUMN2, null);
        map.put(LONG_SV_COLUMN2, null);
        map.put(BIG_DECIMAL_SV_COLUMN2, null);
        map.put(DOUBLE_SV_COLUMN2, null);
        map.put(FLOAT_SV_COLUMN2, null);
      }
      GenericRow row = new GenericRow();
      row.init(map);
      rows.add(row);
    }
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension(INT_SV_COLUMN1, FieldSpec.DataType.INT)
        .addSingleValueDimension(INT_SV_COLUMN2, FieldSpec.DataType.INT)
        .addSingleValueDimension(STRING_SV_COLUMN1, FieldSpec.DataType.STRING)
        .addSingleValueDimension(STRING_SV_COLUMN2, FieldSpec.DataType.STRING)
        .addSingleValueDimension(LONG_SV_COLUMN1, FieldSpec.DataType.LONG)
        .addSingleValueDimension(LONG_SV_COLUMN2, FieldSpec.DataType.LONG)
        .addSingleValueDimension(DOUBLE_SV_COLUMN1, FieldSpec.DataType.DOUBLE)
        .addSingleValueDimension(DOUBLE_SV_COLUMN2, FieldSpec.DataType.DOUBLE)
        .addSingleValueDimension(FLOAT_SV_COLUMN1, FieldSpec.DataType.FLOAT)
        .addSingleValueDimension(FLOAT_SV_COLUMN2, FieldSpec.DataType.FLOAT)
        .addMetric(BIG_DECIMAL_SV_COLUMN1, FieldSpec.DataType.BIG_DECIMAL)
        .addMetric(BIG_DECIMAL_SV_COLUMN2, FieldSpec.DataType.BIG_DECIMAL).build();
    _enableNullDataSourceMap = getDataSourceMap(schema, rows, ENABLE_NULL_SEGMENT_NAME);
    _enableNullProjectionBlock = getProjectionBlock(_enableNullDataSourceMap);
    _disableNullDataSourceMap = getDataSourceMap(schema, rows, DISABLE_NULL_SEGMENT_NAME);
    _disableNullProjectionBlock = getProjectionBlock(_disableNullDataSourceMap);
  }

  private static void testIntTransformFunction(ExpressionContext expression, int[] expectedValues,
      ProjectionBlock projectionBlock, Map<String, DataSource> dataSourceMap)
      throws Exception {
    int[] actualValues =
        TransformFunctionFactory.get(expression, dataSourceMap).transformToIntValuesSV(projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      Assert.assertEquals(actualValues[i], expectedValues[i]);
    }
  }

  private static void testStringTransformFunction(ExpressionContext expression, String[] expectedValues,
      ProjectionBlock projectionBlock, Map<String, DataSource> dataSourceMap)
      throws Exception {
    String[] actualValues =
        TransformFunctionFactory.get(expression, dataSourceMap).transformToStringValuesSV(projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      Assert.assertEquals(actualValues[i], expectedValues[i]);
    }
  }

  private static void testLongTransformFunction(ExpressionContext expression, long[] expectedValues,
      ProjectionBlock projectionBlock, Map<String, DataSource> dataSourceMap)
      throws Exception {
    long[] actualValues =
        TransformFunctionFactory.get(expression, dataSourceMap).transformToLongValuesSV(projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      Assert.assertEquals(actualValues[i], expectedValues[i]);
    }
  }

  private static void testDoubleTransformFunction(ExpressionContext expression, double[] expectedValues,
      ProjectionBlock projectionBlock, Map<String, DataSource> dataSourceMap)
      throws Exception {
    double[] actualValues =
        TransformFunctionFactory.get(expression, dataSourceMap).transformToDoubleValuesSV(projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      Assert.assertEquals(actualValues[i], expectedValues[i]);
    }
  }

  private static void testFloatTransformFunction(ExpressionContext expression, float[] expectedValues,
      ProjectionBlock projectionBlock, Map<String, DataSource> dataSourceMap)
      throws Exception {
    float[] actualValues =
        TransformFunctionFactory.get(expression, dataSourceMap).transformToFloatValuesSV(projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      Assert.assertEquals(actualValues[i], expectedValues[i]);
    }
  }

  private static void testBigDecimalTransformFunction(ExpressionContext expression, BigDecimal[] expectedValues,
      ProjectionBlock projectionBlock, Map<String, DataSource> dataSourceMap)
      throws Exception {
    BigDecimal[] actualValues =
        TransformFunctionFactory.get(expression, dataSourceMap).transformToBigDecimalValuesSV(projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      Assert.assertEquals(actualValues[i], expectedValues[i]);
    }
  }

  // Test the Coalesce on two Int columns where one or the other or both can be null.
  @Test
  public void testCoalesceIntColumns()
      throws Exception {
    ExpressionContext coalesceExpr =
        RequestContextUtils.getExpression(String.format("COALESCE(%s,%s)", INT_SV_COLUMN1, INT_SV_COLUMN2));
    TransformFunction coalesceTransformFunction = TransformFunctionFactory.get(coalesceExpr, _enableNullDataSourceMap);
    Assert.assertEquals(coalesceTransformFunction.getName(), "coalesce");
    int[] expectedResults = new int[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isColumn1Null(i) && isColumn2Null(i)) {
        expectedResults[i] = CoalesceTransformFunction.NULL_INT;
      } else if (isColumn1Null(i)) {
        expectedResults[i] = _intSVValues[i] + INT_VALUE_SHIFT;
      } else if (isColumn2Null(i)) {
        expectedResults[i] = _intSVValues[i];
      } else {
        expectedResults[i] = _intSVValues[i];
      }
    }
    testIntTransformFunction(coalesceExpr, expectedResults, _enableNullProjectionBlock, _enableNullDataSourceMap);
    testIntTransformFunction(coalesceExpr, expectedResults, _disableNullProjectionBlock, _disableNullDataSourceMap);
  }

  // Test the Coalesce on two long columns where one or the other or both can be null.
  @Test
  public void testCoalesceLongColumns()
      throws Exception {
    ExpressionContext coalesceExpr =
        RequestContextUtils.getExpression(String.format("COALESCE(%s,%s)", LONG_SV_COLUMN1, LONG_SV_COLUMN2));
    TransformFunction coalesceTransformFunction = TransformFunctionFactory.get(coalesceExpr, _enableNullDataSourceMap);
    Assert.assertEquals(coalesceTransformFunction.getName(), "coalesce");
    long[] expectedResults = new long[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isColumn1Null(i) && isColumn2Null(i)) {
        expectedResults[i] = CoalesceTransformFunction.NULL_LONG;
      } else if (isColumn1Null(i)) {
        expectedResults[i] = _intSVValues[i] + INT_VALUE_SHIFT;
      } else if (isColumn2Null(i)) {
        expectedResults[i] = _intSVValues[i];
      } else {
        expectedResults[i] = _intSVValues[i];
      }
    }
    testLongTransformFunction(coalesceExpr, expectedResults, _enableNullProjectionBlock, _enableNullDataSourceMap);
    testLongTransformFunction(coalesceExpr, expectedResults, _disableNullProjectionBlock, _disableNullDataSourceMap);
  }

  // Test the Coalesce on two float columns where one or the other or both can be null.
  @Test
  public void testCoalesceFloatColumns()
      throws Exception {
    ExpressionContext coalesceExpr =
        RequestContextUtils.getExpression(String.format("COALESCE(%s,%s)", FLOAT_SV_COLUMN1, FLOAT_SV_COLUMN2));
    TransformFunction coalesceTransformFunction = TransformFunctionFactory.get(coalesceExpr, _enableNullDataSourceMap);
    Assert.assertEquals(coalesceTransformFunction.getName(), "coalesce");
    float[] expectedResults = new float[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isColumn1Null(i) && isColumn2Null(i)) {
        expectedResults[i] = CoalesceTransformFunction.NULL_FLOAT;
      } else if (isColumn1Null(i)) {
        expectedResults[i] = _floatValues[i] + FLOAT_VALUE_SHIFT;
      } else if (isColumn2Null(i)) {
        expectedResults[i] = _floatValues[i];
      } else {
        expectedResults[i] = _floatValues[i];
      }
    }
    testFloatTransformFunction(coalesceExpr, expectedResults, _enableNullProjectionBlock, _enableNullDataSourceMap);
    testFloatTransformFunction(coalesceExpr, expectedResults, _disableNullProjectionBlock, _disableNullDataSourceMap);
  }

  // Test the Coalesce on two double columns where one or the other or both can be null.
  @Test
  public void testCoalesceDoubleColumns()
      throws Exception {
    ExpressionContext coalesceExpr =
        RequestContextUtils.getExpression(String.format("COALESCE(%s,%s)", DOUBLE_SV_COLUMN1, DOUBLE_SV_COLUMN2));
    TransformFunction coalesceTransformFunction = TransformFunctionFactory.get(coalesceExpr, _enableNullDataSourceMap);
    Assert.assertEquals(coalesceTransformFunction.getName(), "coalesce");
    double[] expectedResults = new double[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isColumn1Null(i) && isColumn2Null(i)) {
        expectedResults[i] = CoalesceTransformFunction.NULL_DOUBLE;
      } else if (isColumn1Null(i)) {
        expectedResults[i] = _doubleValues[i] + DOUBLE_VALUE_SHIFT;
      } else if (isColumn2Null(i)) {
        expectedResults[i] = _doubleValues[i];
      } else {
        expectedResults[i] = _doubleValues[i];
      }
    }
    testDoubleTransformFunction(coalesceExpr, expectedResults, _enableNullProjectionBlock, _enableNullDataSourceMap);
    testDoubleTransformFunction(coalesceExpr, expectedResults, _disableNullProjectionBlock, _disableNullDataSourceMap);
  }

  // Test the Coalesce on two big decimal columns where one or the other or both can be null.
  @Test
  public void testCoalesceBigDecimalColumns()
      throws Exception {
    ExpressionContext coalesceExpr = RequestContextUtils.getExpression(
        String.format("COALESCE(%s,%s)", BIG_DECIMAL_SV_COLUMN1, BIG_DECIMAL_SV_COLUMN2));
    TransformFunction coalesceTransformFunction = TransformFunctionFactory.get(coalesceExpr, _enableNullDataSourceMap);
    Assert.assertEquals(coalesceTransformFunction.getName(), "coalesce");
    BigDecimal[] expectedResults = new BigDecimal[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isColumn1Null(i) && isColumn2Null(i)) {
        expectedResults[i] = CoalesceTransformFunction.NULL_BIG_DECIMAL;
      } else if (isColumn1Null(i)) {
        expectedResults[i] = BigDecimal.valueOf(_intSVValues[i] + INT_VALUE_SHIFT);
      } else if (isColumn2Null(i)) {
        expectedResults[i] = BigDecimal.valueOf(_intSVValues[i]);
      } else {
        expectedResults[i] = BigDecimal.valueOf(_intSVValues[i]);
      }
    }
    testBigDecimalTransformFunction(coalesceExpr, expectedResults, _enableNullProjectionBlock,
        _enableNullDataSourceMap);
    testBigDecimalTransformFunction(coalesceExpr, expectedResults, _disableNullProjectionBlock,
        _disableNullDataSourceMap);
  }

  // Test the Coalesce on two String columns where one or the other or both can be null.
  @Test
  public void testCoalesceStringColumns()
      throws Exception {
    ExpressionContext coalesceExpr =
        RequestContextUtils.getExpression(String.format("COALESCE(%s,%s)", STRING_SV_COLUMN1, STRING_SV_COLUMN2));
    TransformFunction coalesceTransformFunction = TransformFunctionFactory.get(coalesceExpr, _enableNullDataSourceMap);
    Assert.assertEquals(coalesceTransformFunction.getName(), "coalesce");
    String[] expectedResults = new String[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isColumn1Null(i) && isColumn2Null(i)) {
        expectedResults[i] = CoalesceTransformFunction.NULL_STRING;
      } else if (isColumn1Null(i)) {
        expectedResults[i] = _stringSVValues[i] + SUFFIX;
      } else if (isColumn2Null(i)) {
        expectedResults[i] = _stringSVValues[i];
      } else {
        expectedResults[i] = _stringSVValues[i];
      }
    }
    testStringTransformFunction(coalesceExpr, expectedResults, _enableNullProjectionBlock, _enableNullDataSourceMap);
    testStringTransformFunction(coalesceExpr, expectedResults, _disableNullProjectionBlock, _disableNullDataSourceMap);
  }

  // Test that non-column-names appear in one of the argument.
  @Test
  public void testIllegalColumnName()
      throws Exception {
    ExpressionContext coalesceExpr =
        RequestContextUtils.getExpression(String.format("COALESCE(%s,%s)", _stringSVValues[0], STRING_SV_COLUMN1));
    Assert.assertThrows(RuntimeException.class, () -> {
      TransformFunctionFactory.get(coalesceExpr, _enableNullDataSourceMap);
    });
    Assert.assertThrows(RuntimeException.class, () -> {
      TransformFunctionFactory.get(coalesceExpr, _disableNullDataSourceMap);
    });
  }

  // Test that wrong data type is illegal argument.
  @Test
  public void testIllegalArgType()
      throws Exception {
    ExpressionContext coalesceExpr =
        RequestContextUtils.getExpression(String.format("COALESCE(%s,%s)", TIMESTAMP_COLUMN, STRING_SV_COLUMN));
    Assert.assertThrows(RuntimeException.class, () -> {
      TransformFunctionFactory.get(coalesceExpr, _enableNullDataSourceMap);
    });
    Assert.assertThrows(RuntimeException.class, () -> {
      TransformFunctionFactory.get(coalesceExpr, _disableNullDataSourceMap);
    });
  }

  // Test that all arguments have to be same type.
  @Test
  public void testDifferentArgumentType()
      throws Exception {
    ExpressionContext coalesceExpr =
        RequestContextUtils.getExpression(String.format("COALESCE(%s,%s)", INT_SV_COLUMN1, STRING_SV_COLUMN1));
    Assert.assertThrows(RuntimeException.class, () -> {
      TransformFunctionFactory.get(coalesceExpr, _enableNullDataSourceMap);
    });
    Assert.assertThrows(RuntimeException.class, () -> {
      TransformFunctionFactory.get(coalesceExpr, _disableNullDataSourceMap);
    });
  }
}
