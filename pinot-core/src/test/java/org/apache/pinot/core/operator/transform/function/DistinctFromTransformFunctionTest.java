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


public abstract class DistinctFromTransformFunctionTest {
  private static final String SEGMENT_NAME = "testSegment";
  private static final String INT_SV_COLUMN = "intSV";
  private static final String INT_SV_NULL_COLUMN = "intSV2";
  private static final Random RANDOM = new Random();
  private static final int NUM_ROWS = 1000;
  private static final int VALUE_MOD = 3;

  private final boolean _isDistinctFrom;
  private final String _expression;
  private final int[] _intSVValues = new int[NUM_ROWS];

  private Map<String, DataSource> _dataSourceMap;
  private ProjectionBlock _projectionBlock;

  DistinctFromTransformFunctionTest(boolean isDistinctFrom) {
    _isDistinctFrom = isDistinctFrom;
    _expression = _isDistinctFrom ? "%s IS DISTINCT FROM %s" : "%s IS NOT DISTINCT FROM %s";
  }

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

  private static boolean isEqualRow(int i) {
    return i % VALUE_MOD == 0;
  }

  private static boolean isNotEqualRow(int i) {
    return i % VALUE_MOD == 1;
  }

  private static boolean isNullRow(int i) {
    return i % VALUE_MOD == 2;
  }

  @BeforeClass
  public void setup()
      throws Exception {
    // Sets up a table with two integer columns: one column with every row filled in with an integer number; the other
    // column with 1/3 rows equal to first column, 1/3 rows not equal to first column, and 1/3 rows as null.
    FileUtils.deleteQuietly(new File(getIndexDirPath(SEGMENT_NAME)));
    for (int i = 0; i < NUM_ROWS; i++) {
      _intSVValues[i] = RANDOM.nextInt();
    }
    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      Map<String, Object> map = new HashMap<>();
      map.put(INT_SV_COLUMN, _intSVValues[i]);
      if (isEqualRow(i)) {
        map.put(INT_SV_NULL_COLUMN, _intSVValues[i]);
      } else if (isNotEqualRow(i)) {
        map.put(INT_SV_NULL_COLUMN, _intSVValues[i] + 1);
      } else if (isNullRow(i)) {
        map.put(INT_SV_NULL_COLUMN, null);
      }
      GenericRow row = new GenericRow();
      row.init(map);
      rows.add(row);
    }

    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension(INT_SV_COLUMN, FieldSpec.DataType.INT)
        .addSingleValueDimension(INT_SV_NULL_COLUMN, FieldSpec.DataType.INT).build();
    _dataSourceMap = getDataSourceMap(schema, rows, SEGMENT_NAME);
    _projectionBlock = getProjectionBlock(_dataSourceMap);
  }

  protected void testTransformFunction(ExpressionContext expression, boolean[] expectedValues,
      ProjectionBlock projectionBlock, Map<String, DataSource> dataSourceMap)
      throws Exception {
    int[] intValues = getTransformFunctionInstance(expression, dataSourceMap).transformToIntValuesSV(projectionBlock);
    long[] longValues =
        getTransformFunctionInstance(expression, dataSourceMap).transformToLongValuesSV(projectionBlock);
    float[] floatValues =
        getTransformFunctionInstance(expression, dataSourceMap).transformToFloatValuesSV(projectionBlock);
    double[] doubleValues =
        getTransformFunctionInstance(expression, dataSourceMap).transformToDoubleValuesSV(projectionBlock);
    // TODO: Support implicit cast from BOOLEAN to STRING
//    String[] stringValues =
//        getTransformFunctionInstance(expression, dataSourceMap).transformToStringValuesSV(projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      Assert.assertEquals(intValues[i] == 1, expectedValues[i]);
      Assert.assertEquals(longValues[i] == 1, expectedValues[i]);
      Assert.assertEquals(floatValues[i] == 1, expectedValues[i]);
      Assert.assertEquals(doubleValues[i] == 1, expectedValues[i]);
//      Assert.assertEquals(stringValues[i], Boolean.toString(expectedValues[i]));
    }
  }

  private TransformFunction getTransformFunctionInstance(ExpressionContext expression,
      Map<String, DataSource> dataSourceMap) {
    return TransformFunctionFactory.get(expression, dataSourceMap);
  }

  @Test
  public void testDistinctFromLeftNull()
      throws Exception {
    ExpressionContext expression =
        RequestContextUtils.getExpression(String.format(_expression, INT_SV_NULL_COLUMN, INT_SV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertEquals(transformFunction.getName(), _isDistinctFrom ? "is_distinct_from" : "is_not_distinct_from");
    boolean[] expectedIntValues = new boolean[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isEqualRow(i)) {
        expectedIntValues[i] = !_isDistinctFrom;
      } else if (isNotEqualRow(i)) {
        expectedIntValues[i] = _isDistinctFrom;
      } else if (isNullRow(i)) {
        expectedIntValues[i] = _isDistinctFrom;
      }
    }

    testTransformFunction(expression, expectedIntValues, _projectionBlock, _dataSourceMap);
  }

  @Test
  public void testDistinctFromRightNull()
      throws Exception {
    ExpressionContext expression =
        RequestContextUtils.getExpression(String.format(_expression, INT_SV_COLUMN, INT_SV_NULL_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertEquals(transformFunction.getName(), _isDistinctFrom ? "is_distinct_from" : "is_not_distinct_from");
    boolean[] expectedIntValues = new boolean[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isEqualRow(i)) {
        expectedIntValues[i] = !_isDistinctFrom;
      } else if (isNotEqualRow(i)) {
        expectedIntValues[i] = _isDistinctFrom;
      } else if (isNullRow(i)) {
        expectedIntValues[i] = _isDistinctFrom;
      }
    }

    testTransformFunction(expression, expectedIntValues, _projectionBlock, _dataSourceMap);
  }

  @Test
  public void testDistinctFromBothNull()
      throws Exception {
    ExpressionContext expression =
        RequestContextUtils.getExpression(String.format(_expression, INT_SV_NULL_COLUMN, INT_SV_NULL_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertEquals(transformFunction.getName(), _isDistinctFrom ? "is_distinct_from" : "is_not_distinct_from");
    boolean[] expectedIntValues = new boolean[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedIntValues[i] = !_isDistinctFrom;
    }

    testTransformFunction(expression, expectedIntValues, _projectionBlock, _dataSourceMap);
  }

  @Test
  public void testDistinctFromLeftLiteralRightIdentifier()
      throws Exception {
    ExpressionContext expression =
        RequestContextUtils.getExpression(String.format(_expression, "NULL", INT_SV_NULL_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertEquals(transformFunction.getName(), _isDistinctFrom ? "is_distinct_from" : "is_not_distinct_from");
    boolean[] expectedIntValues = new boolean[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isNullRow(i)) {
        expectedIntValues[i] = !_isDistinctFrom;
      } else {
        expectedIntValues[i] = _isDistinctFrom;
      }
    }

    testTransformFunction(expression, expectedIntValues, _projectionBlock, _dataSourceMap);
  }

  @Test
  public void testDistinctFromLeftFunctionRightIdentifier()
      throws Exception {
    ExpressionContext expression =
        RequestContextUtils.getExpression(String.format(_expression, "NULL + 1", INT_SV_NULL_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertEquals(transformFunction.getName(), _isDistinctFrom ? "is_distinct_from" : "is_not_distinct_from");
    boolean[] expectedIntValues = new boolean[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isNullRow(i)) {
        expectedIntValues[i] = !_isDistinctFrom;
      } else {
        expectedIntValues[i] = _isDistinctFrom;
      }
    }

    testTransformFunction(expression, expectedIntValues, _projectionBlock, _dataSourceMap);
  }

  @Test
  public void testIllegalNumberOfArgs() {
    String expressionTemplate = _isDistinctFrom ? "is_distinct_from(%s, %s, %s)" : "is_not_distinct_from(%s, %s, %s)";
    ExpressionContext expression = RequestContextUtils.getExpression(
        String.format(expressionTemplate, INT_SV_COLUMN, INT_SV_NULL_COLUMN, INT_SV_COLUMN));

    Assert.assertThrows(RuntimeException.class, () -> {
      TransformFunctionFactory.get(expression, _dataSourceMap);
    });
  }

  @Test
  public void testGetNullBitmapReturnsNull() {
    ExpressionContext isDistinctFromExpression =
        RequestContextUtils.getExpression(String.format(_expression, INT_SV_NULL_COLUMN, INT_SV_COLUMN));
    TransformFunction isDistinctFromTransformFunction =
        TransformFunctionFactory.get(isDistinctFromExpression, _dataSourceMap);

    Assert.assertNull(isDistinctFromTransformFunction.getNullBitmap(_projectionBlock));
  }
}
