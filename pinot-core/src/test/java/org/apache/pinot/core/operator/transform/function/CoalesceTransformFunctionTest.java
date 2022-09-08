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


public class CoalesceTransformFunctionTest extends BaseTransformFunctionTest {

  private static final String ENABLE_NULL_SEGMENT_NAME = "testSegment1";
  private static final String DISABLE_NULL_SEGMENT_NAME = "testSegment2";
  private static final Random RANDOM = new Random();

  private static final int NUM_ROWS = 1000;
  private static final String INT_SV_COLUMN = "intSV";
  private static final String STRING_SV_COLUMN = "StringSV";
  private final int[] _intSVValues = new int[NUM_ROWS];
  private final String[] _stringSVValues = new String[NUM_ROWS];
  private Map<String, DataSource> _enableNullDataSourceMap;
  private Map<String, DataSource> _disableNullDataSourceMap;
  private ProjectionBlock _enableNullProjectionBlock;
  private ProjectionBlock _disableNullProjectionBlock;

  protected static final int INT_NULL_MOD = 3;

  protected static final int STRING_NULL_MOD = 5;

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

  private static boolean isIntNull(int i) {
    return i % INT_NULL_MOD == 0;
  }

  private static boolean isStringNull(int i) {
    return i % STRING_NULL_MOD == 0;
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
      _stringSVValues[i] = "a" + RANDOM.nextInt();
    }
    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      Map<String, Object> map = new HashMap<>();
      map.put(INT_SV_COLUMN, _intSVValues[i]);
      map.put(STRING_SV_COLUMN, _stringSVValues[i]);
      if (isIntNull(i)) {
        map.put(INT_SV_COLUMN, null);
      }
      if (isStringNull(i)) {
        map.put(STRING_SV_COLUMN, null);
      }
      GenericRow row = new GenericRow();
      row.init(map);
      rows.add(row);
    }

    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension(INT_SV_COLUMN, FieldSpec.DataType.INT)
        .addSingleValueDimension(STRING_SV_COLUMN, FieldSpec.DataType.STRING).build();
    _enableNullDataSourceMap = getDataSourceMap(schema, rows, ENABLE_NULL_SEGMENT_NAME);
    _enableNullProjectionBlock = getProjectionBlock(_enableNullDataSourceMap);
    _disableNullDataSourceMap = getDataSourceMap(schema, rows, DISABLE_NULL_SEGMENT_NAME);
    _disableNullProjectionBlock = getProjectionBlock(_disableNullDataSourceMap);
  }

  protected void testTransformFunction(ExpressionContext expression, String[] expectedValues,
      ProjectionBlock projectionBlock, Map<String, DataSource> dataSourceMap)
      throws Exception {
    String[] stringValues =
        TransformFunctionFactory.get(expression, dataSourceMap).transformToStringValuesSV(projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      Assert.assertEquals(stringValues[i], expectedValues[i]);
    }
  }

  // Test the Coalesce on String and Int columns where one or the other or both can be null.
  @Test
  public void testDistinctFromBothNull()
      throws Exception {
    ExpressionContext coalesceExpr =
        RequestContextUtils.getExpression(String.format("COALESCE(%s,%s)", INT_SV_COLUMN, STRING_SV_COLUMN));
    TransformFunction coalesceTransformFunction = TransformFunctionFactory.get(coalesceExpr, _enableNullDataSourceMap);
    Assert.assertEquals(coalesceTransformFunction.getName(), "coalesce");
    String[] expectedResults = new String[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isStringNull(i) && isIntNull(i)) {
        expectedResults[i] = "null";
      } else if (isStringNull(i)) {
        expectedResults[i] = Integer.toString(_intSVValues[i]);
      } else if (isIntNull(i)) {
        expectedResults[i] = _stringSVValues[i];
      } else {
        expectedResults[i] = Integer.toString(_intSVValues[i]);
      }
    }
    testTransformFunction(coalesceExpr, expectedResults, _enableNullProjectionBlock, _enableNullDataSourceMap);
    testTransformFunction(coalesceExpr, expectedResults, _disableNullProjectionBlock, _disableNullDataSourceMap);
  }

  // Test that non-column-names appear in one of the argument.
  @Test
  public void testIllegalColumnName()
      throws Exception {
    ExpressionContext coalesceExpr =
        RequestContextUtils.getExpression(String.format("COALESCE(%s,%s)", _stringSVValues[0], STRING_SV_COLUMN));
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
}
