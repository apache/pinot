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

import com.yscope.clp.compressorfrontend.BuiltInVariableHandlingRuleVersions;
import com.yscope.clp.compressorfrontend.EncodedMessage;
import com.yscope.clp.compressorfrontend.MessageEncoder;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.commons.io.FileUtils;
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
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.data.FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.fail;


public class ClpTransformFunctionsTest {
  private static final String SEGMENT_NAME = "testSegmentForClpDecode";
  private static final String INDEX_DIR_PATH = FileUtils.getTempDirectoryPath() + File.separator + SEGMENT_NAME;
  private static final String TIMESTAMP_COLUMN = "timestampColumn";
  private static final String LOGTYPE_COLUMN = "field_logtype";
  private static final String DICT_VARS_COLUMN = "field_dictionaryVars";
  private static final String ENCODED_VARS_COLUMN = "field_encodedVars";
  private static final String TEST_MESSAGE = "Started job_123 on node-987: 4 cores, 8 threads and 51.4% memory used.";
  private static final int NUM_ROWS = 1000;

  private final long[] _timestampValues = new long[NUM_ROWS];

  private final String[] _logtypeValues = new String[NUM_ROWS];
  private final String[][] _dictVarValues = new String[NUM_ROWS][];

  private final Long[][] _encodedVarValues = new Long[NUM_ROWS][];

  SegmentGeneratorConfig _segmentGenConfig;
  protected Map<String, DataSource> _dataSourceMap;
  protected ProjectionBlock _projectionBlock;

  @BeforeClass
  public void setup()
      throws Exception {
    // Setup the schema and table config
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(LOGTYPE_COLUMN, FieldSpec.DataType.STRING)
        .addMultiValueDimension(DICT_VARS_COLUMN, FieldSpec.DataType.STRING)
        .addMultiValueDimension(ENCODED_VARS_COLUMN, FieldSpec.DataType.LONG)
        .addDateTime(TIMESTAMP_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTableForClpDecode")
        .setTimeColumnName(TIMESTAMP_COLUMN)
        .build();
    _segmentGenConfig = new SegmentGeneratorConfig(tableConfig, schema);
    _segmentGenConfig.setOutDir(INDEX_DIR_PATH);
    _segmentGenConfig.setSegmentName(SEGMENT_NAME);

    // Create the rows
    long currentTimeMs = System.currentTimeMillis();
    Random randomNumGen = new Random();
    MessageEncoder clpMessageEncoder = new MessageEncoder(BuiltInVariableHandlingRuleVersions.VariablesSchemaV2,
        BuiltInVariableHandlingRuleVersions.VariableEncodingMethodsV1);
    EncodedMessage clpEncodedMessage = new EncodedMessage();
    clpMessageEncoder.encodeMessage(TEST_MESSAGE, clpEncodedMessage);
    for (int i = 0; i < NUM_ROWS; i++) {
      _timestampValues[i] = currentTimeMs + randomNumGen.nextInt(365 * 24 * 3600) * 1000L;
      _logtypeValues[i] = clpEncodedMessage.getLogTypeAsString();
      _dictVarValues[i] = clpEncodedMessage.getDictionaryVarsAsStrings();
      _encodedVarValues[i] = clpEncodedMessage.getEncodedVarsAsBoxedLongs();
    }
    // Replace the last row with a null row
    clpMessageEncoder.encodeMessage(DEFAULT_DIMENSION_NULL_VALUE_OF_STRING, clpEncodedMessage);
    _logtypeValues[NUM_ROWS - 1] = clpEncodedMessage.getLogTypeAsString();
    _dictVarValues[NUM_ROWS - 1] = clpEncodedMessage.getDictionaryVarsAsStrings();
    _encodedVarValues[NUM_ROWS - 1] = clpEncodedMessage.getEncodedVarsAsBoxedLongs();
    // Corrupt the previous two rows, so we can test the default value
    _dictVarValues[NUM_ROWS - 2] = null;
    _encodedVarValues[NUM_ROWS - 3] = null;

    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      GenericRow row = new GenericRow();
      row.putValue(TIMESTAMP_COLUMN, _timestampValues[i]);
      row.putValue(LOGTYPE_COLUMN, _logtypeValues[i]);
      row.putValue(DICT_VARS_COLUMN, _dictVarValues[i]);
      row.putValue(ENCODED_VARS_COLUMN, _encodedVarValues[i]);
      rows.add(row);
    }

    // Build a segment
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try {
      driver.init(_segmentGenConfig, new GenericRowRecordReader(rows));
      driver.build();

      IndexSegment indexSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR_PATH, SEGMENT_NAME), ReadMode.heap);
      Set<String> columnNames = indexSegment.getPhysicalColumnNames();
      _dataSourceMap = new HashMap<>(columnNames.size());
      for (String columnName : columnNames) {
        _dataSourceMap.put(columnName, indexSegment.getDataSource(columnName));
      }
    } catch (Exception ex) {
      fail("Failed to build and load segment", ex);
    }

    _projectionBlock = new ProjectionOperator(_dataSourceMap,
        new DocIdSetOperator(new MatchAllFilterOperator(NUM_ROWS), DocIdSetPlanNode.MAX_DOC_PER_CALL)).nextBlock();
  }

  @BeforeTest
  public void deleteOldIndex() {
    FileUtils.deleteQuietly(new File(INDEX_DIR_PATH));
  }

  @Test
  public void testTransform() {
    ExpressionContext expression = RequestContextUtils.getExpression(
        String.format("%s(%s,%s,%s)", TransformFunctionType.CLP_DECODE.getName(), LOGTYPE_COLUMN, DICT_VARS_COLUMN,
            ENCODED_VARS_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof CLPDecodeTransformFunction);

    String[] expectedValues = new String[NUM_ROWS];
    Arrays.fill(expectedValues, TEST_MESSAGE);
    expectedValues[NUM_ROWS - 3] = DEFAULT_DIMENSION_NULL_VALUE_OF_STRING;
    expectedValues[NUM_ROWS - 2] = DEFAULT_DIMENSION_NULL_VALUE_OF_STRING;
    expectedValues[NUM_ROWS - 1] = DEFAULT_DIMENSION_NULL_VALUE_OF_STRING;
    testTransformFunction(transformFunction, expectedValues);
  }

  @Test
  public void testTransformWithDefaultValue() {
    String defaultValue = "default";
    ExpressionContext expression = RequestContextUtils.getExpression(
        String.format("%s(%s,%s,%s,'%s')", TransformFunctionType.CLP_DECODE.getName(), LOGTYPE_COLUMN, DICT_VARS_COLUMN,
            ENCODED_VARS_COLUMN, defaultValue));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof CLPDecodeTransformFunction);

    String[] expectedValues = new String[NUM_ROWS];
    Arrays.fill(expectedValues, TEST_MESSAGE);
    expectedValues[NUM_ROWS - 3] = defaultValue;
    expectedValues[NUM_ROWS - 2] = defaultValue;
    expectedValues[NUM_ROWS - 1] = DEFAULT_DIMENSION_NULL_VALUE_OF_STRING;
    testTransformFunction(transformFunction, expectedValues);
  }

  @Test
  public void testInvalidArgs() {
    String defaultValue = "default";

    // 1st parameter literal
    assertThrows(BadQueryRequestException.class, () -> {
      ExpressionContext expression = RequestContextUtils.getExpression(
          String.format("%s('%s',%s,%s,'%s')", TransformFunctionType.CLP_DECODE.getName(), LOGTYPE_COLUMN,
              DICT_VARS_COLUMN, ENCODED_VARS_COLUMN, defaultValue));
      TransformFunctionFactory.get(expression, _dataSourceMap);
    });

    // 2nd parameter literal
    assertThrows(BadQueryRequestException.class, () -> {
      ExpressionContext expression = RequestContextUtils.getExpression(
          String.format("%s(%s,'%s',%s,'%s')", TransformFunctionType.CLP_DECODE.getName(), LOGTYPE_COLUMN,
              DICT_VARS_COLUMN, ENCODED_VARS_COLUMN, defaultValue));
      TransformFunctionFactory.get(expression, _dataSourceMap);
    });

    // 3rd parameter literal
    assertThrows(BadQueryRequestException.class, () -> {
      ExpressionContext expression = RequestContextUtils.getExpression(
          String.format("%s(%s,%s,'%s','%s')", TransformFunctionType.CLP_DECODE.getName(), LOGTYPE_COLUMN,
              DICT_VARS_COLUMN, ENCODED_VARS_COLUMN, defaultValue));
      TransformFunctionFactory.get(expression, _dataSourceMap);
    });

    // 4th parameter identifier
    assertThrows(BadQueryRequestException.class, () -> {
      ExpressionContext expression = RequestContextUtils.getExpression(
          String.format("%s(%s,%s,%s,%s)", TransformFunctionType.CLP_DECODE.getName(), LOGTYPE_COLUMN,
              DICT_VARS_COLUMN, ENCODED_VARS_COLUMN, defaultValue));
      TransformFunctionFactory.get(expression, _dataSourceMap);
    });

    // Missing arg
    assertThrows(BadQueryRequestException.class, () -> {
      ExpressionContext expression = RequestContextUtils.getExpression(
          String.format("%s(%s,%s)", TransformFunctionType.CLP_DECODE.getName(), LOGTYPE_COLUMN, DICT_VARS_COLUMN));
      TransformFunctionFactory.get(expression, _dataSourceMap);
    });
  }

  private void testTransformFunction(TransformFunction transformFunction, String[] expectedValues) {
    String[] values = transformFunction.transformToStringValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      assertEquals(values[i], expectedValues[i]);
    }
  }
}
