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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.core.data.manager.offline.DimensionTableDataManager;
import org.apache.pinot.core.query.exception.BadQueryRequestException;
import org.apache.pinot.core.query.request.context.ExpressionContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.apache.pinot.spi.utils.ByteArray;
import org.testng.Assert;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class LookupTransformFunctionTest extends BaseTransformFunctionTest {
  private static final String TABLE_NAME = "baseballTeams_OFFLINE";
  private DimensionTableDataManager tableManager;

  @BeforeSuite
  public void setUp()
      throws Exception {
    super.setUp();

    createTestableTableManager();
  }

  private void createTestableTableManager() {
    tableManager = mock(DimensionTableDataManager.class);
    DimensionTableDataManager.registerDimensionTable(TABLE_NAME, tableManager);

    // Creating a mock table which looks like:
    // TeamID (PK, str) | TeamName(str) | TeamName_MV(str[]) | TeamInteger(int) | TeamInteger_MV(int[]) | TeamFloat(float) | ...
    //
    // All values are dynamically created to be variations of the primary key.
    // e.g
    // lookupRowByPrimaryKey(['FOO']) -> (TeamID: 'foo', TeamName: 'teamName_for_foo', TeamInteger: hashCode(['foo']), ...
    //
    when(tableManager.getPrimaryKeyColumns()).thenReturn(Arrays.asList("teamID"));
    when(tableManager.getColumnFieldSpec("teamID"))
        .thenReturn(new DimensionFieldSpec("teamID", FieldSpec.DataType.STRING, true));
    when(tableManager.getColumnFieldSpec("teamName"))
        .thenReturn(new DimensionFieldSpec("teamName", FieldSpec.DataType.STRING, true));
    when(tableManager.getColumnFieldSpec("teamName_MV"))
        .thenReturn(new DimensionFieldSpec("teamName_MV", FieldSpec.DataType.STRING, false));
    when(tableManager.getColumnFieldSpec("teamInteger"))
        .thenReturn(new DimensionFieldSpec("teamInteger", FieldSpec.DataType.INT, true));
    when(tableManager.getColumnFieldSpec("teamInteger_MV"))
        .thenReturn(new DimensionFieldSpec("teamInteger_MV", FieldSpec.DataType.INT, false));
    when(tableManager.getColumnFieldSpec("teamFloat"))
        .thenReturn(new DimensionFieldSpec("teamFloat", FieldSpec.DataType.FLOAT, true));
    when(tableManager.getColumnFieldSpec("teamFloat_MV"))
        .thenReturn(new DimensionFieldSpec("teamFloat_MV", FieldSpec.DataType.FLOAT, false));
    when(tableManager.getColumnFieldSpec("teamDouble"))
        .thenReturn(new DimensionFieldSpec("teamDouble", FieldSpec.DataType.DOUBLE, true));
    when(tableManager.getColumnFieldSpec("teamDouble_MV"))
        .thenReturn(new DimensionFieldSpec("teamDouble_MV", FieldSpec.DataType.DOUBLE, false));
    when(tableManager.getColumnFieldSpec("teamLong"))
        .thenReturn(new DimensionFieldSpec("teamLong", FieldSpec.DataType.LONG, true));
    when(tableManager.getColumnFieldSpec("teamLong_MV"))
        .thenReturn(new DimensionFieldSpec("teamLong_MV", FieldSpec.DataType.LONG, false));
    when(tableManager.getColumnFieldSpec("teamBytes"))
        .thenReturn(new DimensionFieldSpec("teamNameBytes", FieldSpec.DataType.BYTES, true));
    when(tableManager.lookupRowByPrimaryKey(any(PrimaryKey.class))).thenAnswer(invocation -> {
      PrimaryKey key = invocation.getArgument(0);
      GenericRow row = new GenericRow();
      row.putValue("teamName", "teamName_for_" + key.toString());
      row.putValue("teamName_MV",
          new String[]{"teamName_for_" + key.toString() + "_1", "teamName_for_" + key.toString() + "_2",});
      row.putValue("teamInteger", key.hashCode());
      row.putValue("teamInteger_MV", new int[]{key.hashCode(), key.hashCode()});
      row.putValue("teamFloat", (float) key.hashCode());
      row.putValue("teamFloat_MV", new float[]{(float) key.hashCode(), (float) key.hashCode()});
      row.putValue("teamDouble", (double) key.hashCode());
      row.putValue("teamDouble_MV", new double[]{(double) key.hashCode(), (double) key.hashCode()});
      row.putValue("teamLong", (long) key.hashCode());
      row.putValue("teamLong_MV", new long[]{(long) key.hashCode(), (long) key.hashCode()});
      row.putValue("teamBytes", ("teamBytes_for_" + key.toString()).getBytes());
      return row;
    });
  }

  @Test
  public void instantiationTests()
      throws Exception {
    // Success case
    ExpressionContext expression = QueryContextConverterUtils
        .getExpression(String.format("lookup('baseballTeams','teamName','teamID',%s)", STRING_SV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof LookupTransformFunction);
    Assert.assertEquals(transformFunction.getName(), LookupTransformFunction.FUNCTION_NAME);

    // Wrong number of arguments
    Assert.assertThrows(BadQueryRequestException.class, () -> {
      TransformFunctionFactory
          .get(QueryContextConverterUtils.getExpression(String.format("lookup('baseballTeams','teamName','teamID')")),
              _dataSourceMap);
    });

    // Wrong number of join keys
    Assert.assertThrows(BadQueryRequestException.class, () -> {
      TransformFunctionFactory.get(QueryContextConverterUtils.getExpression(
          String.format("lookup('baseballTeams','teamName','teamID', %s, 'danglingKey')", STRING_SV_COLUMN)),
          _dataSourceMap);
    });

    // Non literal tableName argument
    Assert.assertThrows(BadQueryRequestException.class, () -> {
      TransformFunctionFactory.get(QueryContextConverterUtils
              .getExpression(String.format("lookup(%s,'teamName','teamID', %s)", STRING_SV_COLUMN, INT_SV_COLUMN)),
          _dataSourceMap);
    });

    // Non literal lookup columnName argument
    Assert.assertThrows(BadQueryRequestException.class, () -> {
      TransformFunctionFactory.get(QueryContextConverterUtils
              .getExpression(String.format("lookup('baseballTeams',%s,'teamID',%s)", STRING_SV_COLUMN, INT_SV_COLUMN)),
          _dataSourceMap);
    });

    // Non literal lookup columnName argument
    Assert.assertThrows(BadQueryRequestException.class, () -> {
      TransformFunctionFactory.get(QueryContextConverterUtils
              .getExpression(String.format("lookup('baseballTeams','teamName',%s,%s)", STRING_SV_COLUMN, INT_SV_COLUMN)),
          _dataSourceMap);
    });
  }

  @Test
  public void resultDataTypeTest()
      throws Exception {
    HashMap<String, FieldSpec.DataType> testCases = new HashMap<String, FieldSpec.DataType>() {{
      put("teamName", FieldSpec.DataType.STRING);
      put("teamInteger", FieldSpec.DataType.INT);
      put("teamFloat", FieldSpec.DataType.FLOAT);
      put("teamLong", FieldSpec.DataType.LONG);
      put("teamDouble", FieldSpec.DataType.DOUBLE);
    }};

    for (Map.Entry<String, FieldSpec.DataType> testCase : testCases.entrySet()) {
      ExpressionContext expression = QueryContextConverterUtils.getExpression(
          String.format("lookup('baseballTeams','%s','teamID',%s)", testCase.getKey(), STRING_SV_COLUMN));
      TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
      Assert.assertEquals(transformFunction.getResultMetadata().getDataType(), testCase.getValue(),
          String.format("Expecting %s data type for lookup column: '%s'", testCase.getKey(), testCase.getValue()));
    }
  }

  @Test
  public void basicLookupTests()
      throws Exception {
    // Lookup col: StringSV
    // PK: [String]
    ExpressionContext expression = QueryContextConverterUtils
        .getExpression(String.format("lookup('baseballTeams','teamName','teamID',%s)", STRING_SV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    String[] expectedStringValues = new String[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedStringValues[i] = String.format("teamName_for_[%s]", _stringSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedStringValues);

    // Lookup col: IntSV
    // PK: [String]
    expression = QueryContextConverterUtils
        .getExpression(String.format("lookup('baseballTeams','teamInteger','teamID',%s)", STRING_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    int[] expectedIntValues = new int[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedIntValues[i] = (new PrimaryKey(new Object[]{_stringSVValues[i]})).hashCode();
    }
    testTransformFunction(transformFunction, expectedIntValues);

    // Lookup col: DoubleSV
    // PK: [String]
    expression = QueryContextConverterUtils
        .getExpression(String.format("lookup('baseballTeams','teamDouble','teamID',%s)", STRING_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    double[] expectedDoubleValues = new double[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedDoubleValues[i] = (double) (new PrimaryKey(new Object[]{_stringSVValues[i]})).hashCode();
    }
    testTransformFunction(transformFunction, expectedDoubleValues);

    // Lookup col: BytesSV
    // PK: [String]
    expression = QueryContextConverterUtils
        .getExpression(String.format("lookup('baseballTeams','teamBytes','teamID',%s)", STRING_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    byte[][] expectedBytesValues = new byte[NUM_ROWS][];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedBytesValues[i] = String.format("teamBytes_for_[%s]", _stringSVValues[i]).getBytes();
    }
    testTransformFunction(transformFunction, expectedBytesValues);
  }

  @Test
  public void multiValueLookupTests()
      throws Exception {
    // Lookup col: StringMV
    // PK: [String]
    ExpressionContext expression = QueryContextConverterUtils
        .getExpression(String.format("lookup('baseballTeams','teamName_MV','teamID',%s)", STRING_SV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    String[][] expectedStringMVValues = new String[NUM_ROWS][];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedStringMVValues[i] =
          new String[]{String.format("teamName_for_[%s]_1", _stringSVValues[i]), String.format("teamName_for_[%s]_2",
              _stringSVValues[i]),};
    }
    testTransformFunctionMV(transformFunction, expectedStringMVValues);

    // Lookup col: IntegerMV
    // PK: [String]
    expression = QueryContextConverterUtils
        .getExpression(String.format("lookup('baseballTeams','teamInteger_MV','teamID',%s)", STRING_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    int[][] expectedIntegerMVValues = new int[NUM_ROWS][0];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedIntegerMVValues[i] =
          new int[]{(new PrimaryKey(new Object[]{_stringSVValues[i]})).hashCode(), (new PrimaryKey(
              new Object[]{_stringSVValues[i]})).hashCode(),};
    }
    testTransformFunctionMV(transformFunction, expectedIntegerMVValues);

    // Lookup col: FloatMV
    // PK: [String]
    expression = QueryContextConverterUtils
        .getExpression(String.format("lookup('baseballTeams','teamFloat_MV','teamID',%s)", STRING_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    float[][] expectedFloatMVValues = new float[NUM_ROWS][0];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedFloatMVValues[i] =
          new float[]{(float) (new PrimaryKey(new Object[]{_stringSVValues[i]})).hashCode(), (float) (new PrimaryKey(
              new Object[]{_stringSVValues[i]})).hashCode(),};
    }
    testTransformFunctionMV(transformFunction, expectedFloatMVValues);

    // Lookup col: LongMV
    // PK: [String]
    expression = QueryContextConverterUtils
        .getExpression(String.format("lookup('baseballTeams','teamLong_MV','teamID',%s)", STRING_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    long[][] expectedLongMVValues = new long[NUM_ROWS][0];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedLongMVValues[i] =
          new long[]{(long) (new PrimaryKey(new Object[]{_stringSVValues[i]})).hashCode(), (long) (new PrimaryKey(
              new Object[]{_stringSVValues[i]})).hashCode(),};
    }
    testTransformFunctionMV(transformFunction, expectedLongMVValues);

    // Lookup col: DoubleMV
    // PK: [String]
    expression = QueryContextConverterUtils
        .getExpression(String.format("lookup('baseballTeams','teamDouble_MV','teamID',%s)", STRING_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    double[][] expectedDoubleMVValues = new double[NUM_ROWS][0];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedDoubleMVValues[i] =
          new double[]{(double) (new PrimaryKey(new Object[]{_stringSVValues[i]})).hashCode(), (double) (new PrimaryKey(
              new Object[]{_stringSVValues[i]})).hashCode(),};
    }
    testTransformFunctionMV(transformFunction, expectedDoubleMVValues);
  }

  @Test
  public void primaryKeyTypeTest()
      throws Exception {
    // preparing simple tables for testing different primary key types (INT, STRING, LONG)
    Map<String, FieldSpec.DataType> testTables = new HashMap<String, FieldSpec.DataType>() {{
      put("dimTableWithIntPK_OFFLINE", FieldSpec.DataType.INT);
      put("dimTableWithStringPK_OFFLINE", FieldSpec.DataType.STRING);
      put("dimTableWithLongPK_OFFLINE", FieldSpec.DataType.LONG);
      put("dimTableWithFloatPK_OFFLINE", FieldSpec.DataType.FLOAT);
      put("dimTableWithDoublePK_OFFLINE", FieldSpec.DataType.DOUBLE);
      put("dimTableWithBytesPK_OFFLINE", FieldSpec.DataType.BYTES);
    }};
    for (Map.Entry<String, FieldSpec.DataType> table : testTables.entrySet()) {
      DimensionTableDataManager mgr = mock(DimensionTableDataManager.class);
      DimensionTableDataManager.registerDimensionTable(table.getKey(), mgr);
      when(mgr.getPrimaryKeyColumns()).thenReturn(Arrays.asList("primaryColumn"));
      when(mgr.getColumnFieldSpec("primaryColumn"))
          .thenReturn(new DimensionFieldSpec("primaryColumn", table.getValue(), true));
      when(mgr.getColumnFieldSpec("lookupColumn"))
          .thenReturn(new DimensionFieldSpec("lookupColumn", FieldSpec.DataType.STRING, true));
      when(mgr.lookupRowByPrimaryKey(any(PrimaryKey.class))).thenAnswer(invocation -> {
        PrimaryKey key = invocation.getArgument(0);
        GenericRow row = new GenericRow();
        row.putValue("lookupColumn", String.format("lookup_value_for_[%s]", key.hashCode()));
        return row;
      });
    }

    // PK: [Int]
    ExpressionContext expression = QueryContextConverterUtils.getExpression(
        String.format("lookup('dimTableWithIntPK', 'lookupColumn', 'primaryColumn', %s)", INT_SV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    String[] expectedResults = new String[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      PrimaryKey key = new PrimaryKey(new Object[]{(Integer)_intSVValues[i]});
      expectedResults[i] = String.format("lookup_value_for_[%s]", key.hashCode());
    }
    testTransformFunction(transformFunction, expectedResults);

    // PK: [String]
    expression = QueryContextConverterUtils.getExpression(
        String.format("lookup('dimTableWithStringPK', 'lookupColumn', 'primaryColumn', %s)", STRING_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    expectedResults = new String[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      PrimaryKey key = new PrimaryKey(new Object[]{_stringSVValues[i]});
      expectedResults[i] = String.format("lookup_value_for_[%s]", key.hashCode());
    }
    testTransformFunction(transformFunction, expectedResults);

    // PK: [Long]
    expression = QueryContextConverterUtils.getExpression(
        String.format("lookup('dimTableWithLongPK', 'lookupColumn', 'primaryColumn', %s)", LONG_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    expectedResults = new String[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      PrimaryKey key = new PrimaryKey(new Object[]{(Long)_longSVValues[i]});
      expectedResults[i] = String.format("lookup_value_for_[%s]", key.hashCode());
    }
    testTransformFunction(transformFunction, expectedResults);

    // PK: [Float]
    expression = QueryContextConverterUtils.getExpression(
        String.format("lookup('dimTableWithFloatPK', 'lookupColumn', 'primaryColumn', %s)", FLOAT_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    expectedResults = new String[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      PrimaryKey key = new PrimaryKey(new Object[]{(Float)_floatSVValues[i]});
      expectedResults[i] = String.format("lookup_value_for_[%s]", key.hashCode());
    }
    testTransformFunction(transformFunction, expectedResults);

    // PK: [Double]
    expression = QueryContextConverterUtils.getExpression(
        String.format("lookup('dimTableWithDoublePK', 'lookupColumn', 'primaryColumn', %s)", DOUBLE_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    expectedResults = new String[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      PrimaryKey key = new PrimaryKey(new Object[]{(Double)_doubleSVValues[i]});
      expectedResults[i] = String.format("lookup_value_for_[%s]", key.hashCode());
    }
    testTransformFunction(transformFunction, expectedResults);

    // PK: [Byte[]]
    expression = QueryContextConverterUtils.getExpression(
        String.format("lookup('dimTableWithBytesPK', 'lookupColumn', 'primaryColumn', %s)", BYTES_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    expectedResults = new String[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      PrimaryKey key = new PrimaryKey(new Object[]{new ByteArray(_bytesSVValues[i])});
      expectedResults[i] = String.format("lookup_value_for_[%s]", key.hashCode());
    }
    testTransformFunction(transformFunction, expectedResults);
  }
}
