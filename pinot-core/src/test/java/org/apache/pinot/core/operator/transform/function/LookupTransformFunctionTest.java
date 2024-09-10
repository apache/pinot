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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.core.data.manager.offline.DimensionTableDataManager;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.spi.utils.ByteArray;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.fail;


public class LookupTransformFunctionTest extends BaseTransformFunctionTest {
  private static final String TABLE_NAME = "baseballTeams_OFFLINE";

  @BeforeClass
  @Override
  public void setUp()
      throws Exception {
    super.setUp();
    DimensionTableDataManager.registerDimensionTable(TABLE_NAME, mockDataManager());
  }

  /**
   * Mocks a {@link DimensionTableDataManager} which looks like:
   * TeamID (PK, str) | TeamName(str) | TeamName_MV(str[]) | TeamInteger(int) | TeamInteger_MV(int[]) | TeamFloat
   * (float) | ...
   *
   * All values are dynamically created to be variations of the primary key.
   * e.g.
   * lookupRowByPrimaryKey(['FOO']) -> (TeamID: 'foo', TeamName: 'teamName_for_foo', TeamInteger: hashCode(['foo']), ...
   */
  private DimensionTableDataManager mockDataManager() {
    DimensionTableDataManager tableManager = mock(DimensionTableDataManager.class);

    when(tableManager.getPrimaryKeyColumns()).thenReturn(List.of("teamID"));
    when(tableManager.isPopulated()).thenReturn(true);
    when(tableManager.getColumnFieldSpec("teamID")).thenReturn(new DimensionFieldSpec("teamID", DataType.STRING, true));
    when(tableManager.getColumnFieldSpec("teamName")).thenReturn(
        new DimensionFieldSpec("teamName", DataType.STRING, true));
    when(tableManager.getColumnFieldSpec("teamName_MV")).thenReturn(
        new DimensionFieldSpec("teamName_MV", DataType.STRING, false));
    when(tableManager.getColumnFieldSpec("teamInteger")).thenReturn(
        new DimensionFieldSpec("teamInteger", DataType.INT, true));
    when(tableManager.getColumnFieldSpec("teamInteger_MV")).thenReturn(
        new DimensionFieldSpec("teamInteger_MV", DataType.INT, false));
    when(tableManager.getColumnFieldSpec("teamFloat")).thenReturn(
        new DimensionFieldSpec("teamFloat", DataType.FLOAT, true));
    when(tableManager.getColumnFieldSpec("teamFloat_MV")).thenReturn(
        new DimensionFieldSpec("teamFloat_MV", DataType.FLOAT, false));
    when(tableManager.getColumnFieldSpec("teamDouble")).thenReturn(
        new DimensionFieldSpec("teamDouble", DataType.DOUBLE, true));
    when(tableManager.getColumnFieldSpec("teamDouble_MV")).thenReturn(
        new DimensionFieldSpec("teamDouble_MV", DataType.DOUBLE, false));
    when(tableManager.getColumnFieldSpec("teamLong")).thenReturn(
        new DimensionFieldSpec("teamLong", DataType.LONG, true));
    when(tableManager.getColumnFieldSpec("teamLong_MV")).thenReturn(
        new DimensionFieldSpec("teamLong_MV", DataType.LONG, false));
    when(tableManager.getColumnFieldSpec("teamBytes")).thenReturn(
        new DimensionFieldSpec("teamNameBytes", DataType.BYTES, true));
    when(tableManager.lookupValue(any(PrimaryKey.class), any(String.class))).thenAnswer(invocation -> {
      PrimaryKey key = invocation.getArgument(0);
      String column = invocation.getArgument(1);
      switch (column) {
        case "teamName":
          return String.format("teamName_for_%s", key);
        case "teamName_MV":
          return new String[]{String.format("teamName_for_%s_1", key), String.format("teamName_for_%s_2", key)};
        case "teamInteger":
          return key.hashCode();
        case "teamInteger_MV":
          return new int[]{key.hashCode(), key.hashCode()};
        case "teamFloat":
          return (float) key.hashCode();
        case "teamFloat_MV":
          return new float[]{(float) key.hashCode(), (float) key.hashCode()};
        case "teamDouble":
          return (double) key.hashCode();
        case "teamDouble_MV":
          return new double[]{(double) key.hashCode(), (double) key.hashCode()};
        case "teamLong":
          return (long) key.hashCode();
        case "teamLong_MV":
          return new long[]{(long) key.hashCode(), (long) key.hashCode()};
        case "teamBytes":
          return String.format("teamBytes_for_%s", key).getBytes();
        default:
          throw new IllegalArgumentException("Unknown column: " + column);
      }
    });

    return tableManager;
  }

  @Test
  public void instantiationTests() {
    // Success case
    ExpressionContext expression = RequestContextUtils.getExpression(
        String.format("lookup('baseballTeams','teamName','teamID',%s)", STRING_SV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof LookupTransformFunction);
    assertEquals(transformFunction.getName(), LookupTransformFunction.FUNCTION_NAME);

    // Wrong number of arguments
    assertThrows(BadQueryRequestException.class, () -> {
      TransformFunctionFactory.get(RequestContextUtils.getExpression("lookup('baseballTeams','teamName','teamID')"),
          _dataSourceMap);
    });

    // Wrong number of join keys
    assertThrows(BadQueryRequestException.class, () -> {
      TransformFunctionFactory.get(RequestContextUtils.getExpression(
              String.format("lookup('baseballTeams','teamName','teamID', %s, 'danglingKey')", STRING_SV_COLUMN)),
          _dataSourceMap);
    });

    // Non literal tableName argument
    assertThrows(BadQueryRequestException.class, () -> {
      TransformFunctionFactory.get(RequestContextUtils.getExpression(
          String.format("lookup(%s,'teamName','teamID', %s)", STRING_SV_COLUMN, INT_SV_COLUMN)), _dataSourceMap);
    });

    // Non literal lookup columnName argument
    assertThrows(BadQueryRequestException.class, () -> {
      TransformFunctionFactory.get(RequestContextUtils.getExpression(
          String.format("lookup('baseballTeams',%s,'teamID',%s)", STRING_SV_COLUMN, INT_SV_COLUMN)), _dataSourceMap);
    });

    // Non literal lookup columnName argument
    assertThrows(BadQueryRequestException.class, () -> {
      TransformFunctionFactory.get(RequestContextUtils.getExpression(
          String.format("lookup('baseballTeams','teamName',%s,%s)", STRING_SV_COLUMN, INT_SV_COLUMN)), _dataSourceMap);
    });
  }

  @Test
  public void resultDataTypeTest() {
    HashMap<String, DataType> testCases = new HashMap<>() {{
      put("teamName", DataType.STRING);
      put("teamInteger", DataType.INT);
      put("teamFloat", DataType.FLOAT);
      put("teamLong", DataType.LONG);
      put("teamDouble", DataType.DOUBLE);
    }};

    for (Map.Entry<String, DataType> testCase : testCases.entrySet()) {
      ExpressionContext expression = RequestContextUtils.getExpression(
          String.format("lookup('baseballTeams','%s','teamID',%s)", testCase.getKey(), STRING_SV_COLUMN));
      TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
      assertEquals(transformFunction.getResultMetadata().getDataType(), testCase.getValue(),
          String.format("Expecting %s data type for lookup column: '%s'", testCase.getKey(), testCase.getValue()));
    }
  }

  @Test
  public void dimensionTableNotPopulated() {
    DimensionTableDataManager tableManager = mock(DimensionTableDataManager.class);
    when(tableManager.isPopulated()).thenReturn(false);
    when(tableManager.getPrimaryKeyColumns()).thenReturn(List.of("leagueID"));
    when(tableManager.getColumnFieldSpec("leagueID")).thenReturn(
        new DimensionFieldSpec("leagueID", DataType.STRING, true));
    when(tableManager.getColumnFieldSpec("leagueName")).thenReturn(
        new DimensionFieldSpec("leagueName", DataType.STRING, true));

    DimensionTableDataManager.registerDimensionTable("baseballLeagues_OFFLINE", tableManager);

    try {
      ExpressionContext expression = RequestContextUtils.getExpression(
          String.format("lookup('baseballLeagues','leagueName','leagueID',%s)", STRING_SV_COLUMN));
      TransformFunctionFactory.get(expression, _dataSourceMap);
      fail("Should have thrown BadQueryRequestException");
    } catch (Exception ex) {
      assertEquals(ex.getCause().getMessage(), "Dimension table is not populated: baseballLeagues_OFFLINE");
    }
  }

  @Test
  public void dimensionTableIsPopulated() {
    DimensionTableDataManager tableManager = mock(DimensionTableDataManager.class);
    when(tableManager.isPopulated()).thenReturn(true);
    when(tableManager.getPrimaryKeyColumns()).thenReturn(List.of("playerID"));
    when(tableManager.getColumnFieldSpec("playerID")).thenReturn(
        new DimensionFieldSpec("playerID", DataType.STRING, true));
    when(tableManager.getColumnFieldSpec("playerName")).thenReturn(
        new DimensionFieldSpec("playerName", DataType.STRING, true));

    DimensionTableDataManager.registerDimensionTable("baseballPlayers_OFFLINE", tableManager);

    ExpressionContext expression = RequestContextUtils.getExpression(
        String.format("lookup('baseballPlayers','playerName','playerID',%s)", STRING_SV_COLUMN));
    TransformFunctionFactory.get(expression, _dataSourceMap);
  }

  @Test
  public void basicLookupTests() {
    // Lookup col: StringSV
    // PK: [String]
    ExpressionContext expression = RequestContextUtils.getExpression(
        String.format("lookup('baseballTeams','teamName','teamID',%s)", STRING_SV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    String[] expectedStringValues = new String[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedStringValues[i] = String.format("teamName_for_[%s]", _stringSVValues[i]);
    }
    testTransformFunction(transformFunction, expectedStringValues);

    // Lookup col: IntSV
    // PK: [String]
    expression = RequestContextUtils.getExpression(
        String.format("lookup('baseballTeams','teamInteger','teamID',%s)", STRING_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    int[] expectedIntValues = new int[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedIntValues[i] = (new PrimaryKey(new Object[]{_stringSVValues[i]})).hashCode();
    }
    testTransformFunction(transformFunction, expectedIntValues);

    // Lookup col: DoubleSV
    // PK: [String]
    expression = RequestContextUtils.getExpression(
        String.format("lookup('baseballTeams','teamDouble','teamID',%s)", STRING_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    double[] expectedDoubleValues = new double[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedDoubleValues[i] = (new PrimaryKey(new Object[]{_stringSVValues[i]})).hashCode();
    }
    testTransformFunction(transformFunction, expectedDoubleValues);

    // Lookup col: BytesSV
    // PK: [String]
    expression = RequestContextUtils.getExpression(
        String.format("lookup('baseballTeams','teamBytes','teamID',%s)", STRING_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    byte[][] expectedBytesValues = new byte[NUM_ROWS][];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedBytesValues[i] = String.format("teamBytes_for_[%s]", _stringSVValues[i]).getBytes();
    }
    testTransformFunction(transformFunction, expectedBytesValues);
  }

  @Test
  public void multiValueLookupTests() {
    // Lookup col: StringMV
    // PK: [String]
    ExpressionContext expression = RequestContextUtils.getExpression(
        String.format("lookup('baseballTeams','teamName_MV','teamID',%s)", STRING_SV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    String[][] expectedStringMVValues = new String[NUM_ROWS][];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedStringMVValues[i] = new String[]{
          String.format("teamName_for_[%s]_1", _stringSVValues[i]), String.format("teamName_for_[%s]_2",
          _stringSVValues[i]),
      };
    }
    testTransformFunctionMV(transformFunction, expectedStringMVValues);

    // Lookup col: IntegerMV
    // PK: [String]
    expression = RequestContextUtils.getExpression(
        String.format("lookup('baseballTeams','teamInteger_MV','teamID',%s)", STRING_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    int[][] expectedIntegerMVValues = new int[NUM_ROWS][0];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedIntegerMVValues[i] = new int[]{
          (new PrimaryKey(new Object[]{_stringSVValues[i]})).hashCode(), (new PrimaryKey(
          new Object[]{_stringSVValues[i]})).hashCode(),
      };
    }
    testTransformFunctionMV(transformFunction, expectedIntegerMVValues);

    // Lookup col: FloatMV
    // PK: [String]
    expression = RequestContextUtils.getExpression(
        String.format("lookup('baseballTeams','teamFloat_MV','teamID',%s)", STRING_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    float[][] expectedFloatMVValues = new float[NUM_ROWS][0];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedFloatMVValues[i] = new float[]{
          (float) (new PrimaryKey(new Object[]{_stringSVValues[i]})).hashCode(), (float) (new PrimaryKey(
          new Object[]{_stringSVValues[i]})).hashCode(),
      };
    }
    testTransformFunctionMV(transformFunction, expectedFloatMVValues);

    // Lookup col: LongMV
    // PK: [String]
    expression = RequestContextUtils.getExpression(
        String.format("lookup('baseballTeams','teamLong_MV','teamID',%s)", STRING_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    long[][] expectedLongMVValues = new long[NUM_ROWS][0];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedLongMVValues[i] = new long[]{
          (long) (new PrimaryKey(new Object[]{_stringSVValues[i]})).hashCode(), (long) (new PrimaryKey(
          new Object[]{_stringSVValues[i]})).hashCode(),
      };
    }
    testTransformFunctionMV(transformFunction, expectedLongMVValues);

    // Lookup col: DoubleMV
    // PK: [String]
    expression = RequestContextUtils.getExpression(
        String.format("lookup('baseballTeams','teamDouble_MV','teamID',%s)", STRING_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    double[][] expectedDoubleMVValues = new double[NUM_ROWS][0];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedDoubleMVValues[i] = new double[]{
          (double) (new PrimaryKey(new Object[]{_stringSVValues[i]})).hashCode(), (double) (new PrimaryKey(
          new Object[]{_stringSVValues[i]})).hashCode(),
      };
    }
    testTransformFunctionMV(transformFunction, expectedDoubleMVValues);
  }

  @Test
  public void primaryKeyTypeTest() {
    // preparing simple tables for testing different primary key types (INT, STRING, LONG)
    Map<String, DataType> testTables = new HashMap<>() {{
      put("dimTableWithIntPK_OFFLINE", DataType.INT);
      put("dimTableWithStringPK_OFFLINE", DataType.STRING);
      put("dimTableWithLongPK_OFFLINE", DataType.LONG);
      put("dimTableWithFloatPK_OFFLINE", DataType.FLOAT);
      put("dimTableWithDoublePK_OFFLINE", DataType.DOUBLE);
      put("dimTableWithBytesPK_OFFLINE", DataType.BYTES);
    }};
    for (Map.Entry<String, DataType> table : testTables.entrySet()) {
      DimensionTableDataManager mgr = mock(DimensionTableDataManager.class);
      DimensionTableDataManager.registerDimensionTable(table.getKey(), mgr);
      when(mgr.isPopulated()).thenReturn(true);
      when(mgr.getPrimaryKeyColumns()).thenReturn(List.of("primaryColumn"));
      when(mgr.getColumnFieldSpec("primaryColumn")).thenReturn(
          new DimensionFieldSpec("primaryColumn", table.getValue(), true));
      when(mgr.getColumnFieldSpec("lookupColumn")).thenReturn(
          new DimensionFieldSpec("lookupColumn", DataType.STRING, true));
      when(mgr.lookupValue(any(PrimaryKey.class), eq("lookupColumn"))).thenAnswer(
          invocation -> String.format("lookup_value_for_[%s]", invocation.getArgument(0).hashCode()));
    }

    // PK: [Int]
    ExpressionContext expression = RequestContextUtils.getExpression(
        String.format("lookup('dimTableWithIntPK', 'lookupColumn', 'primaryColumn', %s)", INT_SV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    String[] expectedResults = new String[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      PrimaryKey key = new PrimaryKey(new Object[]{_intSVValues[i]});
      expectedResults[i] = String.format("lookup_value_for_[%s]", key.hashCode());
    }
    testTransformFunction(transformFunction, expectedResults);

    // PK: [String]
    expression = RequestContextUtils.getExpression(
        String.format("lookup('dimTableWithStringPK', 'lookupColumn', 'primaryColumn', %s)", STRING_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    expectedResults = new String[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      PrimaryKey key = new PrimaryKey(new Object[]{_stringSVValues[i]});
      expectedResults[i] = String.format("lookup_value_for_[%s]", key.hashCode());
    }
    testTransformFunction(transformFunction, expectedResults);

    // PK: [Long]
    expression = RequestContextUtils.getExpression(
        String.format("lookup('dimTableWithLongPK', 'lookupColumn', 'primaryColumn', %s)", LONG_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    expectedResults = new String[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      PrimaryKey key = new PrimaryKey(new Object[]{_longSVValues[i]});
      expectedResults[i] = String.format("lookup_value_for_[%s]", key.hashCode());
    }
    testTransformFunction(transformFunction, expectedResults);

    // PK: [Float]
    expression = RequestContextUtils.getExpression(
        String.format("lookup('dimTableWithFloatPK', 'lookupColumn', 'primaryColumn', %s)", FLOAT_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    expectedResults = new String[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      PrimaryKey key = new PrimaryKey(new Object[]{_floatSVValues[i]});
      expectedResults[i] = String.format("lookup_value_for_[%s]", key.hashCode());
    }
    testTransformFunction(transformFunction, expectedResults);

    // PK: [Double]
    expression = RequestContextUtils.getExpression(
        String.format("lookup('dimTableWithDoublePK', 'lookupColumn', 'primaryColumn', %s)", DOUBLE_SV_COLUMN));
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    expectedResults = new String[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      PrimaryKey key = new PrimaryKey(new Object[]{_doubleSVValues[i]});
      expectedResults[i] = String.format("lookup_value_for_[%s]", key.hashCode());
    }
    testTransformFunction(transformFunction, expectedResults);

    // PK: [Byte[]]
    expression = RequestContextUtils.getExpression(
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
