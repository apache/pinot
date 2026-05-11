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
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.queries.FluentQueryTest;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.sql.parsers.SqlCompilationException;
import org.assertj.core.api.Assertions;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class JsonExtractScalarTransformFunctionTest extends BaseTransformFunctionTest {

  protected File _baseDir;

  @Test(dataProvider = "testJsonPathTransformFunction")
  public void testJsonPathTransformFunction(String expressionStr, DataType resultsDataType, boolean isSingleValue) {
    ExpressionContext expression = RequestContextUtils.getExpression(expressionStr);
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);

    Assert.assertTrue(transformFunction instanceof JsonExtractScalarTransformFunction);
    Assert.assertEquals(transformFunction.getName(), JsonExtractScalarTransformFunction.FUNCTION_NAME);

    Assert.assertEquals(transformFunction.getResultMetadata().getDataType(), resultsDataType);
    Assert.assertEquals(transformFunction.getResultMetadata().isSingleValue(), isSingleValue);

    if (isSingleValue) {
      switch (resultsDataType) {
        case INT:
          int[] intValues = transformFunction.transformToIntValuesSV(_projectionBlock);
          for (int i = 0; i < NUM_ROWS; i++) {
            Assert.assertEquals(intValues[i], _intSVValues[i]);
          }
          break;
        case LONG:
          long[] longValues = transformFunction.transformToLongValuesSV(_projectionBlock);
          for (int i = 0; i < NUM_ROWS; i++) {
            Assert.assertEquals(longValues[i], _longSVValues[i]);
          }
          break;
        case FLOAT:
          float[] floatValues = transformFunction.transformToFloatValuesSV(_projectionBlock);
          for (int i = 0; i < NUM_ROWS; i++) {
            Assert.assertEquals(floatValues[i], _floatSVValues[i]);
          }
          break;
        case DOUBLE:
          double[] doubleValues = transformFunction.transformToDoubleValuesSV(_projectionBlock);
          for (int i = 0; i < NUM_ROWS; i++) {
            Assert.assertEquals(doubleValues[i], _doubleSVValues[i]);
          }
          break;
        case BIG_DECIMAL:
          BigDecimal[] bigDecimalValues = transformFunction.transformToBigDecimalValuesSV(_projectionBlock);
          for (int i = 0; i < NUM_ROWS; i++) {
            Assert.assertEquals(bigDecimalValues[i].compareTo(_bigDecimalSVValues[i]), 0);
          }
          break;
        case STRING:
          String[] stringValues = transformFunction.transformToStringValuesSV(_projectionBlock);
          for (int i = 0; i < NUM_ROWS; i++) {
            Assert.assertEquals(stringValues[i], _stringSVValues[i]);
          }
          break;
        default:
          throw new UnsupportedOperationException("Not support data type - " + resultsDataType);
      }
    } else {
      switch (resultsDataType) {
        case INT:
          int[][] actualValues = transformFunction.transformToIntValuesMV(_projectionBlock);
          for (int i = 0; i < NUM_ROWS; i++) {
            Assert.assertEquals(actualValues[i].length, _intMVValues[i].length);
            for (int j = 0; j < _intMVValues[i].length; j++) {
              Assert.assertEquals(actualValues[i][j], _intMVValues[i][j]);
            }
          }
          break;
        default:
          throw new UnsupportedOperationException("Not support data type - " + resultsDataType);
      }
    }
  }

  @Test
  public void testExtractWithScalarTypeMismatch() {
    ExpressionContext expression = RequestContextUtils.getExpression("jsonExtractScalar(json,'$.stringSV','DOUBLE')");
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);

    Assert.assertTrue(transformFunction instanceof JsonExtractScalarTransformFunction);
    Assert.assertEquals(transformFunction.getName(), JsonExtractScalarTransformFunction.FUNCTION_NAME);

    long[] longValues = transformFunction.transformToLongValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      Assert.assertEquals(longValues[i], (long) Double.parseDouble(_stringSVValues[i]));
    }

    int[] intValues = transformFunction.transformToIntValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      Assert.assertEquals(intValues[i], (int) Double.parseDouble(_stringSVValues[i]));
    }

    float[] floatValues = transformFunction.transformToFloatValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      Assert.assertEquals(floatValues[i], Float.parseFloat(_stringSVValues[i]));
    }

    BigDecimal[] bdValues = transformFunction.transformToBigDecimalValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      Assert.assertEquals(bdValues[i], new BigDecimal(_stringSVValues[i]));
    }
  }

  @Test
  public void testExtractWithScalarStringToLong() {
    ExpressionContext expression = RequestContextUtils.getExpression("jsonExtractScalar(json,'$.stringSV','LONG')");
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);

    Assert.assertTrue(transformFunction instanceof JsonExtractScalarTransformFunction);
    Assert.assertEquals(transformFunction.getName(), JsonExtractScalarTransformFunction.FUNCTION_NAME);

    long[] longValues = transformFunction.transformToLongValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      Assert.assertEquals(longValues[i], (long) Double.parseDouble(_stringSVValues[i]));
    }
  }

  @DataProvider(name = "testJsonPathTransformFunction")
  public Object[][] testJsonPathTransformFunctionDataProvider() {
    List<Object[]> testArguments = new ArrayList<>();

    // Test operating on both column and output of another transform (trim) to avoid passing the evaluation down to the
    // storage in order to test transformTransformedValuesToXXXValuesSV() methods.
    for (String input : new String[]{JSON_COLUMN, String.format("trim(%s)", JSON_COLUMN)}) {
      // Without default value
      testArguments.add(
          new Object[]{String.format("jsonExtractScalar(%s,'$.intSV','INT')", input), DataType.INT, true});
      testArguments.add(
          new Object[]{String.format("jsonExtractScalar(%s,'$.intMV','INT_ARRAY')", input), DataType.INT, false});
      testArguments.add(
          new Object[]{String.format("jsonExtractScalar(%s,'$.longSV','LONG')", input), DataType.LONG, true});
      testArguments.add(
          new Object[]{String.format("jsonExtractScalar(%s,'$.floatSV','FLOAT')", input), DataType.FLOAT, true});
      testArguments.add(
          new Object[]{String.format("jsonExtractScalar(%s,'$.doubleSV','DOUBLE')", input), DataType.DOUBLE, true});
      testArguments.add(new Object[]{
          String.format("jsonExtractScalar(%s,'$.bigDecimalSV','BIG_DECIMAL')", input), DataType.BIG_DECIMAL, true
      });
      testArguments.add(
          new Object[]{String.format("jsonExtractScalar(%s,'$.stringSV','STRING')", input), DataType.STRING, true});

      // With default value
      testArguments.add(
          new Object[]{String.format("jsonExtractScalar(%s,'$.intSV','INT',0)", input), DataType.INT, true});
      testArguments.add(
          new Object[]{String.format("jsonExtractScalar(%s,'$.intMV','INT_ARRAY',0)", input), DataType.INT, false});
      testArguments.add(
          new Object[]{String.format("jsonExtractScalar(%s,'$.longSV','LONG',0)", input), DataType.LONG, true});
      testArguments.add(
          new Object[]{String.format("jsonExtractScalar(%s,'$.floatSV','FLOAT',0)", input), DataType.FLOAT, true});
      testArguments.add(
          new Object[]{String.format("jsonExtractScalar(%s,'$.doubleSV','DOUBLE',0)", input), DataType.DOUBLE, true});
      testArguments.add(new Object[]{
          String.format("jsonExtractScalar(%s,'$.bigDecimalSV','BIG_DECIMAL',0)", input), DataType.BIG_DECIMAL, true
      });
      testArguments.add(new Object[]{
          String.format("jsonExtractScalar(%s,'$.stringSV','STRING','null')", input), DataType.STRING, true
      });
    }

    return testArguments.toArray(new Object[0][]);
  }

  @Test(dataProvider = "testDefaultValue")
  public void testDefaultValue(String expressionStr, DataType resultsDataType, boolean isSingleValue) {
    ExpressionContext expression = RequestContextUtils.getExpression(expressionStr);
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof JsonExtractScalarTransformFunction);
    Assert.assertEquals(transformFunction.getName(), JsonExtractScalarTransformFunction.FUNCTION_NAME);

    Assert.assertEquals(transformFunction.getResultMetadata().getDataType(), resultsDataType);
    Assert.assertEquals(transformFunction.getResultMetadata().isSingleValue(), isSingleValue);
    if (isSingleValue) {
      switch (resultsDataType) {
        case INT:
          int[] intValues = transformFunction.transformToIntValuesSV(_projectionBlock);
          for (int i = 0; i < NUM_ROWS; i++) {
            Assert.assertEquals(intValues[i], 0);
          }
          break;
        case LONG:
          long[] longValues = transformFunction.transformToLongValuesSV(_projectionBlock);
          for (int i = 0; i < NUM_ROWS; i++) {
            Assert.assertEquals(longValues[i], 0L);
          }
          break;
        case FLOAT:
          float[] floatValues = transformFunction.transformToFloatValuesSV(_projectionBlock);
          for (int i = 0; i < NUM_ROWS; i++) {
            Assert.assertEquals(floatValues[i], 0f);
          }
          break;
        case DOUBLE:
          double[] doubleValues = transformFunction.transformToDoubleValuesSV(_projectionBlock);
          for (int i = 0; i < NUM_ROWS; i++) {
            Assert.assertEquals(doubleValues[i], 0d);
          }
          break;
        case BIG_DECIMAL:
          BigDecimal[] bigDecimalValues = transformFunction.transformToBigDecimalValuesSV(_projectionBlock);
          for (int i = 0; i < NUM_ROWS; i++) {
            Assert.assertEquals(bigDecimalValues[i].compareTo(BigDecimal.ZERO), 0);
          }
          break;
        case STRING:
          String[] stringValues = transformFunction.transformToStringValuesSV(_projectionBlock);
          for (int i = 0; i < NUM_ROWS; i++) {
            Assert.assertEquals(stringValues[i], "null");
          }
          break;
        default:
          throw new UnsupportedOperationException("Not support data type - " + resultsDataType);
      }
    } else {
      switch (resultsDataType) {
        case INT:
          int[][] actualValues = transformFunction.transformToIntValuesMV(_projectionBlock);
          for (int i = 0; i < NUM_ROWS; i++) {
            Assert.assertEquals(actualValues[i].length, 0);
          }
          break;
        default:
          throw new UnsupportedOperationException("Not support data type - " + resultsDataType);
      }
    }
  }

  @DataProvider(name = "testDefaultValue")
  public Object[][] testDefaultValueDataProvider() {
    List<Object[]> testArguments = new ArrayList<>();

    // Test operating on both column and output of another transform (trim) to avoid passing the evaluation down to the
    // storage in order to test transformTransformedValuesToXXXValuesSV() methods.
    for (String input : new String[]{DEFAULT_JSON_COLUMN, String.format("trim(%s)", DEFAULT_JSON_COLUMN)}) {
      testArguments.add(
          new Object[]{String.format("jsonExtractScalar(%s,'$.intSV','INT',0)", input), DataType.INT, true});
      testArguments.add(
          new Object[]{String.format("jsonExtractScalar(%s,'$.intMV','INT_ARRAY',0)", input), DataType.INT, false});
      testArguments.add(
          new Object[]{String.format("jsonExtractScalar(%s,'$.longSV','LONG',0)", input), DataType.LONG, true});
      testArguments.add(
          new Object[]{String.format("jsonExtractScalar(%s,'$.floatSV','FLOAT',0)", input), DataType.FLOAT, true});
      testArguments.add(
          new Object[]{String.format("jsonExtractScalar(%s,'$.doubleSV','DOUBLE',0)", input), DataType.DOUBLE, true});
      testArguments.add(new Object[]{
          String.format("jsonExtractScalar(%s,'$.bigDecimalSV','BIG_DECIMAL',0)", input), DataType.BIG_DECIMAL, true
      });
      testArguments.add(new Object[]{
          String.format("jsonExtractScalar(%s,'$.stringSV','STRING','null')", input), DataType.STRING, true
      });
    }

    return testArguments.toArray(new Object[0][]);
  }

  @Test
  public void testJsonPathTransformFunctionWithPredicate() {
    String jsonPathExpressionStr =
        String.format("jsonExtractScalar(json,'[?($.stringSV==''%s'')]','STRING')", _stringSVValues[0]);
    ExpressionContext expression = RequestContextUtils.getExpression(jsonPathExpressionStr);
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof JsonExtractScalarTransformFunction);
    Assert.assertEquals(transformFunction.getName(), JsonExtractScalarTransformFunction.FUNCTION_NAME);
    // Note: transformToStringValuesSV() calls IdentifierTransformFunction.transformToStringValuesSV() which in turns
    //  call DataFetcher.readStringValues() which calls DefaultJsonPathEvaluator.evaluateBlock() that parses String w/o
    //  support for exact BigDecimal. Therefore, testing string parsing of BigDecimal is disabled.
    String[] resultValues = transformFunction.transformToStringValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      if (_stringSVValues[i].equals(_stringSVValues[0])) {
        try {
          final List<HashMap<String, Object>> resultMap = JsonUtils.stringToObject(resultValues[i], List.class);
          Assert.assertEquals(_intSVValues[i], resultMap.get(0).get("intSV"));
          for (int j = 0; j < _intMVValues[i].length; j++) {
            Assert.assertEquals(_intMVValues[i][j], ((List) resultMap.get(0).get("intMV")).get(j));
          }
          Assert.assertEquals(_longSVValues[i], resultMap.get(0).get("longSV"));
          // Notes: since we use currently a mapper that parses exact big decimals, doubles may get parsed as
          // big decimals. Confirm this is a backward compatible change?
          Assert.assertEquals(Float.compare(_floatSVValues[i], ((Number) resultMap.get(0).get("floatSV")).floatValue()),
              0);
          Assert.assertEquals(_doubleSVValues[i], resultMap.get(0).get("doubleSV"));
          // Disabled:
          // Assert.assertEquals(_bigDecimalSVValues[i], (BigDecimal) resultMap.get(0).get("bigDecimalSV"));
          Assert.assertEquals(_stringSVValues[i], resultMap.get(0).get("stringSV"));
        } catch (IOException e) {
          throw new RuntimeException();
        }
      } else {
        Assert.assertEquals(resultValues[i], "[]");
      }
    }
  }

  @Test
  public void testJsonPathTransformFunctionForIntMV() {
    ExpressionContext expression = RequestContextUtils.getExpression("jsonExtractScalar(json,'$.intMV','INT_ARRAY')");
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof JsonExtractScalarTransformFunction);
    Assert.assertEquals(transformFunction.getName(), JsonExtractScalarTransformFunction.FUNCTION_NAME);
    int[][] intValues = transformFunction.transformToIntValuesMV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      Assert.assertEquals(intValues[i].length, _intMVValues[i].length);
      for (int j = 0; j < _intMVValues[i].length; j++) {
        Assert.assertEquals(intValues[i][j], _intMVValues[i][j]);
      }
    }
  }

  @Test
  public void testJsonPathTransformFunctionForLong() {
    ExpressionContext expression = RequestContextUtils.getExpression("jsonExtractScalar(json,'$.longSV','LONG')");
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof JsonExtractScalarTransformFunction);
    Assert.assertEquals(transformFunction.getName(), JsonExtractScalarTransformFunction.FUNCTION_NAME);
    long[] longValues = transformFunction.transformToLongValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      Assert.assertEquals(longValues[i], _longSVValues[i]);
    }
  }

  @Test
  public void testJsonPathTransformFunctionForFloat() {
    ExpressionContext expression = RequestContextUtils.getExpression("jsonExtractScalar(json,'$.floatSV','FLOAT')");
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof JsonExtractScalarTransformFunction);
    Assert.assertEquals(transformFunction.getName(), JsonExtractScalarTransformFunction.FUNCTION_NAME);
    float[] floatValues = transformFunction.transformToFloatValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      Assert.assertEquals(floatValues[i], _floatSVValues[i]);
    }
  }

  @Test
  public void testJsonPathTransformFunctionForDouble() {
    ExpressionContext expression = RequestContextUtils.getExpression("jsonExtractScalar(json,'$.doubleSV','DOUBLE')");
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof JsonExtractScalarTransformFunction);
    Assert.assertEquals(transformFunction.getName(), JsonExtractScalarTransformFunction.FUNCTION_NAME);
    double[] doubleValues = transformFunction.transformToDoubleValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      Assert.assertEquals(doubleValues[i], _doubleSVValues[i]);
    }
  }

  @Test
  public void testJsonPathTransformFunctionForBigDecimal() {
    ExpressionContext expression =
        RequestContextUtils.getExpression("jsonExtractScalar(json,'$.bigDecimalSV','BIG_DECIMAL')");
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof JsonExtractScalarTransformFunction);
    Assert.assertEquals(transformFunction.getName(), JsonExtractScalarTransformFunction.FUNCTION_NAME);
    BigDecimal[] bigDecimalValues = transformFunction.transformToBigDecimalValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      Assert.assertEquals(bigDecimalValues[i].compareTo(_bigDecimalSVValues[i]), 0);
    }
  }

  @Test
  public void testJsonPathTransformFunctionForString() {
    ExpressionContext expression = RequestContextUtils.getExpression("jsonExtractScalar(json,'$.stringSV','STRING')");
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof JsonExtractScalarTransformFunction);
    Assert.assertEquals(transformFunction.getName(), JsonExtractScalarTransformFunction.FUNCTION_NAME);
    String[] stringValues = transformFunction.transformToStringValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      Assert.assertEquals(stringValues[i], _stringSVValues[i]);
    }
  }

  @Test
  public void testJsonPathKeyTransformFunction() {
    ExpressionContext expression =
        (new Random(System.currentTimeMillis()).nextBoolean()) ? RequestContextUtils.getExpression(
            "jsonExtractKey(json,'$.*')") : RequestContextUtils.getExpression("json_extract_key(json,'$.*')");
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof JsonExtractKeyTransformFunction);
    Assert.assertEquals(transformFunction.getName(), JsonExtractKeyTransformFunction.FUNCTION_NAME);
    // Transform the same block multiple times should give the same result
    for (int i = 0; i < 10; i++) {
      String[][] keysResults = transformFunction.transformToStringValuesMV(_projectionBlock);
      for (int j = 0; j < NUM_ROWS; j++) {
        List<String> keys = Arrays.asList(keysResults[j]);
        Assert.assertTrue(keys.contains(String.format("$['%s']", INT_SV_COLUMN)));
        Assert.assertTrue(keys.contains(String.format("$['%s']", LONG_SV_COLUMN)));
        Assert.assertTrue(keys.contains(String.format("$['%s']", FLOAT_SV_COLUMN)));
        Assert.assertTrue(keys.contains(String.format("$['%s']", DOUBLE_SV_COLUMN)));
        Assert.assertTrue(keys.contains(String.format("$['%s']", BIG_DECIMAL_SV_COLUMN)));
        Assert.assertTrue(keys.contains(String.format("$['%s']", STRING_SV_COLUMN)));
        Assert.assertTrue(keys.contains(String.format("$['%s']", INT_MV_COLUMN)));
        Assert.assertTrue(keys.contains(String.format("$['%s']", TIME_COLUMN)));
      }
    }
  }

  @Test(dataProvider = "testIllegalArguments", expectedExceptions = {BadQueryRequestException.class})
  public void testIllegalArguments(String expressionStr) {
    ExpressionContext expression = RequestContextUtils.getExpression(expressionStr);
    TransformFunctionFactory.get(expression, _dataSourceMap);
  }

  @Test(dataProvider = "testParsingIllegalQueries", expectedExceptions = {SqlCompilationException.class})
  public void testParsingIllegalQueries(String expressionStr) {
    RequestContextUtils.getExpression(expressionStr);
  }

  @Test
  public void testJsonExtractKeyWithDepthTransformFunction() {
    ExpressionContext expression = RequestContextUtils.getExpression("jsonExtractKey(json, '$.*', 'maxDepth=2')");
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof JsonExtractKeyTransformFunction);
    Assert.assertEquals(transformFunction.getName(), JsonExtractKeyTransformFunction.FUNCTION_NAME);
    Assert.assertEquals(transformFunction.getResultMetadata().getDataType(), DataType.STRING);
    Assert.assertFalse(transformFunction.getResultMetadata().isSingleValue());

    // Test with different depth values
    expression = RequestContextUtils.getExpression("jsonExtractKey(json, '$.*', 'maxDepth=1')");
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof JsonExtractKeyTransformFunction);

    // Test with parameters (depth and dotNotation)
    expression = RequestContextUtils.getExpression("jsonExtractKey(json, '$.*', 'maxDepth=2;dotNotation=true')");
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof JsonExtractKeyTransformFunction);

    // Test with parameters (depth and dotNotation = false)
    expression = RequestContextUtils.getExpression("jsonExtractKey(json, '$.*', 'maxDepth=2;dotNotation=false')");
    transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof JsonExtractKeyTransformFunction);

    // Test with an invalid number of arguments (should fail)
    try {
      RequestContextUtils.getExpression("jsonExtractKey(json)");
      Assert.fail("Should have thrown SqlCompilationException");
    } catch (SqlCompilationException e) {
      // Expected
    }

    // Test with too many arguments (should fail)
    try {
      RequestContextUtils.getExpression("jsonExtractKey(json, '$.*', 'maxDepth=2;dotNotation=true', 'extra')");
      Assert.fail("Should have thrown SqlCompilationException");
    } catch (SqlCompilationException e) {
      // Expected
    }

    // Test with invalid optional parameter argument that should fail during initialization
    try {
      expression = RequestContextUtils.getExpression("jsonExtractKey(json, '$.*', 'unknownKey=abc')");
      TransformFunctionFactory.get(expression, _dataSourceMap);
      Assert.fail("Should have thrown BadQueryRequestException");
    } catch (BadQueryRequestException e) {
      // Expected
    }
  }

  @DataProvider(name = "testIllegalArguments")
  public Object[][] testIllegalArguments() {
    //@formatter:off
    return new Object[][]{
        new Object[]{"jsonExtractScalar(5,'$.store.book[0].author','$.store.book[0].author')"},
        new Object[]{String.format("jsonExtractScalar(%s,'$.store.book[0].author','$.store.book[0].author')",
            INT_SV_COLUMN)},
        new Object[]{String.format("jsonExtractScalar(%s,'$.store.book[0].author','$.store.book[0].author')",
            INT_MV_COLUMN)},
        new Object[]{String.format("jsonExtractScalar(%s,'$.store.book[0].author', 'STRINGARRAY')", STRING_SV_COLUMN)}
    };
    //@formatter:on
  }

  @DataProvider(name = "testParsingIllegalQueries")
  public Object[][] testParsingIllegalQueries() {
    //@formatter:off
    return new Object[][]{
        new Object[]{String.format("jsonExtractScalar(%s)", JSON_COLUMN)},
        new Object[]{String.format("jsonExtractScalar(%s,'$.store.book[0].author')", JSON_COLUMN)},
        new Object[]{String.format("jsonExtractScalar(%s,%s,'$.store.book[0].author', 'String','abc')", JSON_COLUMN,
            INT_SV_COLUMN)},
        new Object[]{String.format("jsonExtractScalar(%s, \"$.store.book[0].author\", 'String')", JSON_COLUMN)},
        new Object[]{String.format("jsonExtractScalar(%s, '$.store.book[0].author', \"String\")", JSON_COLUMN)},
        new Object[]{String.format("json_extract_scalar(%s, \"$.store.book[0].author\", 'String','abc')", JSON_COLUMN)},
        new Object[]{String.format("jsonExtractKey(%s, \"$.*\")", JSON_COLUMN)},
        new Object[]{String.format("json_extract_key(%s, \"$.*\")", JSON_COLUMN)}};
    //@formatter:on
  }

  @BeforeClass
  void createBaseDir() {
    try {
      _baseDir = Files.createTempDirectory(getClass().getSimpleName()).toFile();
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    }
  }

  @AfterClass
  void destroyBaseDir()
      throws IOException {
    if (_baseDir != null) {
      FileUtils.deleteDirectory(_baseDir);
    }
  }

  @DataProvider
  public static Object[][] multiValuesAndDefaults() {
    return new Object[][]{
        {"INT_ARRAY", "-123"},
        {"LONG_ARRAY", "-123"},
        {"FLOAT_ARRAY", "0.5"},
        {"DOUBLE_ARRAY", "0.5"},
        {"STRING_ARRAY", "'default'"}
    };
  }

  /**
   *
   * @param resultsType The result type used in the query, either 'INT_ARRAY' or 'STRING_ARRAY'
   * @param notUsed Not used, just here to be able to use the same provider as
   *                {@link #mvWithNullsWithDefault(String, String)}
   */
  @Test(dataProvider = "multiValuesAndDefaults")
  public void mvWithNullsWithoutDefault(String resultsType, String notUsed) {
    // schema with a single column called "json" of type JSON
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("testTable")
        .setEnableColumnBasedNullHandling(true)
        .addDimensionField("json", DataType.JSON)
        .build();
    // trivial table config
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("testTable")
        .build();
    try {
      FluentQueryTest.withBaseDir(_baseDir)
          .withNullHandling(false)
          .givenTable(schema, tableConfig)
          .onFirstInstance(new Object[]{"{\"name\": [null]}"})
          .whenQuery("SELECT jsonExtractScalar(json, '$.name', '" + resultsType + "') FROM testTable")
          // TODO: Change the framework to do not duplicate segments when only one segment is used
          .thenResultIs(new Object[]{"doesn't matter, it should fail"});
    } catch (AssertionError e) {
      Assertions.assertThat(e.getMessage())
          .describedAs("Expected to fail because the JSON array contains nulls")
          .contains("At least one of the resolved JSON arrays include nulls");
    }
  }

  /**
   *
   * @param resultsType The result type used in the query, either 'INT_ARRAY' or 'STRING_ARRAY'
   * @param defaultValSql The default value to use in the query, as passed to jsonExtractScalar forth argument
   */
  @Test(dataProvider = "multiValuesAndDefaults")
  public void mvWithNullsWithDefault(String resultsType, String defaultValSql) {
    // schema with a single column called "json" of type JSON
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("testTable")
        .setEnableColumnBasedNullHandling(true)
        .addDimensionField("json", DataType.JSON)
        .build();
    // trivial table config
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("testTable")
        .build();

    Object[] expectedRow;
    switch (resultsType) {
      case "INT_ARRAY": {
        int[] rowValue = new int[]{Integer.parseInt(defaultValSql)};
        expectedRow = new Object[]{rowValue};
        break;
      }
      case "LONG_ARRAY": {
        long[] rowValue = new long[]{Long.parseLong(defaultValSql)};
        expectedRow = new Object[]{rowValue};
        break;
      }
      case "FLOAT_ARRAY": {
        float[] rowValue = new float[]{Float.parseFloat(defaultValSql)};
        expectedRow = new Object[]{rowValue};
        break;
      }
      case "DOUBLE_ARRAY": {
        double[] rowValue = new double[]{Double.parseDouble(defaultValSql)};
        expectedRow = new Object[]{rowValue};
        break;
      }
      case "STRING_ARRAY": {
        // defaultValSql is quoted, need to remove quotes
        String str = defaultValSql.substring(1, defaultValSql.length() - 1);
        String[] rowValue = new String[]{str};
        expectedRow = new Object[]{rowValue};
        break;
      }
      default:
        throw new UnsupportedOperationException("Not support data type - " + resultsType);
    }

    FluentQueryTest.withBaseDir(_baseDir)
        .withNullHandling(false)
        .givenTable(schema, tableConfig)
        .onFirstInstance(new Object[]{"{\"name\": [null]}"})
        .whenQuery("SELECT jsonExtractScalar(json, '$.name', '" + resultsType + "', " + defaultValSql + ") "
            + "FROM testTable")
        // TODO: Change the framework to do not duplicate segments when only one segment is used
        .thenResultIs(expectedRow, expectedRow); // 2 rows because of segment duplication
  }

  // === Per-stored-type coercion tests for the value-handling helpers. Tests are grouped first by SV
  //     vs MV result type, then ordered within each group by canonical Pinot type order
  //     (INT/LONG/FLOAT/DOUBLE → BIG_DECIMAL → BOOLEAN → TIMESTAMP → STRING). ===

  // -- Single-value (SV) tests --

  /// Runs `SELECT jsonExtractScalar(json, '$.v', resultsType) FROM testTable` against a single-row table
  /// containing the given JSON document, and asserts the result for the (always-duplicated) two
  /// expected rows.
  private void assertJsonExtractScalar(String json, String resultsType, Object expectedValue) {
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("testTable")
        .setEnableColumnBasedNullHandling(true)
        .addDimensionField("json", DataType.JSON)
        .build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("testTable")
        .build();
    Object[] expectedRow = new Object[]{expectedValue};
    FluentQueryTest.withBaseDir(_baseDir)
        .withNullHandling(false)
        .givenTable(schema, tableConfig)
        .onFirstInstance(new Object[]{json})
        .whenQuery("SELECT jsonExtractScalar(json, '$.v', '" + resultsType + "') FROM testTable")
        .thenResultIs(expectedRow, expectedRow);
  }

  @Test
  public void testExtractBooleanAsNumeric() {
    // JSON true / false coerces to 1 / 0 across all numeric result types — matches PinotDataType's
    // BOOLEAN.toInt / toLong / toFloat / toDouble / toBigDecimal convention.
    assertJsonExtractScalar("{\"v\": true}", "INT", 1);
    assertJsonExtractScalar("{\"v\": false}", "INT", 0);
    assertJsonExtractScalar("{\"v\": true}", "LONG", 1L);
    assertJsonExtractScalar("{\"v\": false}", "LONG", 0L);
    assertJsonExtractScalar("{\"v\": true}", "FLOAT", 1f);
    assertJsonExtractScalar("{\"v\": false}", "FLOAT", 0f);
    assertJsonExtractScalar("{\"v\": true}", "DOUBLE", 1d);
    assertJsonExtractScalar("{\"v\": false}", "DOUBLE", 0d);
    // BIG_DECIMAL formatted as String via toPlainString.
    assertJsonExtractScalar("{\"v\": true}", "BIG_DECIMAL", "1");
    assertJsonExtractScalar("{\"v\": false}", "BIG_DECIMAL", "0");
  }

  @Test
  public void testExtractBigDecimalPreservesPrecision() {
    // The BIG_DECIMAL parser preserves full numeric precision — beyond what Double can represent.
    // Broker formats BIG_DECIMAL as String via BigDecimal.toPlainString().
    assertJsonExtractScalar("{\"v\": 12345678901234567890.123456789}", "BIG_DECIMAL",
        "12345678901234567890.123456789");
  }

  @DataProvider(name = "booleanCoercion")
  public Object[][] booleanCoercion() {
    return new Object[][]{
        // JSON boolean — direct map.
        {"true", 1},
        {"false", 0},
        // JSON number — Pinot's numeric BOOLEAN convention: any non-zero → 1.
        {"1", 1},
        {"0", 0},
        {"5", 1},
        {"-1", 1},
        {"0.5", 1},
        {"0.0", 0},
        // JSON string — only "true" / "TRUE" / "1" → 1; anything else (including "yes", "TRUE ") → 0.
        {"\"true\"", 1},
        {"\"TRUE\"", 1},
        {"\"True\"", 1},
        {"\"1\"", 1},
        {"\"false\"", 0},
        {"\"0\"", 0},
        {"\"yes\"", 0},
        {"\"\"", 0}
    };
  }

  @Test(dataProvider = "booleanCoercion")
  public void testExtractBoolean(String jsonValueLiteral, int expected) {
    // The SELECT projection for a BOOLEAN result surfaces as Boolean true / false in the broker rows.
    Object expectedBoolean = expected == 1;
    assertJsonExtractScalar("{\"v\": " + jsonValueLiteral + "}", "BOOLEAN", expectedBoolean);
  }

  @DataProvider(name = "timestampCoercion")
  public Object[][] timestampCoercion() {
    long epochMillis = 1700000000000L;
    return new Object[][]{
        // Numeric epoch millis — straight longValue path.
        {String.valueOf(epochMillis), epochMillis},
        // Numeric epoch millis as string — TimestampUtils accepts numeric strings.
        {"\"" + epochMillis + "\"", epochMillis},
        // ISO-8601 string — TimestampUtils parses to epoch millis.
        {"\"2023-11-14T22:13:20Z\"", epochMillis},
        {"\"2023-11-14 22:13:20\"", epochMillis - java.util.TimeZone.getDefault().getOffset(epochMillis)}
    };
  }

  @Test(dataProvider = "timestampCoercion")
  public void testExtractTimestamp(String jsonValueLiteral, long expectedMillis) {
    // Broker formats TIMESTAMP via Timestamp.toString() (local-TZ wall-clock representation).
    assertJsonExtractScalar("{\"v\": " + jsonValueLiteral + "}", "TIMESTAMP",
        new java.sql.Timestamp(expectedMillis).toString());
  }

  @Test
  public void testExtractTimestampRejectsBoolean() {
    // TIMESTAMP doesn't accept Boolean — Boolean → epoch millis is semantically nonsensical, matching
    // PinotDataType.TIMESTAMP.toBoolean throwing UnsupportedOperationException. JSON true is routed
    // through TimestampUtils.toMillisSinceEpoch("true") which throws IllegalArgumentException; the
    // broker surfaces the failure as a query error.
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("testTable")
        .setEnableColumnBasedNullHandling(true)
        .addDimensionField("json", DataType.JSON)
        .build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("testTable")
        .build();
    try {
      FluentQueryTest.withBaseDir(_baseDir)
          .withNullHandling(false)
          .givenTable(schema, tableConfig)
          .onFirstInstance(new Object[]{"{\"v\": true}"})
          .whenQuery("SELECT jsonExtractScalar(json, '$.v', 'TIMESTAMP') FROM testTable")
          .thenResultIs(new Object[]{"unused"});
      Assert.fail("Expected query to fail when extracting JSON boolean as TIMESTAMP");
    } catch (AssertionError e) {
      // Expected — broker surfaces the parse failure as a query error.
    }
  }

  @Test
  public void testExtractStringFromNonStringJson() {
    // A String JSON value passes through as-is.
    assertJsonExtractScalar("{\"v\": \"hello\"}", "STRING", "hello");
    // Numbers, booleans, arrays, and objects are JSON-serialized via JsonUtils.objectToString.
    assertJsonExtractScalar("{\"v\": 42}", "STRING", "42");
    assertJsonExtractScalar("{\"v\": 3.14}", "STRING", "3.14");
    assertJsonExtractScalar("{\"v\": true}", "STRING", "true");
    assertJsonExtractScalar("{\"v\": [1,2,3]}", "STRING", "[1,2,3]");
    assertJsonExtractScalar("{\"v\": {\"a\":1}}", "STRING", "{\"a\":1}");
  }

  @Test
  public void testExtractStringPreservesNumericPrecision() {
    // STRING uses JSON_PARSER_CONTEXT_WITH_BIG_DECIMAL so floats that exceed Double precision survive
    // the JSON → Java → JSON round-trip without truncation. Symmetric with BIG_DECIMAL precision.
    assertJsonExtractScalar(
        "{\"v\": 12345678901234567890.123456789}", "STRING",
        "12345678901234567890.123456789");
  }

  // -- Multi-value (MV) tests --

  /// Asserts that `SELECT jsonExtractScalar(json, '$.v', resultsType)` over a single-row table with the
  /// given JSON document produces the given primitive-array result.
  private void assertJsonExtractMv(String json, String resultsType, Object expectedArray) {
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("testTable")
        .setEnableColumnBasedNullHandling(true)
        .addDimensionField("json", DataType.JSON)
        .build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("testTable")
        .build();
    Object[] expectedRow = new Object[]{expectedArray};
    FluentQueryTest.withBaseDir(_baseDir)
        .withNullHandling(false)
        .givenTable(schema, tableConfig)
        .onFirstInstance(new Object[]{json})
        .whenQuery("SELECT jsonExtractScalar(json, '$.v', '" + resultsType + "') FROM testTable")
        .thenResultIs(expectedRow, expectedRow);
  }

  @Test
  public void testIntArrayFromHeterogeneousJsonElements() {
    // JSON numbers — direct intValue path.
    assertJsonExtractMv("{\"v\": [1, 2, 3]}", "INT_ARRAY", new int[]{1, 2, 3});
    // JSON string-form numbers — Integer.parseInt(toString()) fallback.
    assertJsonExtractMv("{\"v\": [\"1\", \"2\", \"3\"]}", "INT_ARRAY", new int[]{1, 2, 3});
    // Mixed numeric and string-numeric — both forms coerce.
    assertJsonExtractMv("{\"v\": [1, \"2\", 3]}", "INT_ARRAY", new int[]{1, 2, 3});
  }

  @Test
  public void testNumericArrayWithBooleanElements() {
    // JSON true / false elements coerce to 1 / 0 in numeric MV paths.
    assertJsonExtractMv("{\"v\": [true, false, true]}", "INT_ARRAY", new int[]{1, 0, 1});
    assertJsonExtractMv("{\"v\": [true, false]}", "LONG_ARRAY", new long[]{1L, 0L});
    assertJsonExtractMv("{\"v\": [true, false]}", "FLOAT_ARRAY", new float[]{1f, 0f});
    assertJsonExtractMv("{\"v\": [true, false]}", "DOUBLE_ARRAY", new double[]{1d, 0d});
    assertJsonExtractMv("{\"v\": [true, false]}", "BIG_DECIMAL_ARRAY", new String[]{"1", "0"});
    // Mixed numeric / boolean elements — Boolean coerces to 1/0, Number values pass through.
    assertJsonExtractMv("{\"v\": [1, true, 3, false]}", "INT_ARRAY", new int[]{1, 1, 3, 0});
  }

  @Test
  public void testBigDecimalArrayPreservesPrecision() {
    // Broker formats BIG_DECIMAL_ARRAY as String[] via per-element BigDecimal.toPlainString().
    String[] expected = {
        "12345678901234567890.123456789",
        "0.0000000000000001",
        "3.14"
    };
    assertJsonExtractMv(
        "{\"v\": [12345678901234567890.123456789, 0.0000000000000001, 3.14]}",
        "BIG_DECIMAL_ARRAY", expected);
  }

  @Test
  public void testBooleanArrayResultType() {
    // BOOLEAN_ARRAY uses storedType INT and the BOOLEAN per-element convention (non-zero Number → 1),
    // distinct from INT_ARRAY which uses intValue() directly. JSON [1, 5, 0, 2.5] therefore yields
    // [true, true, false, true] for BOOLEAN_ARRAY but [1, 5, 0, 2] for INT_ARRAY.
    assertJsonExtractMv("{\"v\": [1, 5, 0, 2.5]}", "BOOLEAN_ARRAY",
        new boolean[]{true, true, false, true});
    // Mixed Boolean / Number / String — each element coerced via PinotDataType BOOLEAN convention.
    assertJsonExtractMv("{\"v\": [true, 0, \"true\", false]}", "BOOLEAN_ARRAY",
        new boolean[]{true, false, true, false});
  }

  @Test
  public void testTimestampArrayResultType() {
    // TIMESTAMP_ARRAY: storedType LONG, transformToLongValuesMV with isTimestamp=true. JSON numeric
    // millis pass through Number.longValue(); JSON ISO strings parse via TimestampUtils.
    long epochMillis = 1700000000000L;
    String tsString = new java.sql.Timestamp(epochMillis).toString();
    // Broker formats TIMESTAMP_ARRAY as String[] (per-element Timestamp.toString()).
    assertJsonExtractMv(
        "{\"v\": [" + epochMillis + ", \"2023-11-14T22:13:20Z\"]}",
        "TIMESTAMP_ARRAY",
        new String[]{tsString, tsString});
  }

  @Test
  public void testStringArrayFromNonStringJsonElements() {
    // JSON strings pass through.
    assertJsonExtractMv("{\"v\": [\"a\", \"b\"]}", "STRING_ARRAY", new String[]{"a", "b"});
    // JSON numbers, booleans, and nested structures get JSON-serialized.
    assertJsonExtractMv("{\"v\": [1, 2, 3]}", "STRING_ARRAY", new String[]{"1", "2", "3"});
    assertJsonExtractMv("{\"v\": [true, false]}", "STRING_ARRAY", new String[]{"true", "false"});
    assertJsonExtractMv("{\"v\": [[1, 2], [3, 4]]}", "STRING_ARRAY", new String[]{"[1,2]", "[3,4]"});
  }

  // -- Default values for newly-handled result types --

  @Test
  public void testDefaultValueForBoolean() {
    // BOOLEAN default literal stored as Integer 0/1 in init() to match INT storedType. When the JSON
    // path doesn't resolve, the default surfaces correctly through transformToIntValuesSV.
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("testTable")
        .setEnableColumnBasedNullHandling(true)
        .addDimensionField("json", DataType.JSON)
        .build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("testTable")
        .build();
    FluentQueryTest.withBaseDir(_baseDir)
        .withNullHandling(false)
        .givenTable(schema, tableConfig)
        .onFirstInstance(new Object[]{"{\"other\": 1}"})
        .whenQuery("SELECT jsonExtractScalar(json, '$.missing', 'BOOLEAN', true) FROM testTable")
        .thenResultIs(new Object[]{true}, new Object[]{true});
  }

  @Test
  public void testDefaultValueForTimestamp() {
    // TIMESTAMP default literal stored as Long in init() to match LONG storedType. When the JSON path
    // doesn't resolve, the default surfaces through transformToLongValuesSV.
    long defaultMillis = 1234567890000L;
    String expected = new java.sql.Timestamp(defaultMillis).toString();
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("testTable")
        .setEnableColumnBasedNullHandling(true)
        .addDimensionField("json", DataType.JSON)
        .build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("testTable")
        .build();
    FluentQueryTest.withBaseDir(_baseDir)
        .withNullHandling(false)
        .givenTable(schema, tableConfig)
        .onFirstInstance(new Object[]{"{\"other\": 1}"})
        .whenQuery("SELECT jsonExtractScalar(json, '$.missing', 'TIMESTAMP', " + defaultMillis + ") "
            + "FROM testTable")
        .thenResultIs(new Object[]{expected}, new Object[]{expected});
  }

  // -- Cross-type guard: requesting a type other than the function's declared result type should
  //    route through the base class's cross-type conversion path. --

  @Test
  public void testCrossTypeConversionFromStringResult() {
    // The function's declared result type is STRING, but the caller requests INT — base class should
    // handle the STRING→INT conversion via parsing.
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("testTable")
        .setEnableColumnBasedNullHandling(true)
        .addDimensionField("json", DataType.JSON)
        .build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("testTable")
        .build();
    FluentQueryTest.withBaseDir(_baseDir)
        .withNullHandling(false)
        .givenTable(schema, tableConfig)
        .onFirstInstance(new Object[]{"{\"v\": \"42\"}"})
        // Cast STRING-result to LONG triggers the base-class cross-type path: STRING → parseLong.
        .whenQuery("SELECT CAST(jsonExtractScalar(json, '$.v', 'STRING') AS LONG) FROM testTable")
        .thenResultIs(new Object[]{42L}, new Object[]{42L});
  }
}
