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

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import org.apache.pinot.common.request.transform.TransformExpressionTree;
import org.apache.pinot.core.query.exception.BadQueryRequestException;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class JsonExtractScalarTransformFunctionTest extends BaseTransformFunctionTest {

  @Test(dataProvider = "testJsonPathTransformFunctionArguments")
  public void testJsonPathTransformFunction(String expressionStr, FieldSpec.DataType resultsDataType,
      boolean isSingleValue) {
    TransformExpressionTree expression = TransformExpressionTree.compileToExpressionTree(expressionStr);
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

  @DataProvider(name = "testJsonPathTransformFunctionArguments")
  public Object[][] testJsonPathTransformFunctionArguments() {
    return new Object[][]{new Object[]{"jsonExtractScalar(json,'$.intSV','INT')", FieldSpec.DataType.INT, true}, new Object[]{"jsonExtractScalar(json,'$.intMV','INT_ARRAY')", FieldSpec.DataType.INT, false}, new Object[]{"jsonExtractScalar(json,'$.longSV','LONG')", FieldSpec.DataType.LONG, true}, new Object[]{"jsonExtractScalar(json,'$.floatSV','FLOAT')", FieldSpec.DataType.FLOAT, true}, new Object[]{"jsonExtractScalar(json,'$.doubleSV','DOUBLE')", FieldSpec.DataType.DOUBLE, true}, new Object[]{"jsonExtractScalar(json,'$.stringSV','STRING')", FieldSpec.DataType.STRING, true},};
  }

  @Test
  public void testJsonPathTransformFunctionWithPredicate() {
    String jsonPathExpressionStr =
        String.format("jsonExtractScalar(json,'[?($.stringSV==''%s'')]','STRING')", _stringSVValues[0]);
    TransformExpressionTree expression = TransformExpressionTree.compileToExpressionTree(jsonPathExpressionStr);
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof JsonExtractScalarTransformFunction);
    Assert.assertEquals(transformFunction.getName(), JsonExtractScalarTransformFunction.FUNCTION_NAME);
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
          Assert.assertEquals(Float.compare(_floatSVValues[i], ((Double) resultMap.get(0).get("floatSV")).floatValue()), 0);
          Assert.assertEquals(_doubleSVValues[i], resultMap.get(0).get("doubleSV"));
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
    TransformExpressionTree expression =
        TransformExpressionTree.compileToExpressionTree("jsonExtractScalar(json,'$.intMV','INT_ARRAY')");
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
    TransformExpressionTree expression =
        TransformExpressionTree.compileToExpressionTree("jsonExtractScalar(json,'$.longSV','LONG')");
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
    TransformExpressionTree expression =
        TransformExpressionTree.compileToExpressionTree("jsonExtractScalar(json,'$.floatSV','FLOAT')");
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
    TransformExpressionTree expression =
        TransformExpressionTree.compileToExpressionTree("jsonExtractScalar(json,'$.doubleSV','DOUBLE')");
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof JsonExtractScalarTransformFunction);
    Assert.assertEquals(transformFunction.getName(), JsonExtractScalarTransformFunction.FUNCTION_NAME);
    double[] doubleValues = transformFunction.transformToDoubleValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      Assert.assertEquals(doubleValues[i], _doubleSVValues[i]);
    }
  }

  @Test
  public void testJsonPathTransformFunctionForString() {
    TransformExpressionTree expression =
        TransformExpressionTree.compileToExpressionTree("jsonExtractScalar(json,'$.stringSV','STRING')");
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
    TransformExpressionTree expression = TransformExpressionTree.compileToExpressionTree("jsonExtractKey(json,'$.*')");
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof JsonExtractKeyTransformFunction);
    Assert.assertEquals(transformFunction.getName(), JsonExtractKeyTransformFunction.FUNCTION_NAME);
    String[][] keysResults = transformFunction.transformToStringValuesMV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      List<String> keys = Arrays.asList(keysResults[i]);
      Assert.assertTrue(keys.contains(String.format("$['%s']", INT_SV_COLUMN)));
      Assert.assertTrue(keys.contains(String.format("$['%s']", LONG_SV_COLUMN)));
      Assert.assertTrue(keys.contains(String.format("$['%s']", FLOAT_SV_COLUMN)));
      Assert.assertTrue(keys.contains(String.format("$['%s']", DOUBLE_SV_COLUMN)));
      Assert.assertTrue(keys.contains(String.format("$['%s']", STRING_SV_COLUMN)));
      Assert.assertTrue(keys.contains(String.format("$['%s']", INT_MV_COLUMN)));
      Assert.assertTrue(keys.contains(String.format("$['%s']", TIME_COLUMN)));
    }
  }

  @Test(dataProvider = "testIllegalArguments", expectedExceptions = {BadQueryRequestException.class})
  public void testIllegalArguments(String expressionStr) {
    TransformExpressionTree expression = TransformExpressionTree.compileToExpressionTree(expressionStr);
    TransformFunctionFactory.get(expression, _dataSourceMap);
  }

  @DataProvider(name = "testIllegalArguments")
  public Object[][] testIllegalArguments() {
    return new Object[][]{new Object[]{String.format("jsonExtractScalar(%s)",
        JSON_COLUMN)}, new Object[]{"jsonExtractScalar(5,'$.store.book[0].author','$.store.book[0].author')"}, new Object[]{String.format(
        "jsonExtractScalar(%s,'$.store.book[0].author')", INT_MV_COLUMN)}, new Object[]{String.format(
        "jsonExtractScalar(%s,'$.store.book[0].author')", STRING_SV_COLUMN)}, new Object[]{String.format(
        "jsonExtractScalar(%s,'$.store.book[0].author', 'STRINGARRAY')", STRING_SV_COLUMN)}, new Object[]{String.format(
        "jsonExtractScalar(%s,%s,'$.store.book[0].author', 'String','abc')", JSON_COLUMN, INT_SV_COLUMN)}};
  }
}
