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

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ParseContext;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.pinot.common.function.JsonPathCache;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class JsonExtractIndexTransformFunctionTest extends BaseTransformFunctionTest {
  // Used to verify index value extraction
  private static final ParseContext JSON_PARSER_CONTEXT = JsonPath.using(
      new Configuration.ConfigurationBuilder().jsonProvider(new JacksonJsonProvider())
          .mappingProvider(new JacksonMappingProvider()).options(Option.SUPPRESS_EXCEPTIONS).build());

  @Test(dataProvider = "testJsonExtractIndexTransformFunction")
  public void testJsonExtractIndexTransformFunction(String expressionStr, String jsonPathString,
      DataType resultsDataType, boolean isSingleValue) {
    ExpressionContext expression = RequestContextUtils.getExpression(expressionStr);
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof JsonExtractIndexTransformFunction);
    Assert.assertEquals(transformFunction.getName(), JsonExtractIndexTransformFunction.FUNCTION_NAME);
    Assert.assertEquals(transformFunction.getResultMetadata().getDataType(), resultsDataType);
    Assert.assertEquals(transformFunction.getResultMetadata().isSingleValue(), isSingleValue);
    JsonPath jsonPath = JsonPathCache.INSTANCE.getOrCompute(jsonPathString);
    if (isSingleValue) {
      switch (resultsDataType) {
        case INT:
          int[] intValues = transformFunction.transformToIntValuesSV(_projectionBlock);
          for (int i = 0; i < NUM_ROWS; i++) {
            Assert.assertEquals(intValues[i], Integer.parseInt(getValueForKey(_jsonSVValues[i], jsonPath)));
          }
          break;
        case LONG:
          long[] longValues = transformFunction.transformToLongValuesSV(_projectionBlock);
          for (int i = 0; i < NUM_ROWS; i++) {
            Assert.assertEquals(longValues[i], Long.parseLong(getValueForKey(_jsonSVValues[i], jsonPath)));
          }
          break;
        case FLOAT:
          float[] floatValues = transformFunction.transformToFloatValuesSV(_projectionBlock);
          for (int i = 0; i < NUM_ROWS; i++) {
            Assert.assertEquals(floatValues[i], Float.parseFloat(getValueForKey(_jsonSVValues[i], jsonPath)));
          }
          break;
        case DOUBLE:
          double[] doubleValues = transformFunction.transformToDoubleValuesSV(_projectionBlock);
          for (int i = 0; i < NUM_ROWS; i++) {
            Assert.assertEquals(doubleValues[i], Double.parseDouble(getValueForKey(_jsonSVValues[i], jsonPath)));
          }
          break;
        case BIG_DECIMAL:
          BigDecimal[] bigDecimalValues = transformFunction.transformToBigDecimalValuesSV(_projectionBlock);
          for (int i = 0; i < NUM_ROWS; i++) {
            Assert.assertEquals(bigDecimalValues[i], new BigDecimal(getValueForKey(_jsonSVValues[i], jsonPath)));
          }
          break;
        case STRING:
          String[] stringValues = transformFunction.transformToStringValuesSV(_projectionBlock);
          for (int i = 0; i < NUM_ROWS; i++) {
            Assert.assertEquals(stringValues[i], getValueForKey(_jsonSVValues[i], jsonPath));
          }
          break;
        default:
          throw new UnsupportedOperationException("Not support data type - " + resultsDataType);
      }
    }
  }

  @DataProvider(name = "testJsonExtractIndexTransformFunction")
  public Object[][] testJsonExtractIndexTransformFunctionDataProvider() {
    List<Object[]> testArguments = new ArrayList<>();
    // Without default value
    testArguments.add(new Object[]{
        String.format("jsonExtractIndex(%s,'%s','INT')", JSON_STRING_SV_COLUMN,
            "$.intVal"), "$.intVal", DataType.INT, true
    });
    testArguments.add(new Object[]{
        String.format("jsonExtractIndex(%s,'%s','LONG')", JSON_STRING_SV_COLUMN,
            "$.longVal"), "$.longVal", DataType.LONG, true
    });
    testArguments.add(new Object[]{
        String.format("jsonExtractIndex(%s,'%s','FLOAT')", JSON_STRING_SV_COLUMN,
            "$.floatVal"), "$.floatVal", DataType.FLOAT, true
    });
    testArguments.add(new Object[]{
        String.format("jsonExtractIndex(%s,'%s','DOUBLE')", JSON_STRING_SV_COLUMN,
            "$.doubleVal"), "$.doubleVal", DataType.DOUBLE, true
    });
    testArguments.add(new Object[]{
        String.format("jsonExtractIndex(%s,'%s','BIG_DECIMAL')", JSON_STRING_SV_COLUMN,
            "$.bigDecimalVal"), "$.bigDecimalVal", DataType.BIG_DECIMAL, true
    });
    testArguments.add(new Object[]{
        String.format("jsonExtractIndex(%s,'%s','STRING')", JSON_STRING_SV_COLUMN,
            "$.stringVal"), "$.stringVal", DataType.STRING, true
    });
    return testArguments.toArray(new Object[0][]);
  }

  @Test(dataProvider = "testJsonExtractIndexDefaultValue")
  public void testJsonExtractIndexDefaultValue(String expressionStr, String jsonPathString, DataType resultsDataType,
      boolean isSingleValue) {
    ExpressionContext expression = RequestContextUtils.getExpression(expressionStr);
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof JsonExtractIndexTransformFunction);
    Assert.assertEquals(transformFunction.getName(), JsonExtractIndexTransformFunction.FUNCTION_NAME);
    Assert.assertEquals(transformFunction.getResultMetadata().getDataType(), resultsDataType);
    Assert.assertEquals(transformFunction.getResultMetadata().isSingleValue(), isSingleValue);
    JsonPath jsonPath = JsonPathCache.INSTANCE.getOrCompute(jsonPathString);
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
            Assert.assertEquals(bigDecimalValues[i], BigDecimal.ZERO);
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
    }
  }

  @DataProvider(name = "testJsonExtractIndexDefaultValue")
  public Object[][] testJsonExtractIndexDefaultValueDataProvider() {
    List<Object[]> testArguments = new ArrayList<>();
    // With default value
    testArguments.add(new Object[]{
        String.format("jsonExtractIndex(%s,'%s','INT',0)", JSON_STRING_SV_COLUMN,
            "$.noField"), "$.noField", DataType.INT, true
    });
    testArguments.add(new Object[]{
        String.format("jsonExtractIndex(%s,'%s','LONG',0)", JSON_STRING_SV_COLUMN,
            "$.noField"), "$.noField", DataType.LONG, true
    });
    testArguments.add(new Object[]{
        String.format("jsonExtractIndex(%s,'%s','FLOAT',0)", JSON_STRING_SV_COLUMN,
            "$.noField"), "$.noField", DataType.FLOAT, true
    });
    testArguments.add(new Object[]{
        String.format("jsonExtractIndex(%s,'%s','DOUBLE',0)", JSON_STRING_SV_COLUMN,
            "$.noField"), "$.noField", DataType.DOUBLE, true
    });
    testArguments.add(new Object[]{
        String.format("jsonExtractIndex(%s,'%s','BIG_DECIMAL',0)", JSON_STRING_SV_COLUMN,
            "$.noField"), "$.noField", DataType.BIG_DECIMAL, true
    });
    testArguments.add(new Object[]{
        String.format("jsonExtractIndex(%s,'%s','STRING','null')", JSON_STRING_SV_COLUMN,
            "$.noField"), "$.noField", DataType.STRING, true
    });
    return testArguments.toArray(new Object[0][]);
  }

  // get value for key, excluding nested
  private String getValueForKey(String blob, JsonPath path) {
    Object out = JSON_PARSER_CONTEXT.parse(blob).read(path);
    if (out == null || out instanceof HashMap || out instanceof Object[]) {
      return null;
    }
    return out.toString();
  }
}
