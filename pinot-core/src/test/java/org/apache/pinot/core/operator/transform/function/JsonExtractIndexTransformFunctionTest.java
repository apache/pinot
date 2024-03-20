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
import com.jayway.jsonpath.TypeRef;
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
  private static final TypeRef<List<Integer>> INTEGER_LIST_TYPE = new TypeRef<List<Integer>>() {
  };
  private static final TypeRef<List<Long>> LONG_LIST_TYPE = new TypeRef<List<Long>>() {
  };
  private static final TypeRef<List<Float>> FLOAT_LIST_TYPE = new TypeRef<List<Float>>() {
  };
  private static final TypeRef<List<Double>> DOUBLE_LIST_TYPE = new TypeRef<List<Double>>() {
  };
  private static final TypeRef<List<String>> STRING_LIST_TYPE = new TypeRef<List<String>>() {
  };

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
    } else {
      switch (resultsDataType) {
        case INT:
          int[][] intValues = transformFunction.transformToIntValuesMV(_projectionBlock);
          for (int i = 0; i < NUM_ROWS; i++) {
            List<Integer> values = getValueForKey(_jsonSVValues[i], jsonPath, INTEGER_LIST_TYPE);
            Assert.assertEquals(intValues[i].length, values.size());
            for (int j = 0; j < intValues[i].length; j++) {
              Assert.assertEquals(intValues[i][j], values.get(j));
            }
          }
          break;
        case LONG:
          long[][] longValues = transformFunction.transformToLongValuesMV(_projectionBlock);
          for (int i = 0; i < NUM_ROWS; i++) {
            List<Long> values = getValueForKey(_jsonSVValues[i], jsonPath, LONG_LIST_TYPE);
            Assert.assertEquals(longValues[i].length, values.size());
            for (int j = 0; j < longValues[i].length; j++) {
              Assert.assertEquals(longValues[i][j], values.get(j));
            }
          }
          break;
        case FLOAT:
          float[][] floatValues = transformFunction.transformToFloatValuesMV(_projectionBlock);
          for (int i = 0; i < NUM_ROWS; i++) {
            List<Float> values = getValueForKey(_jsonSVValues[i], jsonPath, FLOAT_LIST_TYPE);
            Assert.assertEquals(floatValues[i].length, values.size());
            for (int j = 0; j < floatValues[i].length; j++) {
              Assert.assertEquals(floatValues[i][j], values.get(j));
            }
          }
          break;
        case DOUBLE:
          double[][] doubleValues = transformFunction.transformToDoubleValuesMV(_projectionBlock);
          for (int i = 0; i < NUM_ROWS; i++) {
            List<Double> values = getValueForKey(_jsonSVValues[i], jsonPath, DOUBLE_LIST_TYPE);
            Assert.assertEquals(doubleValues[i].length, values.size());
            for (int j = 0; j < doubleValues[i].length; j++) {
              Assert.assertEquals(doubleValues[i][j], values.get(j));
            }
          }
          break;
        case STRING:
          String[][] stringValues = transformFunction.transformToStringValuesMV(_projectionBlock);
          for (int i = 0; i < NUM_ROWS; i++) {
            List<String> values = getValueForKey(_jsonSVValues[i], jsonPath, STRING_LIST_TYPE);
            Assert.assertEquals(stringValues[i].length, values.size());
            for (int j = 0; j < stringValues[i].length; j++) {
              Assert.assertEquals(stringValues[i][j], values.get(j));
            }
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

    testArguments.add(new Object[]{
        String.format("jsonExtractIndex(%s,'%s','INT')", JSON_STRING_SV_COLUMN,
            "$.intVals[0]"), "$.intVals[0]", DataType.INT, true
    });
    testArguments.add(new Object[]{
        String.format("jsonExtractIndex(%s,'%s','LONG')", JSON_STRING_SV_COLUMN,
            "$.longVals[1]"), "$.longVals[1]", DataType.LONG, true
    });
    testArguments.add(new Object[]{
        String.format("jsonExtractIndex(%s,'%s','FLOAT')", JSON_STRING_SV_COLUMN,
            "$.floatVals[0]"), "$.floatVals[0]", DataType.FLOAT, true
    });
    testArguments.add(new Object[]{
        String.format("jsonExtractIndex(%s,'%s','DOUBLE')", JSON_STRING_SV_COLUMN,
            "$.doubleVals[1]"), "$.doubleVals[1]", DataType.DOUBLE, true
    });
    testArguments.add(new Object[]{
        String.format("jsonExtractIndex(%s,'%s','BIG_DECIMAL')", JSON_STRING_SV_COLUMN,
            "$.bigDecimalVals[0]"), "$.bigDecimalVals[0]", DataType.BIG_DECIMAL, true
    });
    testArguments.add(new Object[]{
        String.format("jsonExtractIndex(%s,'%s','STRING')", JSON_STRING_SV_COLUMN,
            "$.stringVals[1]"), "$.stringVals[1]", DataType.STRING, true
    });



    // MV tests
    testArguments.add(new Object[]{
        String.format("jsonExtractIndex(%s,'%s','INT_ARRAY')", JSON_STRING_SV_COLUMN,
            "$.intVals[*]"), "$.intVals[*]", DataType.INT, false
    });
    testArguments.add(new Object[]{
        String.format("jsonExtractIndex(%s,'%s','LONG_ARRAY')", JSON_STRING_SV_COLUMN,
            "$.longVals[*]"), "$.longVals[*]", DataType.LONG, false
    });
    testArguments.add(new Object[]{
        String.format("jsonExtractIndex(%s,'%s','DOUBLE_ARRAY')", JSON_STRING_SV_COLUMN,
            "$.doubleVals[*]"), "$.doubleVals[*]", DataType.DOUBLE, false
    });
    testArguments.add(new Object[]{
        String.format("jsonExtractIndex(%s,'%s','STRING_ARRAY')", JSON_STRING_SV_COLUMN,
            "$.stringVals[*]"), "$.stringVals[*]", DataType.STRING, false
    });
    testArguments.add(new Object[]{
        String.format("jsonExtractIndex(%s,'%s','INT_ARRAY')", JSON_STRING_SV_COLUMN,
            "$.arrayField[*].arrIntField"), "$.arrayField[*].arrIntField", DataType.INT, false
    });
    testArguments.add(new Object[]{
        String.format("jsonExtractIndex(%s,'%s','STRING_ARRAY')", JSON_STRING_SV_COLUMN,
            "$.arrayField[*].arrStringField"), "$.arrayField[*].arrStringField", DataType.STRING, false
    });

    // MV with filters
    testArguments.add(new Object[]{
        String.format(
            "jsonExtractIndex(%s,'%s','INT_ARRAY', '0', 'REGEXP_LIKE(\"$.arrayField[*].arrStringField\", ''.*y.*'')')",
            JSON_STRING_SV_COLUMN,
            "$.arrayField[*].arrIntField"), "$.arrayField[?(@.arrStringField =~ /.*y.*/)].arrIntField", DataType.INT,
        false
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

  private <T> T getValueForKey(String blob, JsonPath path, TypeRef<T> typeRef) {
    return JSON_PARSER_CONTEXT.parse(blob).read(path, typeRef);
  }
}
