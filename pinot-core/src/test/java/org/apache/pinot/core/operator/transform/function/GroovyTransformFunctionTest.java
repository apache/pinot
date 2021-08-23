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

import com.google.common.base.Joiner;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.IntSummaryStatistics;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Tests the GROOVY transform function
 */
public class GroovyTransformFunctionTest extends BaseTransformFunctionTest {

  @DataProvider(name = "groovyFunctionDataProvider")
  public Object[][] groovyFunctionDataProvider() {

    String groovyTransformFunction;
    List<Object[]> inputs = new ArrayList<>();

    // max in array (returns SV INT)
    groovyTransformFunction = String
        .format("groovy('{\"returnType\":\"INT\", \"isSingleValue\":true}', 'arg0.toList().max()', %s)",
            INT_MV_COLUMN);
    int[] expectedResult1 = new int[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedResult1[i] = Arrays.stream(_intMVValues[i]).max().getAsInt();
    }
    inputs.add(new Object[]{groovyTransformFunction, FieldSpec.DataType.INT, true, expectedResult1});

    // simple addition (returns SV LONG)
    groovyTransformFunction = String
        .format("groovy('{\"returnType\":\"LONG\", \"isSingleValue\":true}', 'arg0 + arg1', %s, %s)",
            INT_SV_COLUMN, LONG_SV_COLUMN);
    long[] expectedResult2 = new long[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedResult2[i] = _intSVValues[i] + _longSVValues[i];
    }
    inputs.add(new Object[]{groovyTransformFunction, FieldSpec.DataType.LONG, true, expectedResult2});

    // minimum of 2 numbers (returns SV DOUBLE)
    groovyTransformFunction = String.format(
        "groovy('{\"returnType\":\"DOUBLE\", \"isSingleValue\":true}', 'Math.min(arg0, arg1)', %s, %s)",
        DOUBLE_SV_COLUMN, INT_SV_COLUMN);
    double[] expectedResult3 = new double[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedResult3[i] = Math.min(_intSVValues[i], _doubleSVValues[i]);
    }
    inputs.add(new Object[]{groovyTransformFunction, FieldSpec.DataType.DOUBLE, true, expectedResult3});

    // (returns SV FLOAT)
    groovyTransformFunction = String.format("groovy('{\"returnType\":\"FLOAT\", \"isSingleValue\":true}', "
        + "'def result; switch(arg0.length()) { case 10: result = 1.1; break; case 20: result = 1.2; break; default: "
        + "result = 1.3;}; return result.floatValue()', %s)", STRING_ALPHANUM_SV_COLUMN);
    float[] expectedResult4 = new float[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedResult4[i] =
          _stringAlphaNumericSVValues.length == 10 ? 1.1f : (_stringAlphaNumericSVValues.length == 20 ? 1.2f : 1.3f);
    }
    inputs.add(new Object[]{groovyTransformFunction, FieldSpec.DataType.FLOAT, true, expectedResult4});

    // string operations (returns SV STRING)
    groovyTransformFunction = String.format(
        "groovy('{\"returnType\":\"STRING\", \"isSingleValue\":true}', '[arg0, arg1, arg2].join(\"_\")', "
            + "%s, %s, %s)", FLOAT_SV_COLUMN, STRING_SV_COLUMN, DOUBLE_SV_COLUMN);
    String[] expectedResult5 = new String[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedResult5[i] = Joiner.on("_").join(_floatSVValues[i], _stringSVValues[i], _doubleSVValues[i]);
    }
    inputs.add(new Object[]{groovyTransformFunction, FieldSpec.DataType.STRING, true, expectedResult5});

    // find all in array that match (returns MV INT)
    groovyTransformFunction = String
        .format("groovy('{\"returnType\":\"INT\", \"isSingleValue\":false}', 'arg0.findAll{it < 5}', %s)",
            INT_MV_COLUMN);
    int[][] expectedResult6 = new int[NUM_ROWS][];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedResult6[i] = Arrays.stream(_intMVValues[i]).filter(e -> e < 5).toArray();
    }
    inputs.add(new Object[]{groovyTransformFunction, FieldSpec.DataType.INT, false, expectedResult6});

    // (returns MV LONG)
    groovyTransformFunction = String.format(
        "groovy('{\"returnType\":\"LONG\", \"isSingleValue\":false}', 'arg0.findIndexValues{it == 5}', %s)",
        INT_MV_COLUMN);
    long[][] expectedResult7 = new long[NUM_ROWS][];
    for (int i = 0; i < NUM_ROWS; i++) {
      int[] intMVValue = _intMVValues[i];
      expectedResult7[i] =
          IntStream.range(0, intMVValue.length).filter(e -> intMVValue[e] == 5).mapToLong(e -> (long) e).toArray();
    }
    inputs.add(new Object[]{groovyTransformFunction, FieldSpec.DataType.LONG, false, expectedResult7});

    // no-args function (returns MV STRING)
    groovyTransformFunction = "groovy('{\"returnType\":\"STRING\", \"isSingleValue\":false}', '[\"foo\", \"bar\"]')";
    String[][] expectedResult8 = new String[NUM_ROWS][];
    Arrays.fill(expectedResult8, new String[]{"foo", "bar"});
    inputs.add(new Object[]{groovyTransformFunction, FieldSpec.DataType.STRING, false, expectedResult8});

    // nested groovy functions
    String groovy1 = String
        .format("groovy('{\"returnType\":\"INT\", \"isSingleValue\":true}', 'arg0.toList().max()', %s)", INT_MV_COLUMN);
    String groovy2 = String
        .format("groovy('{\"returnType\":\"INT\", \"isSingleValue\":true}', 'arg0.toList().min()', %s)", INT_MV_COLUMN);
    groovyTransformFunction = String
        .format("groovy('{\"returnType\":\"INT\", \"isSingleValue\":false}', '[arg0, arg1, arg2.sum()]', %s, %s, %s)",
            groovy1, groovy2, INT_MV_COLUMN);
    int[][] expectedResult9 = new int[NUM_ROWS][];
    for (int i = 0; i < NUM_ROWS; i++) {
      IntSummaryStatistics stats = Arrays.stream(_intMVValues[i]).summaryStatistics();
      expectedResult9[i] = new int[]{stats.getMax(), stats.getMin(), (int) stats.getSum()};
    }
    inputs.add(new Object[]{groovyTransformFunction, FieldSpec.DataType.INT, false, expectedResult9});

    // nested with other functions
    groovyTransformFunction = String
        .format("groovy('{\"returnType\":\"INT\", \"isSingleValue\":true}', 'arg0 + arg1', %s, arraylength(%s))",
            INT_SV_COLUMN, INT_MV_COLUMN);
    int[] expectedResult10 = new int[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedResult10[i] = _intSVValues[i] + _intMVValues[i].length;
    }
    inputs.add(new Object[]{groovyTransformFunction, FieldSpec.DataType.INT, true, expectedResult10});

    return inputs.toArray(new Object[0][]);
  }

  @Test(dataProvider = "groovyFunctionDataProvider")
  public void testGroovyTransformFunctions(String expressionStr, FieldSpec.DataType resultType,
      boolean isResultSingleValue, Object expectedResult) {
    ExpressionContext expression = RequestContextUtils.getExpressionFromSQL(expressionStr);
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof GroovyTransformFunction);
    Assert.assertEquals(transformFunction.getName(), GroovyTransformFunction.FUNCTION_NAME);
    Assert.assertEquals(transformFunction.getResultMetadata().getDataType(), resultType);
    Assert.assertEquals(transformFunction.getResultMetadata().isSingleValue(), isResultSingleValue);
    Assert.assertFalse(transformFunction.getResultMetadata().hasDictionary());

    if (isResultSingleValue) {
      switch (resultType) {
        case INT:
          int[] intResults = transformFunction.transformToIntValuesSV(_projectionBlock);
          int[] expectedInts = (int[]) expectedResult;
          for (int i = 0; i < NUM_ROWS; i++) {
            Assert.assertEquals(intResults[i], expectedInts[i]);
          }
          break;
        case LONG:
          long[] longResults = transformFunction.transformToLongValuesSV(_projectionBlock);
          long[] expectedLongs = (long[]) expectedResult;
          for (int i = 0; i < NUM_ROWS; i++) {
            Assert.assertEquals(longResults[i], expectedLongs[i]);
          }
          break;
        case FLOAT:
          float[] floatResults = transformFunction.transformToFloatValuesSV(_projectionBlock);
          float[] expectedFloats = (float[]) expectedResult;
          for (int i = 0; i < NUM_ROWS; i++) {
            Assert.assertEquals(floatResults[i], expectedFloats[i]);
          }
          break;
        case DOUBLE:
          double[] doubleResults = transformFunction.transformToDoubleValuesSV(_projectionBlock);
          double[] expectedDoubles = (double[]) expectedResult;
          for (int i = 0; i < NUM_ROWS; i++) {
            Assert.assertEquals(doubleResults[i], expectedDoubles[i]);
          }
          break;
        case STRING:
          String[] stringResults = transformFunction.transformToStringValuesSV(_projectionBlock);
          String[] expectedStrings = (String[]) expectedResult;
          for (int i = 0; i < NUM_ROWS; i++) {
            Assert.assertEquals(stringResults[i], expectedStrings[i]);
          }
          break;
        default:
          break;
      }
    } else {
      switch (resultType) {

        case INT:
          int[][] intResults = transformFunction.transformToIntValuesMV(_projectionBlock);
          int[][] expectedInts = (int[][]) expectedResult;
          for (int i = 0; i < NUM_ROWS; i++) {
            Assert.assertEquals(intResults[i], expectedInts[i]);
          }
          break;
        case LONG:
          long[][] longResults = transformFunction.transformToLongValuesMV(_projectionBlock);
          long[][] expectedLongs = (long[][]) expectedResult;
          for (int i = 0; i < NUM_ROWS; i++) {
            Assert.assertEquals(longResults[i], expectedLongs[i]);
          }
          break;
        case FLOAT:
          float[][] floatResults = transformFunction.transformToFloatValuesMV(_projectionBlock);
          float[][] expectedFloats = (float[][]) expectedResult;
          for (int i = 0; i < NUM_ROWS; i++) {
            Assert.assertEquals(floatResults[i], expectedFloats[i]);
          }
          break;
        case DOUBLE:
          double[][] doubleResults = transformFunction.transformToDoubleValuesMV(_projectionBlock);
          double[][] expectedDoubles = (double[][]) expectedResult;
          for (int i = 0; i < NUM_ROWS; i++) {
            Assert.assertEquals(doubleResults[i], expectedDoubles[i]);
          }
          break;
        case STRING:
          String[][] stringResults = transformFunction.transformToStringValuesMV(_projectionBlock);
          String[][] expectedStrings = (String[][]) expectedResult;
          for (int i = 0; i < NUM_ROWS; i++) {
            Assert.assertEquals(stringResults[i], expectedStrings[i]);
          }
          break;
        default:
          break;
      }
    }
  }

  @Test(dataProvider = "testIllegalArguments", expectedExceptions = {BadQueryRequestException.class})
  public void testIllegalArguments(String expressionStr) {
    ExpressionContext expression = RequestContextUtils.getExpressionFromSQL(expressionStr);
    TransformFunctionFactory.get(expression, _dataSourceMap);
  }

  @DataProvider(name = "testIllegalArguments")
  public Object[][] testIllegalArguments() {
    List<Object[]> inputs = new ArrayList<>();
    // incorrect number of arguments
    inputs.add(new Object[]{String.format("groovy(%s)", STRING_SV_COLUMN)});
    // first argument must be literal
    inputs.add(new Object[]{String.format("groovy(%s, %s)", DOUBLE_SV_COLUMN, STRING_SV_COLUMN)});
    // second argument must be a literal
    inputs.add(new Object[]{String.format("groovy('arg0 + 10', %s)", STRING_SV_COLUMN)});
    // first argument must be a valid json
    inputs.add(new Object[]{String.format("groovy(']]', 'arg0 + 10', %s)", STRING_SV_COLUMN)});
    // first argument json must contain non-null key returnType
    inputs.add(new Object[]{String.format("groovy('{\"isSingleValue\":true}', 'arg0 + 10', %s)", INT_SV_COLUMN)});
    inputs.add(new Object[]{
        String.format("groovy('{\"returnType\":null, \"isSingleValue\":true}', 'arg0 + 10', %s)", INT_SV_COLUMN)
    });
    // first argument json must contain non-null key isSingleValue
    inputs.add(new Object[]{String.format("groovy('{\"returnType\":\"INT\"}', 'arg0 + 10', %s)", INT_SV_COLUMN)});
    inputs.add(new Object[]{
        String.format("groovy('{\"returnType\":\"INT\", \"isSingleValue\":null}', 'arg0 + 10', %s)", INT_SV_COLUMN)
    });
    // return type must be valid DataType enum
    inputs.add(new Object[]{
        String.format("groovy('{\"returnType\":\"foo\", \"isSingleValue\":true}', 'arg0 + 10', %s)", INT_SV_COLUMN)
    });
    // arguments must be columns/transform functions
    inputs.add(new Object[]{"groovy('{\"returnType\":\"INT\", \"isSingleValue\":true}', 'arg0 + 10', 'foo')"});
    inputs.add(new Object[]{
        String.format(
            "groovy('{\"returnType\":\"INT\", \"isSingleValue\":true}', 'arg0 + arg1 + 10', 'arraylength(colB)', %s)",
            INT_SV_COLUMN)
    });
    // invalid groovy expression
    inputs.add(new Object[]{"groovy('{\"returnType\":\"INT\"}', '+-+')"});
    inputs.add(new Object[]{
        String.format("groovy('{\"returnType\":\"INT\"}', '+-+arg0 arg1', %s, %s)", INT_SV_COLUMN, DOUBLE_SV_COLUMN)
    });
    return inputs.toArray(new Object[0][]);
  }
}
