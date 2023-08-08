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
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.spi.data.FieldSpec;
import org.roaringbitmap.RoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


public class NotOperatorTransformFunctionTest extends BaseTransformFunctionTest {

  // Test not true returns false.
  @Test
  public void testNotTrueOperatorTransformFunction() {
    String expression = String.format("Not (%d = %d)", _intSVValues[0], _intSVValues[0]);
    boolean[] expectedValues = new boolean[NUM_ROWS];
    Arrays.fill(expectedValues, false);
    testTransformFunction(expression, expectedValues);
  }

  // Test not false returns true.
  @Test
  public void testNotFalseOperatorTransformFunction() {
    String expression = String.format("Not (%d != %d)", _intSVValues[0], _intSVValues[0]);
    boolean[] expectedValues = new boolean[NUM_ROWS];
    Arrays.fill(expectedValues, true);
    testTransformFunction(expression, expectedValues);
  }

  // Test that not operator also works for not literal
  @Test
  public void testNonLiteralSupport() {
    String expression = String.format("Not (%s != %d)", INT_SV_COLUMN, _intSVValues[0]);
    boolean[] expectedValues = new boolean[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = _intSVValues[i] == _intSVValues[0];
    }
    testTransformFunction(expression, expectedValues);
  }

  private void testTransformFunction(String expression, boolean[] expectedValues) {
    TransformFunction transformFunction =
        TransformFunctionFactory.get(RequestContextUtils.getExpression(expression), _dataSourceMap);
    assertEquals(transformFunction.getName(), TransformFunctionType.NOT.getName());
    TransformResultMetadata resultMetadata = transformFunction.getResultMetadata();
    assertEquals(resultMetadata.getDataType(), FieldSpec.DataType.BOOLEAN);
    assertTrue(resultMetadata.isSingleValue());
    assertFalse(resultMetadata.hasDictionary());
    testTransformFunction(transformFunction, expectedValues);
  }

  // Test illegal arguments for not transform.
  @Test
  public void testIllegalNotOperatorTransformFunction() {
    // Wrong argument type.
    ExpressionContext exprWrongType = RequestContextUtils.getExpression(String.format("Not(%s)", "test"));
    assertThrows(RuntimeException.class, () -> {
      TransformFunctionFactory.get(exprWrongType, _dataSourceMap);
    });
    // Wrong number of arguments.
    ExpressionContext exprNumArg =
        RequestContextUtils.getExpression(String.format("Not(%d, %d)", _intSVValues[0], _intSVValues[1]));
    assertThrows(RuntimeException.class, () -> {
      TransformFunctionFactory.get(exprNumArg, _dataSourceMap);
    });
  }

  @Test
  public void testNotNullLiteral() {
    ExpressionContext expression = RequestContextUtils.getExpression(String.format("Not(null)", INT_SV_NULL_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof NotOperatorTransformFunction);
    Assert.assertEquals(transformFunction.getName(), "not");
    int[] expectedValues = new int[NUM_ROWS];
    RoaringBitmap roaringBitmap = new RoaringBitmap();
    roaringBitmap.add(0L, NUM_ROWS);
    testTransformFunctionWithNull(transformFunction, expectedValues, roaringBitmap);
  }

  @Test
  public void testNotNullColumn() {
    ExpressionContext expression = RequestContextUtils.getExpression(String.format("Not(%s)", INT_SV_NULL_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    Assert.assertTrue(transformFunction instanceof NotOperatorTransformFunction);
    Assert.assertEquals(transformFunction.getName(), "not");
    int[] expectedValues = new int[NUM_ROWS];
    RoaringBitmap roaringBitmap = new RoaringBitmap();
    for (int i = 0; i < NUM_ROWS; i++) {
      if (isNullRow(i)) {
        roaringBitmap.add(i);
      } else {
        expectedValues[i] = _intSVValues[i] == 0 ? 1 : 0;
      }
    }
    testTransformFunctionWithNull(transformFunction, expectedValues, roaringBitmap);
  }
}
