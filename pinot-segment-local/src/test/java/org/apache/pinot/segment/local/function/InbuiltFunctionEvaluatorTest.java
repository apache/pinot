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
package org.apache.pinot.segment.local.function;

import org.apache.pinot.common.function.FunctionUtils;
import org.apache.pinot.common.utils.PinotDataType;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


public class InbuiltFunctionEvaluatorTest {

  @Test
  public void booleanLiteralTest() {
    checkBooleanLiteralExpression("true", 1);
    checkBooleanLiteralExpression("false", 0);
    checkBooleanLiteralExpression("True", 1);
    checkBooleanLiteralExpression("False", 0);
    checkBooleanLiteralExpression("1", 1);
    checkBooleanLiteralExpression("0", 0);
  }

  private void checkBooleanLiteralExpression(String expression, int value) {
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator(expression);
    Object output = evaluator.evaluate(new GenericRow());
    Class<?> outputValueClass = output.getClass();
    PinotDataType outputType = FunctionUtils.getArgumentType(outputValueClass);
    assertNotNull(outputType);
    // as INT is the stored type for BOOLEAN
    assertEquals(outputType.toInt(output), value);
  }

  @Test
  public void integerLiteralTest() {
    checkIntegerLiteralExpression("1", 1);
  }

  private void checkIntegerLiteralExpression(String expression, int value) {
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator(expression);
    Object output = evaluator.evaluate(new GenericRow());
    Class<?> outputValueClass = output.getClass();
    PinotDataType outputType = FunctionUtils.getArgumentType(outputValueClass);
    assertNotNull(outputType);
    // as INT is the stored type for BOOLEAN
    assertEquals(outputType.toInt(output), value);
  }
}
