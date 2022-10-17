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

import org.apache.pinot.common.request.context.LiteralContext;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.Assert;
import org.testng.annotations.Test;


public class LiteralTransformFunctionTest {

  @Test
  public void testLiteralTransformFunction() {
    Assert.assertEquals(LiteralTransformFunction.inferLiteralDataType("abc"), DataType.STRING);
    Assert.assertEquals(LiteralTransformFunction.inferLiteralDataType("123"), DataType.INT);
    Assert.assertEquals(LiteralTransformFunction.inferLiteralDataType("2147483649"), DataType.LONG);
    Assert.assertEquals(LiteralTransformFunction.inferLiteralDataType("1.2"), DataType.FLOAT);
    Assert.assertEquals(LiteralTransformFunction.inferLiteralDataType("41241241.2412"), DataType.DOUBLE);
    Assert.assertEquals(LiteralTransformFunction.inferLiteralDataType("1.7976931348623159e+308"), DataType.BIG_DECIMAL);
    Assert.assertEquals(LiteralTransformFunction.inferLiteralDataType("2020-02-02 20:20:20.20"), DataType.TIMESTAMP);
    LiteralTransformFunction trueBoolean = new LiteralTransformFunction(new LiteralContext(DataType.BOOLEAN, true));
    Assert.assertEquals(trueBoolean.getResultMetadata().getDataType(), DataType.BOOLEAN);
    Assert.assertEquals(trueBoolean.getLiteral(), "true");
    LiteralTransformFunction falseBoolean = new LiteralTransformFunction(new LiteralContext(DataType.BOOLEAN, false));
    Assert.assertEquals(falseBoolean.getResultMetadata().getDataType(), DataType.BOOLEAN);
    Assert.assertEquals(falseBoolean.getLiteral(), "false");
  }
}
