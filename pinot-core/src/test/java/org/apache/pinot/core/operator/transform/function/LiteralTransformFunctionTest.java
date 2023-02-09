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
import org.apache.pinot.common.request.context.NullSentinel;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.Assert;
import org.testng.annotations.Test;


public class LiteralTransformFunctionTest {

  @Test
  public void testLiteralTransformFunction() {
    LiteralTransformFunction trueBoolean = new LiteralTransformFunction(new LiteralContext(DataType.BOOLEAN, true));
    Assert.assertEquals(trueBoolean.getResultMetadata().getDataType(), DataType.BOOLEAN);
    Assert.assertEquals(trueBoolean.getLiteral(), true);
    LiteralTransformFunction falseBoolean = new LiteralTransformFunction(new LiteralContext(DataType.BOOLEAN, false));
    Assert.assertEquals(falseBoolean.getResultMetadata().getDataType(), DataType.BOOLEAN);
    Assert.assertEquals(falseBoolean.getLiteral(), false);
    LiteralTransformFunction nullLiteral = new LiteralTransformFunction(new LiteralContext(DataType.NULL, true));
    Assert.assertEquals(nullLiteral.getLiteral(), NullSentinel.INSTANCE);
  }
}
