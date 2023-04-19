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
package org.apache.pinot.common.request.context;

import java.math.BigDecimal;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.spi.data.FieldSpec;
import org.testng.Assert;
import org.testng.annotations.Test;


public class LiteralContextTest {
  @Test
  public void testNullLiteralContext() {
    // Create literal context from thrift
    Literal literal = new Literal();
    literal.setNullValue(true);
    LiteralContext nullContext1 = new LiteralContext(literal);
    Assert.assertEquals(nullContext1.getValue(), null);
    Assert.assertEquals(nullContext1.toString(), "'null'");
    Assert.assertEquals(nullContext1.getBigDecimalValue(), BigDecimal.ZERO);
    // Create literal context from object and type
    LiteralContext nullContext2 = new LiteralContext(FieldSpec.DataType.UNKNOWN, null);
    Assert.assertEquals(nullContext2.getValue(), null);
    Assert.assertEquals(nullContext2.toString(), "'null'");
    Assert.assertEquals(nullContext2.getBigDecimalValue(), BigDecimal.ZERO);
    // Check different literal objects are equal and have same hash code.
    Assert.assertTrue(nullContext1.equals(nullContext2));
    Assert.assertTrue(nullContext1.hashCode() == nullContext2.hashCode());
  }

  @Test
  public void testInferDataType(){
    Assert.assertEquals(LiteralContext.inferLiteralDataTypeAndValue("abc").getLeft(), FieldSpec.DataType.STRING);
    Assert.assertEquals(LiteralContext.inferLiteralDataTypeAndValue("123").getLeft(), FieldSpec.DataType.STRING);
    Assert.assertEquals(LiteralContext.inferLiteralDataTypeAndValue("2147483649").getLeft(), FieldSpec.DataType.STRING);
    Assert.assertEquals(LiteralContext.inferLiteralDataTypeAndValue("1.2").getLeft(), FieldSpec.DataType.STRING);
    Assert.assertEquals(LiteralContext.inferLiteralDataTypeAndValue("41241241.2412").getLeft(),
        FieldSpec.DataType.STRING);
    // Only big decimal is allowed to pass in as string for numerical type.
    Assert.assertEquals(LiteralContext.inferLiteralDataTypeAndValue("1.7976931348623159e+308").getLeft(),
        FieldSpec.DataType.BIG_DECIMAL);
    Assert.assertEquals(LiteralContext.inferLiteralDataTypeAndValue("2020-02-02 20:20:20.20").getLeft(),
        FieldSpec.DataType.TIMESTAMP);
  }

  @Test
  public void testInferFloatDataType() {
    Literal literalFloat = new Literal();
    literalFloat.setDoubleValue(1.23);
    LiteralContext floatContext = new LiteralContext(literalFloat);
    Assert.assertEquals(floatContext.getType(), FieldSpec.DataType.FLOAT);
    Assert.assertEquals(floatContext.getValue(), (float) literalFloat.getDoubleValue());
  }

  @Test
  public void testInferDoubleDataType() {
    Literal literalFloat = new Literal();
    literalFloat.setDoubleValue(1.234567891011);
    LiteralContext doubleContext = new LiteralContext(literalFloat);
    Assert.assertEquals(doubleContext.getType(), FieldSpec.DataType.DOUBLE);
    Assert.assertEquals(doubleContext.getValue(), literalFloat.getDoubleValue());
  }

  @Test
  public void testInferIntDataType() {
    Literal literalInt = new Literal();
    literalInt.setLongValue(123);
    LiteralContext floatContext = new LiteralContext(literalInt);
    Assert.assertEquals(floatContext.getType(), FieldSpec.DataType.INT);
    Assert.assertEquals(floatContext.getValue(), (int) literalInt.getLongValue());
  }

  @Test
  public void testInferLongDataType() {
    Literal literalLong = new Literal();
    literalLong.setLongValue(Integer.MAX_VALUE + 1L);
    LiteralContext longContext = new LiteralContext(literalLong);
    Assert.assertEquals(longContext.getType(), FieldSpec.DataType.LONG);
    Assert.assertEquals(longContext.getValue(), literalLong.getLongValue());
  }
}
