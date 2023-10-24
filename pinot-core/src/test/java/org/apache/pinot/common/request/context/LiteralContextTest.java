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
import java.sql.Timestamp;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.CommonConstants.NullValuePlaceHolder;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class LiteralContextTest {

  @Test
  public void testNullLiteralContext() {
    // Create literal context from thrift
    Literal literal = new Literal();
    literal.setNullValue(true);
    LiteralContext nullContext1 = new LiteralContext(literal);
    assertEquals(nullContext1.getType(), DataType.UNKNOWN);
    assertNull(nullContext1.getValue());
    assertFalse(nullContext1.getBooleanValue());
    assertEquals(nullContext1.getIntValue(), NullValuePlaceHolder.INT);
    assertEquals(nullContext1.getLongValue(), NullValuePlaceHolder.LONG);
    assertEquals(nullContext1.getFloatValue(), NullValuePlaceHolder.FLOAT);
    assertEquals(nullContext1.getDoubleValue(), NullValuePlaceHolder.DOUBLE);
    assertEquals(nullContext1.getBigDecimalValue(), NullValuePlaceHolder.BIG_DECIMAL);
    assertEquals(nullContext1.getStringValue(), NullValuePlaceHolder.STRING);
    assertEquals(nullContext1.getBytesValue(), NullValuePlaceHolder.BYTES);
    assertTrue(nullContext1.isNull());
    assertEquals(nullContext1.toString(), "'null'");

    // Create literal context from type and value
    LiteralContext nullContext2 = new LiteralContext(DataType.UNKNOWN, null);
    assertEquals(nullContext2, nullContext1);
    assertEquals(nullContext2.hashCode(), nullContext1.hashCode());
  }

  @Test
  public void testBooleanLiteral() {
    LiteralContext literalContext = new LiteralContext(DataType.BOOLEAN, true);
    assertTrue(literalContext.getBooleanValue());
    assertEquals(literalContext.getIntValue(), 1);
    assertEquals(literalContext.getLongValue(), 1L);
    assertEquals(literalContext.getFloatValue(), 1.0f);
    assertEquals(literalContext.getDoubleValue(), 1.0);
    assertEquals(literalContext.getBigDecimalValue(), BigDecimal.ONE);
    assertEquals(literalContext.getStringValue(), "true");
    assertThrows(literalContext::getBytesValue);
    assertFalse(literalContext.isNull());
    assertEquals(literalContext.toString(), "'true'");

    literalContext = new LiteralContext(DataType.BOOLEAN, false);
    assertFalse(literalContext.getBooleanValue());
    assertEquals(literalContext.getIntValue(), 0);
    assertEquals(literalContext.getLongValue(), 0L);
    assertEquals(literalContext.getFloatValue(), 0.0f);
    assertEquals(literalContext.getDoubleValue(), 0.0);
    assertEquals(literalContext.getBigDecimalValue(), BigDecimal.ZERO);
    assertEquals(literalContext.getStringValue(), "false");
    assertThrows(literalContext::getBytesValue);
    assertFalse(literalContext.isNull());
    assertEquals(literalContext.toString(), "'false'");
  }

  @Test
  public void testIntLiteral() {
    LiteralContext literalContext = new LiteralContext(DataType.INT, 123);
    assertTrue(literalContext.getBooleanValue());
    assertEquals(literalContext.getIntValue(), 123);
    assertEquals(literalContext.getLongValue(), 123L);
    assertEquals(literalContext.getFloatValue(), 123.0f);
    assertEquals(literalContext.getDoubleValue(), 123.0);
    assertEquals(literalContext.getBigDecimalValue(), new BigDecimal("123"));
    assertEquals(literalContext.getStringValue(), "123");
    assertThrows(literalContext::getBytesValue);
    assertFalse(literalContext.isNull());
    assertEquals(literalContext.toString(), "'123'");

    assertFalse(new LiteralContext(DataType.INT, 0).getBooleanValue());
  }

  @Test
  public void testLongLiteral() {
    LiteralContext literalContext = new LiteralContext(DataType.LONG, 123L);
    assertTrue(literalContext.getBooleanValue());
    assertEquals(literalContext.getIntValue(), 123);
    assertEquals(literalContext.getLongValue(), 123L);
    assertEquals(literalContext.getFloatValue(), 123.0f);
    assertEquals(literalContext.getDoubleValue(), 123.0);
    assertEquals(literalContext.getBigDecimalValue(), new BigDecimal("123"));
    assertEquals(literalContext.getStringValue(), "123");
    assertThrows(literalContext::getBytesValue);
    assertFalse(literalContext.isNull());
    assertEquals(literalContext.toString(), "'123'");

    assertFalse(new LiteralContext(DataType.LONG, 0L).getBooleanValue());
  }

  @Test
  public void testFloatLiteral() {
    LiteralContext literalContext = new LiteralContext(DataType.FLOAT, 123.45f);
    assertTrue(literalContext.getBooleanValue());
    assertEquals(literalContext.getIntValue(), 123);
    assertEquals(literalContext.getLongValue(), 123L);
    assertEquals(literalContext.getFloatValue(), 123.45f);
    assertEquals(literalContext.getDoubleValue(), (double) 123.45f);
    assertEquals(literalContext.getBigDecimalValue(), new BigDecimal("123.45"));
    assertEquals(literalContext.getStringValue(), "123.45");
    assertThrows(literalContext::getBytesValue);
    assertFalse(literalContext.isNull());
    assertEquals(literalContext.toString(), "'123.45'");

    assertFalse(new LiteralContext(DataType.FLOAT, 0.0f).getBooleanValue());
  }

  @Test
  public void testDoubleLiteral() {
    LiteralContext literalContext = new LiteralContext(DataType.DOUBLE, 123.45);
    assertTrue(literalContext.getBooleanValue());
    assertEquals(literalContext.getIntValue(), 123);
    assertEquals(literalContext.getLongValue(), 123L);
    assertEquals(literalContext.getFloatValue(), (float) 123.45);
    assertEquals(literalContext.getDoubleValue(), 123.45);
    assertEquals(literalContext.getBigDecimalValue(), new BigDecimal("123.45"));
    assertEquals(literalContext.getStringValue(), "123.45");
    assertThrows(literalContext::getBytesValue);
    assertFalse(literalContext.isNull());
    assertEquals(literalContext.toString(), "'123.45'");

    assertFalse(new LiteralContext(DataType.DOUBLE, 0.0).getBooleanValue());
  }

  @Test
  public void testBigDecimalLiteral() {
    LiteralContext literalContext = new LiteralContext(DataType.BIG_DECIMAL, new BigDecimal("123.45"));
    assertTrue(literalContext.getBooleanValue());
    assertEquals(literalContext.getIntValue(), 123);
    assertEquals(literalContext.getLongValue(), 123L);
    assertEquals(literalContext.getFloatValue(), 123.45f);
    assertEquals(literalContext.getDoubleValue(), 123.45);
    assertEquals(literalContext.getBigDecimalValue(), new BigDecimal("123.45"));
    assertEquals(literalContext.getStringValue(), "123.45");
    assertEquals(literalContext.getBytesValue(), BigDecimalUtils.serialize(new BigDecimal("123.45")));
    assertFalse(literalContext.isNull());
    assertEquals(literalContext.toString(), "'123.45'");

    assertFalse(new LiteralContext(DataType.BIG_DECIMAL, BigDecimal.ZERO).getBooleanValue());
  }

  @Test
  public void testStringLiteral() {
    LiteralContext literalContext = new LiteralContext(DataType.STRING, "123");
    assertFalse(literalContext.getBooleanValue());
    assertEquals(literalContext.getIntValue(), 123);
    assertEquals(literalContext.getLongValue(), 123L);
    assertEquals(literalContext.getFloatValue(), 123.0f);
    assertEquals(literalContext.getDoubleValue(), 123.0);
    assertEquals(literalContext.getBigDecimalValue(), new BigDecimal("123"));
    assertEquals(literalContext.getStringValue(), "123");
    assertThrows(literalContext::getBytesValue);
    assertFalse(literalContext.isNull());
    assertEquals(literalContext.toString(), "'123'");

    // Parse string as BOOLEAN
    assertTrue(new LiteralContext(DataType.STRING, "true").getBooleanValue());
    assertTrue(new LiteralContext(DataType.STRING, "TRUE").getBooleanValue());
    assertTrue(new LiteralContext(DataType.STRING, "1").getBooleanValue());
    assertFalse(new LiteralContext(DataType.STRING, "false").getBooleanValue());
    assertFalse(new LiteralContext(DataType.STRING, "FALSE").getBooleanValue());
    assertFalse(new LiteralContext(DataType.STRING, "0").getBooleanValue());
    assertFalse(new LiteralContext(DataType.STRING, "foo").getBooleanValue());

    // Parse string as INT
    assertEquals(new LiteralContext(DataType.STRING, "true").getIntValue(), 1);
    assertEquals(new LiteralContext(DataType.STRING, "TRUE").getIntValue(), 1);
    assertEquals(new LiteralContext(DataType.STRING, "false").getIntValue(), 0);
    assertEquals(new LiteralContext(DataType.STRING, "FALSE").getIntValue(), 0);
    assertThrows(() -> new LiteralContext(DataType.STRING, "123.45").getIntValue());

    // Parse string as LONG
    assertEquals(new LiteralContext(DataType.STRING, "2022-02-02 22:22:22").getLongValue(),
        Timestamp.valueOf("2022-02-02 22:22:22").getTime());
    assertThrows(() -> new LiteralContext(DataType.STRING, "123.45").getLongValue());

    // Parse string as BYTES
    assertEquals(new LiteralContext(DataType.STRING, "deadbeef").getBytesValue(), BytesUtils.toBytes("deadbeef"));
  }

  @Test
  public void testBytesLiteral() {
    LiteralContext literalContext = new LiteralContext(DataType.BYTES, BytesUtils.toBytes("deadbeef"));
    assertThrows(literalContext::getBooleanValue);
    assertThrows(literalContext::getIntValue);
    assertThrows(literalContext::getLongValue);
    assertThrows(literalContext::getFloatValue);
    assertThrows(literalContext::getDoubleValue);
    assertEquals(literalContext.getBigDecimalValue(), BigDecimalUtils.deserialize(BytesUtils.toBytes("deadbeef")));
    assertEquals(literalContext.getStringValue(), "deadbeef");
    assertEquals(literalContext.getBytesValue(), BytesUtils.toBytes("deadbeef"));
    assertFalse(literalContext.isNull());
    assertEquals(literalContext.toString(), "'deadbeef'");
  }
}
