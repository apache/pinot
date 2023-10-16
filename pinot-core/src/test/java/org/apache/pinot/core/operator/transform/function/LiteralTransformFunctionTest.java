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

import java.math.BigDecimal;
import org.apache.pinot.common.request.context.LiteralContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.CommonConstants.NullValuePlaceHolder;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;


public class LiteralTransformFunctionTest {
  private static final int NUM_DOCS = 100;

  @Test
  public void testLiteralTransformFunction() {
    LiteralTransformFunction function = new LiteralTransformFunction(new LiteralContext(DataType.STRING, "1234"));
    assertFalse(function.getBooleanLiteral());
    assertEquals(function.getIntLiteral(), 1234);
    assertEquals(function.getLongLiteral(), 1234L);
    assertEquals(function.getFloatLiteral(), 1234.0f);
    assertEquals(function.getDoubleLiteral(), 1234.0);
    assertEquals(function.getBigDecimalLiteral(), new BigDecimal("1234"));
    assertEquals(function.getStringLiteral(), "1234");
    assertEquals(function.getBytesLiteral(), BytesUtils.toBytes("1234"));
    assertFalse(function.isNull());
    TransformResultMetadata resultMetadata = function.getResultMetadata();
    assertEquals(resultMetadata.getDataType(), DataType.STRING);
    assertTrue(resultMetadata.isSingleValue());
    assertFalse(resultMetadata.hasDictionary());

    ValueBlock valueBlock = mock(ValueBlock.class);
    when(valueBlock.getNumDocs()).thenReturn(NUM_DOCS);
    int[] intValues = function.transformToIntValuesSV(valueBlock);
    assertEquals(intValues.length, NUM_DOCS);
    for (int i = 0; i < NUM_DOCS; i++) {
      assertEquals(intValues[i], 1234);
    }
    long[] longValues = function.transformToLongValuesSV(valueBlock);
    assertEquals(longValues.length, NUM_DOCS);
    for (int i = 0; i < NUM_DOCS; i++) {
      assertEquals(longValues[i], 1234L);
    }
    float[] floatValues = function.transformToFloatValuesSV(valueBlock);
    assertEquals(floatValues.length, NUM_DOCS);
    for (int i = 0; i < NUM_DOCS; i++) {
      assertEquals(floatValues[i], 1234.0f);
    }
    double[] doubleValues = function.transformToDoubleValuesSV(valueBlock);
    assertEquals(doubleValues.length, NUM_DOCS);
    for (int i = 0; i < NUM_DOCS; i++) {
      assertEquals(doubleValues[i], 1234.0);
    }
    String[] stringValues = function.transformToStringValuesSV(valueBlock);
    assertEquals(stringValues.length, NUM_DOCS);
    for (int i = 0; i < NUM_DOCS; i++) {
      assertEquals(stringValues[i], "1234");
    }
    byte[][] bytesValues = function.transformToBytesValuesSV(valueBlock);
    assertEquals(bytesValues.length, NUM_DOCS);
    for (int i = 0; i < NUM_DOCS; i++) {
      assertEquals(bytesValues[i], BytesUtils.toBytes("1234"));
    }
    assertNull(function.getNullBitmap(valueBlock));
  }

  @Test
  public void testNullLiteral() {
    LiteralTransformFunction function = new LiteralTransformFunction(new LiteralContext(DataType.UNKNOWN, null));
    assertFalse(function.getBooleanLiteral());
    assertEquals(function.getIntLiteral(), NullValuePlaceHolder.INT);
    assertEquals(function.getLongLiteral(), NullValuePlaceHolder.LONG);
    assertEquals(function.getFloatLiteral(), NullValuePlaceHolder.FLOAT);
    assertEquals(function.getDoubleLiteral(), NullValuePlaceHolder.DOUBLE);
    assertEquals(function.getBigDecimalLiteral(), NullValuePlaceHolder.BIG_DECIMAL);
    assertEquals(function.getStringLiteral(), NullValuePlaceHolder.STRING);
    assertEquals(function.getBytesLiteral(), NullValuePlaceHolder.BYTES);
    assertTrue(function.isNull());
    TransformResultMetadata resultMetadata = function.getResultMetadata();
    assertEquals(resultMetadata.getDataType(), DataType.UNKNOWN);
    assertTrue(resultMetadata.isSingleValue());
    assertFalse(resultMetadata.hasDictionary());

    ValueBlock valueBlock = mock(ValueBlock.class);
    when(valueBlock.getNumDocs()).thenReturn(NUM_DOCS);
    int[] intValues = function.transformToIntValuesSV(valueBlock);
    assertEquals(intValues.length, NUM_DOCS);
    for (int i = 0; i < NUM_DOCS; i++) {
      assertEquals(intValues[i], NullValuePlaceHolder.INT);
    }
    long[] longValues = function.transformToLongValuesSV(valueBlock);
    assertEquals(longValues.length, NUM_DOCS);
    for (int i = 0; i < NUM_DOCS; i++) {
      assertEquals(longValues[i], NullValuePlaceHolder.LONG);
    }
    float[] floatValues = function.transformToFloatValuesSV(valueBlock);
    assertEquals(floatValues.length, NUM_DOCS);
    for (int i = 0; i < NUM_DOCS; i++) {
      assertEquals(floatValues[i], NullValuePlaceHolder.FLOAT);
    }
    double[] doubleValues = function.transformToDoubleValuesSV(valueBlock);
    assertEquals(doubleValues.length, NUM_DOCS);
    for (int i = 0; i < NUM_DOCS; i++) {
      assertEquals(doubleValues[i], NullValuePlaceHolder.DOUBLE);
    }
    String[] stringValues = function.transformToStringValuesSV(valueBlock);
    assertEquals(stringValues.length, NUM_DOCS);
    for (int i = 0; i < NUM_DOCS; i++) {
      assertEquals(stringValues[i], NullValuePlaceHolder.STRING);
    }
    byte[][] bytesValues = function.transformToBytesValuesSV(valueBlock);
    assertEquals(bytesValues.length, NUM_DOCS);
    for (int i = 0; i < NUM_DOCS; i++) {
      assertEquals(bytesValues[i], NullValuePlaceHolder.BYTES);
    }
    RoaringBitmap nullBitmap = function.getNullBitmap(valueBlock);
    assertNotNull(nullBitmap);
    assertEquals(nullBitmap.getCardinality(), NUM_DOCS);
    for (int i = 0; i < NUM_DOCS; i++) {
      assertTrue(nullBitmap.contains(i));
    }
  }
}
