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
package org.apache.pinot.spi.data.readers;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Set;
import javax.annotation.Nullable;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * Locks in {@link BaseRecordExtractor#convertSingleValue} type-preservation contract:
 * <ul>
 *   <li>Numbers (Byte, Short, Integer, Long, Float, Double, BigDecimal, BigInteger) are returned as-is — Short is
 *       NOT promoted to Integer.</li>
 *   <li>Boolean is returned as-is — NOT converted to "true"/"false" String.</li>
 *   <li>byte[] is returned as-is.</li>
 *   <li>ByteBuffer is materialized into byte[].</li>
 *   <li>Everything else falls back to {@code toString()}.</li>
 * </ul>
 */
public class BaseRecordExtractorTest {
  private static final TestExtractor EXTRACTOR = new TestExtractor();

  @Test
  void testBytePreserved() {
    Object result = EXTRACTOR.convertSingleValue((byte) 42);
    assertSame(result.getClass(), Byte.class, "Byte must remain Byte");
    assertEquals(result, (byte) 42);
  }

  @Test
  void testShortPreserved() {
    Object result = EXTRACTOR.convertSingleValue((short) 42);
    assertSame(result.getClass(), Short.class, "Short must remain Short — not promoted to Integer");
    assertEquals(result, (short) 42);
  }

  @Test
  void testIntegerPreserved() {
    Object result = EXTRACTOR.convertSingleValue(42);
    assertSame(result.getClass(), Integer.class);
    assertEquals(result, 42);
  }

  @Test
  void testLongPreserved() {
    Object result = EXTRACTOR.convertSingleValue(42L);
    assertSame(result.getClass(), Long.class);
    assertEquals(result, 42L);
  }

  @Test
  void testFloatPreserved() {
    Object result = EXTRACTOR.convertSingleValue(1.5f);
    assertSame(result.getClass(), Float.class);
    assertEquals(result, 1.5f);
  }

  @Test
  void testDoublePreserved() {
    Object result = EXTRACTOR.convertSingleValue(1.5);
    assertSame(result.getClass(), Double.class);
    assertEquals(result, 1.5);
  }

  @Test
  void testBigDecimalPreserved() {
    BigDecimal value = new BigDecimal("123.45");
    Object result = EXTRACTOR.convertSingleValue(value);
    assertSame(result, value, "BigDecimal must be returned as-is, not stringified");
  }

  @Test
  void testBigIntegerPreserved() {
    BigInteger value = new BigInteger("12345678901234567890");
    Object result = EXTRACTOR.convertSingleValue(value);
    assertSame(result, value, "BigInteger must be returned as-is, not stringified");
  }

  @Test
  void testBooleanPreserved() {
    Object trueResult = EXTRACTOR.convertSingleValue(true);
    assertSame(trueResult.getClass(), Boolean.class, "Boolean must remain Boolean — not converted to String");
    assertEquals(trueResult, true);

    Object falseResult = EXTRACTOR.convertSingleValue(false);
    assertSame(falseResult.getClass(), Boolean.class);
    assertEquals(falseResult, false);
  }

  @Test
  void testByteArrayPreserved() {
    byte[] bytes = {1, 2, 3};
    Object result = EXTRACTOR.convertSingleValue(bytes);
    assertSame(result, bytes, "byte[] must be returned as-is (same reference)");
  }

  @Test
  void testByteBufferConvertedToByteArray() {
    ByteBuffer buf = ByteBuffer.wrap(new byte[]{1, 2, 3});
    Object result = EXTRACTOR.convertSingleValue(buf);
    assertEquals(result, new byte[]{1, 2, 3}, "ByteBuffer must be materialized into byte[]");
    assertSame(result.getClass(), byte[].class);
  }

  @Test
  void testByteBufferSliceDoesNotMutateOriginal() {
    // Position the buffer to verify the slice — extractor must read the remaining bytes only and not consume
    // the original buffer.
    ByteBuffer buf = ByteBuffer.wrap(new byte[]{0, 1, 2, 3, 4});
    buf.position(2);
    byte[] result = (byte[]) EXTRACTOR.convertSingleValue(buf);
    assertEquals(result, new byte[]{2, 3, 4});
    assertEquals(buf.position(), 2, "Original buffer position must not be advanced");
  }

  @Test
  void testStringPreserved() {
    Object result = EXTRACTOR.convertSingleValue("hello");
    assertSame(result.getClass(), String.class);
    assertEquals(result, "hello");
  }

  @Test
  void testNonStandardObjectFallsBackToToString() {
    Object result = EXTRACTOR.convertSingleValue(new StringBuilder("hello"));
    assertSame(result.getClass(), String.class, "Unknown types fall back to toString()");
    assertEquals(result, "hello");
  }

  /// Minimal concrete subclass to exercise the protected {@code convertSingleValue}.
  private static final class TestExtractor extends BaseRecordExtractor<Object> {
    @Override
    public void init(@Nullable Set<String> fields, RecordExtractorConfig recordExtractorConfig) {
    }

    @Override
    public GenericRow extract(Object from, GenericRow to) {
      return to;
    }
  }
}
