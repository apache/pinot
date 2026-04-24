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
package org.apache.pinot.segment.local.customobject;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class AvgPrecisionPairTest {

  @Test
  public void testConstructorAndGetters() {
    BigDecimal sum = new BigDecimal("123.456");
    long count = 10L;

    AvgPrecisionPair pair = new AvgPrecisionPair(sum, count);

    assertEquals(pair.getSum(), sum);
    assertEquals(pair.getCount(), count);
  }

  @Test
  public void testApply() {
    AvgPrecisionPair pair = new AvgPrecisionPair(new BigDecimal("100"), 5L);

    pair.apply(new BigDecimal("50"), 3L);

    assertEquals(pair.getSum(), new BigDecimal("150"));
    assertEquals(pair.getCount(), 8L);
  }

  @Test
  public void testApplyWithNegativeValues() {
    AvgPrecisionPair pair = new AvgPrecisionPair(new BigDecimal("100"), 5L);

    pair.apply(new BigDecimal("-30"), 2L);

    assertEquals(pair.getSum(), new BigDecimal("70"));
    assertEquals(pair.getCount(), 7L);
  }

  @Test
  public void testApplyWithZeroCount() {
    AvgPrecisionPair pair = new AvgPrecisionPair(new BigDecimal("100"), 5L);

    pair.apply(new BigDecimal("50"), 0L);

    assertEquals(pair.getSum(), new BigDecimal("150"));
    assertEquals(pair.getCount(), 5L);
  }

  @Test
  public void testApplyWithLargeNumbers() {
    BigDecimal largeSum = new BigDecimal("999999999999999999999999999999.999999999");
    AvgPrecisionPair pair = new AvgPrecisionPair(largeSum, 1000000L);

    BigDecimal additionalSum = new BigDecimal("111111111111111111111111111111.111111111");
    pair.apply(additionalSum, 500000L);

    assertEquals(pair.getSum(), largeSum.add(additionalSum));
    assertEquals(pair.getCount(), 1500000L);
  }

  @Test
  public void testSerializationDeserialization() {
    BigDecimal sum = new BigDecimal("12345.6789");
    long count = 42L;
    AvgPrecisionPair original = new AvgPrecisionPair(sum, count);

    byte[] bytes = original.toBytes();
    AvgPrecisionPair deserialized = AvgPrecisionPair.fromBytes(bytes);

    assertEquals(deserialized.getSum(), sum);
    assertEquals(deserialized.getCount(), count);
  }

  @Test
  public void testSerializationDeserializationWithByteBuffer() {
    BigDecimal sum = new BigDecimal("98765.4321");
    long count = 100L;
    AvgPrecisionPair original = new AvgPrecisionPair(sum, count);

    byte[] bytes = original.toBytes();
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    AvgPrecisionPair deserialized = AvgPrecisionPair.fromByteBuffer(byteBuffer);

    assertEquals(deserialized.getSum(), sum);
    assertEquals(deserialized.getCount(), count);
  }

  @Test
  public void testSerializationWithZeroValues() {
    AvgPrecisionPair original = new AvgPrecisionPair(BigDecimal.ZERO, 0L);

    byte[] bytes = original.toBytes();
    AvgPrecisionPair deserialized = AvgPrecisionPair.fromBytes(bytes);

    assertEquals(deserialized.getSum(), BigDecimal.ZERO);
    assertEquals(deserialized.getCount(), 0L);
  }

  @Test
  public void testSerializationWithNegativeSum() {
    BigDecimal negativeSum = new BigDecimal("-12345.6789");
    long count = 10L;
    AvgPrecisionPair original = new AvgPrecisionPair(negativeSum, count);

    byte[] bytes = original.toBytes();
    AvgPrecisionPair deserialized = AvgPrecisionPair.fromBytes(bytes);

    assertEquals(deserialized.getSum(), negativeSum);
    assertEquals(deserialized.getCount(), count);
  }

  @Test
  public void testCompareTo() {
    AvgPrecisionPair pair1 = new AvgPrecisionPair(new BigDecimal("100"), 10L);
    AvgPrecisionPair pair2 = new AvgPrecisionPair(new BigDecimal("200"), 10L);
    AvgPrecisionPair pair3 = new AvgPrecisionPair(new BigDecimal("100"), 10L);

    assertTrue(pair1.compareTo(pair2) < 0);
    assertTrue(pair2.compareTo(pair1) > 0);
    assertEquals(pair1.compareTo(pair3), 0);
  }

  @Test
  public void testCompareToWithDifferentCounts() {
    // Average of pair1: 100/10 = 10
    AvgPrecisionPair pair1 = new AvgPrecisionPair(new BigDecimal("100"), 10L);
    // Average of pair2: 200/10 = 20
    AvgPrecisionPair pair2 = new AvgPrecisionPair(new BigDecimal("200"), 10L);
    // Average of pair3: 150/10 = 15
    AvgPrecisionPair pair3 = new AvgPrecisionPair(new BigDecimal("150"), 10L);

    assertTrue(pair1.compareTo(pair2) < 0);
    assertTrue(pair2.compareTo(pair3) > 0);
    assertTrue(pair1.compareTo(pair3) < 0);
  }

  @Test
  public void testCompareToWithZeroCount() {
    AvgPrecisionPair pair1 = new AvgPrecisionPair(new BigDecimal("100"), 0L);
    AvgPrecisionPair pair2 = new AvgPrecisionPair(new BigDecimal("200"), 0L);

    // Both have zero count, averages are undefined, so they are equal
    assertEquals(pair1.compareTo(pair2), 0);
  }

  @Test
  public void testRoundTripSerializationWithVeryLargeNumbers() {
    BigDecimal veryLargeSum = new BigDecimal("123456789012345678901234567890.123456789012345678901234567890");
    long veryLargeCount = Long.MAX_VALUE;
    AvgPrecisionPair original = new AvgPrecisionPair(veryLargeSum, veryLargeCount);

    byte[] bytes = original.toBytes();
    AvgPrecisionPair deserialized = AvgPrecisionPair.fromBytes(bytes);

    assertEquals(deserialized.getSum(), veryLargeSum);
    assertEquals(deserialized.getCount(), veryLargeCount);
  }

  @Test
  public void testMultipleApplyCalls() {
    AvgPrecisionPair pair = new AvgPrecisionPair(BigDecimal.ZERO, 0L);

    pair.apply(new BigDecimal("10"), 1L);
    pair.apply(new BigDecimal("20"), 2L);
    pair.apply(new BigDecimal("30"), 3L);

    assertEquals(pair.getSum(), new BigDecimal("60"));
    assertEquals(pair.getCount(), 6L);
  }

  @Test
  public void testPrecisionPreservation() {
    // Test that BigDecimal precision is preserved through serialization
    BigDecimal preciseSum = new BigDecimal("0.123456789012345678901234567890");
    AvgPrecisionPair original = new AvgPrecisionPair(preciseSum, 1L);

    byte[] bytes = original.toBytes();
    AvgPrecisionPair deserialized = AvgPrecisionPair.fromBytes(bytes);

    assertEquals(deserialized.getSum(), preciseSum);
    assertEquals(deserialized.getSum().scale(), preciseSum.scale());
  }
}
