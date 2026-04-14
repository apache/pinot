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
package org.apache.pinot.query.runtime.operator.window.aggregate;

import java.math.BigDecimal;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class WindowValueAggregatorTest {

  // ========================
  // Factory dispatch tests
  // ========================

  @Test
  public void testFactoryReturnsSumAggregatorByType() {
    assertTrue(
        WindowValueAggregatorFactory.getWindowValueAggregator("SUM", ColumnDataType.INT, false)
            instanceof SumLongWindowValueAggregator);
    assertTrue(
        WindowValueAggregatorFactory.getWindowValueAggregator("SUM", ColumnDataType.LONG, false)
            instanceof SumLongWindowValueAggregator);
    assertTrue(
        WindowValueAggregatorFactory.getWindowValueAggregator("SUM", ColumnDataType.DOUBLE, false)
            instanceof SumDoubleWindowValueAggregator);
    assertTrue(
        WindowValueAggregatorFactory.getWindowValueAggregator("SUM", ColumnDataType.FLOAT, false)
            instanceof SumDoubleWindowValueAggregator);
    assertTrue(
        WindowValueAggregatorFactory.getWindowValueAggregator("SUM", ColumnDataType.BIG_DECIMAL, false)
            instanceof SumBigDecimalWindowValueAggregator);
  }

  @Test
  public void testFactoryReturnsSumAggregatorForStoredTypes() {
    // BOOLEAN stored as INT → LongSum
    assertTrue(
        WindowValueAggregatorFactory.getWindowValueAggregator("SUM", ColumnDataType.BOOLEAN, false)
            instanceof SumLongWindowValueAggregator);
    // TIMESTAMP stored as LONG → LongSum
    assertTrue(
        WindowValueAggregatorFactory.getWindowValueAggregator("SUM", ColumnDataType.TIMESTAMP, false)
            instanceof SumLongWindowValueAggregator);
  }

  @Test
  public void testFactoryReturnsSum0AggregatorByType() {
    assertTrue(
        WindowValueAggregatorFactory.getWindowValueAggregator("SUM0", ColumnDataType.INT, false)
            instanceof SumLongWindowValueAggregator);
    assertTrue(
        WindowValueAggregatorFactory.getWindowValueAggregator("$SUM0", ColumnDataType.LONG, false)
            instanceof SumLongWindowValueAggregator);
    assertTrue(
        WindowValueAggregatorFactory.getWindowValueAggregator("SUM0", ColumnDataType.DOUBLE, false)
            instanceof SumDoubleWindowValueAggregator);
  }

  @Test
  public void testFactoryReturnsMinAggregatorByType() {
    assertTrue(
        WindowValueAggregatorFactory.getWindowValueAggregator("MIN", ColumnDataType.INT, false)
            instanceof MinIntWindowValueAggregator);
    assertTrue(
        WindowValueAggregatorFactory.getWindowValueAggregator("MIN", ColumnDataType.LONG, false)
            instanceof MinLongWindowValueAggregator);
    assertTrue(
        WindowValueAggregatorFactory.getWindowValueAggregator("MIN", ColumnDataType.FLOAT, false)
            instanceof MinDoubleWindowValueAggregator);
    assertTrue(
        WindowValueAggregatorFactory.getWindowValueAggregator("MIN", ColumnDataType.DOUBLE, false)
            instanceof MinDoubleWindowValueAggregator);
    assertTrue(
        WindowValueAggregatorFactory.getWindowValueAggregator("MIN", ColumnDataType.BIG_DECIMAL, false)
            instanceof MinComparableWindowValueAggregator);
  }

  @Test
  public void testFactoryReturnsMaxAggregatorByType() {
    assertTrue(
        WindowValueAggregatorFactory.getWindowValueAggregator("MAX", ColumnDataType.INT, false)
            instanceof MaxIntWindowValueAggregator);
    assertTrue(
        WindowValueAggregatorFactory.getWindowValueAggregator("MAX", ColumnDataType.LONG, false)
            instanceof MaxLongWindowValueAggregator);
    assertTrue(
        WindowValueAggregatorFactory.getWindowValueAggregator("MAX", ColumnDataType.FLOAT, false)
            instanceof MaxDoubleWindowValueAggregator);
    assertTrue(
        WindowValueAggregatorFactory.getWindowValueAggregator("MAX", ColumnDataType.DOUBLE, false)
            instanceof MaxDoubleWindowValueAggregator);
    assertTrue(
        WindowValueAggregatorFactory.getWindowValueAggregator("MAX", ColumnDataType.BIG_DECIMAL, false)
            instanceof MaxComparableWindowValueAggregator);
  }

  // ========================
  // SumLongWindowValueAggregator tests
  // ========================

  @Test
  public void testLongSumWithIntValues() {
    WindowValueAggregator<Object> agg = new SumLongWindowValueAggregator();
    agg.addValue(1);
    agg.addValue(2);
    agg.addValue(3);
    assertEquals(agg.getCurrentAggregatedValue(), 6L);
    assertTrue(agg.getCurrentAggregatedValue() instanceof Long);
  }

  @Test
  public void testLongSumWithLongValues() {
    WindowValueAggregator<Object> agg = new SumLongWindowValueAggregator();
    // Use values that would lose precision if converted to double
    long largeValue = (1L << 53) + 1;  // 9007199254740993
    agg.addValue(largeValue);
    agg.addValue(1L);
    assertEquals(agg.getCurrentAggregatedValue(), largeValue + 1);
    assertTrue(agg.getCurrentAggregatedValue() instanceof Long);
  }

  @Test
  public void testLongSumNullHandling() {
    WindowValueAggregator<Object> agg = new SumLongWindowValueAggregator();
    assertNull(agg.getCurrentAggregatedValue());
    agg.addValue(null);
    assertNull(agg.getCurrentAggregatedValue());
    agg.addValue(5);
    assertEquals(agg.getCurrentAggregatedValue(), 5L);
    agg.addValue(null);
    assertEquals(agg.getCurrentAggregatedValue(), 5L);
  }

  @Test
  public void testLongSumRemoval() {
    WindowValueAggregator<Object> agg = new SumLongWindowValueAggregator();
    agg.addValue(10L);
    agg.addValue(20L);
    agg.addValue(30L);
    assertEquals(agg.getCurrentAggregatedValue(), 60L);
    agg.removeValue(10L);
    assertEquals(agg.getCurrentAggregatedValue(), 50L);
    agg.removeValue(20L);
    assertEquals(agg.getCurrentAggregatedValue(), 30L);
    agg.removeValue(30L);
    assertNull(agg.getCurrentAggregatedValue());
  }

  @Test
  public void testLongSumClear() {
    WindowValueAggregator<Object> agg = new SumLongWindowValueAggregator();
    agg.addValue(100L);
    agg.clear();
    assertNull(agg.getCurrentAggregatedValue());
  }

  // ========================
  // SumBigDecimalWindowValueAggregator tests
  // ========================

  @Test
  public void testBigDecimalSumWithBigDecimalValues() {
    WindowValueAggregator<Object> agg = new SumBigDecimalWindowValueAggregator();
    agg.addValue(new BigDecimal("123456789.123456789"));
    agg.addValue(new BigDecimal("987654321.987654321"));
    Object result = agg.getCurrentAggregatedValue();
    assertTrue(result instanceof BigDecimal);
    assertEquals(result, new BigDecimal("1111111111.111111110"));
  }

  @Test
  public void testBigDecimalSumWithNumberValues() {
    WindowValueAggregator<Object> agg = new SumBigDecimalWindowValueAggregator();
    agg.addValue(1.5);
    agg.addValue(2.5);
    Object result = agg.getCurrentAggregatedValue();
    assertTrue(result instanceof BigDecimal);
    assertEquals(((BigDecimal) result).doubleValue(), 4.0);
  }

  @Test
  public void testBigDecimalSumNullHandling() {
    WindowValueAggregator<Object> agg = new SumBigDecimalWindowValueAggregator();
    assertNull(agg.getCurrentAggregatedValue());
    agg.addValue(null);
    assertNull(agg.getCurrentAggregatedValue());
    agg.addValue(BigDecimal.TEN);
    assertEquals(agg.getCurrentAggregatedValue(), BigDecimal.TEN);
  }

  @Test
  public void testBigDecimalSumRemoval() {
    WindowValueAggregator<Object> agg = new SumBigDecimalWindowValueAggregator();
    agg.addValue(new BigDecimal("10"));
    agg.addValue(new BigDecimal("20"));
    assertEquals(agg.getCurrentAggregatedValue(), new BigDecimal("30"));
    agg.removeValue(new BigDecimal("10"));
    assertEquals(agg.getCurrentAggregatedValue(), new BigDecimal("20"));
  }

  @Test
  public void testBigDecimalSumPreservesLongPrecision() {
    WindowValueAggregator<Object> agg = new SumBigDecimalWindowValueAggregator();
    // Value beyond double precision range (9007199254740993)
    long largeValue = (1L << 53) + 1;
    agg.addValue(largeValue);
    agg.addValue(1L);
    BigDecimal result = (BigDecimal) agg.getCurrentAggregatedValue();
    assertEquals(result, BigDecimal.valueOf(largeValue + 1));
  }

  @Test
  public void testBigDecimalSumClear() {
    WindowValueAggregator<Object> agg = new SumBigDecimalWindowValueAggregator();
    agg.addValue(BigDecimal.ONE);
    agg.clear();
    assertNull(agg.getCurrentAggregatedValue());
  }

  // ========================
  // MinIntWindowValueAggregator tests
  // ========================

  @Test
  public void testIntMinBasic() {
    WindowValueAggregator<Object> agg = new MinIntWindowValueAggregator(false);
    agg.addValue(3);
    agg.addValue(1);
    agg.addValue(2);
    assertEquals(agg.getCurrentAggregatedValue(), 1);
    assertTrue(agg.getCurrentAggregatedValue() instanceof Integer);
  }

  @Test
  public void testIntMinNullHandling() {
    WindowValueAggregator<Object> agg = new MinIntWindowValueAggregator(false);
    assertNull(agg.getCurrentAggregatedValue());
    agg.addValue(null);
    assertNull(agg.getCurrentAggregatedValue());
    agg.addValue(5);
    assertEquals(agg.getCurrentAggregatedValue(), 5);
  }

  @Test
  public void testIntMinWithRemovalSupport() {
    WindowValueAggregator<Object> agg = new MinIntWindowValueAggregator(true);
    agg.addValue(10);
    agg.addValue(5);
    agg.addValue(8);
    assertEquals(agg.getCurrentAggregatedValue(), 5);

    agg.removeValue(10);
    assertEquals(agg.getCurrentAggregatedValue(), 5);

    agg.removeValue(5);
    assertEquals(agg.getCurrentAggregatedValue(), 8);
  }

  @Test
  public void testIntMinRemovalToEmpty() {
    WindowValueAggregator<Object> agg = new MinIntWindowValueAggregator(true);
    agg.addValue(5);
    agg.removeValue(5);
    assertNull(agg.getCurrentAggregatedValue());
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testIntMinRemovalUnsupported() {
    WindowValueAggregator<Object> agg = new MinIntWindowValueAggregator(false);
    agg.addValue(1);
    agg.removeValue(1);
  }

  // ========================
  // MaxIntWindowValueAggregator tests
  // ========================

  @Test
  public void testIntMaxBasic() {
    WindowValueAggregator<Object> agg = new MaxIntWindowValueAggregator(false);
    agg.addValue(1);
    agg.addValue(3);
    agg.addValue(2);
    assertEquals(agg.getCurrentAggregatedValue(), 3);
    assertTrue(agg.getCurrentAggregatedValue() instanceof Integer);
  }

  @Test
  public void testIntMaxWithRemovalSupport() {
    WindowValueAggregator<Object> agg = new MaxIntWindowValueAggregator(true);
    agg.addValue(5);
    agg.addValue(10);
    agg.addValue(8);
    assertEquals(agg.getCurrentAggregatedValue(), 10);

    agg.removeValue(5);
    assertEquals(agg.getCurrentAggregatedValue(), 10);

    agg.removeValue(10);
    assertEquals(agg.getCurrentAggregatedValue(), 8);
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testIntMaxRemovalUnsupported() {
    WindowValueAggregator<Object> agg = new MaxIntWindowValueAggregator(false);
    agg.addValue(1);
    agg.removeValue(1);
  }

  // ========================
  // MinLongWindowValueAggregator tests
  // ========================

  @Test
  public void testLongMinBasic() {
    WindowValueAggregator<Object> agg = new MinLongWindowValueAggregator(false);
    agg.addValue(300L);
    agg.addValue(100L);
    agg.addValue(200L);
    assertEquals(agg.getCurrentAggregatedValue(), 100L);
    assertTrue(agg.getCurrentAggregatedValue() instanceof Long);
  }

  @Test
  public void testLongMinNullHandling() {
    WindowValueAggregator<Object> agg = new MinLongWindowValueAggregator(false);
    assertNull(agg.getCurrentAggregatedValue());
    agg.addValue(null);
    assertNull(agg.getCurrentAggregatedValue());
    agg.addValue(5L);
    assertEquals(agg.getCurrentAggregatedValue(), 5L);
  }

  @Test
  public void testLongMinWithRemovalSupport() {
    WindowValueAggregator<Object> agg = new MinLongWindowValueAggregator(true);
    agg.addValue(10L);
    agg.addValue(5L);
    agg.addValue(8L);
    assertEquals(agg.getCurrentAggregatedValue(), 5L);

    agg.removeValue(10L);
    assertEquals(agg.getCurrentAggregatedValue(), 5L);

    agg.removeValue(5L);
    assertEquals(agg.getCurrentAggregatedValue(), 8L);
  }

  @Test
  public void testLongMinPreservesPrecision() {
    WindowValueAggregator<Object> agg = new MinLongWindowValueAggregator(false);
    long largeVal = (1L << 53) + 1;
    agg.addValue(largeVal);
    agg.addValue(largeVal + 1);
    assertEquals(agg.getCurrentAggregatedValue(), largeVal);
    assertTrue(agg.getCurrentAggregatedValue() instanceof Long);
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testLongMinRemovalUnsupported() {
    WindowValueAggregator<Object> agg = new MinLongWindowValueAggregator(false);
    agg.addValue(1L);
    agg.removeValue(1L);
  }

  // ========================
  // MaxLongWindowValueAggregator tests
  // ========================

  @Test
  public void testLongMaxBasic() {
    WindowValueAggregator<Object> agg = new MaxLongWindowValueAggregator(false);
    agg.addValue(100L);
    agg.addValue(300L);
    agg.addValue(200L);
    assertEquals(agg.getCurrentAggregatedValue(), 300L);
    assertTrue(agg.getCurrentAggregatedValue() instanceof Long);
  }

  @Test
  public void testLongMaxWithRemovalSupport() {
    WindowValueAggregator<Object> agg = new MaxLongWindowValueAggregator(true);
    agg.addValue(5L);
    agg.addValue(10L);
    agg.addValue(8L);
    assertEquals(agg.getCurrentAggregatedValue(), 10L);

    agg.removeValue(5L);
    assertEquals(agg.getCurrentAggregatedValue(), 10L);

    agg.removeValue(10L);
    assertEquals(agg.getCurrentAggregatedValue(), 8L);
  }

  @Test
  public void testLongMaxPreservesPrecision() {
    WindowValueAggregator<Object> agg = new MaxLongWindowValueAggregator(false);
    long largeVal = (1L << 53) + 1;
    agg.addValue(largeVal);
    agg.addValue(largeVal - 1);
    assertEquals(agg.getCurrentAggregatedValue(), largeVal);
    assertTrue(agg.getCurrentAggregatedValue() instanceof Long);
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testLongMaxRemovalUnsupported() {
    WindowValueAggregator<Object> agg = new MaxLongWindowValueAggregator(false);
    agg.addValue(1L);
    agg.removeValue(1L);
  }

  // ========================
  // MinComparableWindowValueAggregator tests
  // ========================

  @Test
  public void testComparableMinWithIntValues() {
    WindowValueAggregator<Object> agg = new MinComparableWindowValueAggregator(false);
    agg.addValue(3);
    agg.addValue(1);
    agg.addValue(2);
    assertEquals(agg.getCurrentAggregatedValue(), 1);
    assertTrue(agg.getCurrentAggregatedValue() instanceof Integer);
  }

  @Test
  public void testComparableMinWithLongValues() {
    WindowValueAggregator<Object> agg = new MinComparableWindowValueAggregator(false);
    agg.addValue(300L);
    agg.addValue(100L);
    agg.addValue(200L);
    assertEquals(agg.getCurrentAggregatedValue(), 100L);
    assertTrue(agg.getCurrentAggregatedValue() instanceof Long);
  }

  @Test
  public void testComparableMinWithBigDecimalValues() {
    WindowValueAggregator<Object> agg = new MinComparableWindowValueAggregator(false);
    agg.addValue(new BigDecimal("3.0"));
    agg.addValue(new BigDecimal("1.0"));
    agg.addValue(new BigDecimal("2.0"));
    assertEquals(agg.getCurrentAggregatedValue(), new BigDecimal("1.0"));
    assertTrue(agg.getCurrentAggregatedValue() instanceof BigDecimal);
  }

  @Test
  public void testComparableMinNullHandling() {
    WindowValueAggregator<Object> agg = new MinComparableWindowValueAggregator(false);
    assertNull(agg.getCurrentAggregatedValue());
    agg.addValue(null);
    assertNull(agg.getCurrentAggregatedValue());
    agg.addValue(5);
    assertEquals(agg.getCurrentAggregatedValue(), 5);
    agg.addValue(null);
    assertEquals(agg.getCurrentAggregatedValue(), 5);
  }

  @Test
  public void testComparableMinWithRemovalSupport() {
    WindowValueAggregator<Object> agg = new MinComparableWindowValueAggregator(true);
    // Simulate sliding window: [3, 1, 4, 1, 5]
    agg.addValue(3);
    agg.addValue(1);
    assertEquals(agg.getCurrentAggregatedValue(), 1);

    // Remove 3 (oldest), add 4
    agg.removeValue(3);
    agg.addValue(4);
    assertEquals(agg.getCurrentAggregatedValue(), 1);

    // Remove 1, add 1
    agg.removeValue(1);
    agg.addValue(1);
    assertEquals(agg.getCurrentAggregatedValue(), 1);

    // Remove 4, add 5
    agg.removeValue(4);
    agg.addValue(5);
    assertEquals(agg.getCurrentAggregatedValue(), 1);
  }

  @Test
  public void testComparableMinRemovalToEmpty() {
    WindowValueAggregator<Object> agg = new MinComparableWindowValueAggregator(true);
    agg.addValue(5L);
    agg.removeValue(5L);
    assertNull(agg.getCurrentAggregatedValue());
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testComparableMinRemovalUnsupported() {
    WindowValueAggregator<Object> agg = new MinComparableWindowValueAggregator(false);
    agg.addValue(1);
    agg.removeValue(1);
  }

  @Test
  public void testComparableMinClear() {
    WindowValueAggregator<Object> agg = new MinComparableWindowValueAggregator(false);
    agg.addValue(42);
    agg.clear();
    assertNull(agg.getCurrentAggregatedValue());
  }

  @Test
  public void testComparableMinMonotonicDequeWithLong() {
    // Test the monotonic deque behavior preserves long type through sliding window
    WindowValueAggregator<Object> agg = new MinComparableWindowValueAggregator(true);
    agg.addValue(10L);
    agg.addValue(5L);
    agg.addValue(8L);
    assertEquals(agg.getCurrentAggregatedValue(), 5L);
    assertTrue(agg.getCurrentAggregatedValue() instanceof Long);

    // Remove 10 (was already discarded from deque since 5 < 10)
    agg.removeValue(10L);
    assertEquals(agg.getCurrentAggregatedValue(), 5L);

    // Remove 5
    agg.removeValue(5L);
    assertEquals(agg.getCurrentAggregatedValue(), 8L);
    assertTrue(agg.getCurrentAggregatedValue() instanceof Long);
  }

  // ========================
  // MaxComparableWindowValueAggregator tests
  // ========================

  @Test
  public void testComparableMaxWithIntValues() {
    WindowValueAggregator<Object> agg = new MaxComparableWindowValueAggregator(false);
    agg.addValue(1);
    agg.addValue(3);
    agg.addValue(2);
    assertEquals(agg.getCurrentAggregatedValue(), 3);
    assertTrue(agg.getCurrentAggregatedValue() instanceof Integer);
  }

  @Test
  public void testComparableMaxWithLongValues() {
    WindowValueAggregator<Object> agg = new MaxComparableWindowValueAggregator(false);
    agg.addValue(100L);
    agg.addValue(300L);
    agg.addValue(200L);
    assertEquals(agg.getCurrentAggregatedValue(), 300L);
    assertTrue(agg.getCurrentAggregatedValue() instanceof Long);
  }

  @Test
  public void testComparableMaxWithBigDecimalValues() {
    WindowValueAggregator<Object> agg = new MaxComparableWindowValueAggregator(false);
    agg.addValue(new BigDecimal("1.0"));
    agg.addValue(new BigDecimal("3.0"));
    agg.addValue(new BigDecimal("2.0"));
    assertEquals(agg.getCurrentAggregatedValue(), new BigDecimal("3.0"));
    assertTrue(agg.getCurrentAggregatedValue() instanceof BigDecimal);
  }

  @Test
  public void testComparableMaxNullHandling() {
    WindowValueAggregator<Object> agg = new MaxComparableWindowValueAggregator(false);
    assertNull(agg.getCurrentAggregatedValue());
    agg.addValue(null);
    assertNull(agg.getCurrentAggregatedValue());
    agg.addValue(5);
    assertEquals(agg.getCurrentAggregatedValue(), 5);
    agg.addValue(null);
    assertEquals(agg.getCurrentAggregatedValue(), 5);
  }

  @Test
  public void testComparableMaxWithRemovalSupport() {
    WindowValueAggregator<Object> agg = new MaxComparableWindowValueAggregator(true);
    agg.addValue(3);
    agg.addValue(5);
    assertEquals(agg.getCurrentAggregatedValue(), 5);

    // Remove 3 (was already discarded from deque since 5 > 3)
    agg.removeValue(3);
    assertEquals(agg.getCurrentAggregatedValue(), 5);

    // Remove 5, add 2
    agg.removeValue(5);
    agg.addValue(2);
    assertEquals(agg.getCurrentAggregatedValue(), 2);
  }

  @Test
  public void testComparableMaxRemovalToEmpty() {
    WindowValueAggregator<Object> agg = new MaxComparableWindowValueAggregator(true);
    agg.addValue(5L);
    agg.removeValue(5L);
    assertNull(agg.getCurrentAggregatedValue());
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testComparableMaxRemovalUnsupported() {
    WindowValueAggregator<Object> agg = new MaxComparableWindowValueAggregator(false);
    agg.addValue(1);
    agg.removeValue(1);
  }

  @Test
  public void testComparableMaxClear() {
    WindowValueAggregator<Object> agg = new MaxComparableWindowValueAggregator(false);
    agg.addValue(42);
    agg.clear();
    assertNull(agg.getCurrentAggregatedValue());
  }

  @Test
  public void testComparableMaxMonotonicDequeWithLong() {
    WindowValueAggregator<Object> agg = new MaxComparableWindowValueAggregator(true);
    agg.addValue(5L);
    agg.addValue(10L);
    agg.addValue(8L);
    assertEquals(agg.getCurrentAggregatedValue(), 10L);
    assertTrue(agg.getCurrentAggregatedValue() instanceof Long);

    agg.removeValue(5L);
    assertEquals(agg.getCurrentAggregatedValue(), 10L);

    agg.removeValue(10L);
    assertEquals(agg.getCurrentAggregatedValue(), 8L);
    assertTrue(agg.getCurrentAggregatedValue() instanceof Long);
  }

  // ========================
  // Existing aggregator tests (regression)
  // ========================

  @Test
  public void testDoubleSumAggregator() {
    WindowValueAggregator<Object> agg = new SumDoubleWindowValueAggregator();
    agg.addValue(1.5);
    agg.addValue(2.5);
    assertEquals(agg.getCurrentAggregatedValue(), 4.0);
    assertTrue(agg.getCurrentAggregatedValue() instanceof Double);
  }

  @Test
  public void testDoubleSumNullReturnsNull() {
    WindowValueAggregator<Object> agg = new SumDoubleWindowValueAggregator();
    assertNull(agg.getCurrentAggregatedValue());
  }

  @Test
  public void testComparableMinWithDoubleValues() {
    WindowValueAggregator<Object> agg = new MinComparableWindowValueAggregator(false);
    agg.addValue(3.0);
    agg.addValue(1.0);
    agg.addValue(2.0);
    assertEquals(agg.getCurrentAggregatedValue(), 1.0);
    assertTrue(agg.getCurrentAggregatedValue() instanceof Double);
  }

  @Test
  public void testComparableMaxWithDoubleValues() {
    WindowValueAggregator<Object> agg = new MaxComparableWindowValueAggregator(false);
    agg.addValue(1.0);
    agg.addValue(3.0);
    agg.addValue(2.0);
    assertEquals(agg.getCurrentAggregatedValue(), 3.0);
    assertTrue(agg.getCurrentAggregatedValue() instanceof Double);
  }

  @Test
  public void testAvgAggregator() {
    WindowValueAggregator<Object> agg = new AvgWindowValueAggregator();
    agg.addValue(2.0);
    agg.addValue(4.0);
    assertEquals(agg.getCurrentAggregatedValue(), 3.0);
    assertTrue(agg.getCurrentAggregatedValue() instanceof Double);
  }

  @Test
  public void testCountAggregator() {
    WindowValueAggregator<Object> agg = new CountWindowValueAggregator();
    agg.addValue(1);
    agg.addValue(null);
    agg.addValue(3);
    assertEquals(agg.getCurrentAggregatedValue(), 2L);
  }

  // ========================
  // Precision preservation tests
  // ========================

  @Test
  public void testLongSumPreservesPrecisionBeyondDoubleRange() {
    WindowValueAggregator<Object> agg = new SumLongWindowValueAggregator();
    // 2^53 + 1 = 9007199254740993, which cannot be exactly represented as double
    long val1 = (1L << 53) + 1;
    long val2 = 1L;
    agg.addValue(val1);
    agg.addValue(val2);

    long result = (Long) agg.getCurrentAggregatedValue();
    assertEquals(result, val1 + val2);

    // Verify that double would have lost precision
    double doubleSum = (double) val1 + (double) val2;
    assertNotEquals((long) doubleSum, val1 + val2,
        "Double should lose precision for this value, confirming the need for LongSum");
  }

  @Test
  public void testBigDecimalSumPreservesScale() {
    WindowValueAggregator<Object> agg = new SumBigDecimalWindowValueAggregator();
    agg.addValue(new BigDecimal("0.1"));
    agg.addValue(new BigDecimal("0.2"));
    BigDecimal result = (BigDecimal) agg.getCurrentAggregatedValue();
    assertEquals(result, new BigDecimal("0.3"));
  }

  @Test
  public void testComparableMinPreservesLongType() {
    WindowValueAggregator<Object> agg = new MinComparableWindowValueAggregator(false);
    long largeVal = (1L << 53) + 1;
    agg.addValue(largeVal);
    agg.addValue(largeVal + 1);
    Object result = agg.getCurrentAggregatedValue();
    assertTrue(result instanceof Long);
    assertEquals(result, largeVal);
  }

  @Test
  public void testComparableMaxPreservesLongType() {
    WindowValueAggregator<Object> agg = new MaxComparableWindowValueAggregator(false);
    long largeVal = (1L << 53) + 1;
    agg.addValue(largeVal);
    agg.addValue(largeVal - 1);
    Object result = agg.getCurrentAggregatedValue();
    assertTrue(result instanceof Long);
    assertEquals(result, largeVal);
  }
}
