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
package org.apache.pinot.segment.spi.memory;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.apache.pinot.segment.spi.memory.PrimitiveArrays.MIN_BULK_READ_VALUES;
import static org.testng.Assert.assertEquals;


/// Checks that the bulk reads read the same values as one [ByteBuffer] get per value, from heap and direct buffers of
/// either byte order, on both sides of [PrimitiveArrays#MIN_BULK_READ_VALUES], into a range of the target array.
public class PrimitiveArraysTest {
  private static final int NUM_VALUES = 200;
  /// Bytes before the values in the buffer, so they are not aligned to their width.
  private static final int PREFIX = 3;
  /// Slots before the values in the target array.
  private static final int OFFSET = 5;

  @DataProvider
  public Object[][] cases() {
    List<Object[]> cases = new ArrayList<>();
    for (boolean direct : new boolean[]{false, true}) {
      for (ByteOrder order : new ByteOrder[]{ByteOrder.BIG_ENDIAN, ByteOrder.LITTLE_ENDIAN}) {
        for (int length : new int[]{0, 1, MIN_BULK_READ_VALUES - 1, MIN_BULK_READ_VALUES, NUM_VALUES}) {
          cases.add(new Object[]{direct, order, length});
        }
      }
    }
    return cases.toArray(new Object[0][]);
  }

  @Test(dataProvider = "cases")
  public void testReadInts(boolean direct, ByteOrder order, int length) {
    ByteBuffer buffer = newBuffer(direct, order, Integer.BYTES);
    Random random = new Random(length);
    for (int i = 0; i < NUM_VALUES; i++) {
      buffer.putInt(random.nextInt());
    }
    buffer.position(PREFIX);
    int[] values = new int[OFFSET + NUM_VALUES];
    PrimitiveArrays.readInts(buffer, values, OFFSET, length);
    assertEquals(buffer.position(), PREFIX + length * Integer.BYTES);
    random = new Random(length);
    for (int i = 0; i < length; i++) {
      assertEquals(values[OFFSET + i], random.nextInt());
    }
    assertUntouched(values, length);
  }

  @Test(dataProvider = "cases")
  public void testReadLongs(boolean direct, ByteOrder order, int length) {
    ByteBuffer buffer = newBuffer(direct, order, Long.BYTES);
    Random random = new Random(length);
    for (int i = 0; i < NUM_VALUES; i++) {
      buffer.putLong(random.nextLong());
    }
    buffer.position(PREFIX);
    long[] values = new long[OFFSET + NUM_VALUES];
    PrimitiveArrays.readLongs(buffer, values, OFFSET, length);
    assertEquals(buffer.position(), PREFIX + length * Long.BYTES);
    random = new Random(length);
    for (int i = 0; i < length; i++) {
      assertEquals(values[OFFSET + i], random.nextLong());
    }
    assertUntouched(values, length);
  }

  @Test(dataProvider = "cases")
  public void testReadFloats(boolean direct, ByteOrder order, int length) {
    ByteBuffer buffer = newBuffer(direct, order, Float.BYTES);
    // Raw bits, so a NaN keeps its payload
    for (int i = 0; i < NUM_VALUES; i++) {
      buffer.putInt(floatBits(i));
    }
    buffer.position(PREFIX);
    float[] values = new float[OFFSET + NUM_VALUES];
    PrimitiveArrays.readFloats(buffer, values, OFFSET, length);
    assertEquals(buffer.position(), PREFIX + length * Float.BYTES);
    for (int i = 0; i < length; i++) {
      assertEquals(Float.floatToRawIntBits(values[OFFSET + i]), floatBits(i));
    }
    assertUntouched(values, length);
  }

  @Test(dataProvider = "cases")
  public void testReadDoubles(boolean direct, ByteOrder order, int length) {
    ByteBuffer buffer = newBuffer(direct, order, Double.BYTES);
    // Raw bits, so a NaN keeps its payload
    for (int i = 0; i < NUM_VALUES; i++) {
      buffer.putLong(doubleBits(i));
    }
    buffer.position(PREFIX);
    double[] values = new double[OFFSET + NUM_VALUES];
    PrimitiveArrays.readDoubles(buffer, values, OFFSET, length);
    assertEquals(buffer.position(), PREFIX + length * Double.BYTES);
    for (int i = 0; i < length; i++) {
      assertEquals(Double.doubleToRawLongBits(values[OFFSET + i]), doubleBits(i));
    }
    assertUntouched(values, length);
  }

  /// Returns a buffer of `PREFIX` zero bytes, then room for [#NUM_VALUES] values of `width` bytes, positioned after
  /// the prefix.
  private static ByteBuffer newBuffer(boolean direct, ByteOrder order, int width) {
    int capacity = PREFIX + NUM_VALUES * width;
    ByteBuffer buffer = direct ? ByteBuffer.allocateDirect(capacity) : ByteBuffer.allocate(capacity);
    return buffer.order(order).position(PREFIX);
  }

  /// A NaN with a payload at index 1, negative zero at index 2, and distinct values elsewhere.
  private static int floatBits(int index) {
    if (index == 1) {
      return 0x7fa1_2345;
    }
    return Float.floatToRawIntBits(index == 2 ? -0.0f : (index - 100) * 1.5f);
  }

  private static long doubleBits(int index) {
    if (index == 1) {
      return 0x7ff8_0000_0000_0123L;
    }
    return Double.doubleToRawLongBits(index == 2 ? -0.0 : (index - 100) * 1.5);
  }

  private static void assertUntouched(Object values, int length) {
    int size = Array.getLength(values);
    for (int i = 0; i < OFFSET; i++) {
      assertEquals(Array.getDouble(values, i), 0.0);
    }
    for (int i = OFFSET + length; i < size; i++) {
      assertEquals(Array.getDouble(values, i), 0.0);
    }
  }
}
