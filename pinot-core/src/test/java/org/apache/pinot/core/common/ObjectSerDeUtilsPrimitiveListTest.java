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
package org.apache.pinot.core.common;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Function;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;


/// Checks that the bulk reads of the primitive array list SerDes return the same values and leave the buffer at the
/// same position as reading the values one by one, for heap, direct, little-endian and sliced buffers.
public class ObjectSerDeUtilsPrimitiveListTest {
  private static final int PREFIX = 3;
  private static final int SUFFIX = 5;

  /// A list SerDe with a per-value oracle reader.
  private enum ListType {
    INT(Integer.BYTES) {
      @Override
      Object newList(Random random, int size) {
        return new IntArrayList(random.ints(size).toArray());
      }

      @Override
      ObjectSerDeUtils.ObjectSerDe<?> serDe() {
        return ObjectSerDeUtils.INT_ARRAY_LIST_SER_DE;
      }

      @Override
      void putValue(ByteBuffer buffer, Object list, int index) {
        buffer.putInt(((IntArrayList) list).getInt(index));
      }

      @Override
      Object readOneByOne(ByteBuffer buffer) {
        int numValues = buffer.getInt();
        IntArrayList list = new IntArrayList(numValues);
        for (int i = 0; i < numValues; i++) {
          list.add(buffer.getInt());
        }
        return list;
      }
    },
    LONG(Long.BYTES) {
      @Override
      Object newList(Random random, int size) {
        return new LongArrayList(random.longs(size).toArray());
      }

      @Override
      ObjectSerDeUtils.ObjectSerDe<?> serDe() {
        return ObjectSerDeUtils.LONG_ARRAY_LIST_SER_DE;
      }

      @Override
      void putValue(ByteBuffer buffer, Object list, int index) {
        buffer.putLong(((LongArrayList) list).getLong(index));
      }

      @Override
      Object readOneByOne(ByteBuffer buffer) {
        int numValues = buffer.getInt();
        LongArrayList list = new LongArrayList(numValues);
        for (int i = 0; i < numValues; i++) {
          list.add(buffer.getLong());
        }
        return list;
      }
    },
    FLOAT(Float.BYTES) {
      @Override
      Object newList(Random random, int size) {
        FloatArrayList list = new FloatArrayList(size);
        for (int i = 0; i < size; i++) {
          list.add(i == 0 ? Float.intBitsToFloat(0x7fc0_0123) : random.nextFloat() - 0.5f);
        }
        return list;
      }

      @Override
      ObjectSerDeUtils.ObjectSerDe<?> serDe() {
        return ObjectSerDeUtils.FLOAT_ARRAY_LIST_SER_DE;
      }

      @Override
      void putValue(ByteBuffer buffer, Object list, int index) {
        buffer.putFloat(((FloatArrayList) list).getFloat(index));
      }

      @Override
      Object readOneByOne(ByteBuffer buffer) {
        int numValues = buffer.getInt();
        FloatArrayList list = new FloatArrayList(numValues);
        for (int i = 0; i < numValues; i++) {
          list.add(buffer.getFloat());
        }
        return list;
      }
    },
    DOUBLE(Double.BYTES) {
      @Override
      Object newList(Random random, int size) {
        DoubleArrayList list = new DoubleArrayList(size);
        for (int i = 0; i < size; i++) {
          // A NaN with a payload keeps its raw bits on both paths.
          list.add(i == 0 ? Double.longBitsToDouble(0x7ff8_0000_0000_0123L) : random.nextDouble() - 0.5);
        }
        return list;
      }

      @Override
      ObjectSerDeUtils.ObjectSerDe<?> serDe() {
        return ObjectSerDeUtils.DOUBLE_ARRAY_LIST_SER_DE;
      }

      @Override
      void putValue(ByteBuffer buffer, Object list, int index) {
        buffer.putDouble(((DoubleArrayList) list).getDouble(index));
      }

      @Override
      Object readOneByOne(ByteBuffer buffer) {
        int numValues = buffer.getInt();
        DoubleArrayList list = new DoubleArrayList(numValues);
        for (int i = 0; i < numValues; i++) {
          list.add(buffer.getDouble());
        }
        return list;
      }
    };

    final int _width;

    ListType(int width) {
      _width = width;
    }

    abstract Object newList(Random random, int size);

    abstract ObjectSerDeUtils.ObjectSerDe<?> serDe();

    abstract void putValue(ByteBuffer buffer, Object list, int index);

    abstract Object readOneByOne(ByteBuffer buffer);
  }

  /// How the serialized bytes are laid out in the buffer that is read.
  private enum Layout {
    HEAP(size -> ByteBuffer.allocate(size)),
    DIRECT(ByteBuffer::allocateDirect),
    // The SerDes read in the buffer's byte order, whatever it is.
    LITTLE_ENDIAN(size -> ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN));

    final Function<Integer, ByteBuffer> _allocator;

    Layout(Function<Integer, ByteBuffer> allocator) {
      _allocator = allocator;
    }
  }

  @DataProvider
  public Object[][] cases() {
    List<Object[]> cases = new ArrayList<>();
    for (ListType type : ListType.values()) {
      for (Layout layout : Layout.values()) {
        for (int size : new int[]{0, 1, 1000}) {
          for (boolean slice : new boolean[]{false, true}) {
            cases.add(new Object[]{type, layout, size, slice});
          }
        }
      }
    }
    return cases.toArray(new Object[0][]);
  }

  /// Writes `size` values with surrounding bytes, the way a Map or List SerDe nests values: [PREFIX bytes][count]
  /// [values][SUFFIX bytes]. The returned buffer is positioned at the count.
  private static ByteBuffer write(ListType type, Layout layout, Object list, int size, boolean slice) {
    ByteBuffer buffer = layout._allocator.apply(PREFIX + Integer.BYTES + size * type._width + SUFFIX);
    for (int i = 0; i < PREFIX; i++) {
      buffer.put((byte) 0x55);
    }
    buffer.putInt(size);
    for (int i = 0; i < size; i++) {
      type.putValue(buffer, list, i);
    }
    while (buffer.hasRemaining()) {
      buffer.put((byte) 0x66);
    }
    buffer.position(PREFIX);
    return slice ? buffer.slice().order(buffer.order()) : buffer;
  }

  @Test(dataProvider = "cases")
  public void testBulkReadMatchesOneByOne(ListType type, Layout layout, int size, boolean slice) {
    Object list = type.newList(new Random(size), size);
    ByteBuffer bulkBuffer = write(type, layout, list, size, slice);
    ByteBuffer oracleBuffer = bulkBuffer.duplicate().order(bulkBuffer.order());

    @SuppressWarnings("unchecked")
    ObjectSerDeUtils.ObjectSerDe<Object> serDe = (ObjectSerDeUtils.ObjectSerDe<Object>) type.serDe();
    Object actual = serDe.deserialize(bulkBuffer);
    Object expected = type.readOneByOne(oracleBuffer);

    // Compares the serialized bytes, so floating point values must match bit for bit, NaN payloads included.
    assertEquals(serDe.serialize(actual), serDe.serialize(expected));
    assertEquals(bulkBuffer.position(), oracleBuffer.position());
    assertEquals(bulkBuffer.remaining(), SUFFIX);
    if (layout != Layout.LITTLE_ENDIAN) {
      assertEquals(serDe.serialize(actual), serDe.serialize(list));
    }
  }

  @Test(dataProvider = "cases")
  public void testSerializeThenDeserializeRoundTrips(ListType type, Layout layout, int size, boolean slice) {
    Object list = type.newList(new Random(size), size);
    @SuppressWarnings("unchecked")
    ObjectSerDeUtils.ObjectSerDe<Object> serDe = (ObjectSerDeUtils.ObjectSerDe<Object>) type.serDe();
    byte[] bytes = serDe.serialize(list);
    assertEquals(serDe.serialize(serDe.deserialize(bytes)), bytes);
  }

  @Test
  public void testTruncatedAndNegativeCounts() {
    for (ListType type : ListType.values()) {
      ByteBuffer truncated = ByteBuffer.allocate(Integer.BYTES + 2 * type._width).putInt(3);
      truncated.flip().limit(truncated.capacity());
      assertThrows(BufferUnderflowException.class, () -> type.serDe().deserialize(truncated));

      ByteBuffer negative = ByteBuffer.allocate(Integer.BYTES).putInt(0, -1);
      assertThrows(IllegalArgumentException.class, () -> type.serDe().deserialize(negative));
    }
  }
}
