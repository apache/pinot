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

import java.nio.ByteBuffer;


/// Bulk reads of primitive arrays from a [ByteBuffer] through its typed views. The class is stateless and thread-safe.
///
/// On a little-endian CPU, a copy through a big-endian view (`asIntBuffer().get(int[])` and the like) swaps the bytes
/// in a native call with a fixed cost of 20 to 40 ns, so a short read gets one value at a time instead. With JMH on
/// JDK 25.0.5, reading from heap and direct buffers held in a field (as a deserializer gets them), the view was
/// 1 to 16% faster than one value at a time at 96 longs or doubles on Intel Xeon 6973P-C, AMD EPYC 7763 and
/// Neoverse N2 machines, and 39 to 46% slower on an Apple M3 Max, where it caught up at about 160. For ints and
/// floats, one value at a time took several times as long in some JVM launches as in others on those three machines,
/// so no threshold suits every launch; on the M3 the view caught up at 96 to 128.
public final class PrimitiveArrays {
  private PrimitiveArrays() {
  }

  /// The `read*` methods read at least this many values through a typed view, and fewer one at a time.
  public static final int MIN_BULK_READ_VALUES = 96;

  /// Reads `length` ints from the current position of `buffer`, in its byte order, into `values` from `offset`,
  /// and advances the position past them.
  public static void readInts(ByteBuffer buffer, int[] values, int offset, int length) {
    if (length < MIN_BULK_READ_VALUES) {
      for (int i = offset; i < offset + length; i++) {
        values[i] = buffer.getInt();
      }
    } else {
      buffer.asIntBuffer().get(values, offset, length);
      buffer.position(buffer.position() + length * Integer.BYTES);
    }
  }

  /// Reads `length` longs from the current position of `buffer`, in its byte order, into `values` from `offset`,
  /// and advances the position past them.
  public static void readLongs(ByteBuffer buffer, long[] values, int offset, int length) {
    if (length < MIN_BULK_READ_VALUES) {
      for (int i = offset; i < offset + length; i++) {
        values[i] = buffer.getLong();
      }
    } else {
      buffer.asLongBuffer().get(values, offset, length);
      buffer.position(buffer.position() + length * Long.BYTES);
    }
  }

  /// Reads `length` floats from the current position of `buffer`, in its byte order, into `values` from `offset`,
  /// and advances the position past them.
  public static void readFloats(ByteBuffer buffer, float[] values, int offset, int length) {
    if (length < MIN_BULK_READ_VALUES) {
      for (int i = offset; i < offset + length; i++) {
        values[i] = buffer.getFloat();
      }
    } else {
      buffer.asFloatBuffer().get(values, offset, length);
      buffer.position(buffer.position() + length * Float.BYTES);
    }
  }

  /// Reads `length` doubles from the current position of `buffer`, in its byte order, into `values` from `offset`,
  /// and advances the position past them.
  public static void readDoubles(ByteBuffer buffer, double[] values, int offset, int length) {
    if (length < MIN_BULK_READ_VALUES) {
      for (int i = offset; i < offset + length; i++) {
        values[i] = buffer.getDouble();
      }
    } else {
      buffer.asDoubleBuffer().get(values, offset, length);
      buffer.position(buffer.position() + length * Double.BYTES);
    }
  }
}
