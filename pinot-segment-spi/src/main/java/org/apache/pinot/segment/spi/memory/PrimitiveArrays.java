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


/// Bulk reads of primitive arrays from a [ByteBuffer] through its typed views, and the threshold below which one
/// access per value beats a view. The class is stateless and thread-safe.
public final class PrimitiveArrays {
  private PrimitiveArrays() {
  }

  // TODO: Retune this threshold. JMH runs on aarch64 (Apple M3 Max, JDK 25) put the crossover at about 110-200 values
  //   for reads, 300-500 for int writes and beyond 4096 for double writes, so a single threshold may not fit them all.
  /// Below this number of values, reads and writes through a big-endian typed view (`asIntBuffer().get(int[])` and
  /// the like) are slower than one access per value. On a little-endian CPU the bulk copy swaps the bytes in a native
  /// call with a fixed cost of about 10-40 ns, while a single-value access compiles to an inlined byte swap. Measured
  /// with BenchmarkPrimitiveListDeserialize and BenchmarkPagedPinotOutputStream on aarch64 and x86_64.
  public static final int MIN_BULK_VALUES = 64;

  /// Reads `length` ints from the current position of `buffer`, in its byte order, into `values` from `offset`,
  /// and advances the position past them.
  public static void readInts(ByteBuffer buffer, int[] values, int offset, int length) {
    if (length < MIN_BULK_VALUES) {
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
    if (length < MIN_BULK_VALUES) {
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
    if (length < MIN_BULK_VALUES) {
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
    if (length < MIN_BULK_VALUES) {
      for (int i = offset; i < offset + length; i++) {
        values[i] = buffer.getDouble();
      }
    } else {
      buffer.asDoubleBuffer().get(values, offset, length);
      buffer.position(buffer.position() + length * Double.BYTES);
    }
  }
}
