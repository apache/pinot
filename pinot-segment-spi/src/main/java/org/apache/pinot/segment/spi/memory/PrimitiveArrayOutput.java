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

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;


/// Writes ranges of primitive arrays to a [DataOutput], with the same big-endian bytes as one [ByteBuffer#putInt(int)],
/// [ByteBuffer#putLong(long)] or [ByteBuffer#putDouble(double)] call per value. For ints and longs that is also what
/// [DataOutput#writeInt(int)] and [DataOutput#writeLong(long)] write. Doubles keep their raw bits, while
/// [DataOutput#writeDouble(double)] canonicalizes NaN.
///
/// A [PinotOutputStream] writes the values in bulk (see [PinotOutputStream#writeDoubles]). Any other [DataOutput] gets
/// the values encoded in chunks of at most [#CHUNK_SIZE] bytes, so a large array never needs a byte array of its full
/// size. The class is stateless and thread-safe.
public final class PrimitiveArrayOutput {
  /// Size in bytes of the scratch buffer used to encode a chunk. Small enough to be allocated in a TLAB.
  public static final int CHUNK_SIZE = 8 * 1024;

  /// Below this number of values, reads and writes through a big-endian typed view (`asIntBuffer().get(int[])` and
  /// the like) are slower than one access per value. On a little-endian CPU the bulk copy swaps the bytes in a native
  /// call with a fixed cost of about 10-40 ns, while a single-value access compiles to an inlined byte swap. Measured
  /// with BenchmarkPrimitiveListDeserialize and BenchmarkPrimitiveArrayOutput on aarch64 and x86_64.
  public static final int MIN_BULK_VALUES = 64;

  /// Below this number of values, the chunked writes call the [DataOutput] once per value: encoding a chunk costs a
  /// scratch buffer and a typed-view copy, which is more than a few small writes.
  static final int MIN_CHUNKED_VALUES = 16;

  private PrimitiveArrayOutput() {
  }

  public static void writeInts(DataOutput output, int[] values, int offset, int length)
      throws IOException {
    if (output instanceof PinotOutputStream) {
      ((PinotOutputStream) output).writeInts(values, offset, length);
    } else {
      writeIntsChunked(output, values, offset, length);
    }
  }

  public static void writeLongs(DataOutput output, long[] values, int offset, int length)
      throws IOException {
    if (output instanceof PinotOutputStream) {
      ((PinotOutputStream) output).writeLongs(values, offset, length);
    } else {
      writeLongsChunked(output, values, offset, length);
    }
  }

  public static void writeDoubles(DataOutput output, double[] values, int offset, int length)
      throws IOException {
    if (output instanceof PinotOutputStream) {
      ((PinotOutputStream) output).writeDoubles(values, offset, length);
    } else {
      writeDoublesChunked(output, values, offset, length);
    }
  }

  static void writeIntsChunked(DataOutput output, int[] values, int offset, int length)
      throws IOException {
    Objects.checkFromIndexSize(offset, length, values.length);
    if (length < MIN_CHUNKED_VALUES) {
      for (int i = offset; i < offset + length; i++) {
        output.writeInt(values[i]);
      }
      return;
    }
    ByteBuffer scratch = newScratch(length, Integer.BYTES);
    int chunkValues = scratch.capacity() / Integer.BYTES;
    int end = offset + length;
    for (int i = offset; i < end; i += chunkValues) {
      int numValues = Math.min(chunkValues, end - i);
      scratch.asIntBuffer().put(values, i, numValues);
      output.write(scratch.array(), 0, numValues * Integer.BYTES);
    }
  }

  static void writeLongsChunked(DataOutput output, long[] values, int offset, int length)
      throws IOException {
    Objects.checkFromIndexSize(offset, length, values.length);
    if (length < MIN_CHUNKED_VALUES) {
      for (int i = offset; i < offset + length; i++) {
        output.writeLong(values[i]);
      }
      return;
    }
    ByteBuffer scratch = newScratch(length, Long.BYTES);
    int chunkValues = scratch.capacity() / Long.BYTES;
    int end = offset + length;
    for (int i = offset; i < end; i += chunkValues) {
      int numValues = Math.min(chunkValues, end - i);
      scratch.asLongBuffer().put(values, i, numValues);
      output.write(scratch.array(), 0, numValues * Long.BYTES);
    }
  }

  static void writeDoublesChunked(DataOutput output, double[] values, int offset, int length)
      throws IOException {
    Objects.checkFromIndexSize(offset, length, values.length);
    if (length < MIN_CHUNKED_VALUES) {
      for (int i = offset; i < offset + length; i++) {
        // Raw bits, like the chunked path: writeDouble() would canonicalize NaN.
        output.writeLong(Double.doubleToRawLongBits(values[i]));
      }
      return;
    }
    ByteBuffer scratch = newScratch(length, Double.BYTES);
    int chunkValues = scratch.capacity() / Double.BYTES;
    int end = offset + length;
    for (int i = offset; i < end; i += chunkValues) {
      int numValues = Math.min(chunkValues, end - i);
      scratch.asDoubleBuffer().put(values, i, numValues);
      output.write(scratch.array(), 0, numValues * Double.BYTES);
    }
  }

  /// Returns a big-endian heap buffer for one chunk: [#CHUNK_SIZE] bytes, or less when all the values fit.
  private static ByteBuffer newScratch(int length, int width) {
    int capacity = (int) Math.min(CHUNK_SIZE, (long) length * width);
    return ByteBuffer.allocate(capacity).order(ByteOrder.BIG_ENDIAN);
  }
}
