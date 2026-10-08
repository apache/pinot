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
    if (length == 0) {
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
    if (length == 0) {
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
    if (length == 0) {
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
