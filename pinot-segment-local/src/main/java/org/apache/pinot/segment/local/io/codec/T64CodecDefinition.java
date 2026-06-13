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
package org.apache.pinot.segment.local.io.codec;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import org.apache.pinot.segment.spi.codec.ChunkCodecHandler;
import org.apache.pinot.segment.spi.codec.CodecContext;
import org.apache.pinot.segment.spi.codec.CodecKind;
import org.apache.pinot.segment.spi.codec.CodecOptions;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/// Frame-of-reference (FOR) bit-packing transform on fixed-size 64-value blocks for SV INT/LONG.
///
/// Each block of 64 values is encoded as:
/// ```
///   [baseline : elementSize bytes]   // min value in the block
///   [bitWidth : 1 byte]              // 0..elementBits, bits needed for (value - baseline)
///   [packed   : ceil(bitWidth * blockCount / 8) bytes]  // bit-packed deltas
/// ```
/// The final block may contain fewer than 64 values; its bit-packed payload still uses 64-value
/// granularity (zeros for missing slots) so the on-disk size is `ceil(bitWidth * 64 / 8)`.
///
/// Wire format for the full encoded frame:
/// ```
///   [flag : 1 byte]      // 0=INT, 1=LONG
///   [count: 4 bytes]     // total number of values
///   [block_0]
///   [block_1]
///   ...
///   [block_{N-1}]
/// ```
/// `count == 0` is encoded as the 5-byte header with no blocks.
///
/// **Chainability.** Like DELTA/DELTADELTA, T64 is a terminal transform — it consumes column-typed
/// input and produces a headered frame. It chains with a final byte-oriented compression stage
/// (e.g. `CODEC(T64, LZ4)`) but not with another transform until a wire-format redesign lands.
public final class T64CodecDefinition implements ChunkCodecHandler<T64CodecDefinition.Options> {

  /// On-disk permanent name stored verbatim in segment file headers.
  /// This string is a frozen on-disk API contract and must never be changed.
  public static final String NAME = "T64";

  public static final T64CodecDefinition INSTANCE = new T64CodecDefinition();

  /// Block size in values. Frozen on-disk constant.
  static final int BLOCK_SIZE = 64;

  private T64CodecDefinition() {
  }

  /// Singleton options object — T64 has no configurable parameters.
  public static final class Options implements CodecOptions {
    public static final Options INSTANCE = new Options();

    private Options() {
    }
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public CodecKind kind() {
    return CodecKind.TRANSFORM;
  }

  @Override
  public Options parseOptions(List<String> args) {
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("T64 codec takes no arguments but got: " + args);
    }
    return Options.INSTANCE;
  }

  @Override
  public void validateContext(Options options, CodecContext ctx) {
    DataType dt = ctx.getDataType();
    if (dt != DataType.INT && dt != DataType.LONG) {
      throw new IllegalArgumentException(
          "T64 codec only supports INT and LONG columns, but column has type: " + dt);
    }
  }

  @Override
  public String canonicalize(Options options) {
    return NAME;
  }

  @Override
  public boolean requiresDirectDstBuffer() {
    return false;
  }

  @Override
  public int maxEncodedSize(Options options, int inputSize) {
    // The on-disk size is 5-byte frame header + per-block (baseline + bitWidth + packed).
    // A partial last block still writes a full `elementSize * BLOCK_SIZE` packed-payload of
    // bit-packed slots (zero-filled at the tail), so payload size scales with **block count**,
    // not with input value count.
    //
    // Element size isn't knowable from inputSize alone (the SPI passes only byte count), so we
    // use the LONG worst case (elementSize = Long.BYTES) which is a valid upper bound for both
    // INT and LONG inputs. Block count is bounded above using the smaller element size,
    // Integer.BYTES, since inputSize / (Integer.BYTES * BLOCK_SIZE) ≥ inputSize / (Long.BYTES *
    // BLOCK_SIZE) for any element size in {Integer.BYTES, Long.BYTES}.
    int approxBlocks = (inputSize + BLOCK_SIZE * Integer.BYTES - 1) / (BLOCK_SIZE * Integer.BYTES);
    return 1 + Integer.BYTES + approxBlocks * (Long.BYTES + 1 + Long.BYTES * BLOCK_SIZE);
  }

  @Override
  public ByteBuffer encode(Options options, CodecContext ctx, ByteBuffer src) {
    int remaining = src.remaining();
    DataType dt = ctx.getDataType();
    if (dt != DataType.INT && dt != DataType.LONG) {
      throw new IllegalArgumentException("T64 does not support stored type: " + dt);
    }
    int elementSize = dt.size();
    if (remaining % elementSize != 0) {
      throw new IllegalArgumentException(
          "T64: input buffer size (" + remaining + ") is not a multiple of element size (" + elementSize + ")");
    }
    int count = remaining / elementSize;
    boolean isLong = dt == DataType.LONG;

    long[] values = new long[count];
    if (isLong) {
      for (int i = 0; i < count; i++) {
        values[i] = src.getLong();
      }
    } else {
      for (int i = 0; i < count; i++) {
        // Sign-extend INT to LONG so arithmetic uses two's-complement semantics for negative deltas.
        values[i] = src.getInt();
      }
    }

    // Worst-case allocation upper bound — see maxEncodedSize.
    int numBlocks = (count + BLOCK_SIZE - 1) / BLOCK_SIZE;
    int worstCase = 1 + Integer.BYTES + numBlocks * (elementSize + 1 + elementSize * BLOCK_SIZE);
    ByteBuffer out = ByteBuffer.allocateDirect(worstCase);
    out.put((byte) (isLong ? 1 : 0));
    out.putInt(count);

    // Reusable per-encode scratch buffer for packed-bytes; sized for the worst case
    // `elementSize * BLOCK_SIZE` bytes (when bitWidth == elementBits) so it never needs to grow.
    byte[] packedBuf = new byte[elementSize * BLOCK_SIZE];

    for (int blockStart = 0; blockStart < count; blockStart += BLOCK_SIZE) {
      int blockCount = Math.min(BLOCK_SIZE, count - blockStart);
      long min = values[blockStart];
      long max = values[blockStart];
      for (int i = 1; i < blockCount; i++) {
        long v = values[blockStart + i];
        if (v < min) {
          min = v;
        }
        if (v > max) {
          max = v;
        }
      }
      // Range is unsigned: max - min ≥ 0 for any two values in [Long.MIN_VALUE, Long.MAX_VALUE]
      // and ≥ 0 in [Integer.MIN_VALUE, Integer.MAX_VALUE] after sign-extension. The bitWidth
      // upper bound is elementBits (32 for INT, 64 for LONG); this is an invariant guaranteed
      // by the source-type range and the encode loop.
      long range = max - min;
      int bitWidth = (range == 0) ? 0 : 64 - Long.numberOfLeadingZeros(range);
      // Unconditional check — `assert` is off in production JVMs and a silent bitWidth overflow
      // here would corrupt the encoded frame.
      if (bitWidth > elementSize * 8) {
        throw new IllegalStateException(
            "T64 encode: bitWidth " + bitWidth + " exceeds elementBits " + (elementSize * 8));
      }

      // Baseline + bitWidth header.
      if (isLong) {
        out.putLong(min);
      } else {
        out.putInt((int) min);
      }
      out.put((byte) bitWidth);

      if (bitWidth == 0) {
        continue; // all values equal min; no packed bytes
      }

      // Pack BLOCK_SIZE values; missing tail slots get zero (i.e. baseline).
      packBits(values, blockStart, blockCount, min, bitWidth, out, packedBuf);
    }

    out.flip();
    return out;
  }

  @Override
  public ByteBuffer decode(Options options, CodecContext ctx, ByteBuffer src) {
    byte flag = src.get();
    int count = src.getInt();
    validateHeader(flag, count);
    if (count == 0) {
      return ByteBuffer.allocateDirect(0);
    }
    boolean isLong = flag == 1;
    int elementSize = isLong ? Long.BYTES : Integer.BYTES;
    ByteBuffer out = ByteBuffer.allocateDirect(count * elementSize);
    decodeBlocks(src, count, isLong, out);
    out.flip();
    return out;
  }

  @Override
  public void decodeInto(Options options, CodecContext ctx, ByteBuffer src, ByteBuffer dst) {
    dst.clear();
    byte flag = src.get();
    int count = src.getInt();
    validateHeader(flag, count);
    if (count == 0) {
      dst.flip();
      return;
    }
    boolean isLong = flag == 1;
    int elementSize = isLong ? Long.BYTES : Integer.BYTES;
    long requiredCapacity = (long) count * elementSize;
    if (requiredCapacity > dst.capacity()) {
      throw new IllegalArgumentException(
          "T64: decoded size " + requiredCapacity + " exceeds dst capacity " + dst.capacity());
    }
    decodeBlocks(src, count, isLong, dst);
    dst.flip();
  }

  /// Validate the frame header: count must be ≥ 0 and flag must be a known type. Validating
  /// **both** unconditionally (before any count-based short-circuit) means a corrupt segment
  /// with `flag=42, count=0` is rejected rather than silently decoding to empty.
  private static void validateHeader(byte flag, int count) {
    if (count < 0) {
      throw new IllegalStateException(
          "T64: invalid count in header: " + count + ". Segment may be corrupt.");
    }
    if (flag != 0 && flag != 1) {
      throw new IllegalStateException(
          "Unknown T64 type flag: " + flag + ". Expected 0 (INT) or 1 (LONG). Segment may be corrupt.");
    }
  }

  // -------------------------------------------------------------------------
  // Bit-packing helpers
  // -------------------------------------------------------------------------

  /// Pack `blockCount` (≤ {@link #BLOCK_SIZE}) values, subtracting `baseline` from each, into
  /// `bitWidth`-wide slots laid out in a fixed-size BLOCK_SIZE-slot frame. The output is written
  /// at the current `out.position()` and uses `ceil(bitWidth * BLOCK_SIZE / 8)` bytes.
  ///
  /// {@code scratch} is a caller-supplied reusable buffer sized for the worst-case packed bytes
  /// per block (i.e. `elementSize * BLOCK_SIZE`); only the first `ceil(bitWidth * BLOCK_SIZE / 8)`
  /// bytes are read.
  private static void packBits(long[] values, int blockStart, int blockCount, long baseline,
      int bitWidth, ByteBuffer out, byte[] scratch) {
    int packedBytes = (bitWidth * BLOCK_SIZE + 7) / 8;
    // Zero out the bytes we'll write into (writeBits OR-merges into existing bits).
    Arrays.fill(scratch, 0, packedBytes, (byte) 0);

    long bitCursor = 0;
    for (int i = 0; i < BLOCK_SIZE; i++) {
      long value = (i < blockCount) ? (values[blockStart + i] - baseline) : 0L;
      writeBits(scratch, bitCursor, value, bitWidth);
      bitCursor += bitWidth;
    }
    out.put(scratch, 0, packedBytes);
  }

  /// Decode one or more T64 blocks until {@code count} values have been read, appending the
  /// resulting INT/LONG values to {@code dst}.
  private static void decodeBlocks(ByteBuffer src, int count, boolean isLong, ByteBuffer dst) {
    // Reusable per-decode scratch buffer; sized for the worst case (bitWidth = elementBits).
    int elementSize = isLong ? Long.BYTES : Integer.BYTES;
    byte[] packedBuf = new byte[elementSize * BLOCK_SIZE];
    int remainingValues = count;
    while (remainingValues > 0) {
      long baseline = isLong ? src.getLong() : src.getInt();
      // `& 0xFF` guarantees bitWidth ∈ [0, 255], so we only need to check the upper bound.
      int bitWidth = src.get() & 0xFF;
      if (bitWidth > 64 || (!isLong && bitWidth > 32)) {
        throw new IllegalStateException(
            "T64: invalid bit width " + bitWidth + ". Segment may be corrupt.");
      }
      int blockCount = Math.min(BLOCK_SIZE, remainingValues);
      if (bitWidth == 0) {
        // All values in this block equal baseline.
        if (isLong) {
          for (int i = 0; i < blockCount; i++) {
            dst.putLong(baseline);
          }
        } else {
          int baseInt = (int) baseline;
          for (int i = 0; i < blockCount; i++) {
            dst.putInt(baseInt);
          }
        }
      } else {
        int packedBytes = (bitWidth * BLOCK_SIZE + 7) / 8;
        src.get(packedBuf, 0, packedBytes);
        long bitCursor = 0;
        if (isLong) {
          for (int i = 0; i < blockCount; i++) {
            long v = readBits(packedBuf, bitCursor, bitWidth) + baseline;
            dst.putLong(v);
            bitCursor += bitWidth;
          }
        } else {
          for (int i = 0; i < blockCount; i++) {
            int v = (int) (readBits(packedBuf, bitCursor, bitWidth) + baseline);
            dst.putInt(v);
            bitCursor += bitWidth;
          }
        }
      }
      remainingValues -= blockCount;
    }
  }

  /// Write {@code value} into {@code buf} at the given bit offset, packing
  /// {@code bitWidth} bits (LSB-first within each byte). {@code value} must fit in
  /// {@code bitWidth} bits; only the low {@code bitWidth} bits are used.
  ///
  /// Implementation extracts {@code take}-bit chunks **from the low end of {@code value}**
  /// rather than left-shifting {@code value} into a 64-bit register, so it is safe for
  /// {@code bitInByte + bitWidth > 64}.
  private static void writeBits(byte[] buf, long bitOffset, long value, int bitWidth) {
    long cursor = bitOffset;
    int valueShift = 0;
    int bitsRemaining = bitWidth;
    while (bitsRemaining > 0) {
      int byteIndex = (int) (cursor >>> 3);
      int bitInByte = (int) (cursor & 7);
      int take = Math.min(8 - bitInByte, bitsRemaining);
      // `take` is in [1..8] (bounded by `8 - bitInByte`), so a byte-sized mask always suffices.
      long takeMask = (1L << take) - 1L;
      long chunk = (value >>> valueShift) & takeMask;
      buf[byteIndex] |= (byte) (chunk << bitInByte);
      cursor += take;
      valueShift += take;
      bitsRemaining -= take;
    }
  }

  /// Read {@code bitWidth} bits from {@code buf} starting at {@code bitOffset}.
  private static long readBits(byte[] buf, long bitOffset, int bitWidth) {
    long byteIndex = bitOffset >>> 3;
    int bitInByte = (int) (bitOffset & 7);
    long out = 0L;
    int shift = 0;
    int bitsRemaining = bitWidth;
    int idx = (int) byteIndex;
    while (bitsRemaining > 0) {
      long b = ((long) buf[idx]) & 0xFFL;
      int available = 8 - bitInByte;
      int take = Math.min(available, bitsRemaining);
      // `take` is in [1..8] (bounded by `available`), so a byte-sized mask always suffices.
      long mask = (1L << take) - 1L;
      out |= ((b >>> bitInByte) & mask) << shift;
      shift += take;
      bitsRemaining -= take;
      idx++;
      bitInByte = 0;
    }
    return out;
  }
}
