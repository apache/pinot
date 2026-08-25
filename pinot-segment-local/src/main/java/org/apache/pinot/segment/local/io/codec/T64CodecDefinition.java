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
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/// Frame-of-reference (FOR) bit-packing transform on fixed-size 64-value blocks for SV INT/LONG.
///
/// DSL form: `T64` (no arguments)
///
/// Stateless and thread-safe; the [#INSTANCE] singleton is shared across all columns.
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
/// **Chainability.** Unlike the typed-layout-preserving DELTA/DELTADELTA transforms, T64 is a packing
/// transform: it consumes column-typed input and produces a headered byte frame. It must be the
/// last transform, but one or more byte-compression stages may follow (e.g. `T64,LZ4`).
final class T64CodecDefinition implements ChunkCodecHandler<T64CodecDefinition.Options> {

  /// On-disk permanent name stored verbatim in segment file headers.
  /// This string is a frozen on-disk API contract and must never be changed.
  public static final String NAME = "T64";

  public static final T64CodecDefinition INSTANCE = new T64CodecDefinition();

  /// Singleton options — T64 has no configurable parameters.
  public static final Options OPTIONS = new Options();

  /// Block size in values. Frozen on-disk constant.
  static final int BLOCK_SIZE = 64;

  private T64CodecDefinition() {
  }

  /// Typed options for [T64CodecDefinition]. T64 has no configurable parameters.
  public static final class Options implements CodecOptions {
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
    return OPTIONS;
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
    if (inputSize < 0) {
      throw new IllegalArgumentException("T64 inputSize must be non-negative: " + inputSize);
    }
    long approxBlocks = ((long) inputSize + BLOCK_SIZE * Integer.BYTES - 1)
        / (BLOCK_SIZE * Integer.BYTES);
    long bound = 1L + Integer.BYTES + approxBlocks * (Long.BYTES + 1L + Long.BYTES * BLOCK_SIZE);
    if (bound > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("T64 maximum encoded size exceeds Integer.MAX_VALUE: " + bound);
    }
    return (int) bound;
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

    // Worst-case allocation upper bound — see maxEncodedSize.
    int numBlocks = (count + BLOCK_SIZE - 1) / BLOCK_SIZE;
    int worstCase = 1 + Integer.BYTES + numBlocks * (elementSize + 1 + elementSize * BLOCK_SIZE);
    ByteBuffer out = ByteBuffer.allocateDirect(worstCase);
    out.put((byte) (isLong ? 1 : 0));
    out.putInt(count);

    // Reusable per-encode scratch buffer for packed-bytes; sized for the worst case
    // `elementSize * BLOCK_SIZE` bytes (when bitWidth == elementBits) so it never needs to grow.
    byte[] packedBuf = new byte[elementSize * BLOCK_SIZE];
    // Decode and pack one block at a time. Retaining the whole chunk in a long[] would add up to
    // 2 MiB of transient heap for a normal 1 MiB INT chunk, multiplied across concurrent writers.
    long[] blockValues = new long[BLOCK_SIZE];

    for (int blockStart = 0; blockStart < count; blockStart += BLOCK_SIZE) {
      int blockCount = Math.min(BLOCK_SIZE, count - blockStart);
      long min = Long.MAX_VALUE;
      long max = Long.MIN_VALUE;
      for (int i = 0; i < blockCount; i++) {
        // Sign-extend INT to LONG so arithmetic uses two's-complement semantics for negative
        // deltas.
        long v = isLong ? src.getLong() : src.getInt();
        blockValues[i] = v;
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
      packBits(blockValues, blockCount, min, bitWidth, out, packedBuf);
    }

    out.flip();
    return out;
  }

  @Override
  public ByteBuffer decode(Options options, CodecContext ctx, ByteBuffer src) {
    src = src.duplicate().order(ByteOrder.BIG_ENDIAN);
    byte flag = src.get();
    int count = src.getInt();
    validateHeader(flag, count, ctx);
    if (count == 0) {
      ensureFullyConsumed(src);
      return ByteBuffer.allocateDirect(0);
    }
    boolean isLong = flag == 1;
    int elementSize = isLong ? Long.BYTES : Integer.BYTES;
    ByteBuffer out = ByteBuffer.allocateDirect(decodedSize(count, elementSize));
    decodeBlocks(src, count, isLong, out);
    out.flip();
    return out;
  }

  @Override
  public void decodeInto(Options options, CodecContext ctx, ByteBuffer src, ByteBuffer dst) {
    src = src.duplicate().order(ByteOrder.BIG_ENDIAN);
    dst.clear();
    byte flag = src.get();
    int count = src.getInt();
    validateHeader(flag, count, ctx);
    if (count == 0) {
      ensureFullyConsumed(src);
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
  private static void validateHeader(byte flag, int count, CodecContext ctx) {
    if (count < 0) {
      throw new IllegalStateException(
          "T64: invalid count in header: " + count + ". Segment may be corrupt.");
    }
    if (flag != 0 && flag != 1) {
      throw new IllegalStateException(
          "Unknown T64 type flag: " + flag + ". Expected 0 (INT) or 1 (LONG). Segment may be corrupt.");
    }
    DataType dataType = ctx.getDataType();
    if (dataType != DataType.INT && dataType != DataType.LONG) {
      throw new IllegalStateException("T64 cannot decode column type " + dataType);
    }
    byte expectedFlag = dataType == DataType.LONG ? (byte) 1 : (byte) 0;
    if (flag != expectedFlag) {
      throw new IllegalStateException(
          "T64 frame type " + (flag == 1 ? "LONG" : "INT") + " does not match column type "
              + dataType + ". Segment may be corrupt.");
    }
  }

  // -------------------------------------------------------------------------
  // Bit-packing helpers
  // -------------------------------------------------------------------------

  /// Pack `blockCount` (≤ [#BLOCK_SIZE]) values, subtracting `baseline` from each, into
  /// `bitWidth`-wide slots laid out in a fixed-size BLOCK_SIZE-slot frame. The output is written
  /// at the current `out.position()` and uses `ceil(bitWidth * BLOCK_SIZE / 8)` bytes.
  ///
  /// `scratch` is a caller-supplied reusable buffer sized for the worst-case packed bytes
  /// per block (i.e. `elementSize * BLOCK_SIZE`); only the first `ceil(bitWidth * BLOCK_SIZE / 8)`
  /// bytes are read.
  private static void packBits(long[] values, int blockCount, long baseline,
      int bitWidth, ByteBuffer out, byte[] scratch) {
    int packedBytes = (bitWidth * BLOCK_SIZE + 7) / 8;
    // Zero out the bytes we'll write into (writeBits OR-merges into existing bits).
    Arrays.fill(scratch, 0, packedBytes, (byte) 0);

    long bitCursor = 0;
    for (int i = 0; i < BLOCK_SIZE; i++) {
      long value = (i < blockCount) ? (values[i] - baseline) : 0L;
      writeBits(scratch, bitCursor, value, bitWidth);
      bitCursor += bitWidth;
    }
    out.put(scratch, 0, packedBytes);
  }

  /// Decode one or more T64 blocks until `count` values have been read, appending the
  /// resulting INT/LONG values to `dst`.
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
        // The encoder reserves fixed-width slots for the missing tail of a partial block and
        // writes them as zero. Reject non-canonical non-zero tail slots rather than silently
        // accepting alternate bytes for the same value sequence.
        for (int i = blockCount; i < BLOCK_SIZE; i++) {
          if (readBits(packedBuf, bitCursor, bitWidth) != 0) {
            throw new IllegalStateException(
                "T64: non-zero padding slot in final block. Segment may be corrupt.");
          }
          bitCursor += bitWidth;
        }
      }
      remainingValues -= blockCount;
    }
    ensureFullyConsumed(src);
  }

  private static int decodedSize(int count, int elementSize) {
    long decodedSize = (long) count * elementSize;
    if (decodedSize > Integer.MAX_VALUE) {
      throw new IllegalStateException(
          "T64: decoded size " + decodedSize + " exceeds Integer.MAX_VALUE. Segment may be corrupt.");
    }
    return (int) decodedSize;
  }

  private static void ensureFullyConsumed(ByteBuffer src) {
    if (src.hasRemaining()) {
      throw new IllegalStateException(
          "T64: trailing " + src.remaining() + " byte(s) after frame. Segment may be corrupt.");
    }
  }

  /// Write `value` into `buf` at the given bit offset, packing
  /// `bitWidth` bits (LSB-first within each byte). `value` must fit in
  /// `bitWidth` bits; only the low `bitWidth` bits are used.
  ///
  /// Implementation extracts `take`-bit chunks **from the low end of `value`**
  /// rather than left-shifting `value` into a 64-bit register, so it is safe for
  /// `bitInByte + bitWidth > 64`.
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

  /// Read `bitWidth` bits from `buf` starting at `bitOffset`.
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
