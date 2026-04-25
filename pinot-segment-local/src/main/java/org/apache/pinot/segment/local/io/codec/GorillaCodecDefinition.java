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
import java.util.List;
import org.apache.pinot.segment.spi.codec.ChunkCodecHandler;
import org.apache.pinot.segment.spi.codec.CodecContext;
import org.apache.pinot.segment.spi.codec.CodecKind;
import org.apache.pinot.segment.spi.codec.CodecOptions;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/// Gorilla-style XOR compression transform for SV INT/LONG, adapted from
/// "Gorilla: A Fast, Scalable, In-Memory Time Series Database" (Facebook, VLDB 2015).
///
/// For each value v_i (i > 0):
/// - Compute `x = v_i XOR v_{i-1}`.
/// - If `x == 0`: write a single `0` bit.
/// - Else: write `1` bit + meaningful-bit window metadata + the meaningful bits themselves.
///   - If the new window fits inside the previous window (same leading + same width), write
///     a `0` control bit followed by the meaningful bits.
///   - Otherwise, write a `1` control bit, the leading-zero count (5 bits for INT, 6 bits for
///     LONG), the meaningful-bit count (5/6 bits, biased so width=N stored as 0), and the bits.
///
/// Wire format:
/// ```
///   [flag : 1 byte]     // 0=INT (32-bit), 1=LONG (64-bit)
///   [count: 4 bytes]    // total number of values
///   [first: elementSize bytes]   // verbatim first value (if count > 0)
///   [bit_stream: N bytes]        // variable-length per-value encoding (if count > 1)
/// ```
/// `count == 0` is the 5-byte header with no payload.
///
/// **Chainability.** Like the other v1 built-in transforms, Gorilla is terminal: it consumes
/// column-typed input and produces a headered frame, so it chains with a final byte-oriented
/// compression stage (e.g. `CODEC(GORILLA, LZ4)`) but not with another transform.
public final class GorillaCodecDefinition implements ChunkCodecHandler<GorillaCodecDefinition.Options> {

  /// On-disk permanent name stored verbatim in segment file headers.
  /// This string is a frozen on-disk API contract and must never be changed.
  public static final String NAME = "GORILLA";

  public static final GorillaCodecDefinition INSTANCE = new GorillaCodecDefinition();

  private GorillaCodecDefinition() {
  }

  /// Singleton options object — Gorilla has no configurable parameters.
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
      throw new IllegalArgumentException("GORILLA codec takes no arguments but got: " + args);
    }
    return Options.INSTANCE;
  }

  @Override
  public void validateContext(Options options, CodecContext ctx) {
    DataType dt = ctx.getDataType();
    if (dt != DataType.INT && dt != DataType.LONG) {
      throw new IllegalArgumentException(
          "GORILLA codec only supports INT and LONG columns, but column has type: " + dt);
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
    // Worst case: every value is incompressible XOR → control bits + leading + width + full bits.
    // Approx 2*elementBits + 13 bits per value. Round up generously to elementBits * 2 + 16 bits.
    // Cheap upper bound: header + 2 * inputSize bytes.
    return 1 + Integer.BYTES + 2 * inputSize + 16;
  }

  @Override
  public ByteBuffer encode(Options options, CodecContext ctx, ByteBuffer src) {
    int remaining = src.remaining();
    DataType dt = ctx.getDataType();
    if (dt != DataType.INT && dt != DataType.LONG) {
      throw new IllegalArgumentException("GORILLA does not support stored type: " + dt);
    }
    int elementSize = dt.size();
    if (remaining % elementSize != 0) {
      throw new IllegalArgumentException(
          "GORILLA: input buffer size (" + remaining + ") is not a multiple of element size ("
              + elementSize + ")");
    }
    int count = remaining / elementSize;
    boolean isLong = dt == DataType.LONG;
    int elementBits = elementSize * 8;
    int leadingBits = isLong ? 6 : 5;
    int widthBits = isLong ? 6 : 5;

    ByteBuffer out = ByteBuffer.allocateDirect(maxEncodedSize(options, remaining));
    out.put((byte) (isLong ? 1 : 0));
    out.putInt(count);
    if (count == 0) {
      out.flip();
      return out;
    }

    // First value verbatim
    long prev = isLong ? src.getLong() : (long) src.getInt();
    if (isLong) {
      out.putLong(prev);
    } else {
      out.putInt((int) prev);
    }
    if (count == 1) {
      out.flip();
      return out;
    }

    BitWriter writer = new BitWriter(out);
    int prevLeading = -1; // sentinel: no previous window
    int prevWidth = -1;

    for (int i = 1; i < count; i++) {
      long cur = isLong ? src.getLong() : (long) src.getInt();
      long x = cur ^ prev;
      if (x == 0) {
        writer.writeBit(0);
      } else {
        writer.writeBit(1);
        int leading;
        int width;
        int trailing;
        // The `x == 0` branch above guarantees x has at least one set bit, so leading and
        // trailing each return a value strictly less than the element width — no clamp needed.
        if (isLong) {
          leading = Long.numberOfLeadingZeros(x);
          trailing = Long.numberOfTrailingZeros(x);
          width = 64 - leading - trailing;
        } else {
          int xInt = (int) x;
          leading = Integer.numberOfLeadingZeros(xInt);
          trailing = Integer.numberOfTrailingZeros(xInt);
          width = 32 - leading - trailing;
        }
        if (prevLeading >= 0 && leading >= prevLeading
            && (leading + width) <= (prevLeading + prevWidth)) {
          // Reuse previous window
          writer.writeBit(0);
          // Re-extract bits using previous (leading, width)
          long prevMask = (prevWidth == 64) ? -1L : ((1L << prevWidth) - 1L);
          long meaningful = (x >>> (elementBits - prevLeading - prevWidth)) & prevMask;
          writer.writeBits(meaningful, prevWidth);
        } else {
          writer.writeBit(1);
          writer.writeBits(leading, leadingBits);
          // Width stored as (width - 1) using widthBits: values 1..(2^widthBits) map to 0..(2^widthBits - 1).
          int widthEncoded = width - 1;
          writer.writeBits(widthEncoded, widthBits);
          long widthMask = (width == 64) ? -1L : ((1L << width) - 1L);
          long meaningful = (x >>> (elementBits - leading - width)) & widthMask;
          writer.writeBits(meaningful, width);
          prevLeading = leading;
          prevWidth = width;
        }
      }
      prev = cur;
    }

    writer.flush();
    out.flip();
    return out;
  }

  @Override
  public ByteBuffer decode(Options options, CodecContext ctx, ByteBuffer src) {
    byte flag = src.get();
    int count = src.getInt();
    if (count < 0) {
      throw new IllegalStateException(
          "GORILLA: invalid count in header: " + count + ". Segment may be corrupt.");
    }
    if (count == 0) {
      return ByteBuffer.allocateDirect(0);
    }
    if (flag != 0 && flag != 1) {
      throw new IllegalStateException(
          "Unknown GORILLA type flag: " + flag + ". Expected 0 (INT) or 1 (LONG). Segment may be corrupt.");
    }
    boolean isLong = flag == 1;
    int elementSize = isLong ? Long.BYTES : Integer.BYTES;
    ByteBuffer out = ByteBuffer.allocateDirect(count * elementSize);
    decodeValues(src, count, isLong, out);
    out.flip();
    return out;
  }

  @Override
  public void decodeInto(Options options, CodecContext ctx, ByteBuffer src, ByteBuffer dst) {
    dst.clear();
    byte flag = src.get();
    int count = src.getInt();
    if (count < 0) {
      throw new IllegalStateException(
          "GORILLA: invalid count in header: " + count + ". Segment may be corrupt.");
    }
    if (count == 0) {
      dst.flip();
      return;
    }
    if (flag != 0 && flag != 1) {
      throw new IllegalStateException(
          "Unknown GORILLA type flag: " + flag + ". Expected 0 (INT) or 1 (LONG). Segment may be corrupt.");
    }
    boolean isLong = flag == 1;
    int elementSize = isLong ? Long.BYTES : Integer.BYTES;
    long requiredCapacity = (long) count * elementSize;
    if (requiredCapacity > dst.capacity()) {
      throw new IllegalArgumentException(
          "GORILLA: decoded size " + requiredCapacity + " exceeds dst capacity " + dst.capacity());
    }
    decodeValues(src, count, isLong, dst);
    dst.flip();
  }

  private static void decodeValues(ByteBuffer src, int count, boolean isLong, ByteBuffer dst) {
    int elementBits = isLong ? 64 : 32;
    int leadingBits = isLong ? 6 : 5;
    int widthBits = isLong ? 6 : 5;

    long prev = isLong ? src.getLong() : (long) src.getInt();
    if (isLong) {
      dst.putLong(prev);
    } else {
      dst.putInt((int) prev);
    }
    if (count == 1) {
      return;
    }

    BitReader reader = new BitReader(src);
    int prevLeading = -1;
    int prevWidth = -1;
    for (int i = 1; i < count; i++) {
      int bit = reader.readBit();
      if (bit == 0) {
        if (isLong) {
          dst.putLong(prev);
        } else {
          dst.putInt((int) prev);
        }
      } else {
        int control = reader.readBit();
        int leading;
        int width;
        if (control == 0) {
          if (prevLeading < 0) {
            throw new IllegalStateException(
                "GORILLA: window-reuse before any explicit window. Segment may be corrupt.");
          }
          leading = prevLeading;
          width = prevWidth;
        } else {
          leading = (int) reader.readBits(leadingBits);
          width = (int) reader.readBits(widthBits) + 1;
          prevLeading = leading;
          prevWidth = width;
        }
        // Encode-side guarantees width ≥ 1 (explicit window stores width-1; reuse copies a
        // previously-explicit width). Validate `leading + width` against elementBits to reject
        // corrupt segments whose explicit-window header decodes to an out-of-range pair.
        if (leading + width > elementBits) {
          throw new IllegalStateException(
              "GORILLA: invalid window leading=" + leading + " width=" + width + ". Segment may be corrupt.");
        }
        long meaningful = reader.readBits(width);
        long x = meaningful << (elementBits - leading - width);
        long cur = prev ^ x;
        if (isLong) {
          dst.putLong(cur);
        } else {
          dst.putInt((int) cur);
        }
        prev = cur;
      }
    }
  }

  // -------------------------------------------------------------------------
  // Bit-stream helpers (MSB-first within each byte)
  // -------------------------------------------------------------------------

  private static final class BitWriter {
    private final ByteBuffer _out;
    private int _byteAccumulator;
    private int _bitsInAccumulator;

    BitWriter(ByteBuffer out) {
      _out = out;
      _byteAccumulator = 0;
      _bitsInAccumulator = 0;
    }

    void writeBit(int bit) {
      _byteAccumulator = (_byteAccumulator << 1) | (bit & 1);
      _bitsInAccumulator++;
      if (_bitsInAccumulator == 8) {
        _out.put((byte) _byteAccumulator);
        _byteAccumulator = 0;
        _bitsInAccumulator = 0;
      }
    }

    void writeBits(long value, int numBits) {
      // Write most-significant bits first.
      for (int b = numBits - 1; b >= 0; b--) {
        writeBit((int) ((value >>> b) & 1L));
      }
    }

    /// Flush any remaining buffered bits, padding with zeros to the next byte boundary.
    void flush() {
      if (_bitsInAccumulator > 0) {
        _byteAccumulator <<= (8 - _bitsInAccumulator);
        _out.put((byte) _byteAccumulator);
        _byteAccumulator = 0;
        _bitsInAccumulator = 0;
      }
    }
  }

  private static final class BitReader {
    private final ByteBuffer _in;
    private int _byteAccumulator;
    private int _bitsInAccumulator;

    BitReader(ByteBuffer in) {
      _in = in;
      _byteAccumulator = 0;
      _bitsInAccumulator = 0;
    }

    int readBit() {
      if (_bitsInAccumulator == 0) {
        _byteAccumulator = _in.get() & 0xFF;
        _bitsInAccumulator = 8;
      }
      _bitsInAccumulator--;
      return (_byteAccumulator >>> _bitsInAccumulator) & 1;
    }

    long readBits(int numBits) {
      long result = 0L;
      for (int b = numBits - 1; b >= 0; b--) {
        result |= ((long) readBit()) << b;
      }
      return result;
    }
  }
}
