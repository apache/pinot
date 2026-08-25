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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;


/// Focused boundary tests for [GorillaCodecDefinition].
///
/// The codec-pipeline on-disk format does not exist yet at this point in the stack, so this file
/// is the direct coverage of the GORILLA bit-stream, targeting paths an on-disk writer/reader
/// integration test would not exercise:
///  - `count == 0`, `count == 1`
///  - all-equal sequences (every value triggers the `x == 0` repeat branch)
///  - window-reuse vs explicit-window switching across consecutive XOR deltas
///  - INT_MIN/INT_MAX, LONG_MIN/LONG_MAX boundary values
///  - first value verbatim with arbitrary sign-bit
///  - corrupt-segment defenses (invalid flag, negative count, window-reuse before explicit)
public class GorillaCodecDefinitionTest {

  private static final GorillaCodecDefinition CODEC = GorillaCodecDefinition.INSTANCE;
  private static final GorillaCodecDefinition.Options OPTS = GorillaCodecDefinition.OPTIONS;
  private static final CodecContext INT_CTX = new CodecContext(DataType.INT);
  private static final CodecContext LONG_CTX = new CodecContext(DataType.LONG);

  // ---------- Round-trip helpers ----------

  private static void roundTripInt(int[] values) {
    ByteBuffer src = ByteBuffer.allocateDirect(values.length * Integer.BYTES);
    for (int v : values) {
      src.putInt(v);
    }
    src.flip();
    ByteBuffer encoded = CODEC.encode(OPTS, INT_CTX, src);
    assertDecodedInts(CODEC.decode(OPTS, INT_CTX, encoded.duplicate()), values);

    ByteBuffer dst = ByteBuffer.allocateDirect(values.length * Integer.BYTES);
    CODEC.decodeInto(OPTS, INT_CTX, encoded.duplicate(), dst);
    assertDecodedInts(dst, values);
  }

  private static void assertDecodedInts(ByteBuffer decoded, int[] values) {
    assertEquals(decoded.remaining(), values.length * Integer.BYTES,
        "decoded byte length mismatch");
    for (int i = 0; i < values.length; i++) {
      assertEquals(decoded.getInt(), values[i], "INT mismatch at i=" + i);
    }
  }

  private static void roundTripLong(long[] values) {
    ByteBuffer src = ByteBuffer.allocateDirect(values.length * Long.BYTES);
    for (long v : values) {
      src.putLong(v);
    }
    src.flip();
    ByteBuffer encoded = CODEC.encode(OPTS, LONG_CTX, src);
    assertDecodedLongs(CODEC.decode(OPTS, LONG_CTX, encoded.duplicate()), values);

    ByteBuffer dst = ByteBuffer.allocateDirect(values.length * Long.BYTES);
    CODEC.decodeInto(OPTS, LONG_CTX, encoded.duplicate(), dst);
    assertDecodedLongs(dst, values);
  }

  private static void assertDecodedLongs(ByteBuffer decoded, long[] values) {
    assertEquals(decoded.remaining(), values.length * Long.BYTES,
        "decoded byte length mismatch");
    for (int i = 0; i < values.length; i++) {
      assertEquals(decoded.getLong(), values[i], "LONG mismatch at i=" + i);
    }
  }

  // ---------- Count edge cases ----------

  @Test
  public void testEmptyInt() {
    roundTripInt(new int[0]);
  }

  @Test
  public void testEmptyLong() {
    roundTripLong(new long[0]);
  }

  @Test
  public void testSingleValueInt() {
    roundTripInt(new int[]{42});
    roundTripInt(new int[]{Integer.MIN_VALUE});
    roundTripInt(new int[]{Integer.MAX_VALUE});
    roundTripInt(new int[]{0});
  }

  @Test
  public void testSingleValueLong() {
    roundTripLong(new long[]{1_700_000_000_000L});
    roundTripLong(new long[]{Long.MIN_VALUE});
    roundTripLong(new long[]{Long.MAX_VALUE});
    roundTripLong(new long[]{0L});
  }

  // ---------- All-equal exercises the x==0 repeat path ----------

  @Test
  public void testAllEqualInt() {
    int[] values = new int[200];
    Arrays.fill(values, -42);
    roundTripInt(values);
  }

  @Test
  public void testAllEqualLong() {
    long[] values = new long[200];
    Arrays.fill(values, Long.MIN_VALUE + 1);
    roundTripLong(values);
  }

  // ---------- Boundary values ----------

  @Test
  public void testIntBoundary() {
    int[] values = new int[]{0, Integer.MIN_VALUE, Integer.MAX_VALUE, 0, -1, 1,
        Integer.MIN_VALUE + 1, Integer.MAX_VALUE - 1, 0};
    roundTripInt(values);
  }

  @Test
  public void testLongBoundary() {
    long[] values = new long[]{0L, Long.MIN_VALUE, Long.MAX_VALUE, 0L, -1L, 1L,
        Long.MIN_VALUE + 1, Long.MAX_VALUE - 1, 0L};
    roundTripLong(values);
  }

  // ---------- Window-reuse vs explicit transitions ----------

  @Test
  public void testTimestampLikeLong() {
    // Small monotonic deltas — explicit window once, then many reuses
    long[] values = new long[500];
    long base = 1_700_000_000_000L;
    for (int i = 0; i < 500; i++) {
      values[i] = base + i * 1_000L;
    }
    roundTripLong(values);
  }

  @Test
  public void testCounterLikeInt() {
    int[] values = new int[500];
    for (int i = 0; i < 500; i++) {
      values[i] = i;
    }
    roundTripInt(values);
  }

  @Test
  public void testAlternatingSignLong() {
    long[] values = new long[300];
    for (int i = 0; i < 300; i++) {
      values[i] = (i % 2 == 0) ? (long) i : -((long) i);
    }
    roundTripLong(values);
  }

  @Test
  public void testWidelyVaryingMagnitudesLong() {
    // Each XOR delta forces a fresh explicit window
    long[] values = new long[]{
        0L,
        0xFFFFL,
        0xFFFF_FFFFL,
        0xFFFF_FFFF_FFFFL,
        0xFFFF_FFFF_FFFF_FFFFL,
        0L,
        1L
    };
    roundTripLong(values);
  }

  @Test
  public void testRepeatedThenChanging() {
    // Tests that prevLeading/prevWidth are preserved across `x == 0` repeats.
    long[] values = new long[]{
        1000L, 1000L, 1000L,
        1001L,
        1001L, 1001L,
        1002L,
        1002L
    };
    roundTripLong(values);
  }

  @Test
  public void testMixedExecutorPipeline() throws IOException {
    long[] values = new long[257];
    for (int i = 0; i < values.length; i++) {
      values[i] = 1_700_000_000_000L + (long) i * i * 13L;
    }
    ByteBuffer src = ByteBuffer.allocateDirect(values.length * Long.BYTES);
    for (long value : values) {
      src.putLong(value);
    }
    src.flip();

    CodecPipelineExecutor executor = CodecPipelineExecutor.create(
        "DELTADELTA,GORILLA,ZSTD(3)", LONG_CTX, CodecRegistry.DEFAULT);
    ByteBuffer encoded = executor.encode(src.duplicate());
    ByteBuffer decoded = ByteBuffer.allocateDirect(src.remaining());
    executor.decode(encoded, decoded, src.remaining());
    assertDecodedLongs(decoded, values);
  }

  // ---------- Corrupt-segment defenses ----------

  @Test
  public void testInvalidFlagThrows() {
    ByteBuffer buf = ByteBuffer.allocateDirect(5);
    buf.put((byte) 7);
    buf.putInt(1);
    buf.flip();
    assertThrows(IllegalStateException.class, () -> CODEC.decode(OPTS, INT_CTX, buf));
  }

  @Test
  public void testInvalidFlagWithZeroCountThrows() {
    ByteBuffer buf = ByteBuffer.allocateDirect(5);
    buf.put((byte) 7).putInt(0).flip();
    assertThrows(IllegalStateException.class, () -> CODEC.decode(OPTS, INT_CTX, buf.duplicate()));
    assertThrows(IllegalStateException.class,
        () -> CODEC.decodeInto(OPTS, INT_CTX, buf.duplicate(), ByteBuffer.allocateDirect(0)));
  }

  @Test
  public void testFrameTypeMustMatchContext() {
    ByteBuffer longFrame = ByteBuffer.allocateDirect(5);
    longFrame.put((byte) 1).putInt(0).flip();
    assertThrows(IllegalStateException.class,
        () -> CODEC.decode(OPTS, INT_CTX, longFrame.duplicate()));
    assertThrows(IllegalStateException.class,
        () -> CODEC.decodeInto(OPTS, INT_CTX, longFrame.duplicate(), ByteBuffer.allocateDirect(0)));
  }

  @Test
  public void testNegativeCountThrows() {
    ByteBuffer buf = ByteBuffer.allocateDirect(5);
    buf.put((byte) 0);
    buf.putInt(-1);
    buf.flip();
    assertThrows(IllegalStateException.class, () -> CODEC.decode(OPTS, INT_CTX, buf));
  }

  @Test
  public void testWindowReuseBeforeExplicitThrows() {
    // Hand-craft a corrupt stream: flag=INT, count=2, first=0, then bit-stream
    // with bit=1 (nonzero XOR) followed by control=0 (reuse) but no prior explicit window.
    ByteBuffer buf = ByteBuffer.allocateDirect(16);
    buf.put((byte) 0); // INT flag
    buf.putInt(2); // count
    buf.putInt(0); // first value verbatim
    // Bit stream: MSB-first within byte. Encode bits 1, 0, then garbage.
    // 10000000 = 0x80
    buf.put((byte) 0x80);
    buf.flip();
    assertThrows(IllegalStateException.class, () -> CODEC.decode(OPTS, INT_CTX, buf));
  }

  @Test
  public void testTrailingBytesRejectedByCodecAndExecutor() {
    for (int[] values : new int[][]{{}, {42}, {1, 2, 3}}) {
      ByteBuffer src = ByteBuffer.allocateDirect(values.length * Integer.BYTES);
      for (int value : values) {
        src.putInt(value);
      }
      src.flip();
      ByteBuffer withTrailingByte = appendByte(CODEC.encode(OPTS, INT_CTX, src));

      assertThrows(IllegalStateException.class,
          () -> CODEC.decode(OPTS, INT_CTX, withTrailingByte.duplicate()));
      assertThrows(IllegalStateException.class,
          () -> CODEC.decodeInto(OPTS, INT_CTX, withTrailingByte.duplicate(),
              ByteBuffer.allocateDirect(values.length * Integer.BYTES)));

      CodecPipelineExecutor executor = CodecPipelineExecutor.create("GORILLA", INT_CTX, CodecRegistry.DEFAULT);
      assertThrows(RuntimeException.class,
          () -> executor.decode(withTrailingByte.duplicate(),
              ByteBuffer.allocateDirect(values.length * Integer.BYTES), values.length * Integer.BYTES));
    }
  }

  @Test
  public void testNonZeroPaddingBitsRejected() {
    ByteBuffer src = ByteBuffer.allocateDirect(2 * Integer.BYTES);
    src.putInt(7).putInt(7).flip();
    ByteBuffer encoded = CODEC.encode(OPTS, INT_CTX, src);
    encoded.put(encoded.limit() - 1, (byte) 1);

    assertThrows(IllegalStateException.class,
        () -> CODEC.decode(OPTS, INT_CTX, encoded.duplicate()));
    CodecPipelineExecutor executor = CodecPipelineExecutor.create("GORILLA", INT_CTX, CodecRegistry.DEFAULT);
    assertThrows(IllegalStateException.class,
        () -> executor.decode(encoded.duplicate(), ByteBuffer.allocateDirect(2 * Integer.BYTES),
            2 * Integer.BYTES));
  }

  /// Decodes fixed INT and LONG frames that are independent of the encoder under test. Both
  /// represent `[0, 1]` with an explicit one-bit XOR window.
  @Test
  public void testKnownGoodDecodeBytes() {
    byte[] encodedInt = {
        0,
        0, 0, 0, 2,
        0, 0, 0, 0,
        (byte) 0xFE, 0x08
    };
    assertDecodedInts(
        CODEC.decode(OPTS, INT_CTX, ByteBuffer.wrap(encodedInt).order(ByteOrder.LITTLE_ENDIAN)), new int[]{0, 1});

    byte[] encodedLong = {
        1,
        0, 0, 0, 2,
        0, 0, 0, 0, 0, 0, 0, 0,
        (byte) 0xFF, 0x02
    };
    long[] expectedLongs = {0L, 1L};
    assertDecodedLongs(
        CODEC.decode(OPTS, LONG_CTX, ByteBuffer.wrap(encodedLong).order(ByteOrder.LITTLE_ENDIAN)), expectedLongs);
    ByteBuffer dst = ByteBuffer.allocateDirect(expectedLongs.length * Long.BYTES);
    CODEC.decodeInto(OPTS, LONG_CTX, ByteBuffer.wrap(encodedLong).order(ByteOrder.LITTLE_ENDIAN), dst);
    assertDecodedLongs(dst, expectedLongs);
  }

  @Test
  public void testUnsupportedDataType() {
    assertThrows(IllegalArgumentException.class,
        () -> CODEC.validateContext(OPTS, new CodecContext(DataType.STRING)));
    assertThrows(IllegalArgumentException.class,
        () -> CODEC.validateContext(OPTS, new CodecContext(DataType.FLOAT)));
  }

  @Test
  public void testRejectsArguments() {
    assertThrows(IllegalArgumentException.class,
        () -> CODEC.parseOptions(List.of("3")));
  }

  @Test
  public void testMaxEncodedSizeRejectsOverflow() {
    assertThrows(IllegalArgumentException.class,
        () -> CODEC.maxEncodedSize(OPTS, Integer.MAX_VALUE));
  }

  private static ByteBuffer appendByte(ByteBuffer buffer) {
    ByteBuffer withTrailingByte = ByteBuffer.allocateDirect(buffer.remaining() + 1);
    withTrailingByte.put(buffer.duplicate()).put((byte) 1).flip();
    return withTrailingByte;
  }
}
