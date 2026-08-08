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
import java.util.Collections;
import org.apache.pinot.segment.spi.codec.CodecContext;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Focused boundary tests for [T64CodecDefinition].
///
/// `CodecPipelineForwardIndexTest` covers the writer/reader integration path; this file
/// covers code paths that the integration test does not reach with its monotonic test data:
///  - `count == 0`, `count == 1`
///  - bitWidth == 0 (all values in a block equal)
///  - partial last block (count not a multiple of BLOCK_SIZE)
///  - max-range INT and LONG inputs (forces bitWidth = 32 / 64)
///  - alternating ± values, strictly decreasing
///  - corrupt-segment defenses (invalid flag, invalid bitWidth, invalid count, mismatched size)
public class T64CodecDefinitionTest {

  private static final T64CodecDefinition CODEC = T64CodecDefinition.INSTANCE;
  private static final T64CodecDefinition.Options OPTS = T64CodecDefinition.Options.INSTANCE;
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
    ByteBuffer decoded = CODEC.decode(OPTS, INT_CTX, encoded);
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
    ByteBuffer decoded = CODEC.decode(OPTS, LONG_CTX, encoded);
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
  }

  @Test
  public void testSingleValueLong() {
    roundTripLong(new long[]{1_700_000_000_000L});
  }

  @Test
  public void testSingleValueIntMinMax() {
    roundTripInt(new int[]{Integer.MIN_VALUE});
    roundTripInt(new int[]{Integer.MAX_VALUE});
  }

  @Test
  public void testSingleValueLongMinMax() {
    roundTripLong(new long[]{Long.MIN_VALUE});
    roundTripLong(new long[]{Long.MAX_VALUE});
  }

  // ---------- bitWidth == 0 (all equal) ----------

  @Test
  public void testAllEqualInt() {
    int[] values = new int[128];
    Arrays.fill(values, -7);
    roundTripInt(values);
  }

  @Test
  public void testAllEqualLong() {
    long[] values = new long[128];
    Arrays.fill(values, Long.MIN_VALUE);
    roundTripLong(values);
  }

  // ---------- Partial last block ----------

  @Test
  public void testPartialLastBlock65Int() {
    int[] values = new int[65];
    for (int i = 0; i < 65; i++) {
      values[i] = i * 3;
    }
    roundTripInt(values);
  }

  @Test
  public void testPartialLastBlock127Long() {
    long[] values = new long[127];
    for (int i = 0; i < 127; i++) {
      values[i] = (long) i * 1_000L;
    }
    roundTripLong(values);
  }

  @Test
  public void testExactSingleBlockInt() {
    int[] values = new int[64];
    for (int i = 0; i < 64; i++) {
      values[i] = i;
    }
    roundTripInt(values);
  }

  // ---------- Max-range values ----------

  @Test
  public void testIntMaxRange() {
    int[] values = new int[]{Integer.MIN_VALUE, Integer.MAX_VALUE, 0, -1, 1,
        Integer.MIN_VALUE + 1, Integer.MAX_VALUE - 1};
    roundTripInt(values);
  }

  @Test
  public void testLongMaxRange() {
    long[] values = new long[]{Long.MIN_VALUE, Long.MAX_VALUE, 0L, -1L, 1L,
        Long.MIN_VALUE + 1, Long.MAX_VALUE - 1};
    roundTripLong(values);
  }

  @Test
  public void testIntMaxRangeFullBlock() {
    int[] values = new int[64];
    values[0] = Integer.MIN_VALUE;
    values[1] = Integer.MAX_VALUE;
    for (int i = 2; i < 64; i++) {
      values[i] = i;
    }
    roundTripInt(values);
  }

  @Test
  public void testLongMaxRangeFullBlock() {
    long[] values = new long[64];
    values[0] = Long.MIN_VALUE;
    values[1] = Long.MAX_VALUE;
    for (int i = 2; i < 64; i++) {
      values[i] = i;
    }
    roundTripLong(values);
  }

  @Test
  public void testStrictlyDecreasingLong() {
    long[] values = new long[300];
    for (int i = 0; i < 300; i++) {
      values[i] = -((long) i) * 1_000_000L;
    }
    roundTripLong(values);
  }

  @Test
  public void testAlternatingSignInt() {
    int[] values = new int[200];
    for (int i = 0; i < 200; i++) {
      values[i] = (i % 2 == 0) ? i : -i;
    }
    roundTripInt(values);
  }

  // ---------- Corrupt-segment defenses ----------

  @Test
  public void testInvalidFlagThrows() {
    ByteBuffer buf = ByteBuffer.allocateDirect(5);
    buf.put((byte) 7); // invalid flag
    buf.putInt(1);
    buf.flip();
    assertThrows(IllegalStateException.class, () -> CODEC.decode(OPTS, INT_CTX, buf));
  }

  @Test
  public void testInvalidFlagWithZeroCountThrows() {
    // Regression for BUG-CORR-2: flag validation must precede the count==0 short-circuit.
    ByteBuffer buf = ByteBuffer.allocateDirect(5);
    buf.put((byte) 42); // invalid flag
    buf.putInt(0); // count = 0
    buf.flip();
    assertThrows(IllegalStateException.class, () -> CODEC.decode(OPTS, INT_CTX, buf));
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
  public void testInvalidBitWidthThrows() {
    // INT with bitWidth=33 must be rejected
    ByteBuffer buf = ByteBuffer.allocateDirect(64);
    buf.put((byte) 0); // INT
    buf.putInt(64);
    buf.putInt(0); // baseline
    buf.put((byte) 33); // invalid bitWidth for INT
    buf.flip();
    assertThrows(IllegalStateException.class, () -> CODEC.decode(OPTS, INT_CTX, buf));
  }

  @Test
  public void testMisalignedInputSizeThrows() {
    ByteBuffer buf = ByteBuffer.allocateDirect(7); // not a multiple of INT size
    assertThrows(IllegalArgumentException.class, () -> CODEC.encode(OPTS, INT_CTX, buf));
  }

  @Test
  public void testUnsupportedDataType() {
    assertThrows(IllegalArgumentException.class,
        () -> CODEC.validateContext(OPTS, new CodecContext(DataType.STRING)));
    assertThrows(IllegalArgumentException.class,
        () -> CODEC.validateContext(OPTS, new CodecContext(DataType.DOUBLE)));
  }

  @Test
  public void testRejectsArguments() {
    assertThrows(IllegalArgumentException.class,
        () -> CODEC.parseOptions(Collections.singletonList("3")));
  }

  // ---------- Encoded-size bound (BUG-CFG-1 regression) ----------

  @DataProvider(name = "boundaryCounts")
  public Object[][] boundaryCounts() {
    return new Object[][]{
        {0}, {1}, {63}, {64}, {65}, {127}, {128}, {129}, {256}, {1024}
    };
  }

  @Test(dataProvider = "boundaryCounts")
  public void testEncodedSizeWithinMaxEncodedSizeInt(int count) {
    int[] values = new int[count];
    // Force the worst case: alternate MIN_VALUE and MAX_VALUE so range == 2^32 - 1, bitWidth=32
    for (int i = 0; i < count; i++) {
      values[i] = (i % 2 == 0) ? Integer.MIN_VALUE : Integer.MAX_VALUE;
    }
    ByteBuffer src = ByteBuffer.allocateDirect(Math.max(1, count) * Integer.BYTES);
    for (int v : values) {
      src.putInt(v);
    }
    src.flip();
    ByteBuffer encoded = CODEC.encode(OPTS, INT_CTX, src);
    int max = CODEC.maxEncodedSize(OPTS, count * Integer.BYTES);
    assertTrue(encoded.remaining() <= max,
        "count=" + count + " encoded.remaining()=" + encoded.remaining()
            + " exceeds maxEncodedSize=" + max);
  }

  @Test(dataProvider = "boundaryCounts")
  public void testEncodedSizeWithinMaxEncodedSizeLong(int count) {
    long[] values = new long[count];
    for (int i = 0; i < count; i++) {
      values[i] = (i % 2 == 0) ? Long.MIN_VALUE : Long.MAX_VALUE;
    }
    ByteBuffer src = ByteBuffer.allocateDirect(Math.max(1, count) * Long.BYTES);
    for (long v : values) {
      src.putLong(v);
    }
    src.flip();
    ByteBuffer encoded = CODEC.encode(OPTS, LONG_CTX, src);
    int max = CODEC.maxEncodedSize(OPTS, count * Long.BYTES);
    assertTrue(encoded.remaining() <= max,
        "count=" + count + " encoded.remaining()=" + encoded.remaining()
            + " exceeds maxEncodedSize=" + max);
  }

  // ---------- Wire-format byte-layout pin test ----------

  /// Pins the on-disk bytes for a small canonical input. `NAME = "T64"` and the frame format are
  /// documented as frozen on-disk contract — any unintended byte-layout regression must fail
  /// this test.
  @Test
  public void testKnownGoodEncodedBytesInt() {
    int[] values = new int[]{0, 1, 2, 3};
    ByteBuffer src = ByteBuffer.allocateDirect(values.length * Integer.BYTES);
    for (int v : values) {
      src.putInt(v);
    }
    src.flip();
    ByteBuffer encoded = CODEC.encode(OPTS, INT_CTX, src);

    // Frame: flag=0 (INT), count=4, then one block:
    //   baseline = 0 (int, 4 bytes little-endian/BIG depending on bb default = BIG)
    //   bitWidth = 2  (range 0..3 needs 2 bits)
    //   packed: 64 slots of 2 bits each = 16 bytes; slots 0..3 = (0, 1, 2, 3) lsb-first
    //          slot 0 (bits 0-1): 00, slot 1 (bits 2-3): 01, slot 2 (bits 4-5): 10, slot 3 (bits 6-7): 11
    //          → first byte = 0b11_10_01_00 = 0xE4; remaining 15 bytes zero.
    byte[] expected = new byte[1 + 4 + 4 + 1 + 16];
    expected[0] = 0; // flag = INT
    // count = 4 (big-endian)
    expected[1] = 0;
    expected[2] = 0;
    expected[3] = 0;
    expected[4] = 4;
    // baseline = 0 (big-endian)
    expected[5] = 0;
    expected[6] = 0;
    expected[7] = 0;
    expected[8] = 0;
    expected[9] = 2; // bitWidth
    expected[10] = (byte) 0xE4;
    // Remaining 15 bytes default to zero.

    byte[] actual = new byte[encoded.remaining()];
    encoded.duplicate().get(actual);
    assertEquals(actual.length, expected.length, "byte-length mismatch");
    for (int i = 0; i < expected.length; i++) {
      assertEquals(actual[i], expected[i],
          "byte[" + i + "] mismatch: expected " + (expected[i] & 0xFF) + " got " + (actual[i] & 0xFF));
    }
  }

  // ---------- Parameterized bit-width × position coverage (BUG-CORR-1 regression) ----------

  /// For each pair (bitWidth, valuesPerBlock), construct a block that forces *exactly* bitWidth
  /// bits of dynamic range so the encoder lays slots out at every bit-cursor position
  /// {0, bitWidth, 2*bitWidth, ...} mod 8. The previous buggy `writeBits` silently truncated
  /// high bits of `value` whenever `bitInByte + bitWidth > 64`, which manifested for any
  /// bitWidth ∈ {2..63} that produced a non-zero `bitInByte` at some slot.
  @DataProvider(name = "bitWidthsLong")
  public Object[][] bitWidthsLong() {
    Object[][] cases = new Object[64][];
    for (int bw = 1; bw <= 64; bw++) {
      cases[bw - 1] = new Object[]{bw};
    }
    return cases;
  }

  @Test(dataProvider = "bitWidthsLong")
  public void testRoundTripAllBitWidthsLong(int bitWidth) {
    long range = (bitWidth == 64) ? -1L : ((1L << bitWidth) - 1L);
    long[] values = new long[200];
    for (int i = 0; i < 200; i++) {
      // Alternate min and max-bit-set values so the encoder must emit full-width slots
      // at every bit position 0..7. baseline is 0, so encoded slot value == values[i].
      values[i] = (i % 2 == 0) ? 0L : range;
    }
    roundTripLong(values);
  }

  @DataProvider(name = "bitWidthsInt")
  public Object[][] bitWidthsInt() {
    Object[][] cases = new Object[32][];
    for (int bw = 1; bw <= 32; bw++) {
      cases[bw - 1] = new Object[]{bw};
    }
    return cases;
  }

  @Test(dataProvider = "bitWidthsInt")
  public void testRoundTripAllBitWidthsInt(int bitWidth) {
    int range = (bitWidth == 32) ? -1 : ((1 << bitWidth) - 1);
    int[] values = new int[200];
    for (int i = 0; i < 200; i++) {
      values[i] = (i % 2 == 0) ? 0 : range;
    }
    roundTripInt(values);
  }
}
