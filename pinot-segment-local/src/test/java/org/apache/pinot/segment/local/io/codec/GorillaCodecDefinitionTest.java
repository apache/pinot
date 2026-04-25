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
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;


/// Focused boundary tests for [GorillaCodecDefinition].
///
/// Targets paths not exercised by `CodecPipelineForwardIndexTest`:
///  - `count == 0`, `count == 1`
///  - all-equal sequences (every value triggers the `x == 0` repeat branch)
///  - window-reuse vs explicit-window switching across consecutive XOR deltas
///  - INT_MIN/INT_MAX, LONG_MIN/LONG_MAX boundary values
///  - first value verbatim with arbitrary sign-bit
///  - corrupt-segment defenses (invalid flag, negative count, window-reuse before explicit)
public class GorillaCodecDefinitionTest {

  private static final GorillaCodecDefinition CODEC = GorillaCodecDefinition.INSTANCE;
  private static final GorillaCodecDefinition.Options OPTS = GorillaCodecDefinition.Options.INSTANCE;
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
  public void testUnsupportedDataType() {
    assertThrows(IllegalArgumentException.class,
        () -> CODEC.validateContext(OPTS, new CodecContext(DataType.STRING)));
    assertThrows(IllegalArgumentException.class,
        () -> CODEC.validateContext(OPTS, new CodecContext(DataType.FLOAT)));
  }

  @Test
  public void testRejectsArguments() {
    assertThrows(IllegalArgumentException.class,
        () -> CODEC.parseOptions(Collections.singletonList("3")));
  }
}
