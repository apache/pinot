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
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Round-trip coverage for the DELTA and DELTADELTA transform codecs driven through
/// [CodecPipelineExecutor] encode/decode, over INT and LONG value arrays.
///
/// The V7 on-disk format does not exist yet at this point in the stack, so this test is the direct
/// coverage of the delta arithmetic. It locks in the two's-complement wrap-around contract of
/// [BaseDeltaCodecDefinition]: overflow-adjacent sequences (e.g. `MIN_VALUE` next to `MAX_VALUE`)
/// must round-trip bit-for-bit through the wrapping `-`/`+` encode/decode helpers.
public class DeltaCodecRoundTripTest {

  private static final CodecContext INT_CTX = new CodecContext(DataType.INT);
  private static final CodecContext LONG_CTX = new CodecContext(DataType.LONG);

  /// Bare transforms exercise the single-stage decode path; the `,LZ4` chains exercise the
  /// multi-stage path where the compression stage decodes into a bounded intermediate buffer.
  private static final String[] SPECS = {"DELTA", "DELTADELTA", "DELTA,LZ4", "DELTADELTA,LZ4"};

  private static final int[][] INT_DATASETS = {
      // empty
      {},
      // single element
      {42},
      // two elements (DELTADELTA boundary: first delta present, delta-of-delta loop empty)
      {3, -7},
      // constant (all deltas and delta-of-deltas are zero)
      {7, 7, 7, 7, 7},
      // monotonic with regular then irregular strides
      {1, 2, 3, 4, 5, 100, 1000, 1001},
      // negative values and sign changes
      {-5, -4, -10, 0, 3, -100, Integer.MIN_VALUE / 2},
      // overflow-adjacent: deltas wrap around two's-complement and must still round-trip
      {Integer.MIN_VALUE, Integer.MAX_VALUE, Integer.MIN_VALUE + 1, -1, 0, 1, Integer.MAX_VALUE - 1},
  };

  private static final long[][] LONG_DATASETS = {
      // empty
      {},
      // single element
      {42L},
      // two elements (DELTADELTA boundary: first delta present, delta-of-delta loop empty)
      {3L, -7L},
      // constant (all deltas and delta-of-deltas are zero)
      {7L, 7L, 7L, 7L, 7L},
      // timestamp-like monotonic sequence with a jitter
      {1700000000000L, 1700000000010L, 1700000000020L, 1700000000031L, 1700000000040L},
      // negative values and sign changes
      {-5L, -4L, -10L, 0L, 3L, -100L, Long.MIN_VALUE / 2},
      // overflow-adjacent: deltas wrap around two's-complement and must still round-trip
      {Long.MIN_VALUE, Long.MAX_VALUE, Long.MIN_VALUE + 1, -1L, 0L, 1L, Long.MAX_VALUE - 1},
  };

  @Test
  public void testIntRoundTrip() throws IOException {
    for (String spec : SPECS) {
      for (int[] values : INT_DATASETS) {
        assertIntRoundTrip(spec, values);
      }
    }
  }

  @Test
  public void testLongRoundTrip() throws IOException {
    for (String spec : SPECS) {
      for (long[] values : LONG_DATASETS) {
        assertLongRoundTrip(spec, values);
      }
    }
  }

  /// Decodes fixed byte sequences without consulting either encoder. These fixtures freeze the
  /// persisted big-endian layout so an encoder/decoder pair cannot drift together unnoticed.
  @Test
  public void testStableDecodeFixtures() throws IOException {
    assertIntDecodeFixture("DELTA", new byte[]{
        0, 0, 0, 10,
        0, 0, 0, 3,
        -1, -1, -1, -5
    }, new int[]{10, 13, 8});
    assertLongDecodeFixture("DELTA", new byte[]{
        0, 0, 0, 0, 0, 0, 0, 10,
        0, 0, 0, 0, 0, 0, 0, 3,
        -1, -1, -1, -1, -1, -1, -1, -5
    }, new long[]{10, 13, 8});

    assertIntDecodeFixture("DELTADELTA", new byte[]{
        0, 0, 0, 10,
        0, 0, 0, 3,
        -1, -1, -1, -2
    }, new int[]{10, 13, 14});
    assertLongDecodeFixture("DELTADELTA", new byte[]{
        0, 0, 0, 0, 0, 0, 0, 10,
        0, 0, 0, 0, 0, 0, 0, 3,
        -1, -1, -1, -1, -1, -1, -1, -2
    }, new long[]{10, 13, 14});
  }

  /// A frame whose byte length is not a whole number of elements is corrupt and must fail closed
  /// on both the encode and the bounded segment-reader decode path.
  @Test
  public void testMisalignedFrameRejected() {
    CodecPipelineExecutor executor = CodecPipelineExecutor.create("DELTA", INT_CTX, CodecRegistry.DEFAULT);
    ByteBuffer misaligned = ByteBuffer.allocateDirect(3);
    misaligned.put((byte) 1).put((byte) 2).put((byte) 3).flip();

    IllegalArgumentException encodeException = expectThrows(IllegalArgumentException.class,
        () -> executor.encode(misaligned.duplicate()));
    assertTrue(encodeException.getMessage().contains("not a multiple of element size"),
        encodeException.getMessage());

    ByteBuffer dst = ByteBuffer.allocateDirect(Integer.BYTES);
    IllegalArgumentException decodeException = expectThrows(IllegalArgumentException.class,
        () -> executor.decode(misaligned.duplicate(), dst, Integer.BYTES));
    assertTrue(decodeException.getMessage().contains("not a multiple of element size"),
        decodeException.getMessage());
  }

  private static void assertIntRoundTrip(String spec, int[] values) throws IOException {
    CodecPipelineExecutor executor = CodecPipelineExecutor.create(spec, INT_CTX, CodecRegistry.DEFAULT);
    int decodedSize = values.length * Integer.BYTES;
    ByteBuffer src = ByteBuffer.allocateDirect(decodedSize);
    for (int value : values) {
      src.putInt(value);
    }
    src.flip();
    ByteBuffer encoded = executor.encode(src.duplicate());

    // Allocating decode path.
    int[] decoded = readInts(executor.decode(encoded.duplicate()), values.length);
    assertTrue(Arrays.equals(decoded, values),
        describeMismatch(spec, Arrays.toString(values), Arrays.toString(decoded)));

    // Bounded decode-into path (the segment-reader entry point).
    ByteBuffer dst = ByteBuffer.allocateDirect(decodedSize);
    executor.decode(encoded.duplicate(), dst, decodedSize);
    int[] decodedInto = readInts(dst, values.length);
    assertTrue(Arrays.equals(decodedInto, values),
        describeMismatch(spec, Arrays.toString(values), Arrays.toString(decodedInto)));
  }

  private static void assertLongRoundTrip(String spec, long[] values) throws IOException {
    CodecPipelineExecutor executor = CodecPipelineExecutor.create(spec, LONG_CTX, CodecRegistry.DEFAULT);
    int decodedSize = values.length * Long.BYTES;
    ByteBuffer src = ByteBuffer.allocateDirect(decodedSize);
    for (long value : values) {
      src.putLong(value);
    }
    src.flip();
    ByteBuffer encoded = executor.encode(src.duplicate());

    // Allocating decode path.
    long[] decoded = readLongs(executor.decode(encoded.duplicate()), values.length);
    assertTrue(Arrays.equals(decoded, values),
        describeMismatch(spec, Arrays.toString(values), Arrays.toString(decoded)));

    // Bounded decode-into path (the segment-reader entry point).
    ByteBuffer dst = ByteBuffer.allocateDirect(decodedSize);
    executor.decode(encoded.duplicate(), dst, decodedSize);
    long[] decodedInto = readLongs(dst, values.length);
    assertTrue(Arrays.equals(decodedInto, values),
        describeMismatch(spec, Arrays.toString(values), Arrays.toString(decodedInto)));
  }

  private static void assertIntDecodeFixture(String spec, byte[] encoded, int[] expected) throws IOException {
    CodecPipelineExecutor executor = CodecPipelineExecutor.create(spec, INT_CTX, CodecRegistry.DEFAULT);
    int[] decoded = readInts(
        executor.decode(ByteBuffer.wrap(encoded).order(ByteOrder.LITTLE_ENDIAN)), expected.length);
    assertTrue(Arrays.equals(decoded, expected),
        describeMismatch(spec, Arrays.toString(expected), Arrays.toString(decoded)));

    ByteBuffer dst = ByteBuffer.allocateDirect(expected.length * Integer.BYTES);
    executor.decode(ByteBuffer.wrap(encoded).order(ByteOrder.LITTLE_ENDIAN), dst, dst.capacity());
    int[] decodedInto = readInts(dst, expected.length);
    assertTrue(Arrays.equals(decodedInto, expected),
        describeMismatch(spec, Arrays.toString(expected), Arrays.toString(decodedInto)));
  }

  private static void assertLongDecodeFixture(String spec, byte[] encoded, long[] expected) throws IOException {
    CodecPipelineExecutor executor = CodecPipelineExecutor.create(spec, LONG_CTX, CodecRegistry.DEFAULT);
    long[] decoded = readLongs(
        executor.decode(ByteBuffer.wrap(encoded).order(ByteOrder.LITTLE_ENDIAN)), expected.length);
    assertTrue(Arrays.equals(decoded, expected),
        describeMismatch(spec, Arrays.toString(expected), Arrays.toString(decoded)));

    ByteBuffer dst = ByteBuffer.allocateDirect(expected.length * Long.BYTES);
    executor.decode(ByteBuffer.wrap(encoded).order(ByteOrder.LITTLE_ENDIAN), dst, dst.capacity());
    long[] decodedInto = readLongs(dst, expected.length);
    assertTrue(Arrays.equals(decodedInto, expected),
        describeMismatch(spec, Arrays.toString(expected), Arrays.toString(decodedInto)));
  }

  private static int[] readInts(ByteBuffer buffer, int expectedCount) {
    assertTrue(buffer.remaining() == expectedCount * Integer.BYTES,
        "Decoded size " + buffer.remaining() + " != expected " + expectedCount * Integer.BYTES);
    int[] values = new int[expectedCount];
    for (int i = 0; i < expectedCount; i++) {
      values[i] = buffer.getInt();
    }
    return values;
  }

  private static long[] readLongs(ByteBuffer buffer, int expectedCount) {
    assertTrue(buffer.remaining() == expectedCount * Long.BYTES,
        "Decoded size " + buffer.remaining() + " != expected " + expectedCount * Long.BYTES);
    long[] values = new long[expectedCount];
    for (int i = 0; i < expectedCount; i++) {
      values[i] = buffer.getLong();
    }
    return values;
  }

  private static String describeMismatch(String spec, String expected, String actual) {
    return "Round trip through '" + spec + "' failed: expected " + expected + " but got " + actual;
  }
}
