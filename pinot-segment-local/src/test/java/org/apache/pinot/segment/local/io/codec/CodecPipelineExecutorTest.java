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
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Format-independent round-trip and resource-bound tests for [CodecPipelineExecutor].
public class CodecPipelineExecutorTest {
  private static final byte[] INPUT = createInput();

  @DataProvider(name = "singleCodecAndInputKind")
  public Object[][] singleCodecAndInputKind() {
    return new Object[][]{
        {"LZ4", false}, {"LZ4", true},
        {"SNAPPY", false}, {"SNAPPY", true},
        {"GZIP", false}, {"GZIP", true},
        {"ZSTD", false}, {"ZSTD", true},
        {"ZSTD(1)", false}, {"ZSTD(1)", true}
    };
  }

  @Test(dataProvider = "singleCodecAndInputKind")
  public void testSingleCodecRoundTrip(String spec, boolean directInput) throws Exception {
    CodecPipelineExecutor executor = CodecPipelineExecutor.create(spec, DataType.BYTES);
    ByteBuffer encoded = executor.encode(inputBuffer(directInput));

    assertTrue(encoded.remaining() <= executor.maxEncodedSize(INPUT.length));
    assertBytesEqual(executor.decode(encoded.duplicate()));

    ByteBuffer destination = ByteBuffer.allocateDirect(INPUT.length);
    int maxIntermediateSize = executor.maxEncodedSize(INPUT.length);
    executor.decode(encoded.duplicate(), destination, INPUT.length, maxIntermediateSize,
        (long) maxIntermediateSize * 32);
    assertBytesEqual(destination);
  }

  @Test
  public void testMultiCompressionRoundTrips() throws Exception {
    for (String spec : new String[]{"LZ4,SNAPPY", "GZIP,ZSTD(5)", "SNAPPY,LZ4,GZIP"}) {
      CodecPipelineExecutor executor = CodecPipelineExecutor.create(spec, DataType.BYTES);
      ByteBuffer encoded = executor.encode(inputBuffer(false));
      assertBytesEqual(executor.decode(encoded.duplicate()));

      ByteBuffer destination = ByteBuffer.allocateDirect(INPUT.length);
      int maxIntermediateSize = executor.maxEncodedSize(INPUT.length);
      executor.decode(encoded.duplicate(), destination, INPUT.length, maxIntermediateSize,
          (long) maxIntermediateSize * 32);
      assertBytesEqual(destination);
    }
  }

  @Test
  public void testCallerOwnedScratchReusesDirectBuffers()
      throws Exception {
    CodecPipelineExecutor executor = CodecPipelineExecutor.create("LZ4,SNAPPY,GZIP", DataType.BYTES);
    ByteBuffer encoded = executor.encode(inputBuffer(false));
    int maxIntermediateSize = executor.maxEncodedSize(INPUT.length);
    try (CodecPipelineExecutor.DecodeScratch scratch = new CodecPipelineExecutor.DecodeScratch()) {
      ByteBuffer firstDestination = ByteBuffer.allocateDirect(INPUT.length);
      executor.decode(encoded.duplicate(), firstDestination, INPUT.length, maxIntermediateSize,
          (long) maxIntermediateSize * 32, scratch);
      assertBytesEqual(firstDestination);
      assertEquals(scratch.allocationCount(), 2);

      ByteBuffer secondDestination = ByteBuffer.allocateDirect(INPUT.length);
      executor.decode(encoded.duplicate(), secondDestination, INPUT.length, maxIntermediateSize,
          (long) maxIntermediateSize * 32, scratch);
      assertBytesEqual(secondDestination);
      assertEquals(scratch.allocationCount(), 2, "Repeated decode must reuse both ping-pong buffers");
    }
  }

  @Test
  public void testCanonicalNamesAliasesAndOptions() {
    assertEquals(CodecPipelineExecutor.create("zstd", DataType.INT).getCanonicalSpec(), "ZSTD(3)");
    assertEquals(CodecPipelineExecutor.create("zstandard", DataType.INT).getCanonicalSpec(), "ZSTD(3)");
    // Zstd treats level 0 as "use the default level", which would give ZSTD(0) and ZSTD(3) identical
    // behavior under two canonical spellings; it is rejected so each behavior has exactly one spelling.
    assertThrows(IllegalArgumentException.class, () -> CodecPipelineExecutor.create("zstd(0)", DataType.INT));
    assertEquals(CodecPipelineExecutor.create("zstd(5),gzip", DataType.INT).getCanonicalSpec(),
        "ZSTD(5),GZIP");
    assertEquals(CodecPipelineExecutor.create("lz4,snappy", DataType.INT).getCanonicalSpec(),
        "LZ4,SNAPPY");
  }

  @Test
  public void testTypedTransformsIgnoreCallerByteOrder()
      throws Exception {
    for (String spec : new String[]{"DELTA", "DELTADELTA", "T64", "GORILLA"}) {
      assertTypedTransformIgnoresCallerByteOrder(spec, DataType.INT,
          new long[]{Integer.MIN_VALUE, -1, 0, 1, Integer.MAX_VALUE});
      assertTypedTransformIgnoresCallerByteOrder(spec, DataType.LONG,
          new long[]{Long.MIN_VALUE, -1L, 0L, 1L, Long.MAX_VALUE});
    }
  }

  @Test
  public void testCompressionClassification() {
    assertTrue(CodecPipelineExecutor.create("LZ4", DataType.INT).isCompressed());
    assertTrue(CodecPipelineExecutor.create("GZIP,ZSTD", DataType.STRING).isCompressed());
  }

  @Test
  public void testDirectDestinationRequirement() throws Exception {
    for (String spec : new String[]{"SNAPPY", "ZSTD", "SNAPPY,GZIP"}) {
      CodecPipelineExecutor executor = CodecPipelineExecutor.create(spec, DataType.BYTES);
      ByteBuffer encoded = executor.encode(inputBuffer(false));
      IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
          () -> executor.decode(encoded.duplicate(), ByteBuffer.allocate(INPUT.length), INPUT.length));
      assertTrue(exception.getMessage().contains("requires a direct ByteBuffer"), exception.getMessage());
    }
  }

  @Test
  public void testHeapDestinationWhenNoStageRequiresDirectBuffer() throws Exception {
    for (String spec : new String[]{"LZ4", "GZIP", "LZ4,GZIP", "GZIP,SNAPPY"}) {
      CodecPipelineExecutor executor = CodecPipelineExecutor.create(spec, DataType.BYTES);
      ByteBuffer encoded = executor.encode(inputBuffer(true));
      ByteBuffer destination = ByteBuffer.allocate(INPUT.length);
      executor.decode(encoded.duplicate(), destination, INPUT.length, executor.maxEncodedSize(INPUT.length));
      assertBytesEqual(destination);
    }
  }

  @Test
  public void testComposedMaximumEncodedSizeBoundsActualOutput() throws Exception {
    CodecPipelineExecutor executor = CodecPipelineExecutor.create("LZ4,SNAPPY,GZIP,ZSTD", DataType.BYTES);
    int bound = executor.maxEncodedSize(INPUT.length);
    ByteBuffer encoded = executor.encode(inputBuffer(true));
    assertTrue(encoded.remaining() <= bound, encoded.remaining() + " > " + bound);
  }

  @Test
  public void testPerStageMaximumEncodedSizeCap() {
    CodecPipelineExecutor executor = CodecPipelineExecutor.create("LZ4,SNAPPY", DataType.BYTES);
    IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
        () -> executor.maxEncodedSize(INPUT.length, INPUT.length));
    assertTrue(exception.getMessage().contains("maximum encoded size"), exception.getMessage());
  }

  @Test
  public void testCumulativeMaximumEncodedSizeCap() {
    CodecPipelineExecutor executor = CodecPipelineExecutor.create("LZ4,SNAPPY", DataType.BYTES);
    IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
        () -> executor.maxEncodedSize(INPUT.length, Integer.MAX_VALUE, INPUT.length));
    assertTrue(exception.getMessage().contains("cumulative stage-output bound"), exception.getMessage());
  }

  @Test
  public void testDecodePerStageIntermediateCap() throws Exception {
    CodecPipelineExecutor executor = CodecPipelineExecutor.create("LZ4,SNAPPY", DataType.BYTES);
    ByteBuffer encoded = executor.encode(inputBuffer(false));
    IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
        () -> executor.decode(encoded.duplicate(), ByteBuffer.allocateDirect(INPUT.length), INPUT.length,
            INPUT.length));
    assertTrue(exception.getMessage().contains("maximum encoded size"), exception.getMessage());
  }

  @Test
  public void testDecodeCumulativeIntermediateCap() throws Exception {
    CodecPipelineExecutor executor = CodecPipelineExecutor.create("LZ4,SNAPPY", DataType.BYTES);
    ByteBuffer encoded = executor.encode(inputBuffer(false));
    IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
        () -> executor.decode(encoded.duplicate(), ByteBuffer.allocateDirect(INPUT.length), INPUT.length,
            Integer.MAX_VALUE, INPUT.length));
    assertTrue(exception.getMessage().contains("cumulative stage-output bound"), exception.getMessage());
  }

  @Test
  public void testDecodeRejectsUnexpectedFinalSize() throws Exception {
    CodecPipelineExecutor executor = CodecPipelineExecutor.create("GZIP", DataType.BYTES);
    ByteBuffer encoded = executor.encode(inputBuffer(false));
    int expectedDecodedSize = INPUT.length - 1;
    int maxIntermediateSize = executor.maxEncodedSize(INPUT.length);
    IOException exception = expectThrows(IOException.class,
        () -> executor.decode(encoded.duplicate(), ByteBuffer.allocate(INPUT.length), expectedDecodedSize,
            maxIntermediateSize, Long.MAX_VALUE));
    assertTrue(exception.getMessage().contains("but expected " + expectedDecodedSize), exception.getMessage());
  }

  @Test
  public void testInvalidMaximumSizeArguments() {
    CodecPipelineExecutor executor = CodecPipelineExecutor.create("LZ4", DataType.BYTES);
    assertThrows(IllegalArgumentException.class, () -> executor.maxEncodedSize(-1));
    assertThrows(IllegalArgumentException.class,
        () -> executor.maxEncodedSize(INPUT.length, INPUT.length - 1));
    assertThrows(IllegalArgumentException.class,
        () -> executor.maxEncodedSize(INPUT.length, Integer.MAX_VALUE, INPUT.length - 1L));
  }

  @Test
  public void testPublicRuntimeSurfaceIsBounded() throws ReflectiveOperationException {
    assertTrue(Modifier.isPublic(CodecPipelineExecutor.class.getModifiers()));
    assertTrue(Modifier.isPublic(CodecPipelineExecutor.DecodeScratch.class.getModifiers()));
    for (Class<?> closedType : new Class<?>[]{
        ChunkCodecHandler.class, CodecContext.class, CodecDefinition.class, CodecKind.class, CodecOptions.class,
        CodecRegistry.class, CodecPipelineValidator.class, Lz4CodecDefinition.class, SnappyCodecDefinition.class,
        GzipCodecDefinition.class, ZstdCodecDefinition.class}) {
      assertFalse(Modifier.isPublic(closedType.getModifiers()), closedType.getName());
    }

    Method publicFactory = CodecPipelineExecutor.class.getDeclaredMethod("create", String.class, DataType.class);
    assertTrue(Modifier.isPublic(publicFactory.getModifiers()));
    Method testFactory = CodecPipelineExecutor.class.getDeclaredMethod(
        "create", String.class, CodecContext.class, CodecRegistry.class);
    assertFalse(Modifier.isPublic(testFactory.getModifiers()));
    Method allocationReturningDecode = CodecPipelineExecutor.class.getDeclaredMethod("decode", ByteBuffer.class);
    assertFalse(Modifier.isPublic(allocationReturningDecode.getModifiers()));
    Method boundedDecode = CodecPipelineExecutor.class.getDeclaredMethod("decode", ByteBuffer.class,
        ByteBuffer.class, int.class, int.class, long.class);
    assertFalse(Modifier.isPublic(boundedDecode.getModifiers()));
    Method reusableBoundedDecode = CodecPipelineExecutor.class.getDeclaredMethod("decode", ByteBuffer.class,
        ByteBuffer.class, int.class, int.class, long.class, CodecPipelineExecutor.DecodeScratch.class);
    assertTrue(Modifier.isPublic(reusableBoundedDecode.getModifiers()));
  }

  private static ByteBuffer inputBuffer(boolean direct) {
    ByteBuffer buffer = direct ? ByteBuffer.allocateDirect(INPUT.length) : ByteBuffer.allocate(INPUT.length);
    return buffer.put(INPUT).flip();
  }

  private static void assertTypedTransformIgnoresCallerByteOrder(String spec, DataType dataType, long[] values)
      throws Exception {
    int decodedSize = values.length * dataType.size();
    ByteBuffer canonicalInput = ByteBuffer.allocateDirect(decodedSize).order(ByteOrder.BIG_ENDIAN);
    for (long value : values) {
      if (dataType == DataType.INT) {
        canonicalInput.putInt((int) value);
      } else {
        canonicalInput.putLong(value);
      }
    }
    canonicalInput.flip();

    CodecPipelineExecutor executor = CodecPipelineExecutor.create(spec, dataType);
    int maxIntermediateSize = Math.max(decodedSize, executor.maxEncodedSize(decodedSize));

    // A LITTLE_ENDIAN input view must still be interpreted as persisted big-endian typed words.
    ByteBuffer littleEndianInput = canonicalInput.duplicate().order(ByteOrder.LITTLE_ENDIAN);
    ByteBuffer encodedFromLittleEndian = executor.encode(littleEndianInput);
    assertEquals(littleEndianInput.position(), 0, "encode must not consume the caller's input view");
    assertEquals(littleEndianInput.order(), ByteOrder.LITTLE_ENDIAN);
    ByteBuffer bigEndianDestination = ByteBuffer.allocateDirect(decodedSize).order(ByteOrder.BIG_ENDIAN);
    try (CodecPipelineExecutor.DecodeScratch scratch = new CodecPipelineExecutor.DecodeScratch()) {
      executor.decode(encodedFromLittleEndian, bigEndianDestination, decodedSize, maxIntermediateSize,
          Long.MAX_VALUE, scratch);
    }
    assertTypedValues(bigEndianDestination, dataType, values, spec + " LITTLE_ENDIAN encode view");

    // A LITTLE_ENDIAN destination view must receive the same canonical big-endian bytes without
    // changing the caller's configured order.
    ByteBuffer encodedFromBigEndian = executor.encode(canonicalInput.duplicate());
    ByteBuffer littleEndianDestination = ByteBuffer.allocateDirect(decodedSize).order(ByteOrder.LITTLE_ENDIAN);
    try (CodecPipelineExecutor.DecodeScratch scratch = new CodecPipelineExecutor.DecodeScratch()) {
      executor.decode(encodedFromBigEndian, littleEndianDestination, decodedSize, maxIntermediateSize,
          Long.MAX_VALUE, scratch);
    }
    assertEquals(littleEndianDestination.order(), ByteOrder.LITTLE_ENDIAN);
    assertTypedValues(littleEndianDestination, dataType, values, spec + " LITTLE_ENDIAN decode destination");
  }

  private static void assertTypedValues(ByteBuffer actual, DataType dataType, long[] expected, String message) {
    ByteBuffer bigEndian = actual.duplicate().order(ByteOrder.BIG_ENDIAN);
    assertEquals(bigEndian.remaining(), expected.length * dataType.size(), message);
    for (long value : expected) {
      if (dataType == DataType.INT) {
        assertEquals(bigEndian.getInt(), (int) value, message);
      } else {
        assertEquals(bigEndian.getLong(), value, message);
      }
    }
  }

  private static void assertBytesEqual(ByteBuffer actual) {
    byte[] bytes = new byte[actual.remaining()];
    actual.duplicate().get(bytes);
    assertTrue(Arrays.equals(bytes, INPUT));
  }

  private static byte[] createInput() {
    byte[] input = new byte[16 * 1024];
    for (int i = 0; i < input.length; i++) {
      input[i] = (byte) ((i * 31) ^ (i >>> 3));
    }
    return input;
  }
}
