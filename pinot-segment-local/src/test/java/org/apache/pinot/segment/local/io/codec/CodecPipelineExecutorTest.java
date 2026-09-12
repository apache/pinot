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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.segment.spi.memory.CleanerUtil;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.apache.pinot.segment.local.io.codec.CodecTestUtils.encode;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Format-independent round-trip and resource-bound tests for [CodecPipelineExecutor].
public class CodecPipelineExecutorTest {
  private static final byte[] INPUT = createInput();
  private static final int MAX_STAGE_SIZE = 1024 * 1024;
  private static final long MAX_WORK_SIZE = 32L * MAX_STAGE_SIZE;

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
    ByteBuffer input = inputBuffer(directInput);
    try {
      ByteBuffer encoded = encode(executor, input);
      assertTrue(encoded.remaining() <= executor.maxEncodedSize(INPUT.length));
      assertBytesEqual(decode(executor, encoded, INPUT.length));
    } finally {
      CleanerUtil.cleanQuietly(input);
    }
  }

  @Test
  public void testMultiCompressionRoundTrips() throws Exception {
    for (String spec : new String[]{"LZ4,SNAPPY", "GZIP,ZSTD(5)", "SNAPPY,LZ4,GZIP"}) {
      CodecPipelineExecutor executor = CodecPipelineExecutor.create(spec, DataType.BYTES);
      ByteBuffer encoded = encode(executor, inputBuffer(false));
      assertBytesEqual(decode(executor, encoded, INPUT.length));
    }
  }

  @Test
  public void testCallerOwnedScratchReusesDirectBuffers()
      throws Exception {
    CodecPipelineExecutor executor = CodecPipelineExecutor.create("LZ4,SNAPPY,GZIP,ZSTD", DataType.BYTES);
    ByteBuffer encoded = encode(executor, inputBuffer(false));
    int maxStageSize = executor.maxEncodedSize(INPUT.length);
    ByteBuffer destination = ByteBuffer.allocateDirect(INPUT.length);
    try (CodecPipelineExecutor.DecodeScratch scratch = new CodecPipelineExecutor.DecodeScratch()) {
      executor.decode(encoded.duplicate(), destination, INPUT.length, maxStageSize,
          (long) maxStageSize * 32, scratch);
      assertBytesEqual(destination);
      assertEquals(scratch.allocationCount(), 2);
      assertEquals(scratch.viewCreationCount(), 3);

      executor.decode(encoded.duplicate(), destination, INPUT.length, maxStageSize,
          (long) maxStageSize * 32, scratch);
      assertBytesEqual(destination);
      assertEquals(scratch.allocationCount(), 2, "Repeated decode must reuse both ping-pong buffers");
      assertEquals(scratch.viewCreationCount(), 3, "Repeated decode must reuse stage-bounded views");
    } finally {
      CleanerUtil.cleanQuietly(destination);
    }
  }

  @Test
  public void testCallerOwnedEncodeScratchReusesDirectBuffers() throws Exception {
    CodecPipelineExecutor executor = CodecPipelineExecutor.create(
        "DELTA,DELTADELTA,T64,LZ4,SNAPPY,GZIP,ZSTD", DataType.INT);
    int maxStageSize = executor.maxEncodedSize(INPUT.length);
    try (CodecPipelineExecutor.EncodeScratch scratch = new CodecPipelineExecutor.EncodeScratch()) {
      ByteBuffer firstInput = inputBuffer(false);
      ByteBuffer firstEncoded = executor.encode(firstInput, maxStageSize, (long) maxStageSize * 32, scratch);
      assertEquals(firstInput.position(), 0, "encode must not consume the caller's input view");
      assertBytesEqual(decode(executor, firstEncoded.duplicate(), INPUT.length));
      assertEquals(scratch.allocationCount(), 2);
      assertEquals(scratch.viewCreationCount(), 8);

      ByteBuffer secondEncoded = executor.encode(firstInput, maxStageSize,
          (long) maxStageSize * 32, scratch);
      assertBytesEqual(decode(executor, secondEncoded.duplicate(), INPUT.length));
      assertEquals(scratch.allocationCount(), 2, "Repeated encode must reuse both ping-pong buffers");
      assertEquals(scratch.viewCreationCount(), 8, "Repeated encode must reuse source and stage-bounded views");
    }
  }

  @Test
  public void testEncodeScratchResetsReturnedViewByteOrder() throws Exception {
    CodecPipelineExecutor executor = CodecPipelineExecutor.create("DELTA", DataType.INT);
    ByteBuffer input = inputBuffer(false);
    try (CodecPipelineExecutor.EncodeScratch scratch = new CodecPipelineExecutor.EncodeScratch()) {
      ByteBuffer firstEncoded = executor.encode(input, INPUT.length, INPUT.length, scratch);
      firstEncoded.order(ByteOrder.LITTLE_ENDIAN);

      ByteBuffer secondEncoded = executor.encode(input, INPUT.length, INPUT.length, scratch);
      assertEquals(secondEncoded.order(), ByteOrder.BIG_ENDIAN);
      assertBytesEqual(decode(executor, secondEncoded.duplicate(), INPUT.length));
      assertEquals(scratch.allocationCount(), 1);
      assertEquals(scratch.viewCreationCount(), 2);
    }
  }

  @Test
  public void testEncodeScratchRefreshesCachedSourceWindow() throws Exception {
    byte[] firstWindow = INPUT.clone();
    for (int i = 0; i < firstWindow.length; i++) {
      firstWindow[i] ^= 0x5A;
    }
    ByteBuffer source = ByteBuffer.allocateDirect(INPUT.length * 2).order(ByteOrder.LITTLE_ENDIAN);
    source.put(firstWindow).put(INPUT).flip();
    CodecPipelineExecutor executor = CodecPipelineExecutor.create("DELTA,T64,LZ4", DataType.INT);
    try (CodecPipelineExecutor.EncodeScratch scratch = new CodecPipelineExecutor.EncodeScratch()) {
      source.position(INPUT.length).limit(INPUT.length * 2);
      ByteBuffer secondEncoded = executor.encode(source, MAX_STAGE_SIZE, MAX_WORK_SIZE, scratch);
      assertBytesEqual(decode(executor, secondEncoded.duplicate(), INPUT.length), INPUT);
      assertEquals(source.position(), INPUT.length);
      assertEquals(source.limit(), INPUT.length * 2);
      assertEquals(source.order(), ByteOrder.LITTLE_ENDIAN);
      int warmViewCount = scratch.viewCreationCount();

      source.limit(INPUT.length).position(0);
      ByteBuffer firstEncoded = executor.encode(source, MAX_STAGE_SIZE, MAX_WORK_SIZE, scratch);
      assertBytesEqual(decode(executor, firstEncoded.duplicate(), INPUT.length), firstWindow);
      assertEquals(source.position(), 0);
      assertEquals(source.limit(), INPUT.length);
      assertEquals(source.order(), ByteOrder.LITTLE_ENDIAN);
      assertEquals(scratch.viewCreationCount(), warmViewCount,
          "Moving the same source view must refresh its window without creating another view");
    } finally {
      CleanerUtil.cleanQuietly(source);
    }
  }

  @Test
  public void testScratchGrowsShrinksAndReplacesDestination() throws Exception {
    CodecPipelineExecutor executor = CodecPipelineExecutor.create("LZ4,SNAPPY,GZIP,ZSTD", DataType.BYTES);
    ByteBuffer destination = ByteBuffer.allocateDirect(INPUT.length).order(ByteOrder.LITTLE_ENDIAN);
    ByteBuffer replacement = ByteBuffer.allocateDirect(INPUT.length).order(ByteOrder.LITTLE_ENDIAN);
    int[] sizes = {32, INPUT.length, 32, 32};
    try (CodecPipelineExecutor.EncodeScratch encodeScratch = new CodecPipelineExecutor.EncodeScratch();
        CodecPipelineExecutor.DecodeScratch decodeScratch = new CodecPipelineExecutor.DecodeScratch()) {
      for (int i = 0; i < sizes.length; i++) {
        byte[] expected = Arrays.copyOf(INPUT, sizes[i]);
        ByteBuffer source = ByteBuffer.allocate(sizes[i] + 4);
        source.position(4);
        source.put(expected).flip().position(4).order(ByteOrder.LITTLE_ENDIAN);
        ByteBuffer encoded = executor.encode(source, MAX_STAGE_SIZE, MAX_WORK_SIZE, encodeScratch);
        assertEquals(source.position(), 4);
        assertEquals(source.limit(), sizes[i] + 4);
        assertEquals(source.order(), ByteOrder.LITTLE_ENDIAN);

        ByteBuffer output = i == sizes.length - 1 ? replacement : destination;
        int previousViewCount = decodeScratch.viewCreationCount();
        executor.decode(encoded, output, sizes[i], MAX_STAGE_SIZE, MAX_WORK_SIZE, decodeScratch);
        assertBytesEqual(output, expected);
        assertEquals(output.order(), ByteOrder.LITTLE_ENDIAN);
        assertEquals(encodeScratch.allocationCount(), i == 0 ? 2 : 4);
        assertEquals(decodeScratch.allocationCount(), i == 0 ? 2 : 4);
        if (i == sizes.length - 1) {
          assertEquals(decodeScratch.viewCreationCount(), previousViewCount,
              "Replacing the final destination must not recreate unchanged intermediate views");
        }
      }
    } finally {
      CleanerUtil.cleanQuietly(destination);
      CleanerUtil.cleanQuietly(replacement);
    }
  }

  @Test
  public void testWarmLargeScratchRejectsSmallCorruptInnerFrame() throws Exception {
    CodecPipelineExecutor executor = CodecPipelineExecutor.create("GZIP,GZIP", DataType.BYTES);
    byte[] smallInput = new byte[32];
    ByteBuffer smallEncoded = encode(executor, ByteBuffer.wrap(smallInput));
    ByteBuffer corrupt = ByteBuffer.allocate(smallEncoded.remaining()).put(smallEncoded.duplicate()).flip();
    int smallIntermediateBound = CodecPipelineExecutor.create("GZIP", DataType.BYTES)
        .maxEncodedSize(smallInput.length);
    // The outer GZIP footer controls an intermediate output, not the final destination size.
    corrupt.putInt(corrupt.limit() - Integer.BYTES, smallIntermediateBound + 1);
    ByteBuffer destination = ByteBuffer.allocateDirect(INPUT.length);
    try (CodecPipelineExecutor.DecodeScratch scratch = new CodecPipelineExecutor.DecodeScratch()) {
      executor.decode(encode(executor, inputBuffer(false)), destination, INPUT.length, MAX_STAGE_SIZE, MAX_WORK_SIZE,
          scratch);
      assertBytesEqual(destination);
      assertEquals(scratch.allocationCount(), 1);

      IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
          () -> executor.decode(corrupt, destination, smallInput.length, MAX_STAGE_SIZE, MAX_WORK_SIZE, scratch));
      assertTrue(exception.getMessage().contains("exceeds dst capacity " + smallIntermediateBound),
          exception.getMessage());
      assertEquals(scratch.allocationCount(), 1, "A corrupt frame must not grow a warm workspace");
      executor.decode(smallEncoded, destination, smallInput.length, MAX_STAGE_SIZE, MAX_WORK_SIZE, scratch);
      assertBytesEqual(destination, smallInput);
      assertEquals(scratch.allocationCount(), 1);
    } finally {
      CleanerUtil.cleanQuietly(destination);
    }
  }

  @Test
  public void testScratchCloseIsIdempotentAndRejectsReuse() throws Exception {
    CodecPipelineExecutor executor = CodecPipelineExecutor.create("LZ4,GZIP", DataType.BYTES);
    ByteBuffer destination = ByteBuffer.allocateDirect(INPUT.length);
    try (CodecPipelineExecutor.EncodeScratch encodeScratch = new CodecPipelineExecutor.EncodeScratch();
        CodecPipelineExecutor.DecodeScratch decodeScratch = new CodecPipelineExecutor.DecodeScratch()) {
      ByteBuffer encoded = executor.encode(inputBuffer(false), MAX_STAGE_SIZE, MAX_WORK_SIZE, encodeScratch);
      executor.decode(encoded, destination, INPUT.length, MAX_STAGE_SIZE, MAX_WORK_SIZE, decodeScratch);
      assertBytesEqual(destination);
      encodeScratch.close();
      encodeScratch.close();
      decodeScratch.close();
      decodeScratch.close();
      assertThrows(IllegalStateException.class,
          () -> executor.encode(inputBuffer(false), MAX_STAGE_SIZE, MAX_WORK_SIZE, encodeScratch));
      assertThrows(IllegalStateException.class,
          () -> executor.decode(ByteBuffer.allocate(0), destination, INPUT.length, MAX_STAGE_SIZE, MAX_WORK_SIZE,
              decodeScratch));
    } finally {
      CleanerUtil.cleanQuietly(destination);
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

  @Test(timeOut = 20_000)
  public void testSharedExecutorWithWorkerOwnedScratch() throws Exception {
    CodecPipelineExecutor executor = CodecPipelineExecutor.create("LZ4,SNAPPY,GZIP,ZSTD", DataType.BYTES);
    int workers = 3;
    CountDownLatch ready = new CountDownLatch(workers);
    CountDownLatch start = new CountDownLatch(1);
    ExecutorService pool = Executors.newFixedThreadPool(workers);
    List<Future<?>> futures = new ArrayList<>();
    try {
      for (int worker = 0; worker < workers; worker++) {
        int workerId = worker;
        futures.add(pool.submit(() -> {
          ByteBuffer destination = ByteBuffer.allocateDirect(256);
          try (CodecPipelineExecutor.EncodeScratch encodeScratch = new CodecPipelineExecutor.EncodeScratch();
              CodecPipelineExecutor.DecodeScratch decodeScratch = new CodecPipelineExecutor.DecodeScratch()) {
            ready.countDown();
            assertTrue(start.await(5, TimeUnit.SECONDS), "Workers were not released");
            for (int round = 0; round < 16; round++) {
              byte[] expected = Arrays.copyOf(INPUT, 128 + 64 * (round & 1));
              for (int i = 0; i < expected.length; i++) {
                expected[i] ^= (byte) (workerId + round);
              }
              ByteBuffer encoded = executor.encode(ByteBuffer.wrap(expected), MAX_STAGE_SIZE, MAX_WORK_SIZE,
                  encodeScratch);
              executor.decode(encoded, destination, expected.length, MAX_STAGE_SIZE, MAX_WORK_SIZE, decodeScratch);
              assertBytesEqual(destination, expected);
            }
          } finally {
            CleanerUtil.cleanQuietly(destination);
          }
          return null;
        }));
      }
      assertTrue(ready.await(5, TimeUnit.SECONDS), "Workers did not become ready");
      start.countDown();
      for (Future<?> future : futures) {
        future.get(5, TimeUnit.SECONDS);
      }
    } finally {
      start.countDown();
      pool.shutdownNow();
      assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS), "Codec workers did not terminate");
    }
  }

  @Test(timeOut = 20_000)
  public void testSharedSourceWithWorkerOwnedScratch() throws Exception {
    CodecPipelineExecutor executor = CodecPipelineExecutor.create("DELTA,T64,LZ4", DataType.INT);
    ByteBuffer source = ByteBuffer.allocateDirect(INPUT.length + 8).order(ByteOrder.LITTLE_ENDIAN);
    source.position(4).put(INPUT).flip().position(4);
    int workers = 4;
    CountDownLatch ready = new CountDownLatch(workers);
    CountDownLatch start = new CountDownLatch(1);
    ExecutorService pool = Executors.newFixedThreadPool(workers);
    List<Future<?>> futures = new ArrayList<>();
    try {
      for (int worker = 0; worker < workers; worker++) {
        futures.add(pool.submit(() -> {
          ByteBuffer destination = ByteBuffer.allocateDirect(INPUT.length);
          try (CodecPipelineExecutor.EncodeScratch encodeScratch = new CodecPipelineExecutor.EncodeScratch();
              CodecPipelineExecutor.DecodeScratch decodeScratch = new CodecPipelineExecutor.DecodeScratch()) {
            ready.countDown();
            assertTrue(start.await(5, TimeUnit.SECONDS), "Workers were not released");
            for (int round = 0; round < 32; round++) {
              ByteBuffer encoded = executor.encode(source, MAX_STAGE_SIZE, MAX_WORK_SIZE, encodeScratch);
              executor.decode(encoded, destination, INPUT.length, MAX_STAGE_SIZE, MAX_WORK_SIZE, decodeScratch);
              assertBytesEqual(destination);
            }
          } finally {
            CleanerUtil.cleanQuietly(destination);
          }
          return null;
        }));
      }
      assertTrue(ready.await(5, TimeUnit.SECONDS), "Workers did not become ready");
      start.countDown();
      for (Future<?> future : futures) {
        future.get(10, TimeUnit.SECONDS);
      }
      assertEquals(source.position(), 4);
      assertEquals(source.limit(), INPUT.length + 4);
      assertEquals(source.order(), ByteOrder.LITTLE_ENDIAN);
    } finally {
      start.countDown();
      pool.shutdownNow();
      assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS), "Codec workers did not terminate");
      CleanerUtil.cleanQuietly(source);
    }
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
      ByteBuffer encoded = encode(executor, inputBuffer(false));
      IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
          () -> decode(executor, encoded, ByteBuffer.allocate(INPUT.length), INPUT.length, MAX_STAGE_SIZE,
              MAX_WORK_SIZE));
      assertTrue(exception.getMessage().contains("requires a direct ByteBuffer"), exception.getMessage());
    }
  }

  @Test
  public void testHeapDestinationWhenNoStageRequiresDirectBuffer() throws Exception {
    for (String spec : new String[]{"LZ4", "GZIP", "LZ4,GZIP", "GZIP,SNAPPY"}) {
      CodecPipelineExecutor executor = CodecPipelineExecutor.create(spec, DataType.BYTES);
      ByteBuffer encoded = encode(executor, inputBuffer(false));
      ByteBuffer destination = ByteBuffer.allocate(INPUT.length);
      decode(executor, encoded, destination, INPUT.length, MAX_STAGE_SIZE, MAX_WORK_SIZE);
      assertBytesEqual(destination);
    }
  }

  @Test
  public void testComposedMaximumEncodedSizeBoundsActualOutput() throws Exception {
    CodecPipelineExecutor executor = CodecPipelineExecutor.create("LZ4,SNAPPY,GZIP,ZSTD", DataType.BYTES);
    int bound = executor.maxEncodedSize(INPUT.length);
    ByteBuffer encoded = encode(executor, inputBuffer(false));
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
  public void testRejectedBoundsDoNotAllocateAndScratchCanRecover() throws Exception {
    CodecPipelineExecutor executor = CodecPipelineExecutor.create("LZ4,SNAPPY", DataType.BYTES);
    ByteBuffer validEncoded = encode(executor, inputBuffer(false));
    ByteBuffer destination = ByteBuffer.allocateDirect(INPUT.length);
    try {
      for (boolean cumulative : new boolean[]{false, true}) {
        int stageLimit = cumulative ? MAX_STAGE_SIZE : INPUT.length;
        long workLimit = cumulative ? INPUT.length : MAX_WORK_SIZE;
        String expectedMessage = cumulative ? "cumulative stage-output bound" : "maximum encoded size";
        try (CodecPipelineExecutor.EncodeScratch encodeScratch = new CodecPipelineExecutor.EncodeScratch();
            CodecPipelineExecutor.DecodeScratch decodeScratch = new CodecPipelineExecutor.DecodeScratch()) {
          IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
              () -> executor.encode(inputBuffer(false), stageLimit, workLimit, encodeScratch));
          assertTrue(exception.getMessage().contains(expectedMessage), exception.getMessage());
          assertEquals(encodeScratch.allocationCount(), 0);
          exception = expectThrows(IllegalArgumentException.class,
              () -> executor.decode(validEncoded.duplicate(), destination, INPUT.length, stageLimit, workLimit,
                  decodeScratch));
          assertTrue(exception.getMessage().contains(expectedMessage), exception.getMessage());
          assertEquals(decodeScratch.allocationCount(), 0);

          ByteBuffer recovered = executor.encode(inputBuffer(false), MAX_STAGE_SIZE, MAX_WORK_SIZE, encodeScratch);
          executor.decode(recovered, destination, INPUT.length, MAX_STAGE_SIZE, MAX_WORK_SIZE, decodeScratch);
          assertBytesEqual(destination);
          assertEquals(encodeScratch.allocationCount(), 2);
          assertEquals(decodeScratch.allocationCount(), 1);
        }
      }
    } finally {
      CleanerUtil.cleanQuietly(destination);
    }
  }

  @Test
  public void testDecodeRejectsUnexpectedFinalSize() throws Exception {
    CodecPipelineExecutor executor = CodecPipelineExecutor.create("GZIP", DataType.BYTES);
    ByteBuffer encoded = encode(executor, inputBuffer(false));
    int expectedDecodedSize = INPUT.length - 1;
    int maxStageSize = executor.maxEncodedSize(INPUT.length);
    IOException exception = expectThrows(IOException.class,
        () -> decode(executor, encoded.duplicate(), ByteBuffer.allocate(INPUT.length), expectedDecodedSize,
            maxStageSize, Long.MAX_VALUE));
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
    try (CodecPipelineExecutor.DecodeScratch scratch = new CodecPipelineExecutor.DecodeScratch()) {
      IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
          () -> executor.decode(ByteBuffer.allocate(0), ByteBuffer.allocate(INPUT.length), INPUT.length,
              INPUT.length - 1, Long.MAX_VALUE, scratch));
      assertTrue(exception.getMessage().contains("maxStageSize"), exception.getMessage());
    }
  }

  @Test
  public void testPublicRuntimeSurfaceIsBounded() throws ReflectiveOperationException {
    assertTrue(Modifier.isPublic(CodecPipelineExecutor.class.getModifiers()));
    assertTrue(Modifier.isPublic(CodecPipelineExecutor.EncodeScratch.class.getModifiers()));
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
    assertThrows(NoSuchMethodException.class,
        () -> CodecPipelineExecutor.class.getDeclaredMethod("encode", ByteBuffer.class));
    assertThrows(NoSuchMethodException.class,
        () -> CodecPipelineExecutor.class.getDeclaredMethod("decode", ByteBuffer.class));
    Method reusableBoundedEncode = CodecPipelineExecutor.class.getDeclaredMethod("encode", ByteBuffer.class,
        int.class, long.class, CodecPipelineExecutor.EncodeScratch.class);
    assertTrue(Modifier.isPublic(reusableBoundedEncode.getModifiers()));
    assertEquals(Arrays.stream(CodecPipelineExecutor.class.getDeclaredMethods())
        .filter(method -> method.getName().equals("encode")).count(), 1L);
    assertEquals(Arrays.stream(CodecPipelineExecutor.class.getDeclaredMethods())
        .filter(method -> method.getName().equals("decode")).count(), 1L);
    Method reusableBoundedDecode = CodecPipelineExecutor.class.getDeclaredMethod("decode", ByteBuffer.class,
        ByteBuffer.class, int.class, int.class, long.class, CodecPipelineExecutor.DecodeScratch.class);
    assertTrue(Modifier.isPublic(reusableBoundedDecode.getModifiers()));
  }

  private static ByteBuffer inputBuffer(boolean direct) {
    ByteBuffer buffer = direct ? ByteBuffer.allocateDirect(INPUT.length) : ByteBuffer.allocate(INPUT.length);
    return buffer.put(INPUT).flip();
  }

  private static ByteBuffer decode(CodecPipelineExecutor executor, ByteBuffer encoded, int decodedSize)
      throws IOException {
    ByteBuffer destination = ByteBuffer.allocateDirect(decodedSize);
    try {
      decode(executor, encoded, destination, decodedSize, MAX_STAGE_SIZE, MAX_WORK_SIZE);
      return ByteBuffer.allocate(decodedSize).put(destination).flip();
    } finally {
      CleanerUtil.cleanQuietly(destination);
    }
  }

  private static void decode(CodecPipelineExecutor executor, ByteBuffer encoded, ByteBuffer destination,
      int decodedSize, int maxStageSize, long maxWorkSize) throws IOException {
    try (CodecPipelineExecutor.DecodeScratch scratch = new CodecPipelineExecutor.DecodeScratch()) {
      executor.decode(encoded, destination, decodedSize, maxStageSize, maxWorkSize, scratch);
    }
  }

  private static void assertTypedTransformIgnoresCallerByteOrder(String spec, DataType dataType, long[] values)
      throws Exception {
    int decodedSize = values.length * dataType.size();
    ByteBuffer canonicalInput = ByteBuffer.allocateDirect(decodedSize).order(ByteOrder.BIG_ENDIAN);
    ByteBuffer bigEndianDestination = ByteBuffer.allocateDirect(decodedSize).order(ByteOrder.BIG_ENDIAN);
    ByteBuffer littleEndianDestination = ByteBuffer.allocateDirect(decodedSize).order(ByteOrder.LITTLE_ENDIAN);
    try {
      for (long value : values) {
        if (dataType == DataType.INT) {
          canonicalInput.putInt((int) value);
        } else {
          canonicalInput.putLong(value);
        }
      }
      canonicalInput.flip();
      CodecPipelineExecutor executor = CodecPipelineExecutor.create(spec, dataType);
      int maxStageSize = Math.max(decodedSize, executor.maxEncodedSize(decodedSize));

      // A LITTLE_ENDIAN input view must still be interpreted as persisted big-endian typed words.
      ByteBuffer littleEndianInput = canonicalInput.duplicate().order(ByteOrder.LITTLE_ENDIAN);
      ByteBuffer encodedFromLittleEndian = encode(executor, littleEndianInput);
      assertEquals(littleEndianInput.position(), 0, "encode must not consume the caller's input view");
      assertEquals(littleEndianInput.order(), ByteOrder.LITTLE_ENDIAN);
      decode(executor, encodedFromLittleEndian, bigEndianDestination, decodedSize, maxStageSize, MAX_WORK_SIZE);
      assertTypedValues(bigEndianDestination, dataType, values, spec + " LITTLE_ENDIAN encode view");

      // Preserve the caller's LITTLE_ENDIAN destination order while writing canonical big-endian bytes.
      ByteBuffer encodedFromBigEndian = encode(executor, canonicalInput.duplicate());
      decode(executor, encodedFromBigEndian, littleEndianDestination, decodedSize, maxStageSize, MAX_WORK_SIZE);
      assertEquals(littleEndianDestination.order(), ByteOrder.LITTLE_ENDIAN);
      assertTypedValues(littleEndianDestination, dataType, values, spec + " LITTLE_ENDIAN decode destination");
    } finally {
      CleanerUtil.cleanQuietly(canonicalInput);
      CleanerUtil.cleanQuietly(bigEndianDestination);
      CleanerUtil.cleanQuietly(littleEndianDestination);
    }
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
    assertBytesEqual(actual, INPUT);
  }

  private static void assertBytesEqual(ByteBuffer actual, byte[] expected) {
    byte[] bytes = new byte[actual.remaining()];
    actual.duplicate().get(bytes);
    assertEquals(bytes, expected);
  }

  private static byte[] createInput() {
    byte[] input = new byte[16 * 1024];
    for (int i = 0; i < input.length; i++) {
      input[i] = (byte) ((i * 31) ^ (i >>> 3));
    }
    return input;
  }
}
