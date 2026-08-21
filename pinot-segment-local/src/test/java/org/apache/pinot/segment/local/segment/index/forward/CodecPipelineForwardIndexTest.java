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
package org.apache.pinot.segment.local.segment.index.forward;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.Collections;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.PinotBuffersAfterMethodCheckRule;
import org.apache.pinot.segment.local.io.codec.CodecPipelineExecutor;
import org.apache.pinot.segment.local.io.writer.impl.FixedByteChunkForwardIndexWriterV7;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.SingleValueFixedByteRawIndexCreator;
import org.apache.pinot.segment.local.segment.index.readers.forward.ChunkReaderContext;
import org.apache.pinot.segment.local.segment.index.readers.forward.FixedByteChunkSVForwardIndexReaderV7;
import org.apache.pinot.segment.spi.index.ForwardIndexConfig;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Roundtrip encode/decode tests for the codec-pipeline forward index (version 7).
///
/// Covers:
///
/// - INT and LONG single-value columns
/// - DELTA only, ZSTD only, DELTA,ZSTD pipelines
/// - Version check on the written file
/// - Canonical spec round-trips correctly through the header
public class CodecPipelineForwardIndexTest implements PinotBuffersAfterMethodCheckRule {

  private static final int NUM_VALUES = 10_007;
  private static final int DOCS_PER_CHUNK = 1024;
  private static final String TEST_FILE_PREFIX =
      System.getProperty("java.io.tmpdir") + File.separator + "CodecPipelineFwdTest";
  // Fixed seed for deterministic test data — this test verifies correctness, not random coverage
  // Construct a fresh Random per test method to avoid sharing mutable state across data-provider
  // rows when TestNG runs them in parallel.
  private static final long RANDOM_SEED = 42L;

  @DataProvider(name = "specs")
  public static Object[][] specs() {
    return new Object[][]{
        {"DELTA", DataType.INT},
        {"DELTA", DataType.LONG},
        {"ZSTD(3)", DataType.INT},
        {"ZSTD(3)", DataType.LONG},
        {"ZSTD(8)", DataType.INT},
        {"DELTA,ZSTD(3)", DataType.INT},
        {"DELTA,ZSTD(3)", DataType.LONG},
        {"DELTA,ZSTD(8)", DataType.INT},
        {"LZ4", DataType.INT},
        {"LZ4", DataType.LONG},
        {"DELTA,LZ4", DataType.INT},
        {"DELTA,LZ4", DataType.LONG},
        {"SNAPPY", DataType.INT},
        {"SNAPPY", DataType.LONG},
        {"GZIP", DataType.INT},
        {"GZIP", DataType.LONG},
        {"DELTA,SNAPPY", DataType.INT},
        {"DELTA,SNAPPY", DataType.LONG},
        {"DELTA,GZIP", DataType.INT},
        {"DELTA,GZIP", DataType.LONG},
        {"DELTADELTA", DataType.INT},
        {"DELTADELTA", DataType.LONG},
        {"DELTADELTA,LZ4", DataType.INT},
        {"DELTADELTA,LZ4", DataType.LONG},
        {"DELTADELTA,ZSTD(3)", DataType.INT},
        {"DELTADELTA,ZSTD(3)", DataType.LONG},
        {"T64", DataType.INT},
        {"T64", DataType.LONG},
        {"T64,LZ4", DataType.INT},
        {"T64,LZ4", DataType.LONG},
        {"T64,ZSTD(3)", DataType.INT},
        {"T64,ZSTD(3)", DataType.LONG},
        {"GORILLA", DataType.INT},
        {"GORILLA", DataType.LONG},
        {"GORILLA,LZ4", DataType.INT},
        {"GORILLA,LZ4", DataType.LONG},
        {"GORILLA,ZSTD(3)", DataType.INT},
        {"GORILLA,ZSTD(3)", DataType.LONG},
        // Chained pipelines: N value transforms → optional packing transform → N compressions.
        {"DELTA,DELTADELTA,LZ4", DataType.INT},
        {"DELTA,DELTADELTA,LZ4", DataType.LONG},
        {"DELTA,DELTADELTA", DataType.LONG},
        {"DELTA,T64,LZ4", DataType.INT},
        {"DELTA,T64,LZ4", DataType.LONG},
        {"DELTADELTA,GORILLA,ZSTD(3)", DataType.LONG},
        {"DELTA,LZ4,ZSTD(3)", DataType.INT},
        {"DELTA,LZ4,ZSTD(3)", DataType.LONG},
    };
  }

  @Test(dataProvider = "specs")
  public void testRoundtrip(String spec, DataType dataType)
      throws Exception {
    File file = new File(TEST_FILE_PREFIX + "_" + spec.replaceAll("[^A-Za-z0-9]", "_") + "_" + dataType);
    FileUtils.deleteQuietly(file);

    Random random = new Random(RANDOM_SEED);
    // Use monotonically increasing values (good for delta encoding)
    int[] expectedInts = new int[NUM_VALUES];
    long[] expectedLongs = new long[NUM_VALUES];
    for (int i = 0; i < NUM_VALUES; i++) {
      expectedInts[i] = i * 17 + random.nextInt(5);
      expectedLongs[i] = (long) i * 1_000_000L + random.nextInt(100);
    }

    CodecPipelineExecutor executor = CodecPipelineExecutor.create(spec, dataType);

    try (FixedByteChunkForwardIndexWriterV7 writer = new FixedByteChunkForwardIndexWriterV7(
        file, executor, NUM_VALUES, DOCS_PER_CHUNK, dataType.size())) {
      for (int i = 0; i < NUM_VALUES; i++) {
        if (dataType == DataType.INT) {
          writer.putInt(expectedInts[i]);
        } else {
          writer.putLong(expectedLongs[i]);
        }
      }
    }

    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(file);
        FixedByteChunkSVForwardIndexReaderV7 reader = new FixedByteChunkSVForwardIndexReaderV7(
            buffer, dataType);
        ChunkReaderContext ctx = reader.createContext()) {

      // Verify version and explicit format discriminator
      assertEquals(buffer.getInt(0), ForwardIndexConfig.CODEC_PIPELINE_WRITER_VERSION);
      assertEquals(buffer.getInt(Integer.BYTES), FixedByteChunkForwardIndexWriterV7.FORMAT_MAGIC);

      // Verify spec stored in header matches canonical form
      assertNotNull(reader.getCodecSpec());
      assertFalse(reader.getCodecSpec().isBlank());
      assertEquals(reader.getCodecSpec(), executor.getCanonicalSpec());

      // V7 reader contract: getCompressionType() returns null (no legacy enum value),
      // getCodecSpec() returns the canonical spec — these signal "this is a V7 segment" to
      // the ForwardIndexHandler reload path.
      assertNull(reader.getCompressionType(), "V7 reader must return null compression type");

      for (int i = 0; i < NUM_VALUES; i++) {
        if (dataType == DataType.INT) {
          assertEquals(reader.getInt(i, ctx), expectedInts[i],
              "Mismatch at docId " + i + " for spec '" + spec + "'");
        } else {
          assertEquals(reader.getLong(i, ctx), expectedLongs[i],
              "Mismatch at docId " + i + " for spec '" + spec + "'");
        }
      }
    }

    FileUtils.deleteQuietly(file);
  }

  @Test
  public void testCanonicalSpecStoredInHeader()
      throws Exception {
    // ZSTD without explicit level should be canonicalized to ZSTD(3) in the header
    String inputSpec = "ZSTD";
    File file = new File(TEST_FILE_PREFIX + "_canonical");
    FileUtils.deleteQuietly(file);

    CodecPipelineExecutor executor = CodecPipelineExecutor.create(inputSpec, DataType.INT);

    try (FixedByteChunkForwardIndexWriterV7 writer = new FixedByteChunkForwardIndexWriterV7(
        file, executor, 10, 16, Integer.BYTES)) {
      for (int i = 0; i < 10; i++) {
        writer.putInt(i);
      }
    }

    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(file);
        FixedByteChunkSVForwardIndexReaderV7 reader = new FixedByteChunkSVForwardIndexReaderV7(
            buffer, DataType.INT)) {
      // Default level 3 should be included in canonical form
      assertEquals(reader.getCodecSpec(), "ZSTD(3)");
    }

    FileUtils.deleteQuietly(file);
  }

  @Test
  public void testForwardIndexReaderFactoryDispatch()
      throws Exception {
    // Confirm ForwardIndexReaderFactory routes version-7 files to the V7 reader
    File file = new File(TEST_FILE_PREFIX + "_factory");
    FileUtils.deleteQuietly(file);

    CodecPipelineExecutor executor = CodecPipelineExecutor.create("DELTA,ZSTD(3)", DataType.INT);

    int[] expected = new int[100];
    for (int i = 0; i < expected.length; i++) {
      expected[i] = i * 3;
    }

    try (FixedByteChunkForwardIndexWriterV7 writer = new FixedByteChunkForwardIndexWriterV7(
        file, executor, expected.length, 32, Integer.BYTES)) {
      for (int v : expected) {
        writer.putInt(v);
      }
    }

    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(file)) {
      ForwardIndexReaderFactory factory = ForwardIndexReaderFactory.getInstance();
      @SuppressWarnings("rawtypes")
      ForwardIndexReader reader = factory.createRawIndexReader(buffer, DataType.INT, true);
      try (reader) {
        assertTrue(reader instanceof FixedByteChunkSVForwardIndexReaderV7,
            "Expected V7 reader but got: " + reader.getClass().getSimpleName());
        FixedByteChunkSVForwardIndexReaderV7 v7Reader = (FixedByteChunkSVForwardIndexReaderV7) reader;
        try (ChunkReaderContext ctx = v7Reader.createContext()) {
          for (int i = 0; i < expected.length; i++) {
            assertEquals(v7Reader.getInt(i, ctx), expected[i]);
          }
        }
      }
    }

    FileUtils.deleteQuietly(file);
  }

  @Test
  public void testPartialLastChunk()
      throws Exception {
    // NUM_VALUES not divisible by DOCS_PER_CHUNK — tests partial final chunk handling
    int numDocs = 1500; // 1024 + 476
    File file = new File(TEST_FILE_PREFIX + "_partial");
    FileUtils.deleteQuietly(file);

    int[] expected = new int[numDocs];
    for (int i = 0; i < numDocs; i++) {
      expected[i] = i;
    }

    CodecPipelineExecutor executor = CodecPipelineExecutor.create("DELTA", DataType.INT);

    try (FixedByteChunkForwardIndexWriterV7 writer = new FixedByteChunkForwardIndexWriterV7(
        file, executor, numDocs, 1024, Integer.BYTES)) {
      for (int v : expected) {
        writer.putInt(v);
      }
    }

    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(file);
        FixedByteChunkSVForwardIndexReaderV7 reader = new FixedByteChunkSVForwardIndexReaderV7(
            buffer, DataType.INT);
        ChunkReaderContext ctx = reader.createContext()) {
      for (int i = 0; i < numDocs; i++) {
        assertEquals(reader.getInt(i, ctx), expected[i],
            "Mismatch at docId " + i);
      }
    }

    FileUtils.deleteQuietly(file);
  }

  /// DELTA encoding stores `cur - prev` into a fixed-width int/long. When the mathematical
  /// delta does not fit in the value's width (e.g. INT_MIN followed by INT_MAX → delta would be
  /// 2^32 - 1), encode wraps under two's-complement; decode wraps symmetrically and recovers the
  /// exact original sequence. This test locks in that round-trip property at the value boundaries
  /// so any future refactor that switches to checked arithmetic (e.g. Math.subtractExact) breaks
  /// loudly rather than silently corrupting data.
  @Test
  public void testDeltaIntBoundaryRoundTrip()
      throws Exception {
    File file = new File(TEST_FILE_PREFIX + "_intBoundary");
    FileUtils.deleteQuietly(file);

    int[] expected = {Integer.MIN_VALUE, Integer.MAX_VALUE, 0, -1, 1, Integer.MIN_VALUE, Integer.MAX_VALUE};

    CodecPipelineExecutor executor = CodecPipelineExecutor.create("DELTA", DataType.INT);

    try (FixedByteChunkForwardIndexWriterV7 writer = new FixedByteChunkForwardIndexWriterV7(
        file, executor, expected.length, 8, Integer.BYTES)) {
      for (int v : expected) {
        writer.putInt(v);
      }
    }

    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(file);
        FixedByteChunkSVForwardIndexReaderV7 reader = new FixedByteChunkSVForwardIndexReaderV7(
            buffer, DataType.INT);
        ChunkReaderContext ctx = reader.createContext()) {
      for (int i = 0; i < expected.length; i++) {
        assertEquals(reader.getInt(i, ctx), expected[i],
            "Boundary round-trip mismatch at docId " + i);
      }
    }

    FileUtils.deleteQuietly(file);
  }

  @Test
  public void testDeltaLongBoundaryRoundTrip()
      throws Exception {
    File file = new File(TEST_FILE_PREFIX + "_longBoundary");
    FileUtils.deleteQuietly(file);

    long[] expected = {Long.MIN_VALUE, Long.MAX_VALUE, 0L, -1L, 1L, Long.MIN_VALUE, Long.MAX_VALUE};

    CodecPipelineExecutor executor = CodecPipelineExecutor.create("DELTA", DataType.LONG);

    try (FixedByteChunkForwardIndexWriterV7 writer = new FixedByteChunkForwardIndexWriterV7(
        file, executor, expected.length, 8, Long.BYTES)) {
      for (long v : expected) {
        writer.putLong(v);
      }
    }

    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(file);
        FixedByteChunkSVForwardIndexReaderV7 reader = new FixedByteChunkSVForwardIndexReaderV7(
            buffer, DataType.LONG);
        ChunkReaderContext ctx = reader.createContext()) {
      for (int i = 0; i < expected.length; i++) {
        assertEquals(reader.getLong(i, ctx), expected[i],
            "Boundary round-trip mismatch at docId " + i);
      }
    }

    FileUtils.deleteQuietly(file);
  }

  @Test
  public void testDeltaDeltaIntBoundaryRoundTrip()
      throws Exception {
    File file = new File(TEST_FILE_PREFIX + "_ddIntBoundary");
    FileUtils.deleteQuietly(file);

    // DELTADELTA subtracts twice; exercise two's-complement wraparound at INT MIN/MAX where both the
    // delta and the delta-of-delta overflow.
    int[] expected = {Integer.MIN_VALUE, Integer.MAX_VALUE, Integer.MIN_VALUE, Integer.MAX_VALUE, 0, -1, 1};

    CodecPipelineExecutor executor = CodecPipelineExecutor.create("DELTADELTA", DataType.INT);

    try (FixedByteChunkForwardIndexWriterV7 writer = new FixedByteChunkForwardIndexWriterV7(
        file, executor, expected.length, 8, Integer.BYTES)) {
      for (int v : expected) {
        writer.putInt(v);
      }
    }

    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(file);
        FixedByteChunkSVForwardIndexReaderV7 reader = new FixedByteChunkSVForwardIndexReaderV7(
            buffer, DataType.INT);
        ChunkReaderContext ctx = reader.createContext()) {
      for (int i = 0; i < expected.length; i++) {
        assertEquals(reader.getInt(i, ctx), expected[i], "Boundary round-trip mismatch at docId " + i);
      }
    }

    FileUtils.deleteQuietly(file);
  }

  @Test
  public void testDeltaDeltaLongBoundaryRoundTrip()
      throws Exception {
    File file = new File(TEST_FILE_PREFIX + "_ddLongBoundary");
    FileUtils.deleteQuietly(file);

    long[] expected = {Long.MIN_VALUE, Long.MAX_VALUE, Long.MIN_VALUE, Long.MAX_VALUE, 0L, -1L, 1L};

    CodecPipelineExecutor executor = CodecPipelineExecutor.create("DELTADELTA", DataType.LONG);

    try (FixedByteChunkForwardIndexWriterV7 writer = new FixedByteChunkForwardIndexWriterV7(
        file, executor, expected.length, 8, Long.BYTES)) {
      for (long v : expected) {
        writer.putLong(v);
      }
    }

    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(file);
        FixedByteChunkSVForwardIndexReaderV7 reader = new FixedByteChunkSVForwardIndexReaderV7(
            buffer, DataType.LONG);
        ChunkReaderContext ctx = reader.createContext()) {
      for (int i = 0; i < expected.length; i++) {
        assertEquals(reader.getLong(i, ctx), expected[i], "Boundary round-trip mismatch at docId " + i);
      }
    }

    FileUtils.deleteQuietly(file);
  }

  /// Chain framework smoke test — the validator and executor accept and run a chain of value
  /// transforms, an optional packing transform, and zero or more compression stages. This test
  /// exercises one transform followed by one compression (the chained shapes are covered by the
  /// `specs` data provider, e.g. `DELTA,DELTADELTA,LZ4` and `DELTA,T64,LZ4`).
  @Test
  public void testTransformPlusCompressionChainRoundTrip()
      throws Exception {
    String spec = "DELTADELTA,LZ4";
    File file = new File(TEST_FILE_PREFIX + "_chain_LONG");
    FileUtils.deleteQuietly(file);

    long[] expected = new long[NUM_VALUES];
    Random random = new Random(RANDOM_SEED);
    long base = 1_700_000_000_000L;
    for (int i = 0; i < NUM_VALUES; i++) {
      // Monotonic-ish data with a small jitter — the kind of column where transforms help.
      expected[i] = base + i * 1000L + random.nextInt(50);
    }

    CodecPipelineExecutor executor = CodecPipelineExecutor.create(spec, DataType.LONG);

    try (FixedByteChunkForwardIndexWriterV7 writer = new FixedByteChunkForwardIndexWriterV7(
        file, executor, NUM_VALUES, DOCS_PER_CHUNK, Long.BYTES)) {
      for (long v : expected) {
        writer.putLong(v);
      }
    }

    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(file);
        FixedByteChunkSVForwardIndexReaderV7 reader = new FixedByteChunkSVForwardIndexReaderV7(
            buffer, DataType.LONG);
        ChunkReaderContext ctx = reader.createContext()) {
      assertEquals(reader.getCodecSpec(), spec, "Canonical spec must match for chained pipeline");
      for (int i = 0; i < NUM_VALUES; i++) {
        assertEquals(reader.getLong(i, ctx), expected[i],
            "Chained transform+compression round-trip mismatch at docId " + i);
      }
    }

    FileUtils.deleteQuietly(file);
  }

  @Test
  public void testWriterRejectsPipelineWhoseComposedBoundIsUnsafe() {
    String spec = String.join(",", Collections.nCopies(32, "SNAPPY"));
    CodecPipelineExecutor executor = CodecPipelineExecutor.create(spec, DataType.INT);
    File file = new File(TEST_FILE_PREFIX + "_unsafe_composed_bound");
    FileUtils.deleteQuietly(file);

    IllegalArgumentException error = expectThrows(IllegalArgumentException.class,
        () -> new FixedByteChunkForwardIndexWriterV7(file, executor, 0, 1 << 22, Integer.BYTES));
    assertTrue(error.getMessage().contains("cannot safely bound"), error.getMessage());
    assertFalse(file.exists(), "Writer must reject the pipeline before creating an output file");
  }

  @Test
  public void testWriterRejectsPipelineWhoseCumulativeWorkIsUnsafe() {
    String spec = String.join(",", Collections.nCopies(32, "DELTA"));
    CodecPipelineExecutor executor = CodecPipelineExecutor.create(spec, DataType.INT);
    File file = new File(TEST_FILE_PREFIX + "_unsafe_cumulative_work");
    FileUtils.deleteQuietly(file);

    IllegalArgumentException error = expectThrows(IllegalArgumentException.class,
        () -> new FixedByteChunkForwardIndexWriterV7(file, executor, 0, 1 << 24, Integer.BYTES));
    assertTrue(error.getMessage().contains("cumulative-work"), error.getMessage());
    assertFalse(file.exists(), "Writer must reject the pipeline before creating an output file");
  }

  @Test
  public void testWriterRejectsExecutorEntrySizeMismatchBeforeCreatingFile() {
    CodecPipelineExecutor executor = CodecPipelineExecutor.create("DELTA", DataType.LONG);
    File file = new File(TEST_FILE_PREFIX + "_executor_entry_size_mismatch");
    FileUtils.deleteQuietly(file);

    IllegalArgumentException error = expectThrows(IllegalArgumentException.class,
        () -> new FixedByteChunkForwardIndexWriterV7(file, executor, 0, 1, Integer.BYTES));
    assertTrue(error.getMessage().contains("does not match executor stored type LONG"), error.getMessage());
    assertFalse(file.exists(), "Writer must reject the type mismatch before creating an output file");
  }

  @Test
  public void testCreatorRejectsExecutorValueTypeMismatchBeforeCreatingFile()
      throws Exception {
    CodecPipelineExecutor executor = CodecPipelineExecutor.create("DELTA", DataType.INT);
    File indexDir = new File(TEST_FILE_PREFIX + "_creator_type_mismatch");
    FileUtils.deleteQuietly(indexDir);
    assertTrue(indexDir.mkdirs(), "Failed to create test index directory");
    try {
      IllegalArgumentException error = expectThrows(IllegalArgumentException.class,
          () -> new SingleValueFixedByteRawIndexCreator(indexDir, "mismatched", 0, DataType.LONG, 1, executor));
      assertTrue(error.getMessage().contains("does not match codec executor stored type INT"), error.getMessage());
      assertEquals(indexDir.list().length, 0, "Creator must reject the type mismatch before creating an index file");
    } finally {
      FileUtils.deleteQuietly(indexDir);
    }
  }

  @Test
  public void testWriterRejectsWrongValueMethodBeforeMutatingState()
      throws Exception {
    File intFile = new File(TEST_FILE_PREFIX + "_wrong_put_long");
    File longFile = new File(TEST_FILE_PREFIX + "_wrong_put_int");
    FileUtils.deleteQuietly(intFile);
    FileUtils.deleteQuietly(longFile);
    try {
      CodecPipelineExecutor intExecutor = CodecPipelineExecutor.create("DELTA", DataType.INT);
      try (FixedByteChunkForwardIndexWriterV7 writer = new FixedByteChunkForwardIndexWriterV7(
          intFile, intExecutor, 1, 2, Integer.BYTES)) {
        IllegalStateException error = expectThrows(IllegalStateException.class, () -> writer.putLong(42L));
        assertTrue(error.getMessage().contains("putLong cannot write an INT"), error.getMessage());
        writer.putInt(42);
      }
      try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(intFile);
          FixedByteChunkSVForwardIndexReaderV7 reader =
              new FixedByteChunkSVForwardIndexReaderV7(buffer, DataType.INT, 1);
          ChunkReaderContext context = reader.createContext()) {
        assertEquals(reader.getInt(0, context), 42);
      }

      CodecPipelineExecutor longExecutor = CodecPipelineExecutor.create("DELTA", DataType.LONG);
      try (FixedByteChunkForwardIndexWriterV7 writer = new FixedByteChunkForwardIndexWriterV7(
          longFile, longExecutor, 1, 2, Long.BYTES)) {
        IllegalStateException error = expectThrows(IllegalStateException.class, () -> writer.putInt(42));
        assertTrue(error.getMessage().contains("putInt cannot write a LONG"), error.getMessage());
        writer.putLong(42L);
      }
      try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(longFile);
          FixedByteChunkSVForwardIndexReaderV7 reader =
              new FixedByteChunkSVForwardIndexReaderV7(buffer, DataType.LONG, 1);
          ChunkReaderContext context = reader.createContext()) {
        assertEquals(reader.getLong(0, context), 42L);
      }
    } finally {
      FileUtils.deleteQuietly(intFile);
      FileUtils.deleteQuietly(longFile);
    }
  }

  @Test
  public void testWriterTruncatesExistingFile()
      throws Exception {
    File file = new File(TEST_FILE_PREFIX + "_truncate_existing");
    FileUtils.deleteQuietly(file);
    try (RandomAccessFile existing = new RandomAccessFile(file, "rw")) {
      existing.setLength(1_000_000L);
    }

    CodecPipelineExecutor executor = CodecPipelineExecutor.create("DELTA", DataType.INT);
    try (FixedByteChunkForwardIndexWriterV7 writer = new FixedByteChunkForwardIndexWriterV7(
        file, executor, 1, 1, Integer.BYTES)) {
      writer.putInt(42);
    }

    assertTrue(file.length() < 1_000_000L, "Writer must remove bytes left by an older, longer file");
    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(file);
        FixedByteChunkSVForwardIndexReaderV7 reader = new FixedByteChunkSVForwardIndexReaderV7(buffer, DataType.INT);
        ChunkReaderContext context = reader.createContext()) {
      assertEquals(reader.getInt(0, context), 42);
    }
    FileUtils.deleteQuietly(file);
  }
}
