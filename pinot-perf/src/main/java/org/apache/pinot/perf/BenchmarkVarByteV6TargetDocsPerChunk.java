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
package org.apache.pinot.perf;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.io.writer.impl.VarByteChunkForwardIndexWriterV6;
import org.apache.pinot.segment.local.segment.index.readers.forward.VarByteChunkForwardIndexReaderV4;
import org.apache.pinot.segment.local.segment.index.readers.forward.VarByteChunkForwardIndexReaderV6;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;


/// Measures the storage-size / access-latency tradeoff of the `targetDocsPerChunk` cap added to
/// [VarByteChunkForwardIndexWriterV6].
///
/// The cap flushes a chunk once it holds N documents even when the `chunkSize` byte budget is not
/// exhausted. Smaller chunks shrink the compressor's dedup window (worse ratio, bigger index) but
/// make a random point lookup decompress less data (lower latency). This benchmark sweeps the cap
/// and reports both sides.
///
/// Storage size is not a JMH metric — it is recorded during trial setup and dumped by
/// [#printSizes()] at JVM exit.
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
@Warmup(iterations = 2, time = 2)
@Measurement(iterations = 3, time = 3)
@State(Scope.Benchmark)
public class BenchmarkVarByteV6TargetDocsPerChunk {

  private static final File TARGET_DIR = new File(FileUtils.getTempDirectory(), "BenchmarkV6TargetDocsPerChunk");
  /// Collected across all forked trials of this JVM, keyed by the parameter combination.
  private static final Map<String, long[]> SIZES = new TreeMap<>();
  private static final int NUM_RANDOM_LOOKUPS = 2_000;

  static {
    Runtime.getRuntime().addShutdownHook(new Thread(BenchmarkVarByteV6TargetDocsPerChunk::printSizes));
  }

  @Param("1000000")
  int _numDocs;

  /// Shape of the column being written. See [#generate()].
  @Param({"LOW_CARD_URL", "HIGH_CARD_UUID", "JSON_LOG", "SKEWED_LOG"})
  String _dataset;

  @Param({"ZSTANDARD", "LZ4", "SNAPPY"})
  ChunkCompressionType _compression;

  /// Chunk byte budget. 1MB is the Pinot default `targetMaxChunkSize`.
  @Param("1048576")
  int _chunkSize;

  /// -1 disables the cap (current behavior: byte-driven flush only).
  @Param({"-1", "10", "50", "100", "250", "500", "1000", "5000", "20000"})
  int _targetDocsPerChunk;

  private byte[][] _values;
  private File _file;
  private PinotDataBuffer _buffer;
  private VarByteChunkForwardIndexReaderV6 _reader;
  private int[] _randomDocIds;
  private int[] _selectiveDocIds;

  @Setup(Level.Trial)
  public void setup()
      throws IOException {
    FileUtils.forceMkdir(TARGET_DIR);
    _values = generate();
    _file = new File(TARGET_DIR, UUID.randomUUID().toString());
    try (VarByteChunkForwardIndexWriterV6 writer = new VarByteChunkForwardIndexWriterV6(_file, _compression,
        _chunkSize, _targetDocsPerChunk)) {
      for (byte[] value : _values) {
        writer.putBytes(value);
      }
    }
    long rawBytes = 0;
    for (byte[] value : _values) {
      rawBytes += value.length;
    }
    synchronized (SIZES) {
      SIZES.put(String.format("%-14s %-10s docsPerChunk=%-6d", _dataset, _compression, _targetDocsPerChunk),
          new long[]{_file.length(), rawBytes});
    }

    _buffer = PinotDataBuffer.loadBigEndianFile(_file);
    _reader = new VarByteChunkForwardIndexReaderV6(_buffer, FieldSpec.DataType.BYTES, true);

    // Fixed seed so every parameter combination probes the same doc ids.
    Random random = new Random(7);
    _randomDocIds = new int[NUM_RANDOM_LOOKUPS];
    for (int i = 0; i < NUM_RANDOM_LOOKUPS; i++) {
      _randomDocIds[i] = random.nextInt(_numDocs);
    }
    _selectiveDocIds = new int[_numDocs / 100];
    for (int i = 0; i < _selectiveDocIds.length; i++) {
      _selectiveDocIds[i] = i * 100 + random.nextInt(100);
    }
  }

  @TearDown(Level.Trial)
  public void tearDown()
      throws IOException {
    if (_reader != null) {
      _reader.close();
    }
    if (_buffer != null) {
      _buffer.close();
    }
    FileUtils.deleteQuietly(_file);
  }

  /// Full sequential scan — every chunk is decompressed exactly once, so this isolates the
  /// per-chunk overhead paid on a full-column scan.
  @Benchmark
  public void sequentialScan(Blackhole bh)
      throws IOException {
    try (VarByteChunkForwardIndexReaderV4.ReaderContext context = _reader.createContext()) {
      for (int docId = 0; docId < _numDocs; docId++) {
        bh.consume(_reader.getBytes(docId, context));
      }
    }
  }

  /// Uniformly random point lookups — the context caches only the last chunk, so nearly every
  /// lookup decompresses a whole chunk. This is where a smaller chunk pays off.
  @Benchmark
  public void randomAccess(Blackhole bh)
      throws IOException {
    try (VarByteChunkForwardIndexReaderV4.ReaderContext context = _reader.createContext()) {
      for (int docId : _randomDocIds) {
        bh.consume(_reader.getBytes(docId, context));
      }
    }
  }

  /// Ascending doc ids at 1% selectivity — the realistic filtered-query pattern, where matches are
  /// spread thinly but read in order. Sits between the two extremes above.
  @Benchmark
  public void selectiveAscendingScan(Blackhole bh)
      throws IOException {
    try (VarByteChunkForwardIndexReaderV4.ReaderContext context = _reader.createContext()) {
      for (int docId : _selectiveDocIds) {
        bh.consume(_reader.getBytes(docId, context));
      }
    }
  }

  private byte[][] generate() {
    Random random = new Random(42);
    byte[][] values = new byte[_numDocs][];
    switch (_dataset) {
      // Highly repetitive short strings: the case the cap is meant to help/hurt most.
      case "LOW_CARD_URL": {
        String[] dictionary = new String[200];
        for (int i = 0; i < dictionary.length; i++) {
          dictionary[i] = "https://www.example.com/catalog/category/" + i + "/item?ref=homepage&session=abc";
        }
        for (int i = 0; i < _numDocs; i++) {
          values[i] = dictionary[random.nextInt(dictionary.length)].getBytes(StandardCharsets.UTF_8);
        }
        break;
      }
      // Effectively unique values: almost nothing for the compressor to dedup.
      case "HIGH_CARD_UUID": {
        for (int i = 0; i < _numDocs; i++) {
          values[i] = new UUID(random.nextLong(), random.nextLong()).toString().getBytes(StandardCharsets.UTF_8);
        }
        break;
      }
      // Structurally repetitive but individually unique — the common real-world raw-string column.
      case "JSON_LOG": {
        String[] services = {"checkout", "search", "cart", "auth", "recommendation"};
        String[] levels = {"INFO", "WARN", "ERROR", "DEBUG"};
        for (int i = 0; i < _numDocs; i++) {
          String json = "{\"ts\":" + (1700000000000L + i * 37L) + ",\"service\":\"" + services[random.nextInt(
              services.length)] + "\",\"level\":\"" + levels[random.nextInt(levels.length)] + "\",\"latencyMs\":"
              + random.nextInt(5000) + ",\"userId\":\"" + new UUID(random.nextLong(), random.nextLong())
              + "\",\"message\":\"request completed successfully\"}";
          values[i] = json.getBytes(StandardCharsets.UTF_8);
        }
        break;
      }
      // Short values with a handful of very large outliers. `maxLength` is then wildly
      // unrepresentative of the average, which is what breaks the byte-derived docs-per-chunk path.
      case "SKEWED_LOG": {
        String[] services = {"checkout", "search", "cart", "auth", "recommendation"};
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < _numDocs; i++) {
          if (random.nextInt(1000) == 0) {
            sb.setLength(0);
            // A stack trace / payload dump: ~50KB.
            while (sb.length() < 50_000) {
              sb.append("\tat org.apache.pinot.core.operator.filter.FilterOperator.getNextBlock(")
                  .append(services[random.nextInt(services.length)]).append(".java:").append(random.nextInt(2000))
                  .append(")\n");
            }
            values[i] = sb.toString().getBytes(StandardCharsets.UTF_8);
          } else {
            values[i] = ("service=" + services[random.nextInt(services.length)] + " status=200 latencyMs="
                + random.nextInt(5000) + " region=us-east-1").getBytes(StandardCharsets.UTF_8);
          }
        }
        break;
      }
      default:
        throw new IllegalArgumentException("Unknown dataset: " + _dataset);
    }
    return values;
  }

  private static void printSizes() {
    synchronized (SIZES) {
      if (SIZES.isEmpty()) {
        return;
      }
      System.out.println();
      System.out.println("=== INDEX SIZE (targetDocsPerChunk sweep) ===");
      System.out.printf("%-46s %14s %14s %8s%n", "config", "indexBytes", "rawBytes", "ratio");
      for (Map.Entry<String, long[]> entry : SIZES.entrySet()) {
        long indexBytes = entry.getValue()[0];
        long rawBytes = entry.getValue()[1];
        System.out.printf("%-46s %14d %14d %8.2f%n", entry.getKey(), indexBytes, rawBytes,
            (double) rawBytes / indexBytes);
      }
      System.out.println("=== END INDEX SIZE ===");
    }
  }

  /// Writes the index once per configuration and reports on-disk size plus the actual chunk
  /// geometry. Much cheaper than a full JMH run, so it can cover the whole matrix.
  ///
  /// Two ways of bounding a chunk by document count are compared at the same nominal N:
  /// - `byteDerived`: today's table-config path — `chunkSize = getDynamicTargetChunkSize(maxLength, N, 1MB)`,
  ///   no hard cap. Docs per chunk only *approximates* N, and undershoots badly when
  ///   `maxLength` exceeds the average value length.
  /// - `hardCap`: this PR — `chunkSize = 1MB` with `targetDocsPerChunk = N`, giving exactly N docs per chunk.
  private static void sizeSweep()
      throws IOException {
    FileUtils.forceMkdir(TARGET_DIR);
    int numDocs = 1_000_000;
    System.out.printf("%-14s %-10s %-11s %7s %11s %10s %10s %12s %8s%n", "dataset", "codec", "mode", "N", "chunkBytes",
        "numChunks", "docsPerChk", "indexBytes", "ratio");
    for (String dataset : new String[]{"LOW_CARD_URL", "HIGH_CARD_UUID", "JSON_LOG", "SKEWED_LOG"}) {
      BenchmarkVarByteV6TargetDocsPerChunk gen = new BenchmarkVarByteV6TargetDocsPerChunk();
      gen._numDocs = numDocs;
      gen._dataset = dataset;
      byte[][] values = gen.generate();
      int maxLength = 0;
      long rawBytes = 0;
      for (byte[] value : values) {
        maxLength = Math.max(maxLength, value.length);
        rawBytes += value.length;
      }
      for (ChunkCompressionType codec : new ChunkCompressionType[]{
          ChunkCompressionType.ZSTANDARD, ChunkCompressionType.LZ4, ChunkCompressionType.SNAPPY
      }) {
        for (int n : new int[]{10, 50, 100, 250, 500, 1000, 5000, 20000, -1}) {
          // hardCap mode: full 1MB byte budget, chunk closed by doc count.
          write(dataset, codec, "hardCap", n, 1024 * 1024, n, values, rawBytes, numDocs);
          if (n > 0) {
            // byteDerived mode: today's behavior — N only influences the byte budget, via
            // ForwardIndexUtils#getDynamicTargetChunkSize.
            int chunkSize = Math.max((int) Math.min((long) maxLength * n, 1024 * 1024), 4096);
            write(dataset, codec, "byteDerived", n, chunkSize, DISABLED, values, rawBytes, numDocs);
          }
        }
      }
    }
    FileUtils.deleteQuietly(TARGET_DIR);
  }

  private static final int DISABLED = VarByteChunkForwardIndexWriterV6.DISABLE_DOCS_PER_CHUNK;

  private static void write(String dataset, ChunkCompressionType codec, String mode, int n, int chunkSize, int cap,
      byte[][] values, long rawBytes, int numDocs)
      throws IOException {
    File file = new File(TARGET_DIR, UUID.randomUUID().toString());
    try (VarByteChunkForwardIndexWriterV6 writer = new VarByteChunkForwardIndexWriterV6(file, codec, chunkSize, cap)) {
      for (byte[] value : values) {
        writer.putBytes(value);
      }
    }
    long indexBytes = file.length();
    int numChunks = countChunks(file);
    System.out.printf("%-14s %-10s %-11s %7d %11d %10d %10d %12d %8.2f%n", dataset, codec, mode, n, chunkSize,
        numChunks, numDocs / Math.max(numChunks, 1), indexBytes, (double) rawBytes / indexBytes);
    FileUtils.deleteQuietly(file);
  }

  /// Recovers the chunk count from the V4-family file header, which is
  /// `[version][targetChunkSize][compressionType][chunksStartOffset]` followed by an
  /// 8-byte `[firstDocId][chunkStartOffset]` entry per chunk.
  private static int countChunks(File file)
      throws IOException {
    try (PinotDataBuffer buffer = PinotDataBuffer.loadBigEndianFile(file)) {
      int chunksStartOffset = buffer.getInt(3 * Integer.BYTES);
      return (chunksStartOffset - 4 * Integer.BYTES) / (2 * Integer.BYTES);
    }
  }

  public static void main(String[] args)
      throws RunnerException, IOException {
    if (args.length > 0 && args[0].equals("sizes")) {
      sizeSweep();
      return;
    }
    new Runner(new OptionsBuilder().include(BenchmarkVarByteV6TargetDocsPerChunk.class.getSimpleName()).build()).run();
  }
}
