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
import java.util.SplittableRandom;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.io.codec.CodecPipelineExecutor;
import org.apache.pinot.segment.local.io.writer.impl.FixedByteChunkForwardIndexWriter;
import org.apache.pinot.segment.local.io.writer.impl.FixedByteChunkForwardIndexWriterV7;
import org.apache.pinot.segment.local.io.writer.impl.FixedByteChunkWriter;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;


/// Measures the segment-creation (ingestion) write path of the V7 codec-pipeline raw forward index
/// against the legacy [FixedByteChunkForwardIndexWriter] baseline.
///
/// Each invocation writes [#RECORDS] monotonically increasing values through the default ~1024-doc
/// chunking, so a single op spans roughly a thousand chunk encodes and exercises the steady-state
/// per-chunk cost rather than one-off setup. Pipelines cover a compression-only spec, a two-stage
/// transform+compression chain and a three-stage chain, for both INT and LONG.
///
/// The point of comparison is per-chunk buffer churn: the legacy writer reuses one compression
/// buffer for the life of the writer, and the V7 writer reuses a single per-writer
/// `CodecPipelineExecutor.EncodeScratch` workspace instead of allocating and explicitly cleaning a
/// direct buffer per codec stage per chunk. Run with `-prof gc` to observe that:
/// `gc.alloc.rate.norm` counts the `DirectByteBuffer` wrappers and their cleaner registrations, so
/// a regression that reintroduces per-stage allocation shows up as allocated bytes per op scaling
/// with chunk count times stage count.
///
/// Per-writer buffer lifecycle is not symmetric between the two arms: the legacy writer leaves its
/// header, chunk and compression buffers to the GC, while V7 cleans its buffers and scratch at
/// close. Only the slope of allocated bytes against chunk count within one arm is meaningful; the
/// absolute cross-arm delta carries a constant per-op offset and should not be read as churn.
///
/// Build and run:
///
/// ```
/// ./mvnw install -DskipTests -pl pinot-perf -am
/// java -jar pinot-perf/target/benchmarks.jar BenchmarkV7ForwardIndexWriter -prof gc
/// ```
///
/// The shaded `benchmarks.jar` comes from the default `build-shaded-jar` profile; under
/// `-Ppinot-fastdev` that profile is off, so run [#main] off the module classpath instead. Trial
/// teardown prints the encoded bytes per row for the last file the trial wrote, tagged with the
/// writer that produced it, which gives the compression ratio alongside the throughput numbers.
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 2, time = 2)
@Measurement(iterations = 3, time = 3)
public class BenchmarkV7ForwardIndexWriter {

  /// Enough rows to span ~1000 chunks at the default chunk size, matching the per-million-row
  /// allocation accounting raised in review.
  private static final int RECORDS = 1_000_000;

  /// Pinot's default target docs per chunk; both writers round this up to 1024.
  private static final int TARGET_DOCS_PER_CHUNK = 1000;

  /// Terminal compressor shared by every benchmarked pipeline, so the legacy baseline compresses
  /// the same way the V7 pipelines finish. The legacy score therefore does not vary with
  /// [#_codecSpec] and the repeated rows double as a noise estimate.
  private static final ChunkCompressionType LEGACY_COMPRESSION = ChunkCompressionType.LZ4;

  /// The legacy arm ignores [#_codecSpec] entirely, so its output is labelled with the compressor
  /// it actually used rather than the pipeline spec of the surrounding parameter combination.
  private static final String LEGACY_LABEL = "legacy " + LEGACY_COMPRESSION + " (codecSpec ignored)";

  @Param({"INT", "LONG"})
  DataType _dataType;

  @Param({"LZ4", "DELTA,LZ4", "DELTA,T64,LZ4"})
  String _codecSpec;

  private long[] _values;
  private CodecPipelineExecutor _executor;
  private File _targetDir;
  private File _file;
  private long _lastEncodedBytes;
  private String _lastWriter;

  @Setup(Level.Trial)
  public void setup()
      throws IOException {
    // Per-JVM directory: a concurrent run of this class (e.g. baseline vs change) must not delete
    // the other run's in-flight file from its own trial teardown.
    _targetDir = new File(FileUtils.getTempDirectory(), "BenchmarkV7ForwardIndexWriter-" + UUID.randomUUID());
    FileUtils.forceMkdir(_targetDir);
    _file = new File(_targetDir, "forward-index");
    _executor = CodecPipelineExecutor.create(_codecSpec, _dataType);
    // Slowly increasing values (epoch-second-like), the shape DELTA/T64 are meant for, and small
    // enough that the INT and LONG runs encode the same logical sequence.
    SplittableRandom random = new SplittableRandom(42);
    _values = new long[RECORDS];
    long value = 1_600_000_000L;
    for (int i = 0; i < RECORDS; i++) {
      value += random.nextInt(64);
      _values[i] = value;
    }
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    System.out.printf("%n[%s %s] %.3f encoded bytes/row (%d raw)%n", _dataType, _lastWriter,
        (double) _lastEncodedBytes / RECORDS, _dataType.size());
    FileUtils.deleteQuietly(_targetDir);
  }

  /// Sizes the written index outside the measured region, then clears it so the next iteration
  /// starts from an empty file.
  @TearDown(Level.Iteration)
  public void measureAndDeleteFile() {
    if (_file.exists()) {
      _lastEncodedBytes = _file.length();
    }
    FileUtils.deleteQuietly(_file);
  }

  @Benchmark
  public int writeV7()
      throws IOException {
    _lastWriter = _codecSpec;
    try (FixedByteChunkForwardIndexWriterV7 writer = new FixedByteChunkForwardIndexWriterV7(_file, _executor, RECORDS,
        TARGET_DOCS_PER_CHUNK, _dataType.size())) {
      writeAll(writer);
    }
    return RECORDS;
  }

  @Benchmark
  public int writeLegacy()
      throws IOException {
    _lastWriter = LEGACY_LABEL;
    try (FixedByteChunkForwardIndexWriter writer = new FixedByteChunkForwardIndexWriter(_file, LEGACY_COMPRESSION,
        RECORDS, TARGET_DOCS_PER_CHUNK, _dataType.size(), 4)) {
      writeAll(writer);
    }
    return RECORDS;
  }

  private void writeAll(FixedByteChunkWriter writer) {
    if (_dataType == DataType.INT) {
      for (int i = 0; i < RECORDS; i++) {
        writer.putInt((int) _values[i]);
      }
    } else {
      for (int i = 0; i < RECORDS; i++) {
        writer.putLong(_values[i]);
      }
    }
  }

  public static void main(String[] args)
      throws Exception {
    new Runner(new OptionsBuilder().include(BenchmarkV7ForwardIndexWriter.class.getSimpleName()).build()).run();
  }
}
