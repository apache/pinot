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
import java.nio.file.Files;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.segment.local.io.writer.impl.VarByteChunkForwardIndexWriterV4;
import org.apache.pinot.segment.local.realtime.impl.forward.VarByteSVMutableForwardIndex;
import org.apache.pinot.segment.local.segment.index.readers.forward.VarByteChunkForwardIndexReaderV4;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.MapUtils;
import org.apache.pinot.spi.utils.MapUtils.PreparedMapKey;
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
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.CommandLineOptions;
import org.openjdk.jmh.runner.options.OptionsBuilder;


/// Measures selective MAP key access against the current full-map read path.
///
/// Keys are fixed-length and share a common prefix, modelling dotted OpenTelemetry-style attribute names
/// (`k8s.workload.name`, `k8s.namespace.name`, ...). That is the honest case for a scanning extractor: every entry
/// clears the key-length check, so the key bytes are actually compared rather than skipped on a length mismatch.
/// Each benchmark thread owns its forward index and off-heap memory manager.
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(value = 2)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Thread)
public class BenchmarkMapKeyAccess {

  public static void main(String[] args)
      throws Exception {
    // Inherit the command line so `-p`, `-prof`, `-f` and friends take effect. Without the parent options they are
    // parsed and then silently dropped, and the run quietly falls back to the annotated defaults.
    ChainedOptionsBuilder opt = new OptionsBuilder().parent(new CommandLineOptions(args))
        .include(BenchmarkMapKeyAccess.class.getSimpleName());
    new Runner(opt.build()).run();
  }

  @Param({"4", "16", "64"})
  private int _numEntries;

  @Param({"first", "last"})
  private String _targetPosition;

  /// `flat` models scalar string attribute maps (the common ingestion shape); `numeric` models a STRING-valued MAP
  /// carrying integer entries, which `MapFieldTypeMixedValueIngestingIntegrationTest` shows is supported and
  /// projected through the same accessor; `nested` models object-valued entries, the shape that still has to go
  /// through Jackson.
  @Param({"flat", "numeric", "nested"})
  private String _valueShape;

  private String _targetKey;
  private PreparedMapKey _targetMapKey;
  private PinotDataBufferMemoryManager _memoryManager;
  private VarByteSVMutableForwardIndex _forwardIndex;
  private File _sealedIndexDir;
  private PinotDataBuffer _sealedDataBuffer;
  private VarByteChunkForwardIndexReaderV4 _sealedForwardIndex;
  private VarByteChunkForwardIndexReaderV4.ReaderContext _sealedContext;

  @Setup(Level.Trial)
  public void setUp()
      throws IOException {
    Map<String, Object> map = new LinkedHashMap<>();
    for (int i = 0; i < _numEntries; i++) {
      String value = "value-with-enough-bytes-to-exercise-json-parsing-" + i;
      Object stored;
      switch (_valueShape) {
        // A STRING-valued MAP carrying numeric entries, the shape
        // MapFieldTypeMixedValueIngestingIntegrationTest ingests.
        case "numeric":
          stored = 9007199254740990L + i;
          break;
        case "nested":
          stored = Map.of("value", value);
          break;
        default:
          stored = value;
          break;
      }
      map.put(key(i), stored);
    }
    _targetKey = key("first".equals(_targetPosition) ? 0 : _numEntries - 1);
    _targetMapKey = new PreparedMapKey(_targetKey);
    byte[] serialized = MapUtils.serializeMap(map, false);
    _memoryManager = new DirectMemoryManager(BenchmarkMapKeyAccess.class.getSimpleName());
    _forwardIndex =
        new VarByteSVMutableForwardIndex(DataType.MAP, _memoryManager, "mapColumn", 1, serialized.length);
    _forwardIndex.setBytes(0, serialized);

    // The sealed counterpart of the same frame. A completed segment stores the MAP column in a chunked raw forward
    // index, so key access there goes through a different reader than the consuming path above.
    _sealedIndexDir = Files.createTempDirectory(BenchmarkMapKeyAccess.class.getSimpleName()).toFile();
    File indexFile = new File(_sealedIndexDir, "map.fwd");
    try (VarByteChunkForwardIndexWriterV4 writer = new VarByteChunkForwardIndexWriterV4(indexFile,
        ChunkCompressionType.LZ4, Math.max(1024, serialized.length * 2))) {
      writer.putBytes(serialized);
    }
    _sealedDataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
    _sealedForwardIndex = new VarByteChunkForwardIndexReaderV4(_sealedDataBuffer, DataType.MAP, true);
    _sealedContext = _sealedForwardIndex.createContext();
  }

  @TearDown(Level.Trial)
  public void tearDown()
      throws IOException {
    try {
      _sealedContext.close();
      _sealedForwardIndex.close();
      _sealedDataBuffer.close();
      _forwardIndex.close();
    } finally {
      try {
        _memoryManager.close();
      } finally {
        FileUtils.deleteQuietly(_sealedIndexDir);
      }
    }
  }

  private static String key(int i) {
    return String.format("k8s.attribute.%03d.name", i);
  }

  /// Runs the existing consuming path: copy the complete off-heap value, deserialize every MAP entry, then select the
  /// requested key.
  @Benchmark
  public Object fullMapValue() {
    return _forwardIndex.getMap(0, null).get(_targetKey);
  }

  /// Runs the optimized consuming path, including store lookup and creation of the read-only direct view.
  @Benchmark
  public Object selectiveMapValue() {
    return _forwardIndex.getMapEntryValue(0, null, _targetMapKey);
  }

  /// The string baseline: what `MapKeyIndexReader#getString` did - deserialize to an object, then `toString()` it.
  @Benchmark
  public Object selectiveMapValueToString() {
    Object value = _forwardIndex.getMapEntryValue(0, null, _targetMapKey);
    return value == null ? null : value.toString();
  }

  /// Same scan, but decoding the value without handing it to Jackson - what a `STRING`-valued MAP column resolves
  /// to when projected. Plain strings, canonical integers and booleans take that path; object and array values, and
  /// non-integral numbers, still fall back, so `nested` should land on top of [#selectiveMapValueToString] rather
  /// than beating it.
  @Benchmark
  public Object selectiveMapValueAsString() {
    return _forwardIndex.getMapEntryValueAsString(0, null, _targetMapKey);
  }

  /// The selective object lookup against a completed segment's chunked forward index.
  @Benchmark
  public Object sealedSelectiveMapValue() {
    return _sealedForwardIndex.getMapEntryValue(0, _sealedContext, _targetMapKey);
  }

  /// The selective string lookup against a completed segment's chunked forward index.
  @Benchmark
  public Object sealedSelectiveMapValueAsString() {
    return _sealedForwardIndex.getMapEntryValueAsString(0, _sealedContext, _targetMapKey);
  }

  /// The sealed object baseline: deserialize every entry of the chunk value, then select the requested key.
  @Benchmark
  public Object sealedFullMapValue() {
    return _sealedForwardIndex.getMap(0, _sealedContext).get(_targetKey);
  }

  /// The sealed string baseline: deserialize every entry, select the key, then apply the default accessor's
  /// null-safe `toString()` conversion.
  @Benchmark
  public Object sealedFullMapValueAsString() {
    Object value = _sealedForwardIndex.getMap(0, _sealedContext).get(_targetKey);
    return value == null ? null : value.toString();
  }
}
