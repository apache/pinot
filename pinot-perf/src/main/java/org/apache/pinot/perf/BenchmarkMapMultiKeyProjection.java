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
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
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


/// Measures projecting several MAP keys over a block of documents, which is the shape a
/// `SELECT attributes['a'], attributes['b'], ... FROM t` query resolves to.
///
/// Each projected key is its own column, so the engine reads them one at a time and the frame is walked once per
/// key. The `single` benchmarks model that; the `combined` ones walk each frame once and pull every key out of it.
///
/// The map is the real OpenTelemetry attribute shape from the reported workload - 30 dotted keys of varied length -
/// and the projected keys are the four that workload groups by, sitting at different depths in the key-sorted frame.
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(value = 2)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Thread)
public class BenchmarkMapMultiKeyProjection {

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt = new OptionsBuilder().parent(new CommandLineOptions(args))
        .include(BenchmarkMapMultiKeyProjection.class.getSimpleName());
    new Runner(opt.build()).run();
  }

  /// Documents per projected block. The engine hands the readers up to 10k doc ids at a time.
  private static final int NUM_DOCS = 1024;

  private static final String[] PROJECTED_KEYS = {
      "k8s.workload.name", "k8s.namespace.name", "k8s.cluster.name", "k8s.workload.kind"
  };

  @Param({"1", "2", "4"})
  private int _numProjectedKeys;

  private PreparedMapKey[] _keys;
  private String[] _values;
  private PinotDataBufferMemoryManager _memoryManager;
  private VarByteSVMutableForwardIndex _consumingIndex;
  private File _sealedIndexDir;
  private PinotDataBuffer _sealedDataBuffer;
  private VarByteChunkForwardIndexReaderV4 _sealedIndex;
  private VarByteChunkForwardIndexReaderV4.ReaderContext _sealedContext;

  @Setup(Level.Trial)
  public void setUp()
      throws IOException {
    _keys = new PreparedMapKey[_numProjectedKeys];
    for (int i = 0; i < _numProjectedKeys; i++) {
      _keys[i] = new PreparedMapKey(PROJECTED_KEYS[i]);
    }
    _values = new String[_numProjectedKeys];

    byte[][] frames = new byte[NUM_DOCS][];
    int longestFrame = 0;
    for (int i = 0; i < NUM_DOCS; i++) {
      frames[i] = MapUtils.serializeMap(attributes(i));
      longestFrame = Math.max(longestFrame, frames[i].length);
    }

    _memoryManager = new DirectMemoryManager(BenchmarkMapMultiKeyProjection.class.getSimpleName());
    _consumingIndex =
        new VarByteSVMutableForwardIndex(DataType.MAP, _memoryManager, "attributes", NUM_DOCS, longestFrame);
    for (int i = 0; i < NUM_DOCS; i++) {
      _consumingIndex.setBytes(i, frames[i]);
    }

    _sealedIndexDir = Files.createTempDirectory(BenchmarkMapMultiKeyProjection.class.getSimpleName()).toFile();
    File indexFile = new File(_sealedIndexDir, "attributes.fwd");
    try (VarByteChunkForwardIndexWriterV4 writer =
        new VarByteChunkForwardIndexWriterV4(indexFile, ChunkCompressionType.LZ4, 8192)) {
      for (byte[] frame : frames) {
        writer.putBytes(frame);
      }
    }
    _sealedDataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
    _sealedIndex = new VarByteChunkForwardIndexReaderV4(_sealedDataBuffer, DataType.MAP, true);
    _sealedContext = _sealedIndex.createContext();
  }

  /// The reported attribute map: 30 dotted OpenTelemetry keys, values a mix of names, ids and numbers.
  private static Map<String, Object> attributes(int doc) {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("k8s.namespace.name", "default");
    map.put("k8s.cluster.name", "bits-prod-cluster");
    map.put("host.image.id", "ami-09f21c37d0373f7c8");
    map.put("host_kernel_release", "5.10.245-245.983.amzn2.x86_64");
    map.put("k8s.pod.uid", "63e96190-3166-448a-a05e-d2592383f81e");
    map.put("host_physical_cpus", 2);
    map.put("os.type", "linux");
    map.put("AWSUniqueId", "i-0401c8fe8e1cd719c_us-west-1_906383545488");
    map.put("host.type", "t3a.xlarge");
    map.put("cloud.availability_zone", "us-west-1a");
    map.put("host_mem_total", 16238984);
    map.put("metric_source", "kubernetes");
    map.put("k8s.pod.name", "paymentservice-779dff4596-" + doc);
    map.put("host_cpu_cores", 2);
    map.put("host_logical_cpus", 4);
    map.put("host_kernel_name", "linux");
    map.put("host.name", "ip-172-31-60-154.us-west-1.compute.internal");
    map.put("cloud.platform", "aws_eks");
    map.put("host.id", "i-0401c8fe8e1cd719c");
    map.put("deployment.environment.name", "bits-prod-cluster-cldprpt");
    map.put("host_kernel_version", "#1 SMP Wed Dec 3 00:02:10 UTC 2025");
    map.put("host_processor", "x86_64");
    map.put("cloud.region", "us-west-1");
    map.put("receiver", "k8scluster");
    map.put("host_machine", "x86_64");
    map.put("k8s.pod.qos_class", "Burstable");
    map.put("host_cpu_model", "AMD EPYC 7571");
    map.put("cloud.provider", "aws");
    map.put("k8s.node.name", "ip-172-31-61-78.us-west-1.compute.internal");
    map.put("cloud.account.id", "906383545488");
    map.put("k8s.workload.name", "paymentservice-" + doc % 32);
    map.put("k8s.workload.kind", "Deployment");
    return map;
  }

  @TearDown(Level.Trial)
  public void tearDown()
      throws IOException {
    try {
      _sealedContext.close();
      _sealedIndex.close();
      _sealedDataBuffer.close();
      _consumingIndex.close();
    } finally {
      try {
        _memoryManager.close();
      } finally {
        FileUtils.deleteQuietly(_sealedIndexDir);
      }
    }
  }

  /// One column at a time over the whole block - the frame is walked once per projected key.
  @SuppressWarnings({"rawtypes", "unchecked"})
  private static int projectSingleKeyPerPass(ForwardIndexReader reader, ForwardIndexReaderContext context,
      PreparedMapKey[] keys) {
    int checksum = 0;
    for (PreparedMapKey key : keys) {
      for (int docId = 0; docId < NUM_DOCS; docId++) {
        String value = reader.getMapEntryValueAsString(docId, context, key);
        checksum += value == null ? 0 : value.length();
      }
    }
    return checksum;
  }

  /// One walk per frame, pulling every projected key out of it.
  @SuppressWarnings({"rawtypes", "unchecked"})
  private int projectAllKeysPerPass(ForwardIndexReader reader, ForwardIndexReaderContext context) {
    int checksum = 0;
    for (int docId = 0; docId < NUM_DOCS; docId++) {
      reader.getMapEntryValuesAsString(docId, context, _keys, _values);
      for (int k = 0; k < _keys.length; k++) {
        checksum += _values[k] == null ? 0 : _values[k].length();
      }
    }
    return checksum;
  }

  /// The pre-selective-reader baseline: deserialize the whole frame for every key of every document, then pick the
  /// key out of the resulting map. That is what a sealed MAP column did until the chunked readers grew the selective
  /// hooks, and what the consuming one did before #19168.
  @SuppressWarnings({"rawtypes", "unchecked"})
  private static int projectViaFullMap(ForwardIndexReader reader, ForwardIndexReaderContext context,
      PreparedMapKey[] keys) {
    int checksum = 0;
    for (PreparedMapKey key : keys) {
      for (int docId = 0; docId < NUM_DOCS; docId++) {
        Object value = reader.getMap(docId, context).get(key.getKey());
        checksum += value == null ? 0 : value.toString().length();
      }
    }
    return checksum;
  }

  @Benchmark
  public int consumingFullMapPerKey() {
    return projectViaFullMap(_consumingIndex, null, _keys);
  }

  @Benchmark
  public int sealedFullMapPerKey() {
    return projectViaFullMap(_sealedIndex, _sealedContext, _keys);
  }

  @Benchmark
  public int consumingSingleKeyPerPass() {
    return projectSingleKeyPerPass(_consumingIndex, null, _keys);
  }

  @Benchmark
  public int consumingAllKeysPerPass() {
    return projectAllKeysPerPass(_consumingIndex, null);
  }

  @Benchmark
  public int sealedSingleKeyPerPass() {
    return projectSingleKeyPerPass(_sealedIndex, _sealedContext, _keys);
  }

  @Benchmark
  public int sealedAllKeysPerPass() {
    return projectAllKeysPerPass(_sealedIndex, _sealedContext);
  }
}
