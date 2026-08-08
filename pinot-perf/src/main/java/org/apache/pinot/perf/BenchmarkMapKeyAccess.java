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

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.utils.MapUtils;
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

  /// `flat` models scalar attribute maps (the common ingestion shape); `nested` models maps whose values are
  /// themselves objects, where the full-map path pays to materialize a container per value.
  @Param({"flat", "nested"})
  private String _valueShape;

  private byte[] _serialized;
  private ByteBuffer _directBuffer;
  private String _targetKey;

  @Setup(Level.Trial)
  public void setUp() {
    boolean nested = "nested".equals(_valueShape);
    Map<String, Object> map = new LinkedHashMap<>();
    for (int i = 0; i < _numEntries; i++) {
      String value = "value-with-enough-bytes-to-exercise-json-parsing-" + i;
      map.put(key(i), nested ? Map.of("value", value) : value);
    }
    _targetKey = key("first".equals(_targetPosition) ? 0 : _numEntries - 1);
    _serialized = MapUtils.serializeMap(map, false);
    _directBuffer = ByteBuffer.allocateDirect(_serialized.length);
    _directBuffer.put(_serialized);
  }

  private static String key(int i) {
    return String.format("k8s.attribute.%03d.name", i);
  }

  /// Models the existing consuming path: copy the complete off-heap value, then deserialize every MAP entry.
  @Benchmark
  public Object fullMapWithCopy() {
    _directBuffer.position(0);
    byte[] copy = new byte[_serialized.length];
    _directBuffer.get(copy);
    return MapUtils.deserializeMap(copy);
  }

  /// Models the optimized path: scan the direct view and deserialize only the selected JSON value.
  @Benchmark
  public Object selectiveMapValue() {
    _directBuffer.position(0);
    return MapUtils.deserializeMapValue(_directBuffer, _targetKey);
  }
}
