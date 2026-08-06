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
import org.openjdk.jmh.runner.options.OptionsBuilder;


/// Measures selective MAP key access against the current full-map read path.
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(value = 2)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Thread)
public class BenchmarkMapKeyAccess {

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt = new OptionsBuilder().include(BenchmarkMapKeyAccess.class.getSimpleName());
    new Runner(opt.build()).run();
  }

  @Param({"4", "16", "64"})
  private int _numEntries;

  @Param({"first", "last"})
  private String _targetPosition;

  private byte[] _serialized;
  private ByteBuffer _directBuffer;
  private String _targetKey;

  @Setup(Level.Trial)
  public void setUp() {
    Map<String, Object> map = new LinkedHashMap<>();
    for (int i = 0; i < _numEntries; i++) {
      map.put("key-" + i, Map.of("value", "value-with-enough-bytes-to-exercise-json-parsing-" + i));
    }
    _targetKey = "key-" + ("first".equals(_targetPosition) ? 0 : _numEntries - 1);
    _serialized = MapUtils.serializeMap(map, false);
    _directBuffer = ByteBuffer.allocateDirect(_serialized.length);
    _directBuffer.put(_serialized);
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
