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


/// Measures projecting a whole MAP column as a JSON string - the `SELECT attributes` / `LASTWITHTIME(attributes)`
/// shape - rendering straight from the serialized frame versus deserializing into a map and serializing it again.
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(value = 2)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Thread)
public class BenchmarkMapProjection {

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt = new OptionsBuilder().parent(new CommandLineOptions(args))
        .include(BenchmarkMapProjection.class.getSimpleName());
    new Runner(opt.build()).run();
  }

  @Param({"4", "16", "64"})
  private int _numEntries;

  /// `flat` models scalar attribute maps; `nested` models object-valued entries, where the deserializing path pays
  /// to materialize a container per value.
  @Param({"flat", "nested"})
  private String _valueShape;

  private byte[] _serialized;

  @Setup(Level.Trial)
  public void setUp() {
    boolean nested = "nested".equals(_valueShape);
    Map<String, Object> map = new LinkedHashMap<>();
    for (int i = 0; i < _numEntries; i++) {
      String value = "value-with-enough-bytes-to-exercise-json-parsing-" + i;
      map.put(String.format("k8s.attribute.%03d.name", i), nested ? Map.of("value", value) : value);
    }
    _serialized = MapUtils.serializeMap(map);
  }

  /// The existing path: parse every entry into a map, then serialize the map back to JSON.
  @Benchmark
  public String deserializeThenSerialize() {
    return MapUtils.toString(MapUtils.deserializeMap(_serialized));
  }

  /// The optimized path: copy the already-JSON value bytes through and quote only the keys.
  @Benchmark
  public String renderFromFrame() {
    return MapUtils.frameToJsonString(_serialized);
  }
}
