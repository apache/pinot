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

import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;


@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 5, time = 10)
@Measurement(iterations = 20, time = 10)
@State(Scope.Benchmark)
public class BenchmarkObjectOpenHashMap {
  private static final int NUM_GROUPS_LIMIT = 20_000_000;
  private static final int INVALID_ID = -1;
  private final Object[] _values = new Object[NUM_GROUPS_LIMIT];

  @Param({
      "500000",
      "1000000",
      "5000000",
      "20000000",
  })
  public int _cardinality;

  @Setup
  public void setUp()
      throws Exception {
    for (int i = 0; i < _cardinality; i++) {
      _values[i] = UUID.randomUUID().toString();
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  public int object2IntReservedOpenHashMap()
  {
    Object2IntOpenHashMap<Object> map = new Object2IntOpenHashMap<>(_cardinality + 1);
    map.defaultReturnValue(INVALID_ID);
    for (int j = 0; j < _cardinality; j++) {
      getGroupId(map, _values[j]);
    }
    return map.size();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  public int object2IntOpenHashMap()
  {
    Object2IntOpenHashMap<Object> map = new Object2IntOpenHashMap<>();
    map.defaultReturnValue(INVALID_ID);
    for (int j = 0; j < _cardinality; j++) {
      getGroupId(map, _values[j]);
    }
    return map.size();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  public int vanillaReservedHashMap()
  {
    HashMap<Object, Integer> map = new HashMap<>(_cardinality + 1);
    for (int j = 0; j < _cardinality; j++) {
      getGroupId(map, _values[j]);
    }
    return map.size();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  public int vanillaHashMap()
  {
    HashMap<Object, Integer> map = new HashMap<>();
    for (int j = 0; j < _cardinality; j++) {
      getGroupId(map, _values[j]);
    }
    return map.size();
  }

  private int getGroupId(HashMap<Object, Integer> map, Object value) {
    int numGroups = map.size();
    if (numGroups < NUM_GROUPS_LIMIT) {
      return map.computeIfAbsent(value, k -> numGroups);
    } else {
      return map.getOrDefault(value, INVALID_ID);
    }
  }

  private int getGroupId(Object2IntOpenHashMap<Object> map, Object value) {
    int numGroups = map.size();
    if (numGroups < NUM_GROUPS_LIMIT) {
      return map.computeIfAbsent(value, k -> numGroups);
    } else {
      return map.getOrDefault(value, INVALID_ID);
    }
  }

  public static void main(String[] args)
      throws Exception {
    new Runner(new OptionsBuilder().include(BenchmarkObjectOpenHashMap.class.getSimpleName()).build()).run();
  }
}
