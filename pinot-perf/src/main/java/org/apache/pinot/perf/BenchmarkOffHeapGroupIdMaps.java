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

import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.core.query.aggregation.groupby.DictionaryBasedGroupKeyGenerator.IntGroupIdMap;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.core.query.aggregation.groupby.offheap.OffHeapBytesGroupIdMap;
import org.apache.pinot.core.query.aggregation.groupby.offheap.OffHeapGroupByUtils;
import org.apache.pinot.core.query.aggregation.groupby.offheap.OffHeapIntGroupIdMap;
import org.apache.pinot.core.query.aggregation.groupby.offheap.OffHeapLongGroupIdMap;
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


/// Micro-benchmark comparing the off-heap group-by key tables against the on-heap structures they replace, over the
/// per-segment group-by access pattern: a stream of raw keys with duplicates mapped to dense group ids, with the map
/// created and released per pass (mirroring the per-query lifecycle — the on-heap maps are thread-local-cached in
/// production, so the on-heap numbers here are slightly pessimistic on construction cost, while the on-heap string
/// map benefits from cached String hash codes that the real per-block string materialization does not have).
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 3)
@State(Scope.Benchmark)
public class BenchmarkOffHeapGroupIdMaps {
  private static final int NUM_OPS = 2_000_000;
  private static final int GROUP_ID_UPPER_BOUND = Integer.MAX_VALUE;

  @Param({"10000", "100000", "1000000"})
  private int _numDistinct;

  private int[] _intKeys;
  private long[] _longKeys;
  private String[] _stringKeys;
  private byte[] _encodeScratch;

  @Setup
  public void setUp() {
    Random random = new Random(42);
    _intKeys = new int[NUM_OPS];
    _longKeys = new long[NUM_OPS];
    _stringKeys = new String[NUM_OPS];
    // Distinct string values reused by reference (interned per distinct id) so the on-heap map sees cached
    // hash codes — a deliberate bias in favor of the on-heap baseline
    String[] distinctStrings = new String[Math.min(_numDistinct, 1_000_000)];
    for (int i = 0; i < distinctStrings.length; i++) {
      distinctStrings[i] = String.format("key-%08d-abcdefgh", i);
    }
    for (int i = 0; i < NUM_OPS; i++) {
      int distinct = random.nextInt(_numDistinct);
      _intKeys[i] = distinct;
      _longKeys[i] = ((long) distinct << 20) | (distinct & 0xFFFFF);
      _stringKeys[i] = distinctStrings[distinct % distinctStrings.length];
    }
    int maxKeyLength = 0;
    for (String key : _stringKeys) {
      maxKeyLength = Math.max(maxKeyLength, key.length());
    }
    _encodeScratch = new byte[maxKeyLength * 3];
  }

  @Benchmark
  public long onHeapIntMap() {
    IntGroupIdMap map = new IntGroupIdMap();
    long sum = 0;
    for (int key : _intKeys) {
      sum += map.getGroupId(key, GROUP_ID_UPPER_BOUND);
    }
    map.clearAndTrim();
    return sum;
  }

  @Benchmark
  public long offHeapIntMap() {
    long sum = 0;
    try (OffHeapIntGroupIdMap map = new OffHeapIntGroupIdMap(0)) {
      for (int key : _intKeys) {
        sum += map.getGroupId(key, GROUP_ID_UPPER_BOUND);
      }
    }
    return sum;
  }

  @Benchmark
  public long onHeapLongMap() {
    Long2IntOpenHashMap map = new Long2IntOpenHashMap();
    map.defaultReturnValue(GroupKeyGenerator.INVALID_ID);
    long sum = 0;
    for (long key : _longKeys) {
      int numGroups = map.size();
      int groupId = map.putIfAbsent(key, numGroups);
      sum += groupId == GroupKeyGenerator.INVALID_ID ? numGroups : groupId;
    }
    return sum;
  }

  @Benchmark
  public long offHeapLongMap() {
    long sum = 0;
    try (OffHeapLongGroupIdMap map = new OffHeapLongGroupIdMap(0)) {
      for (long key : _longKeys) {
        sum += map.getGroupId(key, GROUP_ID_UPPER_BOUND);
      }
    }
    return sum;
  }

  @Benchmark
  public long onHeapStringMap() {
    Object2IntOpenHashMap<String> map = new Object2IntOpenHashMap<>();
    map.defaultReturnValue(GroupKeyGenerator.INVALID_ID);
    long sum = 0;
    for (String key : _stringKeys) {
      int groupId = map.getInt(key);
      if (groupId == GroupKeyGenerator.INVALID_ID) {
        groupId = map.size();
        map.put(key, groupId);
      }
      sum += groupId;
    }
    return sum;
  }

  @Benchmark
  public long offHeapBytesMap() {
    long sum = 0;
    byte[] scratch = _encodeScratch;
    try (OffHeapBytesGroupIdMap map = new OffHeapBytesGroupIdMap(0)) {
      for (String key : _stringKeys) {
        int length = OffHeapGroupByUtils.encodeUtf8(key, scratch);
        sum += map.getGroupId(scratch, 0, length, GROUP_ID_UPPER_BOUND);
      }
    }
    return sum;
  }

  public static void main(String[] args)
      throws Exception {
    new Runner(
        new OptionsBuilder().include(BenchmarkOffHeapGroupIdMaps.class.getSimpleName()).addProfiler("gc").build())
        .run();
  }
}
