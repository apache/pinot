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

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.core.data.table.IntermediateRecord;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.data.table.TwoLevelLinearProbingRecordHashMap;
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


@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Fork(value = 1)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 3, time = 1)
public class BenchmarkTwoLevelLinearProbingRecordHashMap {

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt =
        new OptionsBuilder().include(BenchmarkTwoLevelLinearProbingRecordHashMap.class.getSimpleName());
    new Runner(opt.build()).run();
  }

  @Param({"10000", "100000"})
  public int _size;
  @Param({"2", "10"})
  public int _payloadSize;

  private IntermediateRecord[] _records;

  @Setup(Level.Iteration)
  public void setup() {
    _records = new IntermediateRecord[_size];
    Random rand = ThreadLocalRandom.current();
    for (int i = 0; i < _size; i++) {
      Object[] payload1 = new Object[_payloadSize];
      Object[] payload2 = new Object[_payloadSize];
      for (int j = 0; j < _payloadSize; j++) {
        payload1[j] = rand.nextInt();
        payload2[j] = rand.nextInt();
      }
      Key key = new Key(payload1);
      _records[i] = IntermediateRecord.create(
          key,
          new Record(payload2),
          key.hashCode() & 0x7fffffff);
    }
  }

  @Benchmark
  public Object testJavaHashMapInsert() {
    Map<Key, Record> map = new HashMap<>(128);
    for (int i = 0; i < _size; i++) {
      IntermediateRecord r = _records[i];
      map.put(r._key, r._record);
    }
    return map;
  }

  @Benchmark
  public Object testTwoLevelHashMapInsert() {
    TwoLevelLinearProbingRecordHashMap map = new TwoLevelLinearProbingRecordHashMap();
    for (int i = 0; i < _size; i++) {
      map.put(_records[i]);
    }
    return map;
  }
}
