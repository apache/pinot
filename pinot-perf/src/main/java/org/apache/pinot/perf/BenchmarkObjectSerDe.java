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

import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pinot.segment.local.utils.ObjectSerDeUtils;
import org.apache.pinot.spi.utils.ByteArray;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import static java.nio.charset.StandardCharsets.UTF_8;


@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 10)
@Measurement(iterations = 5, time = 10)
@State(Scope.Benchmark)
public class BenchmarkObjectSerDe {
  private static final int NUM_VALUES = 5_000_000;

  ObjectArrayList<String> _stringList = new ObjectArrayList<>(NUM_VALUES);
  Set<String> _stringSet = new ObjectOpenHashSet<>(NUM_VALUES);
  Set<ByteArray> _bytesSet = new ObjectOpenHashSet<>(NUM_VALUES);

  @Setup
  public void setUp()
      throws IOException {
    for (int i = 0; i < NUM_VALUES; i++) {
      String stringValue = RandomStringUtils.secure().nextAlphanumeric(10, 201);
      _stringList.add(stringValue);
      _stringSet.add(stringValue);
      _bytesSet.add(new ByteArray(stringValue.getBytes(UTF_8)));
    }
  }

  @Benchmark
  public int stringList() {
    return ObjectSerDeUtils.STRING_ARRAY_LIST_SER_DE.serialize(_stringList).length;
  }

  @Benchmark
  public int stringSet() {
    return ObjectSerDeUtils.STRING_SET_SER_DE.serialize(_stringSet).length;
  }

  @Benchmark
  public int bytesSet() {
    return ObjectSerDeUtils.BYTES_SET_SER_DE.serialize(_bytesSet).length;
  }

  public static void main(String[] args)
      throws Exception {
    new Runner(new OptionsBuilder().include(BenchmarkObjectSerDe.class.getSimpleName()).build()).run();
  }
}
