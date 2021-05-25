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

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;


@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
@Fork(1)
@State(Scope.Benchmark)
public class BenchmarkPinotDataBuffer {
  private static final Random RANDOM = new Random();
  private static final int NUM_ROUNDS = 1_000_000;

  @Param({"1", "2", "4", "8", "16", "32", "64", "128", "256", "512", "1024"})
  private int _valueLength;

  @Benchmark
  public int batchRead()
      throws IOException {
    byte[] value = new byte[_valueLength];
    RANDOM.nextBytes(value);

    int sum = 0;
    try (
        PinotDataBuffer pinotDataBuffer = PinotDataBuffer.allocateDirect(_valueLength, ByteOrder.nativeOrder(), null)) {
      pinotDataBuffer.readFrom(0, value);
      byte[] buffer = new byte[_valueLength];
      for (int i = 0; i < NUM_ROUNDS; i++) {
        pinotDataBuffer.copyTo(0, buffer);
        sum += buffer[0];
      }
    }

    return sum;
  }

  @Benchmark
  public int nonBatchRead()
      throws IOException {
    byte[] value = new byte[_valueLength];
    RANDOM.nextBytes(value);

    int sum = 0;
    try (
        PinotDataBuffer pinotDataBuffer = PinotDataBuffer.allocateDirect(_valueLength, ByteOrder.nativeOrder(), null)) {
      pinotDataBuffer.readFrom(0, value);
      byte[] buffer = new byte[_valueLength];
      for (int i = 0; i < NUM_ROUNDS; i++) {
        for (int j = 0; j < _valueLength; j++) {
          buffer[j] = pinotDataBuffer.getByte(j);
        }
        sum += buffer[0];
      }
    }

    return sum;
  }

  @Benchmark
  public int batchWrite()
      throws IOException {
    byte[] value = new byte[_valueLength];
    RANDOM.nextBytes(value);

    int sum = 0;
    try (
        PinotDataBuffer pinotDataBuffer = PinotDataBuffer.allocateDirect(_valueLength, ByteOrder.nativeOrder(), null)) {
      for (int i = 0; i < NUM_ROUNDS; i++) {
        pinotDataBuffer.readFrom(0, value);
        sum += pinotDataBuffer.getByte(0);
      }
    }

    return sum;
  }

  @Benchmark
  public int nonBatchWrite()
      throws IOException {
    byte[] value = new byte[_valueLength];
    RANDOM.nextBytes(value);

    int sum = 0;
    try (
        PinotDataBuffer pinotDataBuffer = PinotDataBuffer.allocateDirect(_valueLength, ByteOrder.nativeOrder(), null)) {
      for (int i = 0; i < NUM_ROUNDS; i++) {
        for (int j = 0; j < _valueLength; j++) {
          pinotDataBuffer.putByte(j, value[j]);
        }
        sum += pinotDataBuffer.getByte(0);
      }
    }

    return sum;
  }

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt = new OptionsBuilder().include(BenchmarkPinotDataBuffer.class.getSimpleName());
    new Runner(opt.build()).run();
  }
}
