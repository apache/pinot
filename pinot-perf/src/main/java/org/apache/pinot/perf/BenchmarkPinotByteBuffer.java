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
import org.apache.pinot.core.data.table.Table;
import org.apache.pinot.core.segment.memory.PinotByteBuffer;
import org.apache.pinot.core.segment.memory.PinotNativeUnsafeByteBuffer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;


@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(1)
@State(Scope.Benchmark)
public class BenchmarkPinotByteBuffer {
  private static final int SIZE = 100_000_000; // 100MB
  private static final int TYPE_WIDTH = Integer.BYTES;
  private static final int INT_ARRAY_LENGTH = SIZE/Integer.BYTES;
  private PinotByteBuffer _pinotByteBuffer;
  private PinotNativeUnsafeByteBuffer _pinotNativeUnsafeByteBuffer;
  private Random _random;
  private int[] indexes = new int[INT_ARRAY_LENGTH];

  @Setup
  public void setUp() {
    _pinotByteBuffer = PinotByteBuffer.allocateDirect(SIZE, ByteOrder.nativeOrder());
    _pinotNativeUnsafeByteBuffer = PinotNativeUnsafeByteBuffer.allocateDirect(SIZE);
    _random = new Random();
    for (int i = 0; i < INT_ARRAY_LENGTH; i++) {
      indexes[i] = _random.nextInt(INT_ARRAY_LENGTH);
    }
    for (int i = 0; i < INT_ARRAY_LENGTH; i++) {
      _pinotByteBuffer.putInt(i * TYPE_WIDTH, 100 + i);
    }
    for (int i = 0; i < INT_ARRAY_LENGTH; i++) {
      _pinotNativeUnsafeByteBuffer.putInt(i * TYPE_WIDTH, 200 + i);
    }
  }

  @TearDown
  public void tearDown()
      throws IOException {
    _pinotByteBuffer.close();
    _pinotNativeUnsafeByteBuffer.close();
  }

  @Benchmark
  public void pinotByteBufferReadSequential() {
    for (int i = 0; i < INT_ARRAY_LENGTH; i++) {
      _pinotByteBuffer.getInt(i * TYPE_WIDTH);
    }
  }

  @Benchmark
  public void pinotNativeUnsafeByteBufferReadSequential() {
    for (int i = 0; i < INT_ARRAY_LENGTH; i++) {
      _pinotNativeUnsafeByteBuffer.getInt(i * TYPE_WIDTH);
    }
  }

  @Benchmark
  public void pinotByteBufferReadRandom() {
    for (int i = 0; i < INT_ARRAY_LENGTH; i++) {
      _pinotByteBuffer.getInt(indexes[i] * TYPE_WIDTH);
    }
  }

  @Benchmark
  public void pinotNativeUnsafeByteBufferReadRandom() {
    for (int i = 0; i < INT_ARRAY_LENGTH; i++) {
      _pinotNativeUnsafeByteBuffer.getInt(indexes[i] * TYPE_WIDTH);
    }
  }

  @Benchmark
  public void pinotByteBufferWriteRandom() throws Exception {
    try (PinotByteBuffer buffer = PinotByteBuffer.allocateDirect(SIZE, ByteOrder.nativeOrder())){
      for (int i = 0; i < INT_ARRAY_LENGTH; i++) {
        buffer.putInt(indexes[i] * TYPE_WIDTH,  100 + i);
      }
    }
  }

  @Benchmark
  public void pinotNativeUnsafeByteBufferWriteRandom() throws Exception {
    try (PinotNativeUnsafeByteBuffer buffer = PinotNativeUnsafeByteBuffer.allocateDirect(SIZE)){
      for (int i = 0; i < INT_ARRAY_LENGTH; i++) {
        buffer.putInt(indexes[i] * TYPE_WIDTH,  100 + i);
      }
    }
  }

  @Benchmark
  public void pinotByteBufferWriteSequential() throws Exception {
    try (PinotByteBuffer buffer = PinotByteBuffer.allocateDirect(SIZE, ByteOrder.nativeOrder())){
      for (int i = 0; i < INT_ARRAY_LENGTH; i++) {
        buffer.putInt(i * TYPE_WIDTH, 100 + i);
      }
    }
  }

  @Benchmark
  public void pinotByteBufferOptimizedWriteSequential() throws Exception {
    try (PinotNativeUnsafeByteBuffer buffer = PinotNativeUnsafeByteBuffer.allocateDirect(SIZE)){
      for (int i = 0; i < INT_ARRAY_LENGTH; i++) {
        buffer.putInt(i * TYPE_WIDTH, 100 + i);
      }
    }
  }

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt = new OptionsBuilder().include(BenchmarkPinotByteBuffer.class.getSimpleName());
    new Runner(opt.build()).run();
  }
}
