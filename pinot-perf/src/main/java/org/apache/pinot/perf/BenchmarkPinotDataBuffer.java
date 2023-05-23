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
import org.apache.pinot.segment.spi.memory.ByteBufferPinotBufferFactory;
import org.apache.pinot.segment.spi.memory.LArrayPinotBufferFactory;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.memory.SmallWithFallbackPinotBufferFactory;
import org.apache.pinot.segment.spi.memory.unsafe.UnsafePinotBufferFactory;
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
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;


@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(value = 1, jvmArgsPrepend = {
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
})
@State(Scope.Benchmark)
public class BenchmarkPinotDataBuffer {
  private static final Random RANDOM = new Random();
  private static final int BUFFER_SIZE = 1_000_000;

  @Param({"1", "32", "1024"})
  private int _valueLength;
  @Param({"bytebuffer", "larray", "unsafe", "wrapper+unsafe"})
  private String _bufferLibrary;
  private byte[] _bytes;
  private PinotDataBuffer _buffer;

  @Setup(Level.Iteration)
  public void onIterationStart() {
    _bytes = new byte[_valueLength];
    RANDOM.nextBytes(_bytes);

    _buffer = PinotDataBuffer.allocateDirect(BUFFER_SIZE, ByteOrder.nativeOrder(), null);
    int i = 0;
    while (i < BUFFER_SIZE - 8) {
      _buffer.putLong(i, RANDOM.nextLong());
      i += 8;
    }
    while (i < BUFFER_SIZE) {
      _buffer.putByte(i, (byte) (RANDOM.nextInt() & 0xFF));
      i++;
    }
  }

  @TearDown(Level.Iteration)
  public void onIterationFinish()
      throws IOException {
    _buffer.close();
  }

  @Setup
  public void setupBufferLibrary() {

    switch (_bufferLibrary) {
      case "bytebuffer":
        PinotDataBuffer.useFactory(new ByteBufferPinotBufferFactory());
        break;
      case "larray":
        PinotDataBuffer.useFactory(new LArrayPinotBufferFactory());
        break;
      case "unsafe":
        PinotDataBuffer.useFactory(new UnsafePinotBufferFactory());
        break;
      case "wrapper+larray":
        PinotDataBuffer.useFactory(new SmallWithFallbackPinotBufferFactory(
            new ByteBufferPinotBufferFactory(), new LArrayPinotBufferFactory()));
        break;
      case "wrapper+unsafe":
        PinotDataBuffer.useFactory(new SmallWithFallbackPinotBufferFactory(
            new ByteBufferPinotBufferFactory(), new UnsafePinotBufferFactory()));
        break;
      default:
        throw new IllegalArgumentException("Unrecognized buffer library \"" + _bufferLibrary + "\"");
    }
  }

  @Benchmark
  public void allocate(Blackhole bh)
      throws IOException {
    ByteOrder byteOrder = ByteOrder.nativeOrder();
    try (PinotDataBuffer pinotDataBuffer = PinotDataBuffer.allocateDirect(_valueLength, byteOrder, null)) {
      bh.consume(pinotDataBuffer);
    }
  }

  @Benchmark
  public void batchRead() {
    long index = RANDOM.nextInt(BUFFER_SIZE - _valueLength);
    _buffer.copyTo(index, _bytes);
  }

  @Benchmark
  public void nonBatchRead() {
    int index = RANDOM.nextInt(BUFFER_SIZE - _valueLength);
    for (int j = 0; j < _valueLength; j++) {
      _bytes[j] = _buffer.getByte(j + index);
    }
  }

  @Benchmark
  public void batchWrite() {
    int index = RANDOM.nextInt(BUFFER_SIZE - _valueLength);

    _buffer.readFrom(index, _bytes);
  }

  @Benchmark
  public void nonBatchWrite() {
    int index = RANDOM.nextInt(BUFFER_SIZE - _valueLength);

    for (int j = 0; j < _valueLength; j++) {
      _buffer.putByte(j + index, _bytes[j]);
    }
  }

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt = new OptionsBuilder().include(BenchmarkPinotDataBuffer.class.getSimpleName());
    new Runner(opt.build()).run();
  }
}
