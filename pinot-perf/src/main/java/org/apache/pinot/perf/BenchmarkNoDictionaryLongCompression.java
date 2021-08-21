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

import com.github.luben.zstd.Zstd;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import net.jpountz.lz4.LZ4Factory;
import org.apache.commons.lang3.RandomUtils;
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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.xerial.snappy.Snappy;


@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@State(Scope.Benchmark)
// Test to get memory statistics for snappy, zstandard and lz4 long compression techniques
public class BenchmarkNoDictionaryLongCompression {

  @Param({"500000", "1000000", "2000000", "3000000", "4000000", "5000000"})
  public static int _rowLength;

  @State(Scope.Thread)
  public static class BenchmarkNoDictionaryLongCompressionState {

    private static ByteBuffer _uncompressedLong;
    private static ByteBuffer _snappyCompressedLongInput;
    private static ByteBuffer _zstandardCompressedLongInput;
    private static ByteBuffer _snappyCompressedLongOutput;
    private static ByteBuffer _zstandardCompressedLongOutput;
    private static ByteBuffer _snappyLongDecompressedOutput;
    private static ByteBuffer _zstandardLongDecompressedOutput;

    private static ByteBuffer _lz4CompressedLongOutput;
    private static ByteBuffer _lz4CompressedLongInput;
    private static ByteBuffer _lz4LongDecompressed;

    private static LZ4Factory factory;

    @Setup(Level.Invocation)
    public void setUp()
        throws Exception {

      initializeCompressors();
      generateRandomLongBuffer();
      allocateBufferMemory();

      Snappy.compress(_uncompressedLong, _snappyCompressedLongInput);
      Zstd.compress(_zstandardCompressedLongInput, _uncompressedLong);
      // ZSTD compressor with change the position of _uncompressedLong, a flip() operation over input to reset
      // position for lz4 is required
      _uncompressedLong.flip();
      factory.fastCompressor().compress(_uncompressedLong, _lz4CompressedLongInput);

      _zstandardLongDecompressedOutput.rewind();
      _zstandardCompressedLongInput.flip();
      _uncompressedLong.flip();
      _snappyLongDecompressedOutput.flip();
      _lz4CompressedLongInput.flip();
    }

    private void generateRandomLongBuffer() {
      //Generate Random Long
      _uncompressedLong = ByteBuffer.allocateDirect(_rowLength * Long.BYTES);
      for (int i = 0; i < _rowLength; i++) {
        _uncompressedLong.putLong(RandomUtils.nextLong());
      }
      _uncompressedLong.flip();
    }

    private void initializeCompressors() {
      //Initialize compressors and decompressors for lz4
      factory = LZ4Factory.fastestInstance();
    }

    private void allocateBufferMemory() {
      _snappyCompressedLongOutput = ByteBuffer.allocateDirect(_uncompressedLong.capacity() * 2);
      _zstandardCompressedLongOutput = ByteBuffer.allocateDirect(_uncompressedLong.capacity() * 2);
      _snappyLongDecompressedOutput = ByteBuffer.allocateDirect(_uncompressedLong.capacity() * 2);
      _zstandardLongDecompressedOutput = ByteBuffer.allocateDirect(_uncompressedLong.capacity() * 2);
      _snappyCompressedLongInput = ByteBuffer.allocateDirect(_uncompressedLong.capacity() * 2);
      _zstandardCompressedLongInput = ByteBuffer.allocateDirect(_uncompressedLong.capacity() * 2);
      _lz4LongDecompressed = ByteBuffer.allocateDirect(_uncompressedLong.capacity() * 2);
      _lz4CompressedLongOutput = ByteBuffer.allocateDirect(_uncompressedLong.capacity() * 2);
      _lz4CompressedLongInput = ByteBuffer.allocateDirect(_uncompressedLong.capacity() * 2);
    }

    @TearDown(Level.Invocation)
    public void tearDown()
        throws Exception {
      _snappyCompressedLongOutput.clear();
      _snappyLongDecompressedOutput.clear();
      _zstandardCompressedLongOutput.clear();
      _zstandardLongDecompressedOutput.clear();
      _lz4CompressedLongOutput.clear();
      _lz4LongDecompressed.clear();

      _uncompressedLong.rewind();
      _zstandardCompressedLongInput.rewind();
      _lz4CompressedLongInput.rewind();
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int benchmarkSnappyLongCompression(BenchmarkNoDictionaryLongCompressionState state)
      throws IOException {
    int size = Snappy.compress(state._uncompressedLong, state._snappyCompressedLongOutput);
    return size;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int benchmarkSnappyLongDecompression(BenchmarkNoDictionaryLongCompressionState state)
      throws IOException {
    int size = Snappy.uncompress(state._snappyCompressedLongInput, state._snappyLongDecompressedOutput);
    return size;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int benchmarkZstandardLongCompression(BenchmarkNoDictionaryLongCompressionState state)
      throws IOException {
    int size = Zstd.compress(state._zstandardCompressedLongOutput, state._uncompressedLong);
    return size;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int benchmarkZstandardLongDecompression(BenchmarkNoDictionaryLongCompressionState state)
      throws IOException {
    int size = Zstd.decompress(state._zstandardLongDecompressedOutput, state._zstandardCompressedLongInput);
    return size;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int benchmarkLZ4LongCompression(
      BenchmarkNoDictionaryLongCompression.BenchmarkNoDictionaryLongCompressionState state)
      throws IOException {
    state.factory.fastCompressor().compress(state._uncompressedLong, state._lz4CompressedLongOutput);
    return state._lz4CompressedLongOutput.position();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int benchmarkLZ4LongDecompression(
      BenchmarkNoDictionaryLongCompression.BenchmarkNoDictionaryLongCompressionState state)
      throws IOException {
    state.factory.safeDecompressor().decompress(state._lz4CompressedLongInput, state._lz4LongDecompressed);
    return state._lz4LongDecompressed.position();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int benchmarkLZ4HCLongCompression(
      BenchmarkNoDictionaryLongCompression.BenchmarkNoDictionaryLongCompressionState state)
      throws IOException {
    state.factory.highCompressor().compress(state._uncompressedLong, state._lz4CompressedLongOutput);
    return state._lz4CompressedLongOutput.position();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int benchmarkLZ4HCLongDecompression(
      BenchmarkNoDictionaryLongCompression.BenchmarkNoDictionaryLongCompressionState state)
      throws IOException {
    state.factory.safeDecompressor().decompress(state._lz4CompressedLongInput, state._lz4LongDecompressed);
    return state._lz4LongDecompressed.position();
  }

  public static void main(String[] args)
      throws Exception {
    new Runner(new OptionsBuilder().include(BenchmarkNoDictionaryLongCompression.class.getSimpleName()).build()).run();
  }
}
