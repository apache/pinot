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
import org.apache.commons.lang3.RandomUtils;
import org.apache.pinot.segment.local.io.compression.SnappyCompressor;
import org.apache.pinot.segment.local.io.compression.SnappyDecompressor;
import org.apache.pinot.segment.local.io.compression.ZstandardCompressor;
import org.apache.pinot.segment.local.io.compression.ZstandardDecompressor;
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


@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@State(Scope.Benchmark)
// Test to get memory statistics for snappy and zstandard integer compression techniques
public class BenchmarkNoDictionaryIntegerCompression {

  @Param({"500000", "1000000", "2000000", "3000000", "4000000", "5000000"})
  public static int _rowLength;

  @State(Scope.Thread)
  public static class BenchmarkNoDictionaryIntegerCompressionState {

    private static ByteBuffer _uncompressedInt;
    private static ByteBuffer _snappyIntegerIntegerInput;
    private static ByteBuffer _zstandardCompressedIntegerInput;
    private static ByteBuffer _snappyCompressedIntegerOutput;
    private static ByteBuffer _zstdCompressedIntegerOutput;
    private static ByteBuffer _snappyIntegerDecompressed;
    private static ByteBuffer _zstdIntegerDecompressed;
    private static SnappyCompressor snappyCompressor;
    private static SnappyDecompressor snappyDecompressor;
    private static ZstandardCompressor zstandardCompressor;
    private static ZstandardDecompressor zstandardDecompressor;

    @Setup(Level.Invocation)
    public void setUp()
        throws Exception {

      initializeCompressors();
      generateRandomIntegerBuffer();
      allocateBufferMemory();

      snappyCompressor.compress(_uncompressedInt,_snappyIntegerIntegerInput);
      Zstd.compress(_zstandardCompressedIntegerInput, _uncompressedInt);

      _zstdIntegerDecompressed.flip();_zstandardCompressedIntegerInput.flip();_uncompressedInt.flip();_snappyIntegerDecompressed.flip();
    }

    private void generateRandomIntegerBuffer() {
      //Generate Random Int
      _uncompressedInt = ByteBuffer.allocateDirect(_rowLength * Integer.BYTES);
      for (int i = 0; i < _rowLength; i++) {
        _uncompressedInt.putInt(RandomUtils.nextInt());
      }
      _uncompressedInt.flip();

      _snappyCompressedIntegerOutput = ByteBuffer.allocateDirect(_uncompressedInt.capacity()*2);
      _zstdCompressedIntegerOutput = ByteBuffer.allocateDirect(_uncompressedInt.capacity()*2);
    }

    private void initializeCompressors() {
      //Initialize compressors and decompressors for snappy
      snappyCompressor = new SnappyCompressor();
      snappyDecompressor = new SnappyDecompressor();

      //Initialize compressors and decompressors for zstandard
      zstandardCompressor = new ZstandardCompressor();
      zstandardDecompressor = new ZstandardDecompressor();
    }

    private void allocateBufferMemory() {
      _snappyIntegerDecompressed = ByteBuffer.allocateDirect(_uncompressedInt.capacity()*2);
      _zstdIntegerDecompressed = ByteBuffer.allocateDirect(_uncompressedInt.capacity()*2);
      _snappyIntegerIntegerInput = ByteBuffer.allocateDirect(_uncompressedInt.capacity()*2);
      _zstandardCompressedIntegerInput = ByteBuffer.allocateDirect(_uncompressedInt.capacity()*2);
    }

    @TearDown(Level.Invocation)
    public void tearDown()
        throws Exception {
      _snappyCompressedIntegerOutput.clear();
      _snappyIntegerDecompressed.clear();
      _zstdCompressedIntegerOutput.clear();
      _zstdIntegerDecompressed.clear();

      _uncompressedInt.rewind();
      _zstandardCompressedIntegerInput.rewind();
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int benchmarkSnappyIntegerCompression(BenchmarkNoDictionaryIntegerCompressionState state)
      throws IOException {
    int size = state.snappyCompressor.compress(state._uncompressedInt, state._snappyCompressedIntegerOutput);
    return size;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int benchmarkSnappyIntegerDecompression(BenchmarkNoDictionaryIntegerCompressionState state)
      throws IOException {
    int size = state.snappyDecompressor.decompress(state._snappyIntegerIntegerInput, state._snappyIntegerDecompressed);
    return size;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int benchmarkZstandardIntegerCompression(BenchmarkNoDictionaryIntegerCompressionState state)
      throws IOException {
    int size = state.zstandardCompressor.compress(state._zstdCompressedIntegerOutput, state._uncompressedInt);
    return size;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int benchmarkZstandardIntegerDecompression(BenchmarkNoDictionaryIntegerCompressionState state)
      throws IOException {
    int size = state.zstandardDecompressor.decompress(state._zstdIntegerDecompressed, state._zstandardCompressedIntegerInput);
    return size;
  }

  public static void main(String[] args)
      throws Exception {
    new Runner(new OptionsBuilder().include(BenchmarkNoDictionaryIntegerCompression.class.getSimpleName()).build()).run();
  }
}
