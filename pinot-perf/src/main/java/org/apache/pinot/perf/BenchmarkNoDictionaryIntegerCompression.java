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
// Test to get memory statistics for snappy, zstandard and lz4 integer compression techniques
public class BenchmarkNoDictionaryIntegerCompression {

  @Param({"500000", "1000000", "2000000", "3000000", "4000000", "5000000"})
  public static int _rowLength;

  @State(Scope.Thread)
  public static class BenchmarkNoDictionaryIntegerCompressionState {

    private static ByteBuffer _uncompressedInt;
    private static ByteBuffer _snappyCompressedIntegerInput;
    private static ByteBuffer _zstandardCompressedIntegerInput;
    private static ByteBuffer _snappyCompressedIntegerOutput;
    private static ByteBuffer _zstdCompressedIntegerOutput;
    private static ByteBuffer _snappyIntegerDecompressed;
    private static ByteBuffer _zstdIntegerDecompressed;

    private static ByteBuffer _lz4CompressedIntegerOutput;
    private static ByteBuffer _lz4CompressedIntegerInput;
    private static ByteBuffer _lz4IntegerDecompressed;

    private static LZ4Factory factory;

    @Setup(Level.Invocation)
    public void setUp()
        throws Exception {

      initializeCompressors();
      generateRandomIntegerBuffer();
      allocateBufferMemory();

      Snappy.compress(_uncompressedInt, _snappyCompressedIntegerInput);
      Zstd.compress(_zstandardCompressedIntegerInput, _uncompressedInt);
      // ZSTD compressor with change the position of _uncompressedInt, a flip() operation over input to reset position for lz4 is required
      _uncompressedInt.flip();
      factory.fastCompressor().compress(_uncompressedInt, _lz4CompressedIntegerInput);

      _zstdIntegerDecompressed.rewind();_zstandardCompressedIntegerInput.flip();_uncompressedInt.flip();_snappyIntegerDecompressed.rewind();_lz4CompressedIntegerInput.flip();
    }

    private void generateRandomIntegerBuffer() {
      //Generate Random Int
      _uncompressedInt = ByteBuffer.allocateDirect(_rowLength * Integer.BYTES);
      for (int i = 0; i < _rowLength; i++) {
        _uncompressedInt.putInt(RandomUtils.nextInt());
      }
      _uncompressedInt.flip();
    }

    private void initializeCompressors() {
      //Initialize compressors and decompressors for lz4
      factory = LZ4Factory.fastestInstance();
    }

    private void allocateBufferMemory() {
      _snappyIntegerDecompressed = ByteBuffer.allocateDirect(_uncompressedInt.capacity()*2);
      _zstdIntegerDecompressed = ByteBuffer.allocateDirect(_uncompressedInt.capacity()*2);
      _snappyCompressedIntegerInput = ByteBuffer.allocateDirect(_uncompressedInt.capacity()*2);
      _zstandardCompressedIntegerInput = ByteBuffer.allocateDirect(_uncompressedInt.capacity()*2);
      _lz4IntegerDecompressed = ByteBuffer.allocateDirect(_uncompressedInt.capacity()*2);
      _lz4CompressedIntegerOutput = ByteBuffer.allocateDirect(_uncompressedInt.capacity()*2);
      _lz4CompressedIntegerInput = ByteBuffer.allocateDirect(_uncompressedInt.capacity()*2);
      _lz4CompressedIntegerOutput = ByteBuffer.allocateDirect(_uncompressedInt.capacity()*2);
      _snappyCompressedIntegerOutput = ByteBuffer.allocateDirect(_uncompressedInt.capacity()*2);
      _zstdCompressedIntegerOutput = ByteBuffer.allocateDirect(_uncompressedInt.capacity()*2);
    }

    @TearDown(Level.Invocation)
    public void tearDown()
        throws Exception {
      _snappyCompressedIntegerOutput.clear();
      _snappyIntegerDecompressed.clear();
      _zstdCompressedIntegerOutput.clear();
      _zstdIntegerDecompressed.clear();
      _lz4CompressedIntegerOutput.clear();
      _lz4IntegerDecompressed.clear();

      _uncompressedInt.rewind();
      _zstandardCompressedIntegerInput.rewind();
      _lz4CompressedIntegerInput.rewind();
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int benchmarkSnappyIntegerCompression(BenchmarkNoDictionaryIntegerCompressionState state)
      throws IOException {
    int size = Snappy.compress(state._uncompressedInt, state._snappyCompressedIntegerOutput);
    return size;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int benchmarkSnappyIntegerDecompression(BenchmarkNoDictionaryIntegerCompressionState state)
      throws IOException {
    int size = Snappy.uncompress(state._snappyCompressedIntegerInput, state._snappyIntegerDecompressed);
    return size;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int benchmarkZstandardIntegerCompression(BenchmarkNoDictionaryIntegerCompressionState state)
      throws IOException {
    int size = Zstd.compress(state._zstdCompressedIntegerOutput, state._uncompressedInt);
    return size;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int benchmarkZstandardIntegerDecompression(BenchmarkNoDictionaryIntegerCompressionState state)
      throws IOException {
    int size = Zstd.decompress(state._zstdIntegerDecompressed, state._zstandardCompressedIntegerInput);
    return size;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int benchmarkLZ4IntegerCompression(BenchmarkNoDictionaryIntegerCompressionState state)
      throws IOException {
    state.factory.fastCompressor().compress(state._uncompressedInt, state._lz4CompressedIntegerOutput);
    return state._lz4CompressedIntegerOutput.position();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int benchmarkLZ4IntegerDecompression(BenchmarkNoDictionaryIntegerCompressionState state)
      throws IOException {
    state.factory.safeDecompressor().decompress(state._lz4CompressedIntegerInput, state._lz4IntegerDecompressed);
    return state._lz4IntegerDecompressed.position();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int benchmarkLZ4HCIntegerCompression(BenchmarkNoDictionaryIntegerCompressionState state)
      throws IOException {
    state.factory.highCompressor().compress(state._uncompressedInt, state._lz4CompressedIntegerOutput);
    return state._lz4CompressedIntegerOutput.position();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int benchmarkLZ4HCIntegerDecompression(BenchmarkNoDictionaryIntegerCompressionState state)
      throws IOException {
    state.factory.safeDecompressor().decompress(state._lz4CompressedIntegerInput, state._lz4IntegerDecompressed);
    return state._lz4IntegerDecompressed.position();
  }

  public static void main(String[] args)
      throws Exception {
    new Runner(new OptionsBuilder().include(BenchmarkNoDictionaryIntegerCompression.class.getSimpleName()).build()).run();
  }
}
