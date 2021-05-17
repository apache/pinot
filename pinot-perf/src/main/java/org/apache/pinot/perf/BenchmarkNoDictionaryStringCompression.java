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
import java.util.Random;
import java.util.concurrent.TimeUnit;
import net.jpountz.lz4.LZ4Factory;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pinot.common.utils.StringUtil;
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
// Test to get memory statistics for snappy, zstandard and lz4 string compression techniques
public class BenchmarkNoDictionaryStringCompression {

  @Param({"500000", "1000000", "2000000", "3000000", "4000000", "5000000"})
  public static int _rowLength;

  public static Random _random = new Random();

  @State(Scope.Thread)
  public static class BenchmarkNoDictionaryStringCompressionState {
    private static ByteBuffer _uncompressedString;
    private static ByteBuffer _snappyCompressedStringInput;
    private static ByteBuffer _zstandardCompressedStringInput;
    private static ByteBuffer _snappyCompressedStringOutput;
    private static ByteBuffer _zstandardCompressedStringOutput;
    private static ByteBuffer _snappyStringDecompressed;
    private static ByteBuffer _zstandardStringDecompressed;
    private static ByteBuffer _lz4CompressedStringOutput;
    private static ByteBuffer _lz4CompressedStringInput;
    private static ByteBuffer _lz4StringDecompressed;

    private static LZ4Factory factory;

    @Setup(Level.Invocation)
    public void setUp()
        throws Exception {

      initializeCompressors();
      generateRandomStringBuffer();
      allocateMemory();

      Snappy.compress(_uncompressedString,_snappyCompressedStringInput);
      Zstd.compress(_zstandardCompressedStringInput, _uncompressedString);
      // ZSTD compressor with change the position of _uncompressedString, a flip() operation over input to reset position for lz4 is required
      _uncompressedString.flip();
      factory.fastCompressor().compress(_uncompressedString, _lz4CompressedStringInput);

      _zstandardStringDecompressed.rewind();_zstandardCompressedStringInput.flip();_uncompressedString.flip();_snappyStringDecompressed.flip();_lz4CompressedStringInput.flip();
    }

    private void initializeCompressors() {
      //Initialize compressors and decompressors for lz4
      factory = LZ4Factory.fastestInstance();
    }

    private void generateRandomStringBuffer() {
      String[] tempRows = new String[_rowLength];
      int maxStringLengthInBytes = 0;
      int numChars = 100;

      for (int i = 0; i < _rowLength; i++) {
        String value = RandomStringUtils.random(_random.nextInt(numChars), true, true);
        maxStringLengthInBytes = Math.max(maxStringLengthInBytes, StringUtil.encodeUtf8(value).length);
        tempRows[i] = value;
      }

      _uncompressedString = ByteBuffer.allocateDirect(_rowLength * maxStringLengthInBytes);
      for (int i = 0; i < _rowLength; i++) {
        _uncompressedString.put(StringUtil.encodeUtf8(tempRows[i]));
      }
      _uncompressedString.flip();
    }

    private void allocateMemory() {
      _snappyCompressedStringOutput = ByteBuffer.allocateDirect(_uncompressedString.capacity()*2);
      _zstandardCompressedStringOutput = ByteBuffer.allocateDirect(_uncompressedString.capacity()*2);
      _snappyStringDecompressed = ByteBuffer.allocateDirect(_uncompressedString.capacity()*2);
      _zstandardStringDecompressed = ByteBuffer.allocateDirect(_uncompressedString.capacity()*2);
      _snappyCompressedStringInput = ByteBuffer.allocateDirect(_uncompressedString.capacity()*2);
      _zstandardCompressedStringInput = ByteBuffer.allocateDirect(_uncompressedString.capacity()*2);
      _lz4StringDecompressed = ByteBuffer.allocateDirect(_uncompressedString.capacity()*2);
      _lz4CompressedStringOutput = ByteBuffer.allocateDirect(_uncompressedString.capacity()*2);
      _lz4CompressedStringInput = ByteBuffer.allocateDirect(_uncompressedString.capacity()*2);
    }

    @TearDown(Level.Invocation)
    public void tearDown()
        throws Exception {
      _snappyCompressedStringOutput.clear();
      _snappyStringDecompressed.clear();
      _zstandardCompressedStringOutput.clear();
      _zstandardStringDecompressed.clear();
      _lz4CompressedStringOutput.clear();
      _lz4StringDecompressed.clear();

      _uncompressedString.rewind();
      _zstandardCompressedStringInput.rewind();
      _lz4CompressedStringInput.rewind();
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int benchmarkSnappyStringCompression(BenchmarkNoDictionaryStringCompressionState state)
      throws IOException {
    int size = Snappy.compress(state._uncompressedString, state._snappyCompressedStringOutput);
    return size;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int benchmarkSnappyStringDecompression(BenchmarkNoDictionaryStringCompressionState state)
      throws IOException {
    int size = Snappy.uncompress(state._snappyCompressedStringInput, state._snappyStringDecompressed);
    return size;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int benchmarkZstandardStringCompression(BenchmarkNoDictionaryStringCompressionState state)
      throws IOException {
    int size = Zstd.compress(state._zstandardCompressedStringOutput, state._uncompressedString);
    return size;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int benchmarkZstandardStringDecompression(BenchmarkNoDictionaryStringCompressionState state)
      throws IOException {
    int size = Zstd.decompress(state._zstandardStringDecompressed, state._zstandardCompressedStringInput);
    return size;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int benchmarkLZ4StringCompression(
      BenchmarkNoDictionaryStringCompression.BenchmarkNoDictionaryStringCompressionState state)
      throws IOException {
    state.factory.fastCompressor().compress(state._uncompressedString, state._lz4CompressedStringOutput);
    return state._lz4CompressedStringOutput.position();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int benchmarkLZ4StringDecompression(
      BenchmarkNoDictionaryStringCompression.BenchmarkNoDictionaryStringCompressionState state)
      throws IOException {
    state.factory.fastDecompressor().decompress(state._lz4CompressedStringInput, state._lz4StringDecompressed);
    return state._lz4StringDecompressed.position();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int benchmarkLZ4HCStringCompression(
      BenchmarkNoDictionaryStringCompression.BenchmarkNoDictionaryStringCompressionState state)
      throws IOException {
    state.factory.highCompressor().compress(state._uncompressedString, state._lz4CompressedStringOutput);
    return state._lz4CompressedStringOutput.position();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int benchmarkLZ4HCStringDecompression(
      BenchmarkNoDictionaryStringCompression.BenchmarkNoDictionaryStringCompressionState state)
      throws IOException {
    state.factory.fastDecompressor().decompress(state._lz4CompressedStringInput, state._lz4StringDecompressed);
    return state._lz4StringDecompressed.position();
  }

  public static void main(String[] args)
      throws Exception {
    new Runner(new OptionsBuilder().include(BenchmarkNoDictionaryStringCompression.class.getSimpleName()).build()).run();
  }
}
