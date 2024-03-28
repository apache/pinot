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
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pinot.segment.local.io.compression.ChunkCompressorFactory;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.compression.ChunkCompressor;
import org.apache.pinot.segment.spi.compression.ChunkDecompressor;
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

import static java.nio.charset.StandardCharsets.UTF_8;


@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@State(Scope.Benchmark)
// Test to get memory statistics for snappy, zstandard, lz4 and gzip string compression techniques
public class BenchmarkNoDictionaryStringCompression {

  @Param({"500000", "1000000", "2000000", "3000000", "4000000", "5000000"})
  public static int _rowLength;

  private static final int MAX_CHARS_IN_LINE = 30;
  private static final Random RANDOM = new Random();
  private static final ChunkCompressor LZ4_COMPRESSOR = ChunkCompressorFactory.getCompressor(ChunkCompressionType.LZ4);
  private static final ChunkDecompressor LZ4_DECOMPRESSOR =
      ChunkCompressorFactory.getDecompressor(ChunkCompressionType.LZ4);
  private static final ChunkCompressor GZIP_COMPRESSOR =
      ChunkCompressorFactory.getCompressor(ChunkCompressionType.GZIP);
  private static final ChunkDecompressor GZIP_DECOMPRESSOR =
      ChunkCompressorFactory.getDecompressor(ChunkCompressionType.GZIP);

  @State(Scope.Thread)
  public static class CompressionBuffers {

    private ByteBuffer _snappyCompressedStringInput;
    private ByteBuffer _zstandardCompressedStringInput;
    private ByteBuffer _lz4CompressedStringInput;
    private ByteBuffer _gzipCompressedStringInput;
    private ByteBuffer _uncompressedString;
    private ByteBuffer _stringDecompressed;
    private ByteBuffer _stringCompressed;

    @Setup(Level.Trial)
    public void setUp0() {
      // generate random block of text alongside initialising memory buffers
      byte[][] tempRows = new byte[_rowLength][];
      int size = 0;
      for (int i = 0; i < _rowLength; i++) {
        String value = RandomStringUtils.random(RANDOM.nextInt(MAX_CHARS_IN_LINE), true, true);
        byte[] bytes = value.getBytes(UTF_8);
        tempRows[i] = bytes;
        size += bytes.length;
      }
      _uncompressedString = ByteBuffer.allocateDirect(size);
      for (int i = 0; i < _rowLength; i++) {
        _uncompressedString.put(tempRows[i]);
      }
      _uncompressedString.flip();

      int capacity = _uncompressedString.capacity() * 2;
      _stringDecompressed = ByteBuffer.allocateDirect(capacity);
      _stringCompressed = ByteBuffer.allocateDirect(capacity);
      _snappyCompressedStringInput = ByteBuffer.allocateDirect(capacity);
      _zstandardCompressedStringInput = ByteBuffer.allocateDirect(capacity);
      _lz4CompressedStringInput = ByteBuffer.allocateDirect(capacity);
      _gzipCompressedStringInput = ByteBuffer.allocateDirect(capacity);
    }

    @Setup(Level.Invocation)
    public void setUp()
        throws Exception {

      _uncompressedString.rewind();
      _snappyCompressedStringInput.clear();
      _zstandardCompressedStringInput.clear();
      _lz4CompressedStringInput.clear();
      _gzipCompressedStringInput.clear();
      _stringDecompressed.clear();
      _stringCompressed.clear();

      // prepare compressed buffers
      Snappy.compress(_uncompressedString, _snappyCompressedStringInput);
      Zstd.compress(_zstandardCompressedStringInput, _uncompressedString);
      // ZSTD compressor with change the position of _uncompressedString, a flip() operation over input to reset
      // position for lz4 is required
      _uncompressedString.flip();
      _zstandardCompressedStringInput.flip();

      LZ4_COMPRESSOR.compress(_uncompressedString, _lz4CompressedStringInput);
      _uncompressedString.flip();

      GZIP_COMPRESSOR.compress(_uncompressedString, _gzipCompressedStringInput);
      _uncompressedString.flip();
    }

    @TearDown(Level.Invocation)
    public void tearDown()
        throws Exception {
      _snappyCompressedStringInput.clear();
      _zstandardCompressedStringInput.clear();
      _lz4CompressedStringInput.clear();
      _gzipCompressedStringInput.clear();
      _uncompressedString.clear();
      _stringDecompressed.clear();
      _stringCompressed.clear();
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int benchmarkSnappyStringCompression(CompressionBuffers state)
      throws IOException {
    int size = Snappy.compress(state._uncompressedString, state._stringCompressed);
    return size;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int benchmarkSnappyStringDecompression(CompressionBuffers state)
      throws IOException {
    int size = Snappy.uncompress(state._snappyCompressedStringInput, state._stringDecompressed);
    return size;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int benchmarkZstandardStringCompression(CompressionBuffers state) {
    int size = Zstd.compress(state._stringCompressed, state._uncompressedString);
    return size;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int benchmarkZstandardStringDecompression(CompressionBuffers state) {
    int size = Zstd.decompress(state._stringDecompressed, state._zstandardCompressedStringInput);
    return size;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int benchmarkLZ4HCStringCompression(CompressionBuffers state)
      throws IOException {
    LZ4_COMPRESSOR.compress(state._uncompressedString, state._stringCompressed);
    return state._stringCompressed.position();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int benchmarkLZ4HCStringDecompression(CompressionBuffers state)
      throws IOException {
    LZ4_DECOMPRESSOR.decompress(state._lz4CompressedStringInput, state._stringDecompressed);
    return state._stringDecompressed.position();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int benchmarkGZIPStringCompression(CompressionBuffers state)
      throws IOException {
    GZIP_COMPRESSOR.compress(state._uncompressedString, state._stringCompressed);
    return state._stringCompressed.position();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int benchmarkGZIPStringDecompression(CompressionBuffers state)
      throws IOException {
    GZIP_DECOMPRESSOR.decompress(state._gzipCompressedStringInput, state._stringDecompressed);
    return state._stringDecompressed.position();
  }

  public static void main(String[] args)
      throws Exception {
    new Runner(
        new OptionsBuilder().include(BenchmarkNoDictionaryStringCompression.class.getSimpleName()).build()).run();
  }
}
