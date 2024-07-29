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

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.io.writer.impl.FixedByteChunkForwardIndexWriter;
import org.apache.pinot.segment.local.segment.index.readers.forward.ChunkReaderContext;
import org.apache.pinot.segment.local.segment.index.readers.forward.FixedByteChunkSVForwardIndexReader;
import org.apache.pinot.segment.local.segment.index.readers.forward.FixedBytePower2ChunkSVForwardIndexReader;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;


@State(Scope.Benchmark)
public class BenchmarkFixedByteSVForwardIndexReader {

  private static final File INDEX_DIR =
      new File(FileUtils.getTempDirectory(), "BenchmarkFixedByteSVForwardIndexReader");

  @Param("10000")
  int _blockSize;

  @Param("1000")
  int _numBlocks;

  private int[] _docIds;
  private double[] _doubleBuffer;
  private long[] _longBuffer;
  private FixedByteChunkSVForwardIndexReader _compressedReader;
  private FixedBytePower2ChunkSVForwardIndexReader _compressedPow2Reader;
  private FixedByteChunkSVForwardIndexReader _uncompressedReader;

  @Setup(Level.Trial)
  public void setup()
      throws IOException {
    FileUtils.forceMkdir(INDEX_DIR);
    File uncompressedIndexFile = new File(INDEX_DIR, UUID.randomUUID().toString());
    File compressedIndexFile = new File(INDEX_DIR, UUID.randomUUID().toString());
    File pow2CompressedIndexFile = new File(INDEX_DIR, UUID.randomUUID().toString());
    _doubleBuffer = new double[_blockSize];
    _longBuffer = new long[_blockSize];
    try (FixedByteChunkForwardIndexWriter writer = new FixedByteChunkForwardIndexWriter(compressedIndexFile,
        ChunkCompressionType.LZ4, _numBlocks * _blockSize, 1000, Long.BYTES, 3);
        FixedByteChunkForwardIndexWriter passThroughWriter = new FixedByteChunkForwardIndexWriter(uncompressedIndexFile,
            ChunkCompressionType.PASS_THROUGH, _numBlocks * _blockSize, 1000, Long.BYTES, 3);
        FixedByteChunkForwardIndexWriter pow2Writer = new FixedByteChunkForwardIndexWriter(pow2CompressedIndexFile,
            ChunkCompressionType.LZ4, _numBlocks * _blockSize, 1000, Long.BYTES, 4)) {
      for (int i = 0; i < _numBlocks * _blockSize; i++) {
        long next = ThreadLocalRandom.current().nextLong();
        writer.putLong(next);
        pow2Writer.putLong(next);
        passThroughWriter.putLong(next);
      }
    }
    _compressedReader = new FixedByteChunkSVForwardIndexReader(PinotDataBuffer.loadBigEndianFile(compressedIndexFile),
        FieldSpec.DataType.LONG);
    _compressedPow2Reader =
        new FixedBytePower2ChunkSVForwardIndexReader(PinotDataBuffer.loadBigEndianFile(pow2CompressedIndexFile),
            FieldSpec.DataType.LONG);
    _uncompressedReader =
        new FixedByteChunkSVForwardIndexReader(PinotDataBuffer.loadBigEndianFile(uncompressedIndexFile),
            FieldSpec.DataType.LONG);
    _docIds = new int[_blockSize];
  }

  @TearDown(Level.Trial)
  public void teardown()
      throws IOException {
    FileUtils.deleteDirectory(INDEX_DIR);
  }

  @Benchmark
  public void readCompressedDoublesNonContiguousV3(Blackhole bh)
      throws IOException {
    readCompressedDoublesNonContiguous(bh, _compressedReader);
  }

  @Benchmark
  public void readCompressedDoublesNonContiguousV4(Blackhole bh)
      throws IOException {
    readCompressedDoublesNonContiguous(bh, _compressedPow2Reader);
  }

  @Benchmark
  public void readCompressedLongsNonContiguousV3(Blackhole bh)
      throws IOException {
    readCompressedLongsNonContiguous(bh, _compressedReader);
  }

  @Benchmark
  public void readCompressedLongsNonContiguousV4(Blackhole bh)
      throws IOException {
    readCompressedLongsNonContiguous(bh, _compressedPow2Reader);
  }

  private void readCompressedLongsNonContiguous(Blackhole bh, ForwardIndexReader<ChunkReaderContext> reader)
      throws IOException {
    try (ChunkReaderContext context = reader.createContext()) {
      for (int block = 0; block < _numBlocks / 2; block++) {
        for (int i = 0; i < _docIds.length; i++) {
          _docIds[i] = block * _blockSize + i * 2;
        }
        for (int i = 0; i < _docIds.length; i++) {
          _longBuffer[i] = reader.getLong(_docIds[i], context);
        }
        bh.consume(_longBuffer);
      }
    }
  }

  private void readCompressedDoublesNonContiguous(Blackhole bh, ForwardIndexReader<ChunkReaderContext> reader)
      throws IOException {
    try (ChunkReaderContext context = reader.createContext()) {
      for (int block = 0; block < _numBlocks / 2; block++) {
        for (int i = 0; i < _docIds.length; i++) {
          _docIds[i] = block * _blockSize + i * 2;
        }
        for (int i = 0; i < _docIds.length; i++) {
          _doubleBuffer[i] = reader.getDouble(_docIds[i], context);
        }
        bh.consume(_doubleBuffer);
      }
    }
  }

  @Benchmark
  public void readDoublesBatch(Blackhole bh)
      throws IOException {
    try (ChunkReaderContext context = _uncompressedReader.createContext()) {
      for (int block = 0; block < _numBlocks; block++) {
        for (int i = 0; i < _docIds.length; i++) {
          _docIds[i] = block * _blockSize + i;
        }
        _uncompressedReader.readValuesSV(_docIds, _docIds.length, _doubleBuffer, context);
        bh.consume(_doubleBuffer);
      }
    }
  }

  @Benchmark
  public void readDoubles(Blackhole bh)
      throws IOException {
    try (ChunkReaderContext context = _uncompressedReader.createContext()) {
      for (int block = 0; block < _numBlocks; block++) {
        for (int i = 0; i < _docIds.length; i++) {
          _docIds[i] = block * _blockSize + i;
        }
        for (int i = 0; i < _docIds.length; i++) {
          _doubleBuffer[i] = _uncompressedReader.getLong(_docIds[i], context);
        }
        bh.consume(_doubleBuffer);
      }
    }
  }

  @Benchmark
  public void readLongsBatch(Blackhole bh)
      throws IOException {
    try (ChunkReaderContext context = _uncompressedReader.createContext()) {
      for (int block = 0; block < _numBlocks; block++) {
        for (int i = 0; i < _docIds.length; i++) {
          _docIds[i] = block * _blockSize + i;
        }
        _uncompressedReader.readValuesSV(_docIds, _docIds.length, _longBuffer, context);
        bh.consume(_longBuffer);
      }
    }
  }

  @Benchmark
  public void readLongs(Blackhole bh)
      throws IOException {
    try (ChunkReaderContext context = _uncompressedReader.createContext()) {
      for (int block = 0; block < _numBlocks; block++) {
        for (int i = 0; i < _docIds.length; i++) {
          _docIds[i] = block * _blockSize + i;
        }
        for (int i = 0; i < _docIds.length; i++) {
          _longBuffer[i] = _uncompressedReader.getLong(_docIds[i], context);
        }
        bh.consume(_longBuffer);
      }
    }
  }
}
