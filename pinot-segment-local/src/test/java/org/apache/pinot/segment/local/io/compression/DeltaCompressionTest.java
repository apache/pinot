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
package org.apache.pinot.segment.local.io.compression;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.compression.ChunkCompressor;
import org.apache.pinot.segment.spi.compression.ChunkDecompressor;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class DeltaCompressionTest {

  @Test
  public void testRoundTripEmpty()
      throws IOException {
    long[] values = new long[]{};

    ByteBuffer input = ByteBuffer.allocateDirect(values.length * Long.BYTES);
    for (long v : values) {
      input.putLong(v);
    }
    input.flip();
    int numLongs = input.remaining() / Long.BYTES;
    assertEquals(numLongs, 0);

    try (ChunkCompressor compressor = ChunkCompressorFactory.getCompressor(ChunkCompressionType.DELTA)) {
      ByteBuffer compressed = ByteBuffer.allocateDirect(compressor.maxCompressedSize(input.limit()));
      assertEquals(input.limit(), 0);
      int compressedSize = compressor.compress(input.slice(), compressed);
      assertEquals(compressedSize, 5);

      try (ChunkDecompressor decompressor = ChunkCompressorFactory.getDecompressor(ChunkCompressionType.DELTA)) {
        int decompressedSize = decompressor.decompressedLength(compressed);
        ByteBuffer decompressed = ByteBuffer.allocateDirect(decompressedSize);
        int actualSize = decompressor.decompress(compressed, decompressed);
        assertEquals(actualSize, values.length * Long.BYTES);

        decompressed.flip();
        for (long expected : values) {
          assertEquals(decompressed.getLong(), expected);
        }
      }
    }
  }

  @Test
  public void testRoundTripSingleValue()
      throws IOException {
    long[] values = new long[]{10L};

    ByteBuffer input = ByteBuffer.allocateDirect(values.length * Long.BYTES);

    for (long v : values) {
      input.putLong(v);
    }
    input.flip();
    int numLongs = input.remaining() / Long.BYTES;
    assertEquals(numLongs, 1);

    try (ChunkCompressor compressor = ChunkCompressorFactory.getCompressor(ChunkCompressionType.DELTA)) {
      ByteBuffer compressed = ByteBuffer.allocateDirect(compressor.maxCompressedSize(input.limit()));
      assertEquals(input.limit(), 8);
      int compressedSize = compressor.compress(input.slice(), compressed);
      assertEquals(compressedSize, 13);

      try (ChunkDecompressor decompressor = ChunkCompressorFactory.getDecompressor(ChunkCompressionType.DELTA)) {
        int decompressedSize = decompressor.decompressedLength(compressed);
        assertEquals(decompressedSize, 8);
        ByteBuffer decompressed = ByteBuffer.allocateDirect(decompressedSize);
        int actualSize = decompressor.decompress(compressed, decompressed);
        assertEquals(actualSize, values.length * Long.BYTES);

        for (long expected : values) {
          assertEquals(decompressed.getLong(), expected);
        }
      }
    }
  }

  @Test
  public void testRoundTripMultiValues()
      throws IOException {
    long[] values = new long[]{10L, 12L, 15L, 21L, 30L, 30L, 31L, Long.MIN_VALUE + 10L, Long.MAX_VALUE - 10L};

    ByteBuffer input = ByteBuffer.allocateDirect(values.length * Long.BYTES);

    for (long v : values) {
      input.putLong(v);
    }
    input.flip();
    int numLongs = input.remaining() / Long.BYTES;

    try (ChunkCompressor compressor = ChunkCompressorFactory.getCompressor(ChunkCompressionType.DELTA)) {
      ByteBuffer compressed = ByteBuffer.allocateDirect(compressor.maxCompressedSize(input.limit()));
      int compressedSize = compressor.compress(input.slice(), compressed);

      try (ChunkDecompressor decompressor = ChunkCompressorFactory.getDecompressor(ChunkCompressionType.DELTA)) {
        int decompressedSize = decompressor.decompressedLength(compressed);
        ByteBuffer decompressed = ByteBuffer.allocateDirect(decompressedSize);
        int actualSize = decompressor.decompress(compressed, decompressed);
        assertEquals(actualSize, values.length * Long.BYTES);

        for (long expected : values) {
          assertEquals(decompressed.getLong(), expected);
        }
      }
    }
  }
}
