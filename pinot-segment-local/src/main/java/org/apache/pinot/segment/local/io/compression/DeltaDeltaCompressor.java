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
import net.jpountz.lz4.LZ4Factory;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.compression.ChunkCompressor;


/**
 * A Delta of delta implementation of {@link ChunkCompressor}, that simply returns the input uncompressed data
 * with performing delta of delta encoding. This is useful in cases where cost of de-compression out-weighs benefit of
 * compression.
 */
class DeltaDeltaCompressor implements ChunkCompressor {

  static final DeltaDeltaCompressor INSTANCE = new DeltaDeltaCompressor();
  static final LZ4Factory LZ4_FACTORY = LZ4Factory.fastestInstance();
  // Only support long values for now, TODO: add support for INT
  private static final byte LONG_FLAG = 1;

  private DeltaDeltaCompressor() {
  }

  /**
   * The compression works by:
   * (1) Storing the first value as-is
   * (2) Computing and storing the first delta (difference between second and first value)
   * (3) For all subsequent values, storing the difference between consecutive deltas (delta of delta)
   * (4) During decompression, the process is reversed to reconstruct the original values
   *
   * The following scenarios data will be benefited from delta of delta compression
   * The data is sorted
   * The differences between consecutive values are relatively constant
   * The data consists of integers (it's specifically designed for integer sequences)
   * */
  @Override
  public int compress(ByteBuffer inUncompressed, ByteBuffer outCompressed)
      throws IOException {
    // Store original position to calculate compressed size
    int outStartPosition = outCompressed.position();

    // TODO: add support for INT
    int remaining = inUncompressed.remaining();
    if (remaining % Long.BYTES != 0) {
      throw new IOException("Invalid input size: must be multiple of 8 bytes for LONG");
    }
    return compressForLong(inUncompressed, outCompressed, outStartPosition);
  }

  private int compressForLong(ByteBuffer inUncompressed, ByteBuffer outCompressed, int startPosition)
      throws IOException {
    outCompressed.put(LONG_FLAG);
    // Get number of longs to compress
    int numLongs = inUncompressed.remaining() / Long.BYTES;
    if (numLongs == 0) {
      outCompressed.putInt(0);
      outCompressed.flip();
      return 5; // 1 byte flag + 4 bytes for numLongs
    }

    // Store number of longs at the start
    outCompressed.putInt(numLongs);

    // Store first value as-is
    long prevValue = inUncompressed.getLong();
    outCompressed.putLong(prevValue);

    if (numLongs == 1) {
      outCompressed.flip();
      return outCompressed.limit() - startPosition;
    }

    // Create temporary buffer for delta values before LZ4 compression
    ByteBuffer deltaBuffer = ByteBuffer.allocate((numLongs - 1) * Long.BYTES);

    // Store first delta
    long prevDelta = inUncompressed.getLong() - prevValue;
    deltaBuffer.putLong(prevDelta);
    prevValue += prevDelta;

    // Calculate remaining deltas
    for (int i = 2; i < numLongs; i++) {
      long currentValue = inUncompressed.getLong();
      long currentDelta = currentValue - prevValue;
      long deltaOfDelta = currentDelta - prevDelta;

      deltaBuffer.putLong(deltaOfDelta);

      prevValue = currentValue;
      prevDelta = currentDelta;
    }

    // Prepare delta buffer for reading
    deltaBuffer.flip();

    // Reserve space for compressed size
    outCompressed.position(outCompressed.position() + Integer.BYTES);
    int compressedStart = outCompressed.position();

    // Compress delta values using LZ4
    LZ4_FACTORY.fastCompressor().compress(deltaBuffer, outCompressed);

    // Record compressed size
    int compressedSize = outCompressed.position() - compressedStart;
    outCompressed.putInt(compressedStart - Integer.BYTES, compressedSize);

    // Make buffer ready for reading
    outCompressed.flip();
    return outCompressed.limit() - startPosition;
  }

  @Override
  public int maxCompressedSize(int uncompressedSize) {
    // Add 1 byte for int or long flag
    int flagSize = 1;

    // todo: add support for INT
    int numLongs = uncompressedSize / Long.BYTES;
    if (numLongs == 0) {
      return flagSize + 4; // flag + num of Longs
    }
    if (numLongs == 1) {
      return flagSize + 12; // flag + num of Longs + one long value
    }
    int deltaSize = (numLongs - 1) * Long.BYTES;
    // flag + num of Longs + first value + compressed size + compressed delta
    return flagSize + 16 + LZ4_FACTORY.fastCompressor().maxCompressedLength(deltaSize);
  }

  @Override
  public ChunkCompressionType compressionType() {
    return ChunkCompressionType.DELTADELTA;
  }
}
