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
 * Implementation of {@link ChunkCompressor} using delta compression with LZ4.
 * The delta values are further compressed using LZ4.
 */
class DeltaCompressor implements ChunkCompressor {

  static final DeltaCompressor INSTANCE = new DeltaCompressor();
  static final LZ4Factory LZ4_FACTORY = LZ4Factory.fastestInstance();
  // Only support long values for now, TODO: add support for INT
  private static final byte LONG_FLAG = 1;

  private DeltaCompressor() {
  }

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
    outCompressed.put(LONG_FLAG);
    return compressForLong(inUncompressed, outCompressed, outStartPosition);
  }

  private int compressForLong(ByteBuffer inUncompressed, ByteBuffer outCompressed, int outStartPosition)
      throws IOException {
    // Count the number of long values to be compressed.
    int numLongs = inUncompressed.remaining() / Long.BYTES;
    if (numLongs == 0) {
      outCompressed.putInt(0);
      outCompressed.flip();
      return 5; // 1 byte flag + 4 bytes for numLongs
    }

    // Store the number of long values at the start
    outCompressed.putInt(numLongs);

    // Store the first value as-is
    long prevValue = inUncompressed.getLong();
    outCompressed.putLong(prevValue);

    if (numLongs == 1) {
      outCompressed.flip();
      return outCompressed.limit() - outStartPosition;
    }

    // Create temporary buffer for delta values before LZ4 compression
    ByteBuffer deltaBuffer = ByteBuffer.allocate((numLongs - 1) * Long.BYTES);

    // Calculate deltas
    for (int i = 1; i < numLongs; i++) {
      long currentValue = inUncompressed.getLong();
      long delta = currentValue - prevValue;
      deltaBuffer.putLong(delta);
      prevValue = currentValue;
    }

    // Prepare the delta buffer for reading
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
    return outCompressed.limit() - outStartPosition;
  }

  @Override
  public int maxCompressedSize(int uncompressedSize) {
    // Add 1 byte for type flag
    int baseSize = 1;
    if (uncompressedSize % Long.BYTES != 0) {
      throw new IllegalArgumentException("Invalid input size: must be multiple of 8 bytes for LONG");
    }
    int numLongs = uncompressedSize / Long.BYTES;
    if (numLongs == 0) {
      return baseSize + 4; // flag + numLongs
    }
    if (numLongs == 1) {
      return baseSize + 12; // flag + numLongs + one long value
    }
    int deltaSize = (numLongs - 1) * Long.BYTES;
    return baseSize + 16 + LZ4_FACTORY.fastCompressor()
        .maxCompressedLength(deltaSize); // flag + numLongs + first value + compressed size + compressed data
  }

  @Override
  public ChunkCompressionType compressionType() {
    return ChunkCompressionType.DELTA;
  }
}
