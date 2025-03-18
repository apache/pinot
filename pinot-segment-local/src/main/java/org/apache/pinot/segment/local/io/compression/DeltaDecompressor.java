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
import org.apache.pinot.segment.spi.compression.ChunkDecompressor;


/**
 * Implementation of {@link ChunkDecompressor} for delta compression with LZ4.
 * This decompressor reconstructs the original integer sequence from LZ4 compressed delta encoded values.
 */
class DeltaDecompressor implements ChunkDecompressor {

  static final DeltaDecompressor INSTANCE = new DeltaDecompressor();
  private static final LZ4Factory LZ4_FACTORY = LZ4Factory.fastestInstance();
  private static final byte INT_FLAG = 0;
  private static final byte LONG_FLAG = 1;

  private DeltaDecompressor() {
  }

  @Override
  public int decompress(ByteBuffer compressedInput, ByteBuffer decompressedOutput)
      throws IOException {
    // Read type flag
    byte flag = compressedInput.get();
    if (flag == INT_FLAG) {
      return decompressForInt(compressedInput, decompressedOutput);
    } else if (flag == LONG_FLAG) {
      return decompressForLong(compressedInput, decompressedOutput);
    } else {
      throw new IOException("Invalid input: " + flag);
    }
  }

  private int decompressForInt(ByteBuffer compressedInput, ByteBuffer decompressedOutput)
      throws IOException {
    // Get number of integers
    int numInts = compressedInput.getInt();
    if (numInts == 0) {
      decompressedOutput.flip();
      return 0;
    }

    // Get first value
    int prevValue = compressedInput.getInt();
    decompressedOutput.putInt(prevValue);

    if (numInts == 1) {
      decompressedOutput.flip();
      return Integer.BYTES;
    }

    // Get size of compressed delta values
    int compressedSize = compressedInput.getInt();

    // Create temporary buffer for decompressed deltas
    ByteBuffer deltaBuffer = ByteBuffer.allocate((numInts - 1) * Integer.BYTES);

    // Get compressed delta values position
    ByteBuffer compressedDeltas = compressedInput.slice();
    compressedDeltas.limit(compressedSize);

    // Decompress delta values using LZ4
    LZ4_FACTORY.safeDecompressor().decompress(compressedDeltas, deltaBuffer);
    deltaBuffer.flip();

    // Reconstruct remaining values
    for (int i = 1; i < numInts; i++) {
      int delta = deltaBuffer.getInt();
      int currentValue = prevValue + delta;
      decompressedOutput.putInt(currentValue);
      prevValue = currentValue;
    }

    // Make buffer ready for reading
    decompressedOutput.flip();
    return numInts * Integer.BYTES;
  }

  private int decompressForLong(ByteBuffer compressedInput, ByteBuffer decompressedOutput)
      throws IOException {
    // Get number of longs
    int numLongs = compressedInput.getInt();
    if (numLongs == 0) {
      decompressedOutput.flip();
      return 0;
    }

    // Get first value
    long prevValue = compressedInput.getLong();
    decompressedOutput.putLong(prevValue);

    if (numLongs == 1) {
      decompressedOutput.flip();
      return Long.BYTES;
    }

    // Get size of compressed delta values
    int compressedSize = compressedInput.getInt();

    // Create temporary buffer for decompressed deltas
    ByteBuffer deltaBuffer = ByteBuffer.allocate((numLongs - 1) * Long.BYTES);

    // Get compressed delta values position
    ByteBuffer compressedDeltas = compressedInput.slice();
    compressedDeltas.limit(compressedSize);

    // Decompress delta values using LZ4
    LZ4_FACTORY.safeDecompressor().decompress(compressedDeltas, deltaBuffer);
    deltaBuffer.flip();

    // Reconstruct remaining values
    for (int i = 1; i < numLongs; i++) {
      long delta = deltaBuffer.getLong();
      long currentValue = prevValue + delta;
      decompressedOutput.putLong(currentValue);
      prevValue = currentValue;
    }

    // Make buffer ready for reading
    decompressedOutput.flip();
    return numLongs * Long.BYTES;
  }

  @Override
  public int decompressedLength(ByteBuffer compressedInput) {
    // flag for INT or LONG
    byte flag = compressedInput.get(compressedInput.position());
    // skip the flag
    int numValues = compressedInput.getInt(compressedInput.position() + 1);

    if (flag == INT_FLAG) {
      return numValues * Integer.BYTES;
    } else if (flag == LONG_FLAG) {
      return numValues * Long.BYTES;
    } else {
      throw new IllegalStateException("Invalid input: " + flag);
    }
  }
}
