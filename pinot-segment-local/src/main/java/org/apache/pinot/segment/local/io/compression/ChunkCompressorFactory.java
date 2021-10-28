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

import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.compression.ChunkCompressor;
import org.apache.pinot.segment.spi.compression.ChunkDecompressor;


/**
 * Factory for Chunk compressors/de-compressors.
 */
public class ChunkCompressorFactory {

  // Private constructor to avoid object instantiation
  private ChunkCompressorFactory() {

  }

  /**
   * Returns the chunk compressor for the specified name.
   *
   * @param compressionType Type of compressor.
   * @return Compressor for the specified type.
   */
  public static ChunkCompressor getCompressor(ChunkCompressionType compressionType) {
    return getCompressor(compressionType, false);
  }

  /**
   * Returns the chunk compressor for the specified name.
   *
   * @param compressionType Type of compressor.
   * @param upgradeToLengthPrefixed if true, guarantee the compressed chunk contains metadata about the decompressed
   *                                size. Most formats do this anyway, but LZ4 requires a length prefix.
   * @return Compressor for the specified type.
   */
  public static ChunkCompressor getCompressor(ChunkCompressionType compressionType,
      boolean upgradeToLengthPrefixed) {
    switch (compressionType) {

      case PASS_THROUGH:
        return PassThroughCompressor.INSTANCE;

      case SNAPPY:
        return SnappyCompressor.INSTANCE;

      case ZSTANDARD:
        return ZstandardCompressor.INSTANCE;

      case LZ4:
        return upgradeToLengthPrefixed ? LZ4WithLengthCompressor.INSTANCE : LZ4Compressor.INSTANCE;

      case LZ4_LENGTH_PREFIXED:
        return LZ4WithLengthCompressor.INSTANCE;

      default:
        throw new IllegalArgumentException("Illegal compressor name " + compressionType);
    }
  }

  /**
   * Returns the chunk decompressor for the specified name.
   *
   * @param compressionType Type of compression
   * @return decompressor for the specified name
   */
  public static ChunkDecompressor getDecompressor(ChunkCompressionType compressionType) {
    switch (compressionType) {
      case PASS_THROUGH:
        return PassThroughDecompressor.INSTANCE;

      case SNAPPY:
        return SnappyDecompressor.INSTANCE;

      case ZSTANDARD:
        return ZstandardDecompressor.INSTANCE;

      case LZ4:
        return LZ4Decompressor.INSTANCE;

      case LZ4_LENGTH_PREFIXED:
        return LZ4WithLengthDecompressor.INSTANCE;

      default:
        throw new IllegalArgumentException("Illegal compressor name " + compressionType);
    }
  }
}
