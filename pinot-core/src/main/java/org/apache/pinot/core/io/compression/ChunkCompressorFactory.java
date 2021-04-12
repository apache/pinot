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
package org.apache.pinot.core.io.compression;

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
    switch (compressionType) {

      case PASS_THROUGH:
        return new PassThroughCompressor();

      case SNAPPY:
        return new SnappyCompressor();

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
        return new PassThroughDecompressor();

      case SNAPPY:
        return new SnappyDecompressor();

      default:
        throw new IllegalArgumentException("Illegal compressor name " + compressionType);
    }
  }
}
