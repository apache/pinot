/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.io.compression;

/**
 * Factory for Chunk compressors/de-compressors.
 */
public class ChunkCompressorFactory {

  // Private constructor to avoid object instantiation
  private ChunkCompressorFactory() {

  }

  public enum CompressionType {
    PASS_THROUGH(0),
    SNAPPY(1);

    private final int _value;

    CompressionType(int value) {
      _value = value;
    }

    public int getValue() {
      return _value;
    }
  }

  /**
   * Returns the chunk compressor for the specified name.
   *
   * @param compressionType Type of compressor.
   * @return Compressor for the specified type.
   */
  public static ChunkCompressor getCompressor(CompressionType compressionType) {
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
  public static ChunkDecompressor getDecompressor(CompressionType compressionType) {
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
