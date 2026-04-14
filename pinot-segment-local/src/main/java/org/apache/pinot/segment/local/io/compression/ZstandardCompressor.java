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

import com.github.luben.zstd.Zstd;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.pinot.segment.spi.compression.ChunkCompressor;
import org.apache.pinot.spi.config.table.CompressionCodec;


/**
 * Implementation of {@link ChunkCompressor} using Zstandard(Zstd) compression algorithm.
 *
 * <p>The default singleton {@link #INSTANCE} uses the library's default compression level.
 * Instances created via {@link #ZstandardCompressor(int)} use the specified level, which
 * must be in the range 1..22.
 */
class ZstandardCompressor implements ChunkCompressor {

  static final ZstandardCompressor INSTANCE = new ZstandardCompressor();

  private final int _compressionLevel;
  private final boolean _hasExplicitLevel;

  private ZstandardCompressor() {
    _compressionLevel = 0;
    _hasExplicitLevel = false;
  }

  /**
   * Creates a compressor with an explicit Zstandard compression level.
   *
   * @param compressionLevel the compression level (1..22)
   */
  ZstandardCompressor(int compressionLevel) {
    _compressionLevel = compressionLevel;
    _hasExplicitLevel = true;
  }

  @Override
  public int compress(ByteBuffer inUncompressed, ByteBuffer outCompressed)
      throws IOException {
    int compressedSize;
    if (_hasExplicitLevel) {
      compressedSize = Zstd.compress(outCompressed, inUncompressed, _compressionLevel);
    } else {
      compressedSize = Zstd.compress(outCompressed, inUncompressed);
    }
    // When the compress method returns successfully,
    // dstBuf's position() will be set to its current position() plus the compressed size of the data.
    // and srcBuf's position() will be set to its limit()
    // Flip operation Make the destination ByteBuffer(outCompressed) ready for read by setting the position to 0
    outCompressed.flip();
    return compressedSize;
  }

  @Override
  public int maxCompressedSize(int uncompressedSize) {
    return (int) Zstd.compressBound(uncompressedSize);
  }

  @Override
  public CompressionCodec compressionCodec() {
    return CompressionCodec.ZSTANDARD;
  }
}
