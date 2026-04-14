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
import org.apache.pinot.segment.spi.compression.ChunkCompressor;
import org.apache.pinot.spi.config.table.CompressionCodec;


/**
 * Implementation of {@link ChunkCompressor} using LZ4 compression algorithm.
 *
 * <p>The default singleton {@link #INSTANCE} uses the LZ4 fast compressor. Instances created
 * via {@link #LZ4Compressor(int)} use LZ4HC (high compression) at the specified level, which
 * trades compression speed for smaller output.
 */
class LZ4Compressor implements ChunkCompressor {

  static final LZ4Factory LZ4_FACTORY = LZ4Factory.fastestInstance();

  static final LZ4Compressor INSTANCE = new LZ4Compressor();

  private final net.jpountz.lz4.LZ4Compressor _compressor;

  private LZ4Compressor() {
    _compressor = LZ4_FACTORY.fastCompressor();
  }

  /**
   * Creates a compressor using LZ4HC (high compression) at the specified level.
   *
   * <p>Higher levels produce smaller output at the cost of slower compression.
   * Decompression speed is unaffected by the compression level.
   *
   * @param compressionLevel the LZ4HC compression level
   */
  LZ4Compressor(int compressionLevel) {
    _compressor = LZ4_FACTORY.highCompressor(compressionLevel);
  }

  @Override
  public int compress(ByteBuffer inUncompressed, ByteBuffer outCompressed)
      throws IOException {
    _compressor.compress(inUncompressed, outCompressed);
    // When the compress method returns successfully,
    // dstBuf's position() will be set to its current position() plus the compressed size of the data.
    // and srcBuf's position() will be set to its limit()
    // Flip operation Make the destination ByteBuffer(outCompressed) ready for read by setting the position to 0
    outCompressed.flip();
    return outCompressed.limit();
  }

  @Override
  public int maxCompressedSize(int uncompressedSize) {
    return _compressor.maxCompressedLength(uncompressedSize);
  }

  @Override
  public CompressionCodec compressionCodec() {
    return CompressionCodec.LZ4;
  }
}
