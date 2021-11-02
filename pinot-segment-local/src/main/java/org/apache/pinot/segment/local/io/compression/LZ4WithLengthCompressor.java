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
import net.jpountz.lz4.LZ4CompressorWithLength;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.compression.ChunkCompressor;


/**
 * Identical to {@code LZ4Compressor} but prefixes the chunk with the
 * decompressed length.
 */
class LZ4WithLengthCompressor implements ChunkCompressor {

  static final LZ4WithLengthCompressor INSTANCE = new LZ4WithLengthCompressor();

  private final LZ4CompressorWithLength _compressor;

  private LZ4WithLengthCompressor() {
    _compressor = new LZ4CompressorWithLength(LZ4Compressor.LZ4_FACTORY.fastCompressor());
  }

  @Override
  public int compress(ByteBuffer inUncompressed, ByteBuffer outCompressed)
      throws IOException {
    _compressor.compress(inUncompressed, outCompressed);
    outCompressed.flip();
    return outCompressed.limit();
  }

  @Override
  public int maxCompressedSize(int uncompressedSize) {
    return _compressor.maxCompressedLength(uncompressedSize);
  }

  @Override
  public ChunkCompressionType compressionType() {
    return ChunkCompressionType.LZ4_LENGTH_PREFIXED;
  }
}
