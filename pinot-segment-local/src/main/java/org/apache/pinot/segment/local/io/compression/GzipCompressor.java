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
import java.util.zip.Deflater;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.compression.ChunkCompressor;


/**
 * Implementation of {@link ChunkCompressor} using GZIP compression algorithm.
 */
class GzipCompressor implements ChunkCompressor {

  private final Deflater _compressor;

  public GzipCompressor() {
    _compressor = new Deflater();
  }

  @Override
  public int compress(ByteBuffer inUncompressed, ByteBuffer outCompressed)
      throws IOException {
    _compressor.reset();
    _compressor.setInput(inUncompressed);
    _compressor.finish();
    _compressor.deflate(outCompressed);
    outCompressed.putInt((int) _compressor.getBytesRead()); // append uncompressed size
    int size = outCompressed.position();
    outCompressed.flip();
    return size;
  }

  @Override
  public int maxCompressedSize(int uncompressedSize) {
    // https://github.com/luvit/zlib/blob/8de57bce969eb9dafc1f1f5c256ac608d0a73ec4/compress.c#L75
    return uncompressedSize + (uncompressedSize >> 12) + (uncompressedSize >> 14) + (uncompressedSize >> 25) + 13
        + Integer.BYTES;
  }

  @Override
  public ChunkCompressionType compressionType() {
    return ChunkCompressionType.GZIP;
  }

  @Override
  public void close()
      throws IOException {
    _compressor.end();
  }
}
