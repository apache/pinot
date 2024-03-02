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
import net.jpountz.lz4.LZ4DecompressorWithLength;
import org.apache.pinot.segment.spi.compression.ChunkDecompressor;


/**
 * Identical to {@code LZ4Decompressor} but can determine the length of
 * the decompressed output
 */
class LZ4WithLengthDecompressor implements ChunkDecompressor {

  static final LZ4WithLengthDecompressor INSTANCE = new LZ4WithLengthDecompressor();

  private final LZ4DecompressorWithLength _decompressor;

  private LZ4WithLengthDecompressor() {
    _decompressor = new LZ4DecompressorWithLength(LZ4Compressor.LZ4_FACTORY.fastDecompressor());
  }

  @Override
  public int decompress(ByteBuffer compressedInput, ByteBuffer decompressedOutput)
      throws IOException {
    // LZ4DecompressorWithLength.decompress(src, out) should not be called directly as it can move src.position
    // beyond src.limit(). This implementation only moves dest.position
    _decompressor.decompress(compressedInput, compressedInput.position(), decompressedOutput,
        decompressedOutput.position());
    decompressedOutput.position(decompressedOutput.position() + decompressedLength(compressedInput));
    decompressedOutput.flip();
    return decompressedOutput.limit();
  }

  @Override
  public int decompressedLength(ByteBuffer compressedInput) {
    return LZ4DecompressorWithLength.getDecompressedLength(compressedInput);
  }
}
