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
package org.apache.pinot.common.compression;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdException;
import java.nio.ByteBuffer;


public class ZstdCompressor implements Compressor {
  public static final ZstdCompressor DEFAULT_INSTANCE = new ZstdCompressor();

  private final int _level;

  public ZstdCompressor() {
    this(Zstd.defaultCompressionLevel());
  }

  public ZstdCompressor(int level) {
    _level = level;
  }

  @Override
  public byte[] compress(byte[] input) {
    try {
      int compressedSize = (int) Zstd.compressBound(input.length);
      ByteBuffer buffer = ByteBuffer.allocate(4 + compressedSize);
      buffer.putInt(input.length); // Store original length
      int actualCompressedSize =
          (int) Zstd.compressByteArray(buffer.array(), 4, compressedSize, input, 0, input.length, _level, true);
      return java.util.Arrays.copyOfRange(buffer.array(), 0, 4 + actualCompressedSize);
    } catch (ZstdException e) {
      throw new RuntimeException("Failed to compress data using Zstd", e);
    }
  }

  @Override
  public byte[] decompress(byte[] input) {
    try {
      ByteBuffer buffer = ByteBuffer.wrap(input);
      int originalLength = buffer.getInt(); // Retrieve original length
      byte[] output = new byte[originalLength];
      long decompressedSize = Zstd.decompressByteArray(output, 0, originalLength, input, 4, input.length - 4);
      if (decompressedSize != originalLength) {
        throw new RuntimeException(
            "Decompressed size mismatch: expected " + originalLength + " but got " + decompressedSize);
      }
      return output;
    } catch (ZstdException e) {
      throw new RuntimeException("Failed to decompress data using Zstd", e);
    }
  }
}
