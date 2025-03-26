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

import java.nio.ByteBuffer;
import java.util.Arrays;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;


public class Lz4Compressor implements Compressor {
  private static final LZ4Factory FACTORY = LZ4Factory.fastestInstance();
  public static final Lz4Compressor FAST_INSTANCE = new Lz4Compressor(false);
  public static final Lz4Compressor HIGH_INSTANCE = new Lz4Compressor(true);

  private final LZ4Compressor _compressor;

  private Lz4Compressor(boolean highCompression) {
    _compressor = highCompression ? FACTORY.highCompressor() : FACTORY.fastCompressor();
  }

  @Override
  public byte[] compress(byte[] input) {
    int maxCompressedLength = _compressor.maxCompressedLength(input.length);
    ByteBuffer buffer = ByteBuffer.allocate(4 + maxCompressedLength);
    buffer.putInt(input.length); // Store original length
    int compressedSize = _compressor.compress(input, 0, input.length, buffer.array(), 4, maxCompressedLength);
    System.out.println("original size = " + input.length + ", compressed size = " + compressedSize);
    // Return only the valid portion of the array
    return Arrays.copyOfRange(buffer.array(), 0, 4 + compressedSize);
  }

  @Override
  public byte[] decompress(byte[] input) {
    LZ4FastDecompressor decompressor = FACTORY.fastDecompressor();
    ByteBuffer buffer = ByteBuffer.wrap(input);
    int originalLength = buffer.getInt(); // Retrieve original length
    byte[] restored = new byte[originalLength];
    decompressor.decompress(input, 4, restored, 0, originalLength);
    return restored;
  }
}
