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
package org.apache.pinot.segment.spi.compression;

import java.io.IOException;
import java.nio.ByteBuffer;


/**
 * Interface to decompress a chunk of data.
 */
public interface ChunkDecompressor {

  /**
   * This method decompresses chunk of data that was compressed using {@link
   * org.apache.pinot.segment.spi.compression.ChunkCompressor}. Assumes that size of output
   * ByteBuffer is large enough to de-compress the input.
   *
   * @param compressedInput Compressed data
   * @param decompressedOutput ByteBuffer where the decompressed data is put.
   * @return Size of decompressed data.
   * @throws IOException
   */
  int decompress(ByteBuffer compressedInput, ByteBuffer decompressedOutput)
      throws IOException;

  /**
   * Returns the length in bytes of the decompressed chunk
   * @param compressedInput compressed input
   * @return the decompressed length in bytes, if known, otherwise -1
   * @throws IOException if the buffer is not in the expected compressed format
   */
  int decompressedLength(ByteBuffer compressedInput)
      throws IOException;
}
