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
package org.apache.pinot.segment.local.io.writer.impl;

import java.io.File;
import java.io.IOException;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;


/**
 * Forward index writer that extends {@link VarByteChunkForwardIndexWriterV5} and delta-encodes the chunk header
 * when compression is enabled, writing individual entry sizes instead of cumulative byte offsets.
 *
 * - **V4 chunk header**: `[numDocs][offset0][offset1]...[offsetN-1]` — cumulative byte offsets.
 * - **V6 chunk header** (compressed): `[numDocs][size0][size1]...[sizeN-1]` — individual entry sizes.
 *   Sizes compress dramatically better (e.g. 11x with ZSTD) because they are small, repetitive values.
 *   The reader converts sizes back to offsets at read time with a single forward pass.
 * - **PASS_THROUGH**: delta encoding provides no benefit, so V6 falls back to V4's offset-based header.
 *
 * @see VarByteChunkForwardIndexWriterV4
 * @see VarByteChunkForwardIndexWriterV5
 */
@NotThreadSafe
public class VarByteChunkForwardIndexWriterV6 extends VarByteChunkForwardIndexWriterV5 {
  public static final int VERSION = 6;

  private final boolean _deltaEncoding;

  public VarByteChunkForwardIndexWriterV6(File file, ChunkCompressionType compressionType, int chunkSize)
      throws IOException {
    super(file, compressionType, chunkSize);
    _deltaEncoding = compressionType != ChunkCompressionType.PASS_THROUGH;
  }

  @Override
  public int getVersion() {
    return VERSION;
  }

  /**
   * When compression is enabled, delta-encodes cumulative offsets into individual entry sizes
   * directly into the chunk buffer for better compression. When PASS_THROUGH (no compression),
   * delegates to V4's offset-based header since delta encoding provides no benefit.
   */
  @Override
  protected void writeChunkHeader(int numDocs, int[] offsets, int limit) {
    if (!_deltaEncoding) {
      super.writeChunkHeader(numDocs, offsets, limit);
      return;
    }
    _chunkBuffer.position(Integer.BYTES);
    for (int i = 0; i < numDocs - 1; i++) {
      _chunkBuffer.putInt(offsets[i + 1] - offsets[i]);
    }
    _chunkBuffer.putInt(limit - offsets[numDocs - 1]);
  }
}
