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
package org.apache.pinot.segment.local.segment.index.readers.forward;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.pinot.segment.local.io.writer.impl.VarByteChunkForwardIndexWriterV6;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.compression.ChunkDecompressor;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * Reader for the V6 forward index format, extending V5 (which extends V4).
 *
 * <p>When compressed, V6 stores individual entry sizes in the chunk header instead of cumulative
 * offsets (V4). After decompression, sizes are converted to cumulative offsets in a single forward
 * pass, then V4's standard random-access read logic is reused unchanged.
 *
 * <p>When uncompressed (PASS_THROUGH), V6 writes V4 offsets directly, so V4's standard
 * uncompressed context is reused with no delta decoding overhead.
 *
 * <p>MV fixed-byte deserialization (without length prefix) is inherited from V5.
 *
 * @see VarByteChunkForwardIndexWriterV6
 */
public class VarByteChunkForwardIndexReaderV6 extends VarByteChunkForwardIndexReaderV5 {

  public VarByteChunkForwardIndexReaderV6(PinotDataBuffer dataBuffer, FieldSpec.DataType storedType,
      boolean isSingleValue) {
    super(dataBuffer, storedType, isSingleValue);
  }

  @Override
  public int getVersion() {
    return VarByteChunkForwardIndexWriterV6.VERSION;
  }

  @Override
  public ReaderContext createContext() {
    if (_chunkCompressionType == ChunkCompressionType.PASS_THROUGH) {
      // No compression — V6 writes V4 offsets directly, so reuse V4's uncompressed context
      return new UncompressedReaderContext(_chunks, _metadata, _chunksStartOffset);
    }
    return new V6CompressedReaderContext(_metadata, _chunks, _chunksStartOffset, _chunkDecompressor,
        _chunkCompressionType, _targetDecompressedChunkSize);
  }

  /**
   * Converts sizes to cumulative offsets in a single forward pass.
   * After conversion, the buffer has the same layout as V4 and standard read logic applies.
   */
  private static void convertSizesToOffsets(ByteBuffer buffer, int numDocs) {
    int headerSize = (numDocs + 1) * Integer.BYTES;
    int cumOffset = headerSize;
    int pos = Integer.BYTES;
    for (int i = 0; i < numDocs; i++) {
      int size = buffer.getInt(pos);
      buffer.putInt(pos, cumOffset);
      cumOffset += size;
      pos += Integer.BYTES;
    }
  }

  /**
   * Compressed reader context for V6. After decompression, converts sizes to offsets
   * in-place so V4's readSmallUncompressedValue works unchanged.
   */
  private static final class V6CompressedReaderContext extends CompressedReaderContext {

    V6CompressedReaderContext(PinotDataBuffer metadata, PinotDataBuffer chunks, long chunkStartOffset,
        ChunkDecompressor chunkDecompressor, ChunkCompressionType chunkCompressionType, int targetChunkSize) {
      super(metadata, chunks, chunkStartOffset, chunkDecompressor, chunkCompressionType, targetChunkSize);
    }

    @Override
    protected void decompressChunk(ByteBuffer compressed)
        throws IOException {
      super.decompressChunk(compressed);
      convertSizesToOffsets(_decompressedBuffer, _numDocsInCurrentChunk);
    }
  }
}
