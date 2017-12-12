/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.io.writer.impl.v1;

import com.linkedin.pinot.core.io.compression.ChunkCompressorFactory;
import java.io.File;
import java.io.IOException;
import javax.annotation.concurrent.NotThreadSafe;


/**
 * Class to write out fixed length bytes into a single column.
 * Client responsible to ensure that they call the correct set method that
 * matches the sizeOfEntry. Avoiding checks here, as they can be expensive
 * when called for each row.
 *
 * The layout of the file is as follows:
 * <p> Header Section: </p>
 * <ul>
 *   <li> Integer: File format version. </li>
 *   <li> Integer: Total number of chunks. </li>
 *   <li> Integer: Number of docs per chunk. </li>
 *   <li> Integer: Length of entry (in bytes). </li>
 *   <li> Integer: Total number of docs (version 2 onwards). </li>
 *   <li> Integer: Compression type enum value (version 2 onwards). </li>
 *   <li> Integer: Start offset of data header (version 2 onwards). </li>
 *   <li> Integer array: Integer offsets for all chunks in the data .</li>
 * </ul>
 *
 * <p> Individual Chunks: </p>
 * <ul>
 *   <li> Data bytes. </li>
 * </ul>
 *
 * Only sequential writes are supported.
 */
@NotThreadSafe
public class FixedByteChunkSingleValueWriter extends BaseChunkSingleValueWriter {

  private static final int CURRENT_VERSION = 2;
  private int _chunkDataOffset;

  /**
   * Constructor for the class.
   *
   * @param file File to write to.
   * @param compressionType Type of compression to use.
   * @param totalDocs Total number of docs to write.
   * @param numDocsPerChunk Number of documents per chunk.
   * @param sizeOfEntry Size of entry (in bytes).
   * @throws IOException
   */
  public FixedByteChunkSingleValueWriter(File file, ChunkCompressorFactory.CompressionType compressionType,
      int totalDocs, int numDocsPerChunk, int sizeOfEntry)
      throws IOException {

    super(file, compressionType, totalDocs, numDocsPerChunk, (sizeOfEntry * numDocsPerChunk), sizeOfEntry,
        CURRENT_VERSION);
    _chunkDataOffset = 0;
  }

  @Override
  public void setInt(int row, int value) {
    _chunkBuffer.putInt(value);
    _chunkDataOffset += INT_SIZE;
    flushChunkIfNeeded();
  }

  @Override
  public void setFloat(int row, float value) {
    _chunkBuffer.putFloat(value);
    _chunkDataOffset += FLOAT_SIZE;
    flushChunkIfNeeded();
  }

  @Override
  public void setLong(int row, long value) {
    _chunkBuffer.putLong(value);
    _chunkDataOffset += LONG_SIZE;
    flushChunkIfNeeded();
  }

  @Override
  public void setDouble(int row, double value) {
    _chunkBuffer.putDouble(value);
    _chunkDataOffset += DOUBLE_SIZE;
    flushChunkIfNeeded();
  }

  @Override
  protected void writeChunk() {
    super.writeChunk();
    _chunkDataOffset = 0;
  }

  private void flushChunkIfNeeded() {
    // If buffer filled, then compress and write to file.
    if (_chunkDataOffset == _chunkSize) {
      writeChunk();
    }
  }
}
