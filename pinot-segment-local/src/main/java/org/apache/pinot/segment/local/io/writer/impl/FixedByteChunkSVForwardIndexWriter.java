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
import java.io.FileNotFoundException;
import java.io.IOException;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;


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
 *   <li> Integer array: Integer offsets for all chunks in the data (upto version 2),
 *   Long array: Long offsets for all chunks in the data (version 3 onwards) </li>
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
public class FixedByteChunkSVForwardIndexWriter extends BaseChunkSVForwardIndexWriter {
  private int _chunkDataOffset;

  /**
   * Constructor for the class.
   *
   * @param file File to write to.
   * @param compressionType Type of compression to use.
   * @param totalDocs Total number of docs to write.
   * @param numDocsPerChunk Number of documents per chunk.
   * @param sizeOfEntry Size of entry (in bytes)
   * @param writerVersion writer format version
   * @throws FileNotFoundException Throws {@link FileNotFoundException} if the specified file is not found.
   * @throws IOException Throws {@link IOException} if there are any errors mapping the underlying ByteBuffer.
   */
  public FixedByteChunkSVForwardIndexWriter(File file, ChunkCompressionType compressionType, int totalDocs,
      int numDocsPerChunk, int sizeOfEntry, int writerVersion)
      throws IOException {
    super(file, compressionType, totalDocs, numDocsPerChunk, (sizeOfEntry * numDocsPerChunk), sizeOfEntry,
        writerVersion);
    _chunkDataOffset = 0;
  }

  public void putInt(int value) {
    _chunkBuffer.putInt(value);
    _chunkDataOffset += Integer.BYTES;
    flushChunkIfNeeded();
  }

  public void putLong(long value) {
    _chunkBuffer.putLong(value);
    _chunkDataOffset += Long.BYTES;
    flushChunkIfNeeded();
  }

  public void putFloat(float value) {
    _chunkBuffer.putFloat(value);
    _chunkDataOffset += Float.BYTES;
    flushChunkIfNeeded();
  }

  public void putDouble(double value) {
    _chunkBuffer.putDouble(value);
    _chunkDataOffset += Double.BYTES;
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
