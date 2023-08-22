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
 * Chunk-based raw (non-dictionary-encoded) forward index writer where each chunk contains fixed number of docs, and
 * each entry has fixed number of bytes.
 */
@NotThreadSafe
public class FixedByteChunkForwardIndexWriter extends BaseChunkForwardIndexWriter {
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
  public FixedByteChunkForwardIndexWriter(File file, ChunkCompressionType compressionType, int totalDocs,
      int numDocsPerChunk, int sizeOfEntry, int writerVersion)
      throws IOException {
    super(file, compressionType, totalDocs, normalizeDocsPerChunk(writerVersion, numDocsPerChunk),
        (sizeOfEntry * normalizeDocsPerChunk(writerVersion, numDocsPerChunk)), sizeOfEntry, writerVersion, true);
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

  private static int normalizeDocsPerChunk(int version, int numDocsPerChunk) {
    // V4 uses power of 2 chunk sizes for random access efficiency
    if (version >= 4 && (numDocsPerChunk & (numDocsPerChunk - 1)) != 0) {
      return 1 << (32 - Integer.numberOfLeadingZeros(numDocsPerChunk - 1));
    }
    return numDocsPerChunk;
  }
}
