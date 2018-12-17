/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.common.utils.StringUtil;
import com.linkedin.pinot.core.io.compression.ChunkCompressorFactory;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import javax.annotation.concurrent.NotThreadSafe;


/**
 * Class to write out variable length bytes into a single column.
 *
 * The layout of the file is as follows:
 * <p> Header Section: </p>
 * <ul>
 *   <li> Integer: File format version. </li>
 *   <li> Integer: Total number of chunks. </li>
 *   <li> Integer: Number of docs per chunk. </li>
 *   <li> Integer: Length of longest entry (in bytes). </li>
 *   <li> Integer array: Integer offsets for all chunks in the data .</li>
 * </ul>
 *
 * <p> Individual Chunks: </p>
 * <ul>
 *   <li> Integer offsets to start position of rows: For partial chunks, offset values are 0 for missing rows. </li>
 *   <li> Data bytes. </li>
 * </ul>
 *
 * Only sequential writes are supported.
 */
@NotThreadSafe
public class VarByteChunkSingleValueWriter extends BaseChunkSingleValueWriter {
  private static final int CURRENT_VERSION = 2;

  private final int _chunkHeaderSize;
  private int _chunkHeaderOffset;
  private int _chunkDataOffSet;

  /**
   * Constructor for the class.
   *
   * @param file File to write to.
   * @param compressionType Type of compression to use.
   * @param totalDocs Total number of docs to write.
   * @param numDocsPerChunk Number of documents per chunk.
   * @param lengthOfLongestEntry Length of longest entry (in bytes).
   * @throws FileNotFoundException Throws {@link FileNotFoundException} if the specified file is not found.
   */
  public VarByteChunkSingleValueWriter(File file, ChunkCompressorFactory.CompressionType compressionType, int totalDocs,
      int numDocsPerChunk, int lengthOfLongestEntry) throws FileNotFoundException {

    super(file, compressionType, totalDocs, numDocsPerChunk,
        ((numDocsPerChunk * Integer.BYTES) + (lengthOfLongestEntry * numDocsPerChunk)), // chunkSize
        lengthOfLongestEntry, CURRENT_VERSION);

    _chunkHeaderOffset = 0;
    _chunkHeaderSize = numDocsPerChunk * Integer.BYTES;
    _chunkDataOffSet = _chunkHeaderSize;
  }

  @Override
  public void setString(int row, String string) {
    byte[] bytes = StringUtil.encodeUtf8(string);
    setBytes(row, bytes);
  }

  @Override
  public void setBytes(int row, byte[] bytes) {
    _chunkBuffer.putInt(_chunkHeaderOffset, _chunkDataOffSet);
    _chunkHeaderOffset += Integer.BYTES;

    _chunkBuffer.position(_chunkDataOffSet);
    _chunkBuffer.put(bytes);
    _chunkDataOffSet += bytes.length;

    // If buffer filled, then compress and write to file.
    if (_chunkHeaderOffset == _chunkHeaderSize) {
      writeChunk();
    }
  }

  @Override
  public void close() throws IOException {

    // Write the chunk if it is non-empty.
    if (_chunkBuffer.position() > 0) {
      writeChunk();
    }

    // Write the header and close the file.
    _header.flip();
    _dataFile.write(_header, 0);
    _dataFile.close();
  }

  /**
   * Helper method to compress and write the current chunk.
   * <ul>
   *   <li> Chunk header is of fixed size, so fills out any remaining offsets for partially filled chunks. </li>
   *   <li> Compresses and writes the chunk to the data file. </li>
   *   <li> Updates the header with the current chunks offset. </li>
   *   <li> Clears up the buffers, so that they can be reused. </li>
   * </ul>
   *
   */
  protected void writeChunk() {
    // For partially filled chunks, we still need to clear the offsets for remaining rows, as we reuse this buffer.
    for (int i = _chunkHeaderOffset; i < _chunkHeaderSize; i += Integer.BYTES) {
      _chunkBuffer.putInt(i, 0);
    }

    super.writeChunk();

    // Reset the chunk offsets.
    _chunkHeaderOffset = 0;
    _chunkDataOffSet = _chunkHeaderSize;
  }
}
