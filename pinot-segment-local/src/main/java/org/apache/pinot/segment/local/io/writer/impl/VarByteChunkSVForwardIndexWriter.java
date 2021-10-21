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

import static java.nio.charset.StandardCharsets.UTF_8;


/**
 * Class to write out variable length bytes into a single column.
 *
 * The layout of the file is as follows:
 * <p> Header Section: </p>
 * <ul>
 * <li> Integer: File format version. </li>
 * <li> Integer: Total number of chunks. </li>
 * <li> Integer: Number of docs per chunk. </li>
 * <li> Integer: Length of longest entry (in bytes). </li>
 * <li> Integer: Total number of docs (version 2 onwards). </li>
 * <li> Integer: Compression type enum value (version 2 onwards). </li>
 * <li> Integer: Start offset of data header (version 2 onwards). </li>
 * <li> Integer array: Integer offsets for all chunks in the data (upto version 2),
 * Long array: Long offsets for all chunks in the data (version 3 onwards) </li>
 * </ul>
 *
 * <p> Individual Chunks: </p>
 * <ul>
 * <li> Integer offsets to start position of rows: For partial chunks, offset values are 0 for
 * missing rows. </li>
 * <li> Data bytes. </li>
 * </ul>
 *
 * Only sequential writes are supported.
 */
@NotThreadSafe
public class VarByteChunkSVForwardIndexWriter extends BaseChunkSVForwardIndexWriter {

  public static final int CHUNK_HEADER_ENTRY_ROW_OFFSET_SIZE = Integer.BYTES;

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
   * @param lengthOfLongestEntry Length of longest entry (in bytes)
   * @param writerVersion writer format version
   * @throws FileNotFoundException Throws {@link FileNotFoundException} if the specified file is
   *     not found.
   */
  public VarByteChunkSVForwardIndexWriter(File file, ChunkCompressionType compressionType,
      int totalDocs,
      int numDocsPerChunk, int lengthOfLongestEntry, int writerVersion)
      throws IOException {
    super(file, compressionType, totalDocs, numDocsPerChunk,
        numDocsPerChunk * (CHUNK_HEADER_ENTRY_ROW_OFFSET_SIZE + lengthOfLongestEntry),
        // chunkSize
        lengthOfLongestEntry, writerVersion);

    _chunkHeaderOffset = 0;
    _chunkHeaderSize = numDocsPerChunk * CHUNK_HEADER_ENTRY_ROW_OFFSET_SIZE;
    _chunkDataOffSet = _chunkHeaderSize;
  }

  public void putString(String value) {
    putBytes(value.getBytes(UTF_8));
  }

  public void putBytes(byte[] value) {
    _chunkBuffer.putInt(_chunkHeaderOffset, _chunkDataOffSet);
    _chunkHeaderOffset += CHUNK_HEADER_ENTRY_ROW_OFFSET_SIZE;

    _chunkBuffer.position(_chunkDataOffSet);
    _chunkBuffer.put(value);
    _chunkDataOffSet += value.length;

    writeChunkIfNecessary();
  }

  // Note: some duplication is tolerated between these overloads for the sake of memory efficiency

  public void putStrings(String[] values) {
    // the entire String[] will be encoded as a single string, write the header here
    _chunkBuffer.putInt(_chunkHeaderOffset, _chunkDataOffSet);
    _chunkHeaderOffset += CHUNK_HEADER_ENTRY_ROW_OFFSET_SIZE;
    // write all the strings into the data buffer as if it's a single string,
    // but with its own embedded header so offsets to strings within the body
    // can be located
    int headerPosition = _chunkDataOffSet;
    int headerSize = Integer.BYTES + Integer.BYTES * values.length;
    int bodyPosition = headerPosition + headerSize;
    _chunkBuffer.position(bodyPosition);
    int bodySize = 0;
    for (int i = 0, h = headerPosition + Integer.BYTES; i < values.length; i++, h += Integer.BYTES) {
      byte[] utf8 = values[i].getBytes(UTF_8);
      _chunkBuffer.putInt(h, utf8.length);
      _chunkBuffer.put(utf8);
      bodySize += utf8.length;
    }
    _chunkDataOffSet += headerSize + bodySize;
    // go back to write the number of strings embedded in the big string
    _chunkBuffer.putInt(headerPosition, values.length);

    writeChunkIfNecessary();
  }

  public void putByteArrays(byte[][] values) {
    // the entire byte[][] will be encoded as a single string, write the header here
    _chunkBuffer.putInt(_chunkHeaderOffset, _chunkDataOffSet);
    _chunkHeaderOffset += CHUNK_HEADER_ENTRY_ROW_OFFSET_SIZE;
    // write all the byte[]s into the data buffer as if it's a single byte[],
    // but with its own embedded header so offsets to byte[]s within the body
    // can be located
    int headerPosition = _chunkDataOffSet;
    int headerSize = Integer.BYTES + Integer.BYTES * values.length;
    int bodyPosition = headerPosition + headerSize;
    _chunkBuffer.position(bodyPosition);
    int bodySize = 0;
    for (int i = 0, h = headerPosition + Integer.BYTES; i < values.length; i++, h += Integer.BYTES) {
      byte[] utf8 = values[i];
      _chunkBuffer.putInt(h, utf8.length);
      _chunkBuffer.put(utf8);
      bodySize += utf8.length;
    }
    _chunkDataOffSet += headerSize + bodySize;
    // go back to write the number of byte[]s embedded in the big byte[]
    _chunkBuffer.putInt(headerPosition, values.length);

    writeChunkIfNecessary();
  }

  private void writeChunkIfNecessary() {
    // If buffer filled, then compress and write to file.
    if (_chunkHeaderOffset == _chunkHeaderSize) {
      writeChunk();
    }
  }

  /**
   * Helper method to compress and write the current chunk.
   * <ul>
   * <li> Chunk header is of fixed size, so fills out any remaining offsets for partially filled
   * chunks. </li>
   * <li> Compresses and writes the chunk to the data file. </li>
   * <li> Updates the header with the current chunks offset. </li>
   * <li> Clears up the buffers, so that they can be reused. </li>
   * </ul>
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
