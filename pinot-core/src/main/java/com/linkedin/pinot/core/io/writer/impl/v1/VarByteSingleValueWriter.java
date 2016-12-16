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

import com.linkedin.pinot.core.io.compression.ChunkCompressor;
import com.linkedin.pinot.core.io.writer.SingleColumnSingleValueWriter;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
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
 */
@NotThreadSafe
public class VarByteSingleValueWriter implements SingleColumnSingleValueWriter {

  private static final int INT_SIZE = Integer.SIZE / Byte.SIZE;
  private static final Charset UTF_8 = Charset.forName("UTF-8");
  private static final int VERSION = 1;

  private final FileChannel _dataFile;
  private final ByteBuffer _header;
  private final ByteBuffer _chunkBuffer;
  private final ByteBuffer _compressedBuffer;
  private final ChunkCompressor _chunkCompressor;

  private int _chunkHeaderSize;
  private int _chunkHeaderOffset;
  private int _chunkDataOffSet;
  private int _dataOffset;

  /**
   * Constructor for the class.
   *
   * @param file File to write to.
   * @param compressor Compressor for compressing individual chunks of data.
   * @param totalDocs Total number of docs to write.
   * @param numDocsPerChunk Number of documents per chunk.
   * @param lengthOfLongestEntry Length of longest entry (in bytes).
   * @throws IOException
   */
  public VarByteSingleValueWriter(File file, ChunkCompressor compressor, int totalDocs, int numDocsPerChunk,
      int lengthOfLongestEntry)
      throws IOException {
    _chunkCompressor = compressor;

    int numChunks = (totalDocs + numDocsPerChunk - 1) / numDocsPerChunk;
    int headerSize = (numChunks + 4) * INT_SIZE; // 4 items written before chunk indexing.
    _chunkHeaderSize = numDocsPerChunk * INT_SIZE;

    _header = ByteBuffer.allocateDirect(headerSize);
    _header.putInt(VERSION);
    _header.putInt(numChunks);
    _header.putInt(numDocsPerChunk);
    _header.putInt(lengthOfLongestEntry);
    _dataOffset = headerSize;

    _chunkHeaderOffset = 0;
    _chunkDataOffSet = _chunkHeaderSize;

    int chunkSize = _chunkDataOffSet + (lengthOfLongestEntry * numDocsPerChunk);
    _chunkBuffer = ByteBuffer.allocateDirect(chunkSize);
    _compressedBuffer = ByteBuffer.allocateDirect(chunkSize * 2);
    _dataFile = new RandomAccessFile(file, "rw").getChannel();
  }

  @Override
  public void setChar(int row, char ch) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setInt(int row, int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setShort(int row, short s) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setLong(int row, long l) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setFloat(int row, float f) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setDouble(int row, double d) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setString(int row, String string)
      throws IOException {
    byte[] bytes = string.getBytes(UTF_8);
    int length = bytes.length;

    _chunkBuffer.putInt(_chunkHeaderOffset, _chunkDataOffSet);
    _chunkHeaderOffset += INT_SIZE;

    _chunkBuffer.position(_chunkDataOffSet);
    _chunkBuffer.put(bytes);
    _chunkDataOffSet += length;

    // If buffer filled, then compress and write to file.
    if (_chunkHeaderOffset == _chunkHeaderSize) {
      writeChunk();
    }
  }

  @Override
  public void setBytes(int row, byte[] bytes) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close()
      throws IOException {

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
   * @throws IOException
   */
  private void writeChunk()
      throws IOException {
    _chunkBuffer.flip();
    _compressedBuffer.flip();

    // For partially filled chunks, we still need to clear the offsets for remaining rows, as we reuse this buffer.
    for (int i = _chunkHeaderOffset; i < _chunkHeaderSize; i += INT_SIZE) {
      _chunkBuffer.putInt(_chunkHeaderOffset, 0);
    }
    int compressedSize = _chunkCompressor.compress(_chunkBuffer, _compressedBuffer);
    _dataFile.write(_compressedBuffer, _dataOffset);

    _header.putInt(_dataOffset);
    _dataOffset += compressedSize;

    _chunkBuffer.clear();
    _compressedBuffer.clear();
    _chunkHeaderOffset = 0;
    _chunkDataOffSet = _chunkHeaderSize;
  }
}
