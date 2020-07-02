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
package org.apache.pinot.core.io.writer.impl.v1;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import org.apache.pinot.core.io.compression.ChunkCompressor;
import org.apache.pinot.core.io.compression.ChunkCompressorFactory;
import org.apache.pinot.core.io.writer.SingleColumnSingleValueWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Abstract implementation for {@link SingleColumnSingleValueWriter}
 * Base class for fixed and variable byte writer implementations.
 */
public abstract class BaseChunkSingleValueWriter implements SingleColumnSingleValueWriter {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseChunkSingleValueWriter.class);
  private static final int FILE_HEADER_ENTRY_CHUNK_OFFSET_SIZE_V1V2 = Integer.BYTES;
  private static final int FILE_HEADER_ENTRY_CHUNK_OFFSET_SIZE_V3 = Long.BYTES;

  protected final FileChannel _dataFile;
  protected ByteBuffer _header;
  protected final ByteBuffer _chunkBuffer;
  protected final ByteBuffer _compressedBuffer;
  protected final ChunkCompressor _chunkCompressor;

  protected int _chunkSize;
  protected long _dataOffset;

  private final int _headerEntryChunkOffsetSize;

  /**
   * Constructor for the class.
   *
   * @param file Data file to write into
   * @param compressionType Type of compression
   * @param totalDocs Total docs to write
   * @param numDocsPerChunk Number of docs per data chunk
   * @param chunkSize Size of chunk
   * @param sizeOfEntry Size of entry (in bytes), max size for variable byte implementation.
   * @param version Version of file
   * @throws FileNotFoundException
   */
  protected BaseChunkSingleValueWriter(File file, ChunkCompressorFactory.CompressionType compressionType, int totalDocs,
      int numDocsPerChunk, int chunkSize, int sizeOfEntry, int version)
      throws FileNotFoundException {
    _chunkSize = chunkSize;
    _chunkCompressor = ChunkCompressorFactory.getCompressor(compressionType);
    _headerEntryChunkOffsetSize = getHeaderEntryChunkOffsetSize(version);
    _dataOffset = writeHeader(compressionType, totalDocs, numDocsPerChunk, sizeOfEntry, version);
    _chunkBuffer = ByteBuffer.allocateDirect(chunkSize);
    _compressedBuffer = ByteBuffer.allocateDirect(chunkSize * 2);
    _dataFile = new RandomAccessFile(file, "rw").getChannel();
  }

  public static int getHeaderEntryChunkOffsetSize(int version) {
    switch (version) {
      case 1:
      case 2:
        return FILE_HEADER_ENTRY_CHUNK_OFFSET_SIZE_V1V2;
      case 3:
        return FILE_HEADER_ENTRY_CHUNK_OFFSET_SIZE_V3;
      default:
        throw new IllegalStateException("Invalid version: " + version);
    }
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
  public void setString(int row, String string) {
    throw new UnsupportedOperationException();
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
   * Helper method to write header information.
   *
   * @param compressionType Compression type for the data
   * @param totalDocs Total number of records
   * @param numDocsPerChunk Number of documents per chunk
   * @param sizeOfEntry Size of each entry
   * @param version Version of file
   * @return Size of header
   */
  private int writeHeader(ChunkCompressorFactory.CompressionType compressionType, int totalDocs, int numDocsPerChunk,
      int sizeOfEntry, int version) {
    int numChunks = (totalDocs + numDocsPerChunk - 1) / numDocsPerChunk;
    int headerSize = (7 * Integer.BYTES) + (numChunks * _headerEntryChunkOffsetSize);

    _header = ByteBuffer.allocateDirect(headerSize);

    int offset = 0;
    _header.putInt(version);
    offset += Integer.BYTES;

    _header.putInt(numChunks);
    offset += Integer.BYTES;

    _header.putInt(numDocsPerChunk);
    offset += Integer.BYTES;

    _header.putInt(sizeOfEntry);
    offset += Integer.BYTES;

    if (version > 1) {
      // Write total number of docs.
      _header.putInt(totalDocs);
      offset += Integer.BYTES;

      // Write the compressor type
      _header.putInt(compressionType.getValue());
      offset += Integer.BYTES;

      // Start of chunk offsets.
      int dataHeaderStart = offset + Integer.BYTES;
      _header.putInt(dataHeaderStart);
    }

    return headerSize;
  }

  /**
   * Helper method to compress and write the current chunk.
   * <ul>
   *   <li> Chunk header is of fixed size, so fills out any remaining offsets for partially filled chunks. </li>
   *   <li> Compresses (if required) and writes the chunk to the data file. </li>
   *   <li> Updates the header with the current chunks offset. </li>
   *   <li> Clears up the buffers, so that they can be reused. </li>
   * </ul>
   *
   */
  protected void writeChunk() {
    int sizeToWrite;
    _chunkBuffer.flip();

    try {
      sizeToWrite = _chunkCompressor.compress(_chunkBuffer, _compressedBuffer);
      _dataFile.write(_compressedBuffer, _dataOffset);
      _compressedBuffer.clear();
    } catch (IOException e) {
      LOGGER.error("Exception caught while compressing/writing data chunk", e);
      throw new RuntimeException(e);
    }

    if (_headerEntryChunkOffsetSize == Integer.BYTES) {
      _header.putInt((int)_dataOffset);
    } else if (_headerEntryChunkOffsetSize == Long.BYTES) {
      _header.putLong(_dataOffset);
    }

    _dataOffset += sizeToWrite;

    _chunkBuffer.clear();
  }
}
