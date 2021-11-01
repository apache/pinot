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

import com.google.common.base.Preconditions;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import org.apache.pinot.segment.local.io.compression.ChunkCompressorFactory;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.compression.ChunkCompressor;
import org.apache.pinot.segment.spi.memory.CleanerUtil;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Base implementation for chunk-based single-value raw (non-dictionary-encoded) forward index writer.
 */
public abstract class BaseChunkSVForwardIndexWriter implements Closeable {
  // TODO: Remove this before release 0.5.0
  public static final int DEFAULT_VERSION = 2;
  public static final int CURRENT_VERSION = 3;

  private static final Logger LOGGER = LoggerFactory.getLogger(BaseChunkSVForwardIndexWriter.class);
  private static final int FILE_HEADER_ENTRY_CHUNK_OFFSET_SIZE_V1V2 = Integer.BYTES;
  private static final int FILE_HEADER_ENTRY_CHUNK_OFFSET_SIZE_V3 = Long.BYTES;

  private final File _file;
  private final FileChannel _dataChannel;
  private final ByteBuffer _header;
  protected final ByteBuffer _chunkBuffer;
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
   * @param version version of File
   * @throws IOException if the file isn't found or can't be mapped
   */
  protected BaseChunkSVForwardIndexWriter(File file, ChunkCompressionType compressionType, int totalDocs,
      int numDocsPerChunk, int chunkSize, int sizeOfEntry, int version)
      throws IOException {
    Preconditions.checkArgument(version == DEFAULT_VERSION || version == CURRENT_VERSION);
    _file = file;
    _headerEntryChunkOffsetSize = getHeaderEntryChunkOffsetSize(version);
    _dataOffset = headerSize(totalDocs, numDocsPerChunk, _headerEntryChunkOffsetSize);
    _chunkSize = chunkSize;
    _chunkCompressor = ChunkCompressorFactory.getCompressor(compressionType);
    _chunkBuffer = ByteBuffer.allocateDirect(chunkSize);
    _dataChannel = new RandomAccessFile(file, "rw").getChannel();
    _header = _dataChannel.map(FileChannel.MapMode.READ_WRITE, 0, _dataOffset);
    writeHeader(compressionType, totalDocs, numDocsPerChunk, sizeOfEntry, version);
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
  public void close()
      throws IOException {

    // Write the chunk if it is non-empty.
    if (_chunkBuffer.position() > 0) {
      writeChunk();
    }

    if (CleanerUtil.UNMAP_SUPPORTED) {
      CleanerUtil.getCleaner().freeBuffer(_header);
    }

    // we will have overmapped by (maxCompressedSize - actualCompressedSize) for the most recent chunk
    _dataChannel.truncate(_dataOffset);
    _dataChannel.close();
  }

  /**
   * Helper method to write header information.
   *
   * @param compressionType Compression type for the data
   * @param totalDocs Total number of records
   * @param numDocsPerChunk Number of documents per chunk
   * @param sizeOfEntry Size of each entry
   * @param version Version of file
   */
  private void writeHeader(ChunkCompressionType compressionType, int totalDocs, int numDocsPerChunk, int sizeOfEntry,
      int version) {
    int numChunks = (totalDocs + numDocsPerChunk - 1) / numDocsPerChunk;

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
  }

  private static int headerSize(int totalDocs, int numDocsPerChunk, int headerEntryChunkOffsetSize) {
    int numChunks = (totalDocs + numDocsPerChunk - 1) / numDocsPerChunk;
    return (7 * Integer.BYTES) + (numChunks * headerEntryChunkOffsetSize);
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
    int sizeWritten;
    _chunkBuffer.flip();

    int maxCompressedSize = _chunkCompressor.maxCompressedSize(_chunkBuffer.limit());
    // compress directly in to the mapped output rather keep a large buffer to compress into
    try (PinotDataBuffer compressedBuffer = PinotDataBuffer.mapFile(_file, false, _dataOffset,
        maxCompressedSize, ByteOrder.BIG_ENDIAN, "forward index chunk")) {
      ByteBuffer view = compressedBuffer.toDirectByteBuffer(0, maxCompressedSize);
      sizeWritten = _chunkCompressor.compress(_chunkBuffer, view);
    } catch (IOException e) {
      LOGGER.error("Exception caught while compressing/writing data chunk", e);
      throw new RuntimeException(e);
    }

    if (_headerEntryChunkOffsetSize == Integer.BYTES) {
      _header.putInt((int) _dataOffset);
    } else if (_headerEntryChunkOffsetSize == Long.BYTES) {
      _header.putLong(_dataOffset);
    }

    _dataOffset += sizeWritten;

    _chunkBuffer.clear();
  }
}
