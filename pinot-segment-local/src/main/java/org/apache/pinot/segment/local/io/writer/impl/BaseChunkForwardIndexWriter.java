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
import java.nio.channels.FileChannel;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.io.codec.compression.ChunkCompressorFactory;
import org.apache.pinot.segment.spi.codec.ChunkCodec;
import org.apache.pinot.segment.spi.codec.ChunkCodecPipeline;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.compression.ChunkCompressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Base implementation for chunk-based raw (non-dictionary-encoded) forward index writer where each chunk contains fixed
 * number of docs.
 *
 * <p>Forward index file format versions:
 * <ul>
 *   <li>V1 — implicit Snappy compression, no explicit compression field in header.</li>
 *   <li>V2 — added explicit compression type in header; chunk offsets stored as int.</li>
 *   <li>V3 — chunk offsets stored as long (supports &gt;2 GB data sections).</li>
 *   <li>V4 — fixed-width only; added derive-num-docs-per-chunk support
 *       (also the base for VarByte V4 writers).</li>
 *   <li>V5 — fixed-width only; similar to V4
 *       (also the base for VarByte V5 writers).</li>
 *   <li>V6 — variable-byte only ({@link VarByteChunkForwardIndexWriterV6}); delta-encodes chunk
 *       header (individual entry sizes instead of cumulative offsets) for better compression.</li>
 *   <li>V7 — fixed-width only; codec pipeline header replaces single compression type with a
 *       pipeline length and array of {@link ChunkCodec} values.
 *       Supports pre-compression transforms (DELTA, DOUBLE_DELTA, XOR).</li>
 * </ul>
 *
 * <p>The layout of the file is as follows:
 * <ul>
 *   <li>Header Section
 *   <ul>
 *     <li>File format version (int)</li>
 *     <li>Total number of chunks (int)</li>
 *     <li>Number of docs per chunk (int)</li>
 *     <li>Size of entry in bytes (int)</li>
 *     <li>Total number of docs (int)</li>
 *     <li>For V2–V5: compression type enum value (int)</li>
 *     <li>For V7: pipeline length (int) followed by N codec enum values (int each)</li>
 *     <li>Start offset of data header (int)</li>
 *     <li>Data header (start offsets for all chunks)
 *     <ul>
 *       <li>For version 2, offset is stored as int</li>
 *       <li>For version 3 onwards, offset is stored as long</li>
 *     </ul>
 *     </li>
 *   </ul>
 *   </li>
 *   <li>Individual Chunks</li>
 * </ul>
 */
public abstract class BaseChunkForwardIndexWriter implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseChunkForwardIndexWriter.class);

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
   * @param version version of File
   * @param fixed if the data type is fixed width (required for version validation)
   * @throws IOException if the file isn't found or can't be mapped
   */
  protected BaseChunkForwardIndexWriter(File file, ChunkCompressionType compressionType, int totalDocs,
      int numDocsPerChunk, long chunkSize, int sizeOfEntry, int version, boolean fixed)
      throws IOException {
    this(file, compressionType, null, sizeOfEntry, totalDocs, numDocsPerChunk, chunkSize, sizeOfEntry, version, fixed);
  }

  /**
   * Constructor with optional codec pipeline support.
   *
   * @param file Data file to write into
   * @param compressionType Type of compression (used when pipeline is null)
   * @param codecPipeline Optional codec pipeline; when non-null, requires version 7
   * @param valueSizeInBytes Size of each typed value (4 for INT, 8 for LONG); used by pipeline transforms
   * @param totalDocs Total docs to write
   * @param numDocsPerChunk Number of docs per data chunk
   * @param chunkSize Size of chunk
   * @param sizeOfEntry Size of entry (in bytes)
   * @param version version of File
   * @param fixed if the data type is fixed width
   */
  protected BaseChunkForwardIndexWriter(File file, ChunkCompressionType compressionType,
      @Nullable ChunkCodecPipeline codecPipeline, int valueSizeInBytes, int totalDocs,
      int numDocsPerChunk, long chunkSize, int sizeOfEntry, int version, boolean fixed)
      throws IOException {
    boolean hasPipeline = codecPipeline != null;
    Preconditions.checkArgument(
        version == 2 || version == 3 || version == 6
            || (fixed && (version == 4 || version == 5 || version == 7)),
        "Illegal version: %s for %s bytes values", version, fixed ? "fixed" : "variable");
    if (hasPipeline) {
      Preconditions.checkArgument(version == 7, "codecPipeline requires writer version 7, got: %s", version);
    }
    if (version == 7) {
      Preconditions.checkArgument(hasPipeline,
          "Writer version 7 requires a non-null codecPipeline (version 7 header layout is pipeline-only)");
    }
    if (hasPipeline && codecPipeline.hasTransforms()) {
      Preconditions.checkArgument(valueSizeInBytes == Integer.BYTES || valueSizeInBytes == Long.BYTES,
          "Codec pipeline transforms require valueSizeInBytes to be 4 (INT/FLOAT) or 8 (LONG/DOUBLE), got: %s",
          valueSizeInBytes);
    }
    Preconditions.checkArgument(chunkSize <= Integer.MAX_VALUE, "Chunk size limited to 2GB");
    _chunkSize = (int) chunkSize;
    if (hasPipeline) {
      _chunkCompressor = ChunkCompressorFactory.getCompressor(codecPipeline, valueSizeInBytes);
    } else {
      _chunkCompressor = ChunkCompressorFactory.getCompressor(compressionType);
    }
    _headerEntryChunkOffsetSize = version == 2 ? Integer.BYTES : Long.BYTES;
    _dataOffset = writeHeader(compressionType, codecPipeline, totalDocs, numDocsPerChunk, sizeOfEntry, version);
    _chunkBuffer = ByteBuffer.allocateDirect(_chunkSize);
    int maxCompressedChunkSize = _chunkCompressor.maxCompressedSize(_chunkSize);
    _compressedBuffer = ByteBuffer.allocateDirect(maxCompressedChunkSize);
    _dataFile = new RandomAccessFile(file, "rw").getChannel();
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
    _chunkCompressor.close();
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
  /**
   * Writes the header for the forward index file.
   *
   * <p>For version ≤ 5, the header layout is:
   * [version][numChunks][numDocsPerChunk][sizeOfEntry][totalDocs][compressionType][dataHeaderStart]
   *
   * <p>For version 7 (codec pipeline), the header layout is:
   * [version][numChunks][numDocsPerChunk][sizeOfEntry][totalDocs][pipelineLength][codec0]...[codecN-1][dataHeaderStart]
   */
  private int writeHeader(ChunkCompressionType compressionType, @Nullable ChunkCodecPipeline codecPipeline,
      int totalDocs, int numDocsPerChunk, int sizeOfEntry, int version) {
    int numChunks = (totalDocs + numDocsPerChunk - 1) / numDocsPerChunk;

    // Calculate fixed header size based on version
    int fixedHeaderInts;
    if (version == 7 && codecPipeline != null) {
      // version + numChunks + numDocsPerChunk + sizeOfEntry + totalDocs + pipelineLength + N codec ints +
      // dataHeaderStart
      fixedHeaderInts = 6 + codecPipeline.size() + 1;
    } else {
      // version + numChunks + numDocsPerChunk + sizeOfEntry + totalDocs + compressionType + dataHeaderStart
      fixedHeaderInts = 7;
    }
    int headerSize = (fixedHeaderInts * Integer.BYTES) + (numChunks * _headerEntryChunkOffsetSize);

    _header = ByteBuffer.allocateDirect(headerSize);

    _header.putInt(version);
    _header.putInt(numChunks);
    _header.putInt(numDocsPerChunk);
    _header.putInt(sizeOfEntry);
    _header.putInt(totalDocs);

    if (version == 7 && codecPipeline != null) {
      // Write pipeline: length + codec values
      _header.putInt(codecPipeline.size());
      for (ChunkCodec codec : codecPipeline.getStages()) {
        _header.putInt(codec.getValue());
      }
    } else {
      // Legacy: single compression type
      _header.putInt(compressionType.getValue());
    }

    // dataHeaderStart = current position + sizeof(int) for the dataHeaderStart field itself
    int dataHeaderStart = _header.position() + Integer.BYTES;
    _header.putInt(dataHeaderStart);

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
      Preconditions.checkState(_dataOffset <= Integer.MAX_VALUE, "Integer overflow detected. "
          + "Try to use raw version 3 or 4, reduce targetDocsPerChunk or targetMaxChunkSize");
      _header.putInt((int) _dataOffset);
    } else if (_headerEntryChunkOffsetSize == Long.BYTES) {
      _header.putLong(_dataOffset);
    }

    _dataOffset += sizeToWrite;
    _chunkBuffer.clear();
  }
}
