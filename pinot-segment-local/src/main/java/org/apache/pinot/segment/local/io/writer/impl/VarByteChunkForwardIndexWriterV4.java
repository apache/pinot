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
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.io.compression.ChunkCompressorFactory;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.compression.ChunkCompressor;
import org.apache.pinot.segment.spi.memory.CleanerUtil;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.charset.StandardCharsets.UTF_8;


/**
 * Chunk-based raw (non-dictionary-encoded) forward index writer where each chunk contains variable number of docs, and
 * the entries are variable length.
 *
 * <p>The layout of the file is as follows:
 * <ul>
 *   <li>Header Section
 *   <ul>
 *     <li>File format version (int)</li>
 *     <li>Target decompressed chunk size (int)</li>
 *     <li>Compression type enum value (int)</li>
 *     <li>Start offset of chunk data (int)</li>
 *     <li>Data header (for each chunk)
 *     <ul>
 *       <li>First docId in the chunk (int), where MSB is used to mark huge chunk</li>
 *       <li>Start offset of the chunk (unsigned int)</li>
 *     </ul>
 *     </li>
 *   </ul>
 *   </li>
 *   <li>Individual Chunks
 *   <ul>
 *     <li>Regular chunk
 *     <ul>
 *       <li>Header Section: start offsets (stored as int) of the entry within the data section</li>
 *       <li>Data Section</li>
 *     </ul>
 *     </li>
 *     <li>Huge chunk: contains one single value</li>
 *   </ul>
 *   </li>
 * </ul>
 */
@NotThreadSafe
public class VarByteChunkForwardIndexWriterV4 implements VarByteChunkWriter {
  public static final int VERSION = 4;

  private static final Logger LOGGER = LoggerFactory.getLogger(VarByteChunkForwardIndexWriterV4.class);
  private static final String DATA_BUFFER_SUFFIX = ".buf";

  private final File _dataBuffer;
  private final RandomAccessFile _output;
  private final FileChannel _dataChannel;
  private final ByteBuffer _chunkBuffer;
  private final ByteBuffer _compressionBuffer;
  private final ChunkCompressor _chunkCompressor;

  private int _docIdOffset = 0;
  private int _nextDocId = 0;
  private int _metadataSize = 0;
  private long _chunkOffset = 0;

  public VarByteChunkForwardIndexWriterV4(File file, ChunkCompressionType compressionType, int chunkSize)
      throws IOException {
    _dataBuffer = new File(file.getName() + DATA_BUFFER_SUFFIX);
    _output = new RandomAccessFile(file, "rw");
    _dataChannel = new RandomAccessFile(_dataBuffer, "rw").getChannel();
    _chunkCompressor = ChunkCompressorFactory.getCompressor(compressionType, true);
    _chunkBuffer = ByteBuffer.allocateDirect(chunkSize).order(ByteOrder.LITTLE_ENDIAN);
    _compressionBuffer =
        ByteBuffer.allocateDirect(_chunkCompressor.maxCompressedSize(chunkSize)).order(ByteOrder.LITTLE_ENDIAN);
    // reserve space for numDocs
    _chunkBuffer.position(Integer.BYTES);
    writeHeader(_chunkCompressor.compressionType(), chunkSize);
  }

  private void writeHeader(ChunkCompressionType compressionType, int targetDecompressedChunkSize)
      throws IOException {
    // keep metadata BE for backwards compatibility
    // (e.g. the version needs to be read by a factory which assumes BE)
    _output.writeInt(VERSION);
    _output.writeInt(targetDecompressedChunkSize);
    _output.writeInt(compressionType.getValue());
    // reserve a slot to write the data offset into
    _output.writeInt(0);
    _metadataSize += 4 * Integer.BYTES;
  }

  @Override
  public void putBigDecimal(BigDecimal bigDecimal) {
    putBytes(BigDecimalUtils.serialize(bigDecimal));
  }

  @Override
  public void putString(String string) {
    putBytes(string.getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public void putBytes(byte[] bytes) {
    Preconditions.checkState(_chunkOffset < (1L << 32), "exceeded 4GB of compressed chunks");
    int sizeRequired = Integer.BYTES + bytes.length;
    if (_chunkBuffer.position() > _chunkBuffer.capacity() - sizeRequired) {
      flushChunk();
      if (sizeRequired > _chunkBuffer.capacity() - Integer.BYTES) {
        writeHugeChunk(bytes);
        return;
      }
    }
    _chunkBuffer.putInt(bytes.length);
    _chunkBuffer.put(bytes);
    _nextDocId++;
  }

  @Override
  public void putStringMV(String[] values) {
    // num values + length of each value
    int headerSize = Integer.BYTES + Integer.BYTES * values.length;
    int size = headerSize;
    for (String value : values) {
      size += value.getBytes(UTF_8).length;
    }

    // Format : [numValues][length1][length2]...[lengthN][value1][value2]...[valueN]
    byte[] serializedBytes = new byte[size];
    ByteBuffer byteBuffer = ByteBuffer.wrap(serializedBytes);
    byteBuffer.putInt(values.length);
    byteBuffer.position(headerSize);
    for (int i = 0; i < values.length; i++) {
      byte[] utf8 = values[i].getBytes(UTF_8);
      byteBuffer.putInt((i + 1) * Integer.BYTES, utf8.length);
      byteBuffer.put(utf8);
    }

    putBytes(byteBuffer.array());
  }

  @Override
  public void putBytesMV(byte[][] values) {
    // num values + length of each value
    int headerSize = Integer.BYTES + Integer.BYTES * values.length;
    int size = headerSize;
    for (byte[] value : values) {
      size += value.length;
    }

    // Format : [numValues][length1][length2]...[lengthN][bytes1][bytes2]...[bytesN]
    byte[] serializedBytes = new byte[size];
    ByteBuffer byteBuffer = ByteBuffer.wrap(serializedBytes);
    byteBuffer.putInt(values.length);
    byteBuffer.position(headerSize);
    for (int i = 0; i < values.length; i++) {
      byteBuffer.putInt((i + 1) * Integer.BYTES, values[i].length);
      byteBuffer.put(values[i]);
    }

    putBytes(byteBuffer.array());
  }

  private void writeHugeChunk(byte[] bytes) {
    // huge values where the bytes and their length prefix don't fit in to the remainder of the buffer after the prefix
    // for the number of documents in a regular chunk are written as a single value without metadata, and these chunks
    // are detected by marking the MSB in the doc id offset
    final ByteBuffer buffer;
    if (_chunkCompressor.compressionType() == ChunkCompressionType.SNAPPY
        || _chunkCompressor.compressionType() == ChunkCompressionType.ZSTANDARD) {
      // SNAPPY and ZSTANDARD libraries don't work with on heap buffers,
      // so the already allocated bytes are not good enough
      buffer = ByteBuffer.allocateDirect(bytes.length);
      buffer.put(bytes);
      buffer.flip();
    } else {
      buffer = ByteBuffer.wrap(bytes);
    }
    try {
      _nextDocId++;
      write(buffer, true);
    } finally {
      CleanerUtil.cleanQuietly(buffer);
    }
  }

  private void flushChunk() {
    if (_nextDocId > _docIdOffset) {
      writeChunk();
    }
  }

  private void writeChunk() {
    /*
    This method translates from the current state of the buffer, assuming there are 3 values of lengths a,b, and c:
    [-][a][a bytes][b][b bytes][c][c bytes]
    to:
    [3][16][a+16][a+b+16][a bytes][b bytes][c bytes]
    [------16-bytes-----][----a+b+c bytes----------]
     */
    int numDocs = _nextDocId - _docIdOffset;
    _chunkBuffer.putInt(0, numDocs);
    // collect offsets
    int[] offsets = new int[numDocs];
    int offset = Integer.BYTES;
    for (int i = 0; i < numDocs; i++) {
      offsets[i] = offset;
      int size = _chunkBuffer.getInt(offset);
      offset += size + Integer.BYTES;
    }
    // now iterate backwards shifting variable length content backwards to make space for prefixes at the start
    // this pays for itself by allowing random access to readers
    int limit = _chunkBuffer.position();
    int accumulatedOffset = Integer.BYTES;
    for (int i = numDocs - 2; i >= 0; i--) {
      int length = _chunkBuffer.getInt(offsets[i]);
      ByteBuffer source = _chunkBuffer.duplicate();
      int copyFrom = offsets[i] + Integer.BYTES;
      source.position(copyFrom).limit(copyFrom + length);
      _chunkBuffer.position(copyFrom + accumulatedOffset);
      _chunkBuffer.put(source);
      offsets[i + 1] = _chunkBuffer.position();
      accumulatedOffset += Integer.BYTES;
    }
    offsets[0] = Integer.BYTES * (numDocs + 1);
    // write the offsets into the space created at the front
    _chunkBuffer.position(Integer.BYTES);
    _chunkBuffer.asIntBuffer().put(offsets);
    _chunkBuffer.position(0);
    _chunkBuffer.limit(limit);
    write(_chunkBuffer, false);
    clearChunkBuffer();
  }

  private void write(ByteBuffer buffer, boolean huge) {
    ByteBuffer mapped = null;
    final int compressedSize;
    try {
      if (huge) {
        // the compression buffer isn't guaranteed to be large enough for huge chunks,
        // so use mmap and compress directly into the file if this ever happens
        int maxCompressedSize = _chunkCompressor.maxCompressedSize(buffer.limit());
        mapped = _dataChannel.map(FileChannel.MapMode.READ_WRITE, _chunkOffset, maxCompressedSize)
            .order(ByteOrder.LITTLE_ENDIAN);
        compressedSize = _chunkCompressor.compress(buffer, mapped);
        _dataChannel.position(_chunkOffset + compressedSize);
      } else {
        compressedSize = _chunkCompressor.compress(buffer, _compressionBuffer);
        int written = 0;
        while (written < compressedSize) {
          written += _dataChannel.write(_compressionBuffer);
        }
      }
      // reverse bytes here because the file writes BE and we want to read the metadata LE
      _output.writeInt(Integer.reverseBytes(_docIdOffset | (huge ? 0x80000000 : 0)));
      _output.writeInt(Integer.reverseBytes((int) (_chunkOffset & 0xFFFFFFFFL)));
      _metadataSize += 8;
      _chunkOffset += compressedSize;
      _docIdOffset = _nextDocId;
    } catch (IOException e) {
      LOGGER.error("Exception caught while compressing/writing data chunk", e);
      throw new RuntimeException(e);
    } finally {
      if (mapped != null) {
        CleanerUtil.cleanQuietly(mapped);
      } else {
        _compressionBuffer.clear();
      }
    }
  }

  private void clearChunkBuffer() {
    _chunkBuffer.clear();
    _chunkBuffer.position(Integer.BYTES);
  }

  @Override
  public void close()
      throws IOException {
    flushChunk();
    // write out where the chunks start into slot reserved at offset 12
    _output.seek(3 * Integer.BYTES);
    _output.writeInt(_metadataSize);
    _output.seek(_metadataSize);
    _dataChannel.truncate(_chunkOffset);
    _output.setLength(_metadataSize + _chunkOffset);
    long total = _chunkOffset;
    long position = 0;
    while (total > 0) {
      long transferred = _dataChannel.transferTo(position, total, _output.getChannel());
      total -= transferred;
      position += transferred;
    }
    _dataChannel.close();
    _output.close();
    CleanerUtil.cleanQuietly(_compressionBuffer);
    CleanerUtil.cleanQuietly(_chunkBuffer);
    FileUtils.deleteQuietly(_dataBuffer);
  }
}
