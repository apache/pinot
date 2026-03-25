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
import java.io.ByteArrayOutputStream;
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
import org.apache.pinot.segment.local.utils.ArraySerDeUtils;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.compression.ChunkCompressor;
import org.apache.pinot.segment.spi.memory.CleanerUtil;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Forward index writer that uses a two-stream chunk format for improved compression.
 *
 * <p>In V4, each chunk interleaves per-entry offsets with data:
 * {@code [numDocs][offset0][offset1]...[offsetN][data0][data1]...[dataN]}
 * and compresses the whole thing as one block. The offset table adds overhead that hurts compression.
 *
 * <p>V6 splits each chunk into two independently compressed streams:
 * <ul>
 *   <li><b>Size stream</b>: {@code [numDocs][size0][size1]...[sizeN-1]} — highly compressible
 *       because entry sizes tend to be repetitive</li>
 *   <li><b>Data stream</b>: {@code [bytes0][bytes1]...[bytesN-1]} — compresses better without
 *       interleaved size integers</li>
 * </ul>
 *
 * <p>On disk each regular chunk is laid out as:
 * <pre>
 *   [sizeStreamCompressedLength (4 bytes LE)][compressed size stream][compressed data stream]
 * </pre>
 *
 * <p>Like V5, multi-value fixed-byte arrays are serialized without an explicit element count
 * (the count is inferred from {@code bytes.length / dataType.size()}).
 *
 * <p>The file-level header and per-chunk metadata entries are identical to V4/V5.
 *
 * @see VarByteChunkForwardIndexWriterV4
 * @see VarByteChunkForwardIndexWriterV5
 */
@NotThreadSafe
public class VarByteChunkForwardIndexWriterV6 implements VarByteChunkWriter {
  public static final int VERSION = 6;

  private final Logger _logger = LoggerFactory.getLogger(getClass());

  private static final String DATA_BUFFER_SUFFIX = ".buf";

  private final File _dataBuffer;
  private final RandomAccessFile _output;
  private final FileChannel _dataChannel;
  private final ChunkCompressor _chunkCompressor;

  // Data buffer: holds raw entry bytes (no sizes interleaved)
  private final ByteBuffer _chunkDataBuffer;
  // Size stream: uses ByteArrayOutputStream to grow dynamically
  private final ByteArrayOutputStream _chunkSizeStream;
  // Compression output buffer for the data stream
  private final ByteBuffer _dataCompressionBuffer;

  private int _docIdOffset = 0;
  private int _nextDocId = 0;
  private int _metadataSize = 0;
  private long _chunkOffset = 0;

  public VarByteChunkForwardIndexWriterV6(File file, ChunkCompressionType compressionType, int chunkSize)
      throws IOException {
    _dataBuffer = new File(file.getParentFile(), file.getName() + DATA_BUFFER_SUFFIX);
    _output = new RandomAccessFile(file, "rw");
    _dataChannel = new RandomAccessFile(_dataBuffer, "rw").getChannel();
    _chunkCompressor = ChunkCompressorFactory.getCompressor(compressionType, true);

    _chunkDataBuffer = ByteBuffer.allocateDirect(chunkSize).order(ByteOrder.LITTLE_ENDIAN);
    // Initial capacity: numDocs (4 bytes) + some entries; grows as needed
    _chunkSizeStream = new ByteArrayOutputStream(Integer.BYTES + 256 * Integer.BYTES);
    // Reserve space for numDocs at position 0
    writeLittleEndianInt(_chunkSizeStream, 0);

    _dataCompressionBuffer =
        ByteBuffer.allocateDirect(_chunkCompressor.maxCompressedSize(chunkSize)).order(ByteOrder.LITTLE_ENDIAN);

    writeHeader(_chunkCompressor.compressionType(), chunkSize);
  }

  private void writeHeader(ChunkCompressionType compressionType, int targetDecompressedChunkSize)
      throws IOException {
    // keep metadata BE for backwards compatibility
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
    Preconditions.checkState(_chunkOffset < (1L << 32),
        "exceeded 4GB of compressed chunks for: " + _dataBuffer.getName());
    if (_chunkDataBuffer.remaining() < bytes.length) {
      flushChunk();
      if (bytes.length > _chunkDataBuffer.capacity()) {
        writeHugeChunk(bytes);
        return;
      }
    }
    writeLittleEndianInt(_chunkSizeStream, bytes.length);
    _chunkDataBuffer.put(bytes);
    _nextDocId++;
  }

  @Override
  public void putIntMV(int[] values) {
    putBytes(ArraySerDeUtils.serializeIntArrayWithoutLength(values));
  }

  @Override
  public void putLongMV(long[] values) {
    putBytes(ArraySerDeUtils.serializeLongArrayWithoutLength(values));
  }

  @Override
  public void putFloatMV(float[] values) {
    putBytes(ArraySerDeUtils.serializeFloatArrayWithoutLength(values));
  }

  @Override
  public void putDoubleMV(double[] values) {
    putBytes(ArraySerDeUtils.serializeDoubleArrayWithoutLength(values));
  }

  @Override
  public void putStringMV(String[] values) {
    putBytes(ArraySerDeUtils.serializeStringArray(values));
  }

  @Override
  public void putBytesMV(byte[][] values) {
    putBytes(ArraySerDeUtils.serializeBytesArray(values));
  }

  private void writeHugeChunk(byte[] bytes) {
    final ByteBuffer buffer;
    if (_chunkCompressor.compressionType() == ChunkCompressionType.SNAPPY
        || _chunkCompressor.compressionType() == ChunkCompressionType.ZSTANDARD) {
      buffer = ByteBuffer.allocateDirect(bytes.length);
      buffer.put(bytes);
      buffer.flip();
    } else {
      buffer = ByteBuffer.wrap(bytes);
    }
    try {
      _nextDocId++;
      writeHugeChunkToFile(buffer);
    } finally {
      CleanerUtil.cleanQuietly(buffer);
    }
  }

  private void writeHugeChunkToFile(ByteBuffer buffer) {
    ByteBuffer mapped = null;
    try {
      int maxCompressedSize = _chunkCompressor.maxCompressedSize(buffer.limit());
      mapped = _dataChannel.map(FileChannel.MapMode.READ_WRITE, _chunkOffset, maxCompressedSize)
          .order(ByteOrder.LITTLE_ENDIAN);
      int compressedSize = _chunkCompressor.compress(buffer, mapped);
      _dataChannel.position(_chunkOffset + compressedSize);
      // MSB marks huge chunk
      _output.writeInt(Integer.reverseBytes(_docIdOffset | 0x80000000));
      _output.writeInt(Integer.reverseBytes((int) (_chunkOffset & 0xFFFFFFFFL)));
      _metadataSize += 8;
      _chunkOffset += compressedSize;
      _docIdOffset = _nextDocId;
    } catch (IOException e) {
      _logger.error("Exception caught while compressing/writing huge chunk", e);
      throw new RuntimeException(e);
    } finally {
      if (mapped != null) {
        CleanerUtil.cleanQuietly(mapped);
      }
    }
  }

  private void flushChunk() {
    if (_nextDocId > _docIdOffset) {
      writeChunk();
    }
  }

  private void writeChunk() {
    int numDocs = _nextDocId - _docIdOffset;
    // Write numDocs at position 0 in the size stream (overwrite the reserved slot)
    byte[] sizeBytes = _chunkSizeStream.toByteArray();
    sizeBytes[0] = (byte) numDocs;
    sizeBytes[1] = (byte) (numDocs >> 8);
    sizeBytes[2] = (byte) (numDocs >> 16);
    sizeBytes[3] = (byte) (numDocs >> 24);

    _chunkDataBuffer.flip();
    ByteBuffer sizeCompressionBuffer = null;
    try {
      // Wrap size bytes for compression (needs direct buffer for some compressors)
      ByteBuffer sizeBuffer;
      if (_chunkCompressor.compressionType() == ChunkCompressionType.SNAPPY
          || _chunkCompressor.compressionType() == ChunkCompressionType.ZSTANDARD) {
        sizeBuffer = ByteBuffer.allocateDirect(sizeBytes.length).order(ByteOrder.LITTLE_ENDIAN);
        sizeBuffer.put(sizeBytes);
        sizeBuffer.flip();
      } else {
        sizeBuffer = ByteBuffer.wrap(sizeBytes).order(ByteOrder.LITTLE_ENDIAN);
      }

      // Allocate compression output for sizes
      int maxSizeCompressed = _chunkCompressor.maxCompressedSize(sizeBytes.length);
      sizeCompressionBuffer = ByteBuffer.allocateDirect(maxSizeCompressed).order(ByteOrder.LITTLE_ENDIAN);

      // Compress size stream
      int sizeCompressedLen = _chunkCompressor.compress(sizeBuffer, sizeCompressionBuffer);
      CleanerUtil.cleanQuietly(sizeBuffer);

      // Compress data stream
      int dataCompressedLen = _chunkDataBuffer.remaining() > 0
          ? _chunkCompressor.compress(_chunkDataBuffer, _dataCompressionBuffer) : 0;

      // Write: [sizeCompressedLen (4 bytes LE)][compressed sizes][compressed data]
      ByteBuffer lenBuf = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
      lenBuf.putInt(sizeCompressedLen);
      lenBuf.flip();
      while (lenBuf.hasRemaining()) {
        _dataChannel.write(lenBuf);
      }

      int written = 0;
      while (written < sizeCompressedLen) {
        written += _dataChannel.write(sizeCompressionBuffer);
      }

      if (dataCompressedLen > 0) {
        written = 0;
        while (written < dataCompressedLen) {
          written += _dataChannel.write(_dataCompressionBuffer);
        }
      }

      int totalCompressedSize = Integer.BYTES + sizeCompressedLen + dataCompressedLen;
      _output.writeInt(Integer.reverseBytes(_docIdOffset));
      _output.writeInt(Integer.reverseBytes((int) (_chunkOffset & 0xFFFFFFFFL)));
      _metadataSize += 8;
      _chunkOffset += totalCompressedSize;
      _docIdOffset = _nextDocId;
    } catch (IOException e) {
      _logger.error("Exception caught while compressing/writing data chunk", e);
      throw new RuntimeException(e);
    } finally {
      _dataCompressionBuffer.clear();
      CleanerUtil.cleanQuietly(sizeCompressionBuffer);
    }
    clearChunkBuffers();
  }

  private void clearChunkBuffers() {
    _chunkDataBuffer.clear();
    _chunkSizeStream.reset();
    // Reserve space for numDocs at position 0
    writeLittleEndianInt(_chunkSizeStream, 0);
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
    CleanerUtil.cleanQuietly(_dataCompressionBuffer);
    CleanerUtil.cleanQuietly(_chunkDataBuffer);
    FileUtils.deleteQuietly(_dataBuffer);
    _chunkCompressor.close();
  }

  private static void writeLittleEndianInt(ByteArrayOutputStream out, int value) {
    out.write(value & 0xFF);
    out.write((value >> 8) & 0xFF);
    out.write((value >> 16) & 0xFF);
    out.write((value >> 24) & 0xFF);
  }
}
