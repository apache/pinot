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
package org.apache.pinot.segment.local.segment.index.readers.forward;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.io.compression.ChunkCompressorFactory;
import org.apache.pinot.segment.local.io.writer.impl.VarByteChunkSVForwardIndexWriterV4;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.compression.ChunkDecompressor;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.memory.CleanerUtil;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class VarByteChunkSVForwardIndexReaderV4
    implements ForwardIndexReader<VarByteChunkSVForwardIndexReaderV4.ReaderContext>,
               ForwardIndexReader.ValueRangeProvider<VarByteChunkSVForwardIndexReaderV4.ReaderContext> {

  private static final Logger LOGGER = LoggerFactory.getLogger(VarByteChunkSVForwardIndexReaderV4.class);

  private static final int METADATA_ENTRY_SIZE = 8;

  private final FieldSpec.DataType _storedType;
  private final int _targetDecompressedChunkSize;
  private final ChunkDecompressor _chunkDecompressor;
  private final ChunkCompressionType _chunkCompressionType;

  private final PinotDataBuffer _metadata;
  private final PinotDataBuffer _chunks;

  private final long _chunksStartOffset;

  public VarByteChunkSVForwardIndexReaderV4(PinotDataBuffer dataBuffer, FieldSpec.DataType storedType) {
    if (dataBuffer.getInt(0) < VarByteChunkSVForwardIndexWriterV4.VERSION) {
      throw new IllegalStateException(
          "version " + dataBuffer.getInt(0) + " < " + VarByteChunkSVForwardIndexWriterV4.VERSION);
    }
    _storedType = storedType;
    _targetDecompressedChunkSize = dataBuffer.getInt(4);
    _chunkCompressionType = ChunkCompressionType.valueOf(dataBuffer.getInt(8));
    _chunkDecompressor = ChunkCompressorFactory.getDecompressor(_chunkCompressionType);
    int chunksOffset = dataBuffer.getInt(12);
    // the file has a BE header for compatability reasons (version selection) but the content is LE
    _metadata = dataBuffer.view(16, chunksOffset, ByteOrder.LITTLE_ENDIAN);
    _chunksStartOffset = chunksOffset;
    _chunks = dataBuffer.view(chunksOffset, dataBuffer.size(), ByteOrder.LITTLE_ENDIAN);
  }

  @Override
  public boolean isDictionaryEncoded() {
    return false;
  }

  @Override
  public boolean isSingleValue() {
    return true;
  }

  @Override
  public FieldSpec.DataType getStoredType() {
    return _storedType;
  }

  @Override
  public BigDecimal getBigDecimal(int docId, ReaderContext context) {
    return BigDecimalUtils.deserialize(context.getValue(docId));
  }

  @Override
  public String getString(int docId, ReaderContext context) {
    return new String(context.getValue(docId), StandardCharsets.UTF_8);
  }

  @Override
  public byte[] getBytes(int docId, ReaderContext context) {
    return context.getValue(docId);
  }

  @Nullable
  @Override
  public ReaderContext createContext() {
    return _chunkCompressionType == ChunkCompressionType.PASS_THROUGH ? new UncompressedReaderContext(_metadata,
        _chunks, _chunksStartOffset)
        : new CompressedReaderContext(_metadata, _chunks, _chunksStartOffset, _chunkDecompressor, _chunkCompressionType,
            _targetDecompressedChunkSize);
  }

  @Override
  public void close()
      throws IOException {
  }

  @Override
  public List<ValueRange> getDocIdRange(int docId, ReaderContext context, @Nullable List<ValueRange> ranges) {
    if (ranges == null) {
      ranges = new ArrayList<>();
    }
    context.recordRangesForDocId(docId, ranges);
    return ranges;
  }

  @Override
  public boolean isFixedLengthType() {
    return false;
  }

  @Override
  public long getBaseOffset() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getDocLength() {
    throw new UnsupportedOperationException();
  }

  public static abstract class ReaderContext implements ForwardIndexReaderContext {

    protected final PinotDataBuffer _chunks;
    protected final PinotDataBuffer _metadata;
    protected int _docIdOffset;
    protected int _nextDocIdOffset;
    protected boolean _regularChunk;
    protected int _numDocsInCurrentChunk;
    protected long _chunkStartOffset;
    private List<ValueRange> _ranges;

    protected ReaderContext(PinotDataBuffer metadata, PinotDataBuffer chunks, long chunkStartOffset) {
      _chunks = chunks;
      _metadata = metadata;
      _chunkStartOffset = chunkStartOffset;
    }

    public void recordRangesForDocId(int docId, List<ValueRange> ranges) {
      if (docId >= _docIdOffset && docId < _nextDocIdOffset) {
        ranges.addAll(_ranges);
      } else {
        recordRangesForDocIdUncompressed(docId, ranges);
      }
    }

    public byte[] getValue(int docId) {
      if (docId >= _docIdOffset && docId < _nextDocIdOffset) {
        return readSmallUncompressedValue(docId);
      } else {
        try {
          return decompressAndRead(docId);
        } catch (IOException e) {
          LOGGER.error("Exception caught while decompressing data chunk", e);
          throw new RuntimeException(e);
        }
      }
    }

    protected long chunkIndexFor(int docId) {
      long low = 0;
      long high = (_metadata.size() / METADATA_ENTRY_SIZE) - 1;
      while (low <= high) {
        long mid = (low + high) >>> 1;
        long position = mid * METADATA_ENTRY_SIZE;
        int midDocId = _metadata.getInt(position) & 0x7FFFFFFF;
        if (midDocId < docId) {
          low = mid + 1;
        } else if (midDocId > docId) {
          high = mid - 1;
        } else {
          return position;
        }
      }
      return (low - 1) * METADATA_ENTRY_SIZE;
    }

    protected abstract byte[] processChunkAndReadFirstValue(int docId, long offset, long limit)
        throws IOException;

    protected abstract void recordRangesForChunk(int docId, long offset, long limit,
        List<ValueRange> ranges);
    protected abstract byte[] readSmallUncompressedValue(int docId);

    private byte[] decompressAndRead(int docId)
        throws IOException {
      long metadataEntry = chunkIndexFor(docId);
      int info = _metadata.getInt(metadataEntry);
      _docIdOffset = info & 0x7FFFFFFF;
      _regularChunk = _docIdOffset == info;
      long offset = _metadata.getInt(metadataEntry + Integer.BYTES) & 0xFFFFFFFFL;
      long limit;
      if (_metadata.size() - METADATA_ENTRY_SIZE > metadataEntry) {
        _nextDocIdOffset = _metadata.getInt(metadataEntry + METADATA_ENTRY_SIZE) & 0x7FFFFFFF;
        limit = _metadata.getInt(metadataEntry + METADATA_ENTRY_SIZE + Integer.BYTES) & 0xFFFFFFFFL;
      } else {
        _nextDocIdOffset = Integer.MAX_VALUE;
        limit = _chunks.size();
      }
      return processChunkAndReadFirstValue(docId, offset, limit);
    }

    private void recordRangesForDocIdUncompressed(int docId, List<ValueRange> ranges) {
      // We'd practically end up reading entire metadata buffer
      _ranges = new ArrayList<>();
      _ranges.add(ValueRange.newByteRange(0, (int) _metadata.size()));
      long metadataEntry = chunkIndexFor(docId);
      int info = _metadata.getInt(metadataEntry);
      _docIdOffset = info & 0x7FFFFFFF;
      _regularChunk = _docIdOffset == info;
      long offset = _metadata.getInt(metadataEntry + Integer.BYTES) & 0xFFFFFFFFL;
      long limit;
      if (_metadata.size() - METADATA_ENTRY_SIZE > metadataEntry) {
        _nextDocIdOffset = _metadata.getInt(metadataEntry + METADATA_ENTRY_SIZE) & 0x7FFFFFFF;
        limit = _metadata.getInt(metadataEntry + METADATA_ENTRY_SIZE + Integer.BYTES) & 0xFFFFFFFFL;
      } else {
        _nextDocIdOffset = Integer.MAX_VALUE;
        limit = _chunks.size();
      }
      recordRangesForChunk(docId, offset, limit, _ranges);
      ranges.addAll(_ranges);
    }
  }

  private static final class UncompressedReaderContext extends ReaderContext {

    private ByteBuffer _chunk;

    private List<ValueRange> _ranges;

    UncompressedReaderContext(PinotDataBuffer metadata, PinotDataBuffer chunks, long chunkStartOffset) {
      super(chunks, metadata, chunkStartOffset);
    }

    @Override
    protected byte[] processChunkAndReadFirstValue(int docId, long offset, long limit) {
      _chunk = _chunks.toDirectByteBuffer(offset, (int) (limit - offset));
      if (!_regularChunk) {
        return readHugeValue();
      }
      _numDocsInCurrentChunk = _chunk.getInt(0);
      return readSmallUncompressedValue(docId);
    }

    @Override
    protected void recordRangesForChunk(int docId, long offset, long limit,
        List<ValueRange> ranges) {
      ranges.add(ValueRange.newByteRange(_chunkStartOffset + offset, (int) (limit - offset)));
    }

    private byte[] readHugeValue() {
      byte[] value = new byte[_chunk.capacity()];
      _chunk.get(value);
      return value;
    }

    @Override
    protected byte[] readSmallUncompressedValue(int docId) {
      int index = docId - _docIdOffset;
      int offset = _chunk.getInt((index + 1) * Integer.BYTES);
      int nextOffset =
          index == _numDocsInCurrentChunk - 1 ? _chunk.limit() : _chunk.getInt((index + 2) * Integer.BYTES);
      byte[] bytes = new byte[nextOffset - offset];
      _chunk.position(offset);
      _chunk.get(bytes);
      _chunk.position(0);
      return bytes;
    }

    @Override
    public void close()
        throws IOException {
    }
  }

  private static final class CompressedReaderContext extends ReaderContext {

    private final ByteBuffer _decompressedBuffer;
    private final ChunkDecompressor _chunkDecompressor;
    private final ChunkCompressionType _chunkCompressionType;

    CompressedReaderContext(PinotDataBuffer metadata, PinotDataBuffer chunks, long chunkStartOffset,
        ChunkDecompressor chunkDecompressor, ChunkCompressionType chunkCompressionType, int targetChunkSize) {
      super(metadata, chunks, chunkStartOffset);
      _chunkDecompressor = chunkDecompressor;
      _chunkCompressionType = chunkCompressionType;
      _decompressedBuffer = ByteBuffer.allocateDirect(targetChunkSize).order(ByteOrder.LITTLE_ENDIAN);
    }

    @Override
    protected byte[] processChunkAndReadFirstValue(int docId, long offset, long limit)
        throws IOException {
      _decompressedBuffer.clear();
      ByteBuffer compressed = _chunks.toDirectByteBuffer(offset, (int) (limit - offset));
      if (_regularChunk) {
        _chunkDecompressor.decompress(compressed, _decompressedBuffer);
        _numDocsInCurrentChunk = _decompressedBuffer.getInt(0);
        return readSmallUncompressedValue(docId);
      }
      // huge value, no benefit from buffering, return the whole thing
      return readHugeCompressedValue(compressed, _chunkDecompressor.decompressedLength(compressed));
    }

    @Override
    protected void recordRangesForChunk(int docId, long offset, long limit,
        List<ValueRange> ranges) {
      ranges.add(ValueRange.newByteRange(_chunkStartOffset + offset, (int) (limit - offset)));
    }

    @Override
    protected byte[] readSmallUncompressedValue(int docId) {
      int index = docId - _docIdOffset;
      int offset = _decompressedBuffer.getInt((index + 1) * Integer.BYTES);
      int nextOffset = index == _numDocsInCurrentChunk - 1 ? _decompressedBuffer.limit()
          : _decompressedBuffer.getInt((index + 2) * Integer.BYTES);
      byte[] bytes = new byte[nextOffset - offset];
      _decompressedBuffer.position(offset);
      _decompressedBuffer.get(bytes);
      _decompressedBuffer.position(0);
      return bytes;
    }

    private byte[] readHugeCompressedValue(ByteBuffer compressed, int decompressedLength)
        throws IOException {
      // huge values don't have length prefixes; they occupy the entire chunk so are unambiguous
      byte[] value = new byte[decompressedLength];
      if (_chunkCompressionType == ChunkCompressionType.SNAPPY
          || _chunkCompressionType == ChunkCompressionType.ZSTANDARD) {
        // snappy and zstandard don't work without direct buffers
        decompressViaDirectBuffer(compressed, value);
      } else {
        _chunkDecompressor.decompress(compressed, ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN));
      }
      return value;
    }

    private void decompressViaDirectBuffer(ByteBuffer compressed, byte[] target)
        throws IOException {
      ByteBuffer buffer = ByteBuffer.allocateDirect(target.length).order(ByteOrder.LITTLE_ENDIAN);
      try {
        _chunkDecompressor.decompress(compressed, buffer);
        buffer.get(target);
      } finally {
        if (CleanerUtil.UNMAP_SUPPORTED) {
          CleanerUtil.getCleaner().freeBuffer(buffer);
        }
      }
    }

    @Override
    public void close()
        throws IOException {
      CleanerUtil.cleanQuietly(_decompressedBuffer);
    }
  }
}
