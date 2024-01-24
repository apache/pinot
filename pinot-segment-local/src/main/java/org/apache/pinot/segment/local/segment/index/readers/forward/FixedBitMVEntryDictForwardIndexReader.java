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

import com.google.common.base.Preconditions;
import java.util.List;
import org.apache.pinot.segment.local.io.util.FixedBitIntReaderWriter;
import org.apache.pinot.segment.local.io.util.PinotDataBitSet;
import org.apache.pinot.segment.local.io.writer.impl.FixedBitMVEntryDictForwardIndexWriter;
import org.apache.pinot.segment.spi.compression.DictIdCompressionType;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * Bit-compressed dictionary-encoded forward index reader for multi-value columns, where a second level dictionary
 * encoding for multi-value entries (instead of individual values within the entry) are maintained within the forward
 * index.
 * See {@link FixedBitMVEntryDictForwardIndexWriter} for index layout.
 */
public final class FixedBitMVEntryDictForwardIndexReader implements ForwardIndexReader<ForwardIndexReaderContext> {
  public static final int MAGIC_MARKER = FixedBitMVEntryDictForwardIndexWriter.MAGIC_MARKER;
  public static final short VERSION = FixedBitMVEntryDictForwardIndexWriter.VERSION;
  private static final int HEADER_SIZE = FixedBitMVEntryDictForwardIndexWriter.HEADER_SIZE;

  private final FixedBitIntReaderWriter _idReader;
  private final int _offsetBufferOffset;
  private final FixedBitIntReaderWriter _offsetReader;
  private final int _valueBufferOffset;
  private final FixedBitIntReaderWriter _valueReader;

  public FixedBitMVEntryDictForwardIndexReader(PinotDataBuffer dataBuffer, int numDocs, int numBitsPerValue) {
    int magicMarker = dataBuffer.getInt(0);
    Preconditions.checkState(magicMarker == MAGIC_MARKER, "Invalid magic marker: %s (expected: %s)", magicMarker,
        MAGIC_MARKER);
    short version = dataBuffer.getShort(4);
    Preconditions.checkState(version == VERSION, "Invalid version: %s (expected: %s)", version, VERSION);
    int numBitsPerValueInHeader = dataBuffer.getByte(6);
    Preconditions.checkState(numBitsPerValueInHeader == numBitsPerValue, "Invalid numBitsPerValue: %s (expected: %s)",
        numBitsPerValueInHeader, numBitsPerValue);
    int numBitsPerId = dataBuffer.getByte(7);
    int numUniqueEntries = dataBuffer.getInt(8);
    int numTotalValues = dataBuffer.getInt(12);
    _offsetBufferOffset = dataBuffer.getInt(16);
    _valueBufferOffset = dataBuffer.getInt(20);
    _idReader = new FixedBitIntReaderWriter(dataBuffer.view(HEADER_SIZE, _offsetBufferOffset), numDocs, numBitsPerId);
    _offsetReader =
        new FixedBitIntReaderWriter(dataBuffer.view(_offsetBufferOffset, _valueBufferOffset), numUniqueEntries + 1,
            PinotDataBitSet.getNumBitsPerValue(numTotalValues));
    _valueReader = new FixedBitIntReaderWriter(dataBuffer.view(_valueBufferOffset, dataBuffer.size()), numTotalValues,
        numBitsPerValue);
  }

  @Override
  public boolean isDictionaryEncoded() {
    return true;
  }

  @Override
  public boolean isSingleValue() {
    return false;
  }

  @Override
  public DataType getStoredType() {
    return DataType.INT;
  }

  @Override
  public DictIdCompressionType getDictIdCompressionType() {
    return DictIdCompressionType.MV_ENTRY_DICT;
  }

  @Override
  public int getDictIdMV(int docId, int[] dictIdBuffer, ForwardIndexReaderContext context) {
    int id = _idReader.readInt(docId);
    int startIndex = _offsetReader.readInt(id);
    int numValues = _offsetReader.readInt(id + 1) - startIndex;
    _valueReader.readInt(startIndex, numValues, dictIdBuffer);
    return numValues;
  }

  @Override
  public int[] getDictIdMV(int docId, ForwardIndexReaderContext context) {
    int id = _idReader.readInt(docId);
    int startIndex = _offsetReader.readInt(id);
    int numValues = _offsetReader.readInt(id + 1) - startIndex;
    int[] dictIdBuffer = new int[numValues];
    _valueReader.readInt(startIndex, numValues, dictIdBuffer);
    return dictIdBuffer;
  }

  @Override
  public int getNumValuesMV(int docId, ForwardIndexReaderContext context) {
    int id = _idReader.readInt(docId);
    return _offsetReader.readInt(id + 1) - _offsetReader.readInt(id);
  }

  @Override
  public boolean isBufferByteRangeInfoSupported() {
    return true;
  }

  @Override
  public void recordDocIdByteRanges(int docId, ForwardIndexReaderContext context, List<ByteRange> ranges) {
    int id = _idReader.readInt(docId);
    int idReaderStartByteOffset = _idReader.getStartByteOffset(docId);
    int idReaderEndByteOffset = _idReader.getEndByteOffset(docId);
    ranges.add(new ByteRange(HEADER_SIZE + idReaderStartByteOffset, idReaderEndByteOffset - idReaderStartByteOffset));

    int startIndex = _offsetReader.readInt(id);
    int numValues = _offsetReader.readInt(id + 1) - startIndex;
    int offsetReaderStartByteOffset = _offsetReader.getStartByteOffset(id);
    int offsetReaderEndByteOffset = _offsetReader.getEndByteOffset(id + 1);
    ranges.add(new ByteRange(_offsetBufferOffset + offsetReaderStartByteOffset,
        offsetReaderEndByteOffset - offsetReaderStartByteOffset));

    int valueReaderStartByteOffset = _valueReader.getStartByteOffset(startIndex);
    int valueReaderEndByteOffset = _valueReader.getEndByteOffset(startIndex + numValues - 1);
    ranges.add(new ByteRange(_valueBufferOffset + valueReaderStartByteOffset,
        valueReaderEndByteOffset - valueReaderStartByteOffset));
  }

  @Override
  public boolean isFixedOffsetMappingType() {
    return false;
  }

  @Override
  public void close() {
    // NOTE: DO NOT close the PinotDataBuffer here because it is tracked by the caller and might be reused later. The
    // caller is responsible of closing the PinotDataBuffer.
    _idReader.close();
    _offsetReader.close();
    _valueReader.close();
  }
}
