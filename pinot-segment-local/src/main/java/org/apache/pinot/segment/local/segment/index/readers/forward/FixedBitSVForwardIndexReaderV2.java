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

import java.util.List;
import org.apache.pinot.segment.local.io.reader.impl.FixedBitIntReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexByteRange;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * Bit-compressed dictionary-encoded forward index reader for single-value columns. The values returned are dictionary
 * ids.
 */
public final class FixedBitSVForwardIndexReaderV2 implements ForwardIndexReader<ForwardIndexReaderContext>,
                                                             ForwardIndexReader.DocIdRangeProvider
                                                                 <ForwardIndexReaderContext> {
  private final FixedBitIntReader _reader;
  private final int _numDocs;
  private final int _numBitsPerValue;

  public FixedBitSVForwardIndexReaderV2(PinotDataBuffer dataBuffer, int numDocs, int numBitsPerValue) {
    _reader = FixedBitIntReader.getReader(dataBuffer, numBitsPerValue);
    _numDocs = numDocs;
    _numBitsPerValue = numBitsPerValue;
  }

  @Override
  public boolean isDictionaryEncoded() {
    return true;
  }

  @Override
  public boolean isSingleValue() {
    return true;
  }

  @Override
  public DataType getStoredType() {
    return DataType.INT;
  }

  @Override
  public int getDictId(int docId, ForwardIndexReaderContext context) {
    return _reader.read(docId);
  }

  @Override
  public void readDictIds(int[] docIds, int length, int[] dictIdBuffer, ForwardIndexReaderContext context) {
    int firstDocId = docIds[0];
    int lastDocId = docIds[length - 1];
    int index = 0;

    // Use bulk read if the doc ids are sequential
    if (lastDocId - firstDocId + 1 == length && length >= 64) {
      int bulkStartIndex = (firstDocId + 31) & 0xffffffe0;
      int bulkEndIndex = lastDocId & 0xffffffe0;

      for (int i = firstDocId; i < bulkStartIndex; i++) {
        dictIdBuffer[index++] = _reader.readUnchecked(i);
      }
      for (int i = bulkStartIndex; i < bulkEndIndex; i += 32) {
        _reader.read32(i, dictIdBuffer, index);
        index += 32;
      }
    }

    // Process the remaining docs
    if (lastDocId < _numDocs - 2) {
      for (int i = index; i < length; i++) {
        dictIdBuffer[i] = _reader.readUnchecked(docIds[i]);
      }
    } else {
      dictIdBuffer[length - 1] = _reader.read(lastDocId);
      int uncheckedEndIndex = length - 2;
      if (uncheckedEndIndex >= index) {
        dictIdBuffer[uncheckedEndIndex] = _reader.read(docIds[uncheckedEndIndex]);
        for (int i = index; i < uncheckedEndIndex; i++) {
          dictIdBuffer[i] = _reader.readUnchecked(docIds[i]);
        }
      }
    }
  }

  @Override
  public void close() {
  }

  @Override
  public List<ForwardIndexByteRange> getDocIdRange(int docId, ForwardIndexReaderContext context) {
    throw new UnsupportedOperationException("Unsupported");
  }

  @Override
  public boolean isFixedOffsetType() {
    return true;
  }

  @Override
  public long getBaseOffset() {
    return 0;
  }

  @Override
  public int getDocLength() {
    return _numBitsPerValue;
  }

  @Override
  public boolean isDocLengthInIBits() {
    return true;
  }
}
