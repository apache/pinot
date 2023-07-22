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

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.segment.local.io.util.FixedBitIntReaderWriter;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexByteRange;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * Bit-compressed dictionary-encoded forward index reader for single-value columns. The values returned are dictionary
 * ids.
 */
public final class FixedBitSVForwardIndexReader implements ForwardIndexReader<ForwardIndexReaderContext> {
  private final FixedBitIntReaderWriter _reader;

  public FixedBitSVForwardIndexReader(PinotDataBuffer dataBuffer, int numDocs, int numBitsPerValue) {
    _reader = new FixedBitIntReaderWriter(dataBuffer, numDocs, numBitsPerValue);
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
    return _reader.readInt(docId);
  }

  @Override
  public void readDictIds(int[] docIds, int length, int[] dictIdBuffer, ForwardIndexReaderContext context) {
    for (int i = 0; i < length; i++) {
      dictIdBuffer[i] = _reader.readInt(docIds[i]);
    }
  }

  @Override
  public List<ForwardIndexByteRange> getForwardIndexByteRange(int docId, ForwardIndexReaderContext context) {
    List<ForwardIndexByteRange> ranges = new ArrayList<>();
    _reader.readIntAndRecordRanges(docId, 0, ranges);
    return ranges;
  }

  @Override
  public void close() {
    _reader.close();
  }
}
