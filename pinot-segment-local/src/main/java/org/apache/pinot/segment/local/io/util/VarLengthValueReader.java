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
package org.apache.pinot.segment.local.io.util;

import java.util.List;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;

import static java.nio.charset.StandardCharsets.UTF_8;


/**
 * The value reader for var-length values (STRING and BYTES). See {@link VarLengthValueWriter} for the file layout.
 */
public class VarLengthValueReader implements ValueReader {
  private final PinotDataBuffer _dataBuffer;
  private final int _numValues;

  /**
   * The offset of the data section in the buffer/store. This info will be persisted in the header
   * so it has to be read from the buffer while initializing the store in read cases.
   */
  private final int _dataSectionStartOffSet;

  public VarLengthValueReader(PinotDataBuffer dataBuffer) {
    _dataBuffer = dataBuffer;
    _numValues = dataBuffer.getInt(VarLengthValueWriter.NUM_VALUES_OFFSET);
    _dataSectionStartOffSet = dataBuffer.getInt(VarLengthValueWriter.DATA_SECTION_OFFSET_POSITION);
  }

  public static boolean isVarLengthValueBuffer(PinotDataBuffer buffer) {
    // If the buffer is smaller than header size + one value offset, it's not a var length dictionary.
    if (buffer.size() < VarLengthValueWriter.HEADER_LENGTH + Integer.BYTES) {
      return false;
    }
    byte[] magicBytes = VarLengthValueWriter.MAGIC_BYTES;
    int length = magicBytes.length;
    for (int i = 0; i < length; i++) {
      if (buffer.getByte(i) != magicBytes[i]) {
        return false;
      }
    }
    return buffer.getInt(VarLengthValueWriter.VERSION_OFFSET) == VarLengthValueWriter.VERSION;
  }

  public int getNumValues() {
    return _numValues;
  }

  @Override
  public int getInt(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getLong(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public float getFloat(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public double getDouble(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[] getUnpaddedBytes(int index, int numBytesPerValue, byte[] buffer) {
    return getBytes(index, numBytesPerValue);
  }

  @Override
  public String getUnpaddedString(int index, int numBytesPerValue, byte[] buffer) {
    assert buffer.length >= numBytesPerValue;

    // Read the offset of the byte array first and then read the actual byte array.
    int offsetPosition = _dataSectionStartOffSet + Integer.BYTES * index;
    int startOffset = _dataBuffer.getInt(offsetPosition);
    int endOffset = _dataBuffer.getInt(offsetPosition + Integer.BYTES);
    int length = endOffset - startOffset;

    assert numBytesPerValue >= length;
    _dataBuffer.copyTo(startOffset, buffer, 0, length);
    return new String(buffer, 0, length, UTF_8);
  }

  public void recordOffsetRanges(int index, long baseOffset, List<ForwardIndexReader.ByteRange> rangeList) {
    int offsetPosition = _dataSectionStartOffSet + Integer.BYTES * index;
    int startOffset = _dataBuffer.getInt(offsetPosition);
    int endOffset = _dataBuffer.getInt(offsetPosition + Integer.BYTES);
    rangeList.add(new ForwardIndexReader.ByteRange(offsetPosition + baseOffset, 2 * Integer.BYTES));
    int length = endOffset - startOffset;
    rangeList.add(new ForwardIndexReader.ByteRange(startOffset + baseOffset, length));
  }

  @Override
  public String getPaddedString(int index, int numBytesPerValue, byte[] buffer) {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[] getBytes(int index, int numBytesPerValue) {
    // Read the offset of the byte array first and then read the actual byte array.
    int offsetPosition = _dataSectionStartOffSet + Integer.BYTES * index;
    int startOffset = _dataBuffer.getInt(offsetPosition);
    int endOffset = _dataBuffer.getInt(offsetPosition + Integer.BYTES);
    int length = endOffset - startOffset;

    byte[] value = new byte[length];
    _dataBuffer.copyTo(startOffset, value);
    return value;
  }

  @Override
  public int compareUtf8Bytes(int index, int numBytesPerValue, byte[] bytes) {
    int offsetPosition = _dataSectionStartOffSet + Integer.BYTES * index;
    int startOffset = _dataBuffer.getInt(offsetPosition);
    int endOffset = _dataBuffer.getInt(offsetPosition + Integer.BYTES);
    int length = endOffset - startOffset;
    return ValueReaderComparisons.compareUtf8Bytes(_dataBuffer, startOffset, length, false, bytes);
  }

  @Override
  public int compareBytes(int index, int numBytesPerValue, byte[] bytes) {
    int offsetPosition = _dataSectionStartOffSet + Integer.BYTES * index;
    int startOffset = _dataBuffer.getInt(offsetPosition);
    int endOffset = _dataBuffer.getInt(offsetPosition + Integer.BYTES);
    int length = endOffset - startOffset;
    return ValueReaderComparisons.compareBytes(_dataBuffer, startOffset, length, bytes);
  }

  @Override
  public void close() {
    // NOTE: DO NOT close the PinotDataBuffer here because it is tracked by the caller and might be reused later. The
    // caller is responsible of closing the PinotDataBuffer.
  }
}
