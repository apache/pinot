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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;

import static java.nio.charset.StandardCharsets.UTF_8;


public final class FixedByteValueReaderWriter implements ValueReader {
  private final PinotDataBuffer _dataBuffer;

  public FixedByteValueReaderWriter(PinotDataBuffer dataBuffer) {
    _dataBuffer = dataBuffer;
  }

  @Override
  public int getInt(int index) {
    return _dataBuffer.getInt((long) index * Integer.BYTES);
  }

  @Override
  public long getLong(int index) {
    return _dataBuffer.getLong((long) index * Long.BYTES);
  }

  @Override
  public float getFloat(int index) {
    return _dataBuffer.getFloat((long) index * Float.BYTES);
  }

  @Override
  public double getDouble(int index) {
    return _dataBuffer.getDouble((long) index * Double.BYTES);
  }

  /**
   * Reads the unpadded bytes into the given buffer and returns the length.
   */
  private int readUnpaddedBytes(int index, int numBytesPerValue, byte paddingByte, byte[] buffer) {
    // Based on the ZeroInWord algorithm: http://graphics.stanford.edu/~seander/bithacks.html#ZeroInWord
    assert buffer.length >= numBytesPerValue;
    long startOffset = (long) index * numBytesPerValue;
    long pattern = (paddingByte & 0xFFL) * 0x101010101010101L;
    ByteBuffer wrapper = ByteBuffer.wrap(buffer);
    if (_dataBuffer.order() == ByteOrder.LITTLE_ENDIAN) {
      wrapper.order(ByteOrder.LITTLE_ENDIAN);
    }
    int position = 0;
    int endIndex = numBytesPerValue & 0xFFFFFFF8;
    for (int i = 0; i < endIndex; i += Long.BYTES) {
      long word = _dataBuffer.getLong(startOffset + i);
      wrapper.putLong(i, word);
      long zeroed = word ^ pattern;
      long tmp = (zeroed & 0x7F7F7F7F7F7F7F7FL) + 0x7F7F7F7F7F7F7F7FL;
      tmp = ~(tmp | zeroed | 0x7F7F7F7F7F7F7F7FL);
      if (tmp == 0) {
        position += 8;
      } else {
        position += _dataBuffer.order() == ByteOrder.LITTLE_ENDIAN ? Long.numberOfTrailingZeros(tmp) >>> 3
            : Long.numberOfLeadingZeros(tmp) >>> 3;
        return position;
      }
    }
    for (; position < numBytesPerValue; position++) {
      byte b = _dataBuffer.getByte(startOffset + position);
      if (b == paddingByte) {
        break;
      }
      buffer[position] = b;
    }
    return position;
  }

  @Override
  public byte[] getUnpaddedBytes(int index, int numBytesPerValue, byte paddingByte, byte[] buffer) {
    int length = readUnpaddedBytes(index, numBytesPerValue, paddingByte, buffer);
    byte[] bytes = new byte[length];
    System.arraycopy(buffer, 0, bytes, 0, length);
    return bytes;
  }

  @Override
  public String getUnpaddedString(int index, int numBytesPerValue, byte paddingByte, byte[] buffer) {
    int length = readUnpaddedBytes(index, numBytesPerValue, paddingByte, buffer);
    return new String(buffer, 0, length, UTF_8);
  }

  @Override
  public String getPaddedString(int index, int numBytesPerValue, byte[] buffer) {
    assert buffer.length >= numBytesPerValue;

    long startOffset = (long) index * numBytesPerValue;
    _dataBuffer.copyTo(startOffset, buffer, 0, numBytesPerValue);
    return new String(buffer, 0, numBytesPerValue, UTF_8);
  }

  @Override
  public byte[] getBytes(int index, int numBytesPerValue) {
    long startOffset = (long) index * numBytesPerValue;
    byte[] value = new byte[numBytesPerValue];
    _dataBuffer.copyTo(startOffset, value, 0, numBytesPerValue);
    return value;
  }

  public void writeInt(int index, int value) {
    _dataBuffer.putInt((long) index * Integer.BYTES, value);
  }

  public void writeLong(int index, long value) {
    _dataBuffer.putLong((long) index * Long.BYTES, value);
  }

  public void writeFloat(int index, float value) {
    _dataBuffer.putFloat((long) index * Float.BYTES, value);
  }

  public void writeDouble(int index, double value) {
    _dataBuffer.putDouble((long) index * Double.BYTES, value);
  }

  public void writeBytes(int index, int numBytesPerValue, byte[] value) {
    assert value.length <= numBytesPerValue;

    long startIndex = (long) index * numBytesPerValue;
    if (value.length < numBytesPerValue) {
      value = Arrays.copyOf(value, numBytesPerValue);
    }
    _dataBuffer.readFrom(startIndex, value);
  }

  @Override
  public void close() {
    // NOTE: DO NOT close the PinotDataBuffer here because it is tracked by the caller and might be reused later. The
    // caller is responsible of closing the PinotDataBuffer.
  }
}
