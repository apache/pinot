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

import java.util.Arrays;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;


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

  @Override
  public String getUnpaddedString(int index, int numBytesPerValue, byte paddingByte, byte[] buffer) {
    assert buffer.length >= numBytesPerValue;

    long startOffset = (long) index * numBytesPerValue;
    for (int i = 0; i < numBytesPerValue; i++) {
      byte currentByte = _dataBuffer.getByte(startOffset + i);
      if (currentByte == paddingByte) {
        return StringUtil.decodeUtf8(buffer, 0, i);
      }
      buffer[i] = currentByte;
    }
    return StringUtil.decodeUtf8(buffer, 0, numBytesPerValue);
  }

  @Override
  public String getPaddedString(int index, int numBytesPerValue, byte[] buffer) {
    assert buffer.length >= numBytesPerValue;

    long startOffset = (long) index * numBytesPerValue;
    _dataBuffer.copyTo(startOffset, buffer, 0, numBytesPerValue);
    return StringUtil.decodeUtf8(buffer, 0, numBytesPerValue);
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
