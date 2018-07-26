/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.io.util;

import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.Charset;


public final class FixedByteValueReaderWriter implements Closeable {
  private static final Charset UTF_8 = Charset.forName("UTF-8");

  private final PinotDataBuffer _dataBuffer;

  public FixedByteValueReaderWriter(PinotDataBuffer dataBuffer) {
    _dataBuffer = dataBuffer;
  }

  public int getInt(int index) {
    return _dataBuffer.getInt(index * Integer.BYTES);
  }

  public long getLong(int index) {
    return _dataBuffer.getLong(index * Long.BYTES);
  }

  public float getFloat(int index) {
    return _dataBuffer.getFloat(index * Float.BYTES);
  }

  public double getDouble(int index) {
    return _dataBuffer.getDouble(index * Double.BYTES);
  }

  public String getUnpaddedString(int index, int numBytesPerValue, byte paddingByte, byte[] buffer) {
    int startOffset = index * numBytesPerValue;
    for (int i = 0; i < numBytesPerValue; i++) {
      byte currentByte = _dataBuffer.getByte(startOffset + i);
      if (currentByte == paddingByte) {
        return new String(buffer, 0, i, UTF_8);
      }
      buffer[i] = currentByte;
    }
    return new String(buffer, UTF_8);
  }

  public String getPaddedString(int index, int numBytesPerValue, byte[] buffer) {
    int startOffset = index * numBytesPerValue;
    for (int i = 0; i < numBytesPerValue; i++) {
      buffer[i] = _dataBuffer.getByte(startOffset + i);
    }
    return new String(buffer, UTF_8);
  }

  public byte[] getBytes(int index, int numBytesPerValue, byte[] output) {
    assert output.length == numBytesPerValue;
    int startOffset = index * numBytesPerValue;
    for (int i = 0; i < numBytesPerValue; i++) {
      output[i] = _dataBuffer.getByte(startOffset + i);
    }
    return output;
  }

  public void writeInt(int index, int value) {
    _dataBuffer.putInt(index * Integer.BYTES, value);
  }

  public void writeLong(int index, long value) {
    _dataBuffer.putLong(index * Long.BYTES, value);
  }

  public void writeFloat(int index, float value) {
    _dataBuffer.putFloat(index * Float.BYTES, value);
  }

  public void writeDouble(int index, double value) {
    _dataBuffer.putDouble(index * Double.BYTES, value);
  }

  public void writeUnpaddedString(int index, int numBytesPerValue, byte[] value) {
    int startIndex = index * numBytesPerValue;
    int endIndex = startIndex + numBytesPerValue;

    int i = startIndex;
    for (byte b : value) {
      _dataBuffer.putByte(i++, b);
    }
    while (i < endIndex) {
      _dataBuffer.putByte(i++, (byte) 0);
    }
  }

  @Override
  public void close() throws IOException {
    _dataBuffer.close();
  }
}
