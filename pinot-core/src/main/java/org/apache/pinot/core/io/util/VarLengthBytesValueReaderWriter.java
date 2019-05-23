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
package org.apache.pinot.core.io.util;

import com.google.common.base.Preconditions;
import java.io.Closeable;
import java.io.IOException;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;

public class VarLengthBytesValueReaderWriter implements Closeable, ValueReader {
  private final PinotDataBuffer _dataBuffer;
  private transient int _numElements;

  public static long getRequiredSize(byte[][] byteArrays) {
    // First include the length size and offset of each byte array.
    long length = Integer.BYTES * (byteArrays.length + 1);

    for (byte[] array: byteArrays) {
      length += array.length;
    }
    return length;
  }

  public VarLengthBytesValueReaderWriter(PinotDataBuffer dataBuffer) {
    this._dataBuffer = dataBuffer;
  }

  public void init(byte[][] byteArrays) {
    this._numElements = byteArrays.length;

    // Add the number of elements as the first field in the buffer.
    _dataBuffer.putInt(0, byteArrays.length);

    // Then write the offset of each of the byte array in the data buffer.
    int nextOffset = Integer.BYTES;
    int nextArrayStartOffset = Integer.BYTES * (byteArrays.length + 1);
    for (byte[] array: byteArrays) {
      _dataBuffer.putInt(nextOffset, nextArrayStartOffset);
      nextOffset += Integer.BYTES;
      nextArrayStartOffset += array.length;
    }

    // Finally write the byte arrays.
    nextArrayStartOffset = Integer.BYTES * (byteArrays.length + 1);
    for (byte[] array: byteArrays) {
      _dataBuffer.readFrom(nextArrayStartOffset, array);
      nextArrayStartOffset += array.length;
    }
  }

  int getNumElements() {
    // Lazily initialize the numElements.
    if (_numElements == 0) {
      _numElements = _dataBuffer.getInt(0);
      Preconditions.checkArgument(_numElements > 0);
    }
    return this._numElements;
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
  public String getUnpaddedString(int index, int numBytesPerValue, byte paddingByte, byte[] buffer) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getPaddedString(int index, int numBytesPerValue, byte[] buffer) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getString(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[] getBytes(int index, int numBytesPerValue, byte[] buffer) {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[] getBytes(int index) {
    // Read the offset of the byte array first and then read the actual byte array.
    int offset = _dataBuffer.getInt(Integer.BYTES * (index + 1));

    // To get the length of the byte array, we use the next byte array offset. However,
    // for last element there is no next byte array so we use buffer size.
    int length;
    if (index == getNumElements() - 1) {
      // This is the last byte array in the buffer so use buffer size to get the length.
      length = (int) (_dataBuffer.size() - offset);
    }
    else {
      length = _dataBuffer.getInt(Integer.BYTES * (index + 2)) - offset;
    }

    byte[] buffer = new byte[length];
    _dataBuffer.copyTo(offset, buffer, 0, length);
    return buffer;
  }

  @Override
  public void close() throws IOException {
    _dataBuffer.close();
  }
}
