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
import java.util.Arrays;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;

/**
 * Implementation of {@link ValueReader} that will allow each value to be of variable
 * length and there by avoiding the unnecessary padding.
 *
 * @see FixedByteValueReaderWriter
 */
public class VarLengthBytesValueReaderWriter implements Closeable, ValueReader {

  /**
   * Header used to identify the dictionary files written in variable length bytes
   * format. Keeping this a mix of alphanumeric with special character to avoid
   * collisions with the regular int/string dictionary values written in fixed size
   * format.
   */
  private static final byte[] MAGIC_HEADER = StringUtil.encodeUtf8("vl1;");
  private static final int MAGIC_HEADER_LENGTH = MAGIC_HEADER.length;
  private static final int INDEX_ARRAY_START_OFFSET = MAGIC_HEADER.length + Integer.BYTES;

  private final PinotDataBuffer _dataBuffer;
  private transient int _numElements;

  public static long getRequiredSize(byte[][] byteArrays) {
    // First include the magic header, length field and offsets array.
    long length = INDEX_ARRAY_START_OFFSET + Integer.BYTES * (byteArrays.length);

    for (byte[] array: byteArrays) {
      length += array.length;
    }
    return length;
  }

  public static boolean isVarLengthBytesDictBuffer(PinotDataBuffer buffer) {
    byte[] header = new byte[MAGIC_HEADER_LENGTH];
    buffer.copyTo(0, header, 0, MAGIC_HEADER_LENGTH);

    return Arrays.equals(MAGIC_HEADER, header);
  }

  public VarLengthBytesValueReaderWriter(PinotDataBuffer dataBuffer) {
    this._dataBuffer = dataBuffer;
  }

  private void writeHeader() {
    for (int offset = 0; offset < MAGIC_HEADER_LENGTH; offset++) {
      _dataBuffer.putByte(offset, MAGIC_HEADER[offset]);
    }
  }

  public void init(byte[][] byteArrays) {
    this._numElements = byteArrays.length;

    writeHeader();

    // Add the number of elements as the first field in the buffer.
    _dataBuffer.putInt(MAGIC_HEADER_LENGTH, byteArrays.length);

    // Then write the offset of each of the byte array in the data buffer.
    int nextOffset = INDEX_ARRAY_START_OFFSET;
    int nextArrayStartOffset = INDEX_ARRAY_START_OFFSET + Integer.BYTES * byteArrays.length;
    for (byte[] array: byteArrays) {
      _dataBuffer.putInt(nextOffset, nextArrayStartOffset);
      nextOffset += Integer.BYTES;
      nextArrayStartOffset += array.length;
    }

    // Finally write the byte arrays.
    nextArrayStartOffset = INDEX_ARRAY_START_OFFSET + Integer.BYTES * byteArrays.length;
    for (byte[] array: byteArrays) {
      _dataBuffer.readFrom(nextArrayStartOffset, array);
      nextArrayStartOffset += array.length;
    }
  }

  int getNumElements() {
    // Lazily initialize the numElements.
    if (_numElements == 0) {
      _numElements = _dataBuffer.getInt(MAGIC_HEADER_LENGTH);
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
    return StringUtil.decodeUtf8(getBytes(index, numBytesPerValue, buffer));
  }

  @Override
  public String getPaddedString(int index, int numBytesPerValue, byte[] buffer) {
    return StringUtil.decodeUtf8(getBytes(index, numBytesPerValue, buffer));
  }

  @Override
  public byte[] getBytes(int index, int numBytesPerValue, byte[] buffer) {
    // Read the offset of the byte array first and then read the actual byte array.
    int offset = _dataBuffer.getInt(INDEX_ARRAY_START_OFFSET + Integer.BYTES * index);

    // To get the length of the byte array, we use the next byte array offset. However,
    // for last element there is no next byte array so we use buffer size.
    int length;
    if (index == getNumElements() - 1) {
      // This is the last byte array in the buffer so use buffer size to get the length.
      length = (int) (_dataBuffer.size() - offset);
    }
    else {
      length = _dataBuffer.getInt(INDEX_ARRAY_START_OFFSET + Integer.BYTES * (index + 1)) - offset;
    }

    byte[] b = buffer == null ? new byte[length] :
        (buffer.length == length ? buffer : new byte[length]);
    _dataBuffer.copyTo(offset, b, 0, length);
    return b;
  }

  @Override
  public void close() throws IOException {
    _dataBuffer.close();
  }
}
