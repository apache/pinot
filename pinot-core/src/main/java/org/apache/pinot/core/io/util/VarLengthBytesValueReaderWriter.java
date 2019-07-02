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

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;


/**
 * Implementation of {@link ValueReader} that will allow each value to be of variable
 * length and there by avoiding the unnecessary padding.
 *
 * The layout of the file is as follows:
 * <p> Header Section: </p>
 * <ul>
 *   <li> Magic bytes: Chose this to be ".vl;" to avoid conflicts with the fixed size
 *        {@link ValueReader} implementations. By having special characters, this avoids conflicts
 *        with regular bytes/strings dictionaries.
 *   </li>
 *   <li> Version number: This is an integer and can be used for the evolution of the store
 *        implementation by incrementing version for every incompatible change to the store/format.
 *   </li>
 *   <li> Number of elements in the store. </li>
 * </ul>
 *
 * <p> Data section: </p>
 * <ul>
 *   <li> Offsets Array: Integer offsets to start position of byte arrays.
 *        Example: [O(1), O(2),...O(n)] where O is the Offset function. Length of nth element
 *        is computed as: O(n+1) - O(n). Since the last element's length can't be computed
 *        using this formula, we store an extra offset at the end to be able to compute last
 *        element's length with the same formula, without depending on underlying buffer's size.
 *   </li>
 *   <li> All byte arrays or values. </li>
 * </ul>
 *
 * Only sequential writes are supported.
 *
 * @see FixedByteValueReaderWriter
 */
public class VarLengthBytesValueReaderWriter implements Closeable, ValueReader {

  /**
   * Magic bytes used to identify the dictionary files written in variable length bytes format.
   */
  private static final byte[] MAGIC_BYTES = StringUtil.encodeUtf8(".vl;");

  /**
   * Increment this version if there are any structural changes in the store format and
   * deal with backward compatibility correctly based on old versions.
   */
  private static final int VERSION = 1;

  private static final int NUM_ELEMENTS_OFFSET = MAGIC_BYTES.length + Integer.BYTES;

  // version, numElements
  private static final int HEADER_LENGTH = MAGIC_BYTES.length + 2 * Integer.BYTES;

  private final PinotDataBuffer _dataBuffer;

  /**
   * Total number of values present. Initialize this to -1 to handle '0' values as well.
   */
  private transient int _numElements = -1;

  public VarLengthBytesValueReaderWriter(PinotDataBuffer dataBuffer) {
    _dataBuffer = dataBuffer;
  }

  public static long getRequiredSize(byte[][] byteArrays) {
    // First include the header and then the data section.
    // Remember there are n+1 offsets
    long length = HEADER_LENGTH + Integer.BYTES * (byteArrays.length + 1);

    for (byte[] array : byteArrays) {
      length += array.length;
    }
    return length;
  }

  public static boolean isVarLengthBytesDictBuffer(PinotDataBuffer buffer) {
    // If the buffer is smaller than header size, it's not var length dictionary.
    if (buffer.size() > HEADER_LENGTH) {
      byte[] magicBytes = new byte[MAGIC_BYTES.length];
      buffer.copyTo(0, magicBytes, 0, MAGIC_BYTES.length);

      if (Arrays.equals(MAGIC_BYTES, magicBytes)) {
        // Verify the version.
        if (VERSION == buffer.getInt(MAGIC_BYTES.length)) {
          // Also verify that there is a valid numElements value.
          return buffer.getInt(NUM_ELEMENTS_OFFSET) >= 0;
        }
      }
    }

    return false;
  }

  private void writeHeader(int numElements) {
    for (int offset = 0; offset < MAGIC_BYTES.length; offset++) {
      _dataBuffer.putByte(offset, MAGIC_BYTES[offset]);
    }
    _dataBuffer.putInt(MAGIC_BYTES.length, VERSION);
    _dataBuffer.putInt(NUM_ELEMENTS_OFFSET, numElements);
  }

  public void write(byte[][] byteArrays) {
    _numElements = byteArrays.length;

    writeHeader(_numElements);

    // Then write the offset of each of the byte array in the data buffer.
    int nextOffset = HEADER_LENGTH;
    int nextArrayStartOffset = HEADER_LENGTH + Integer.BYTES * (byteArrays.length + 1);
    for (byte[] array : byteArrays) {
      _dataBuffer.putInt(nextOffset, nextArrayStartOffset);
      nextOffset += Integer.BYTES;
      nextArrayStartOffset += array.length;
    }

    // Write the additional offset to easily get the length of last array.
    _dataBuffer.putInt(nextOffset, nextArrayStartOffset);

    // Finally write the byte arrays.
    nextArrayStartOffset = HEADER_LENGTH + Integer.BYTES * (byteArrays.length + 1);
    for (byte[] array : byteArrays) {
      _dataBuffer.readFrom(nextArrayStartOffset, array);
      nextArrayStartOffset += array.length;
    }
  }

  int getNumElements() {
    // Lazily initialize the numElements.
    if (_numElements == -1) {
      _numElements = _dataBuffer.getInt(NUM_ELEMENTS_OFFSET);
    }
    return _numElements;
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
    int offset = _dataBuffer.getInt(HEADER_LENGTH + Integer.BYTES * index);

    // To get the length of the byte array, we use the next byte array offset.
    int length = _dataBuffer.getInt(HEADER_LENGTH + Integer.BYTES * (index + 1)) - offset;

    byte[] b;
    // If the caller didn't pass a buffer, create one with exact length.
    if (buffer == null) {
      b = new byte[length];
    } else {
      // If the buffer passed by the caller isn't big enough, create a new one.
      b = buffer.length == length ? buffer : new byte[length];
    }
    _dataBuffer.copyTo(offset, b, 0, length);
    return b;
  }

  @Override
  public void close()
      throws IOException {
    _dataBuffer.close();
  }
}
