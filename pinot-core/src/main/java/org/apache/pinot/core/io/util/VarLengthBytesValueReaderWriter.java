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

import java.util.Arrays;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;


/**
 * An immutable implementation of {@link ValueReader} that will allow each byte[] to be of variable
 * length and there by avoiding the unnecessary padding. Since this is an immutable data structure,
 * full data has to be given at initialization time an doesn't have any write() methods.
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
 *   <li> The offset where the data section starts. Though the data section usually starts right after
 *        the header, having this explicitly will let the store to be evolved freely without any
 *        assumptions about where the data section starts.
 *   </li>
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
 * @see FixedByteValueReaderWriter
 */
public class VarLengthBytesValueReaderWriter implements ValueReader {

  /**
   * Magic bytes used to identify the dictionary files written in variable length bytes format.
   */
  private static final byte[] MAGIC_BYTES = StringUtil.encodeUtf8(".vl;");

  /**
   * Increment this version if there are any structural changes in the store format and
   * deal with backward compatibility correctly based on old versions.
   */
  private static final int VERSION = 1;

  // Offsets of different fields in the header. Having as constants for readability.
  private static final int VERSION_OFFSET = MAGIC_BYTES.length;
  private static final int NUM_ELEMENTS_OFFSET = VERSION_OFFSET + Integer.BYTES;
  private static final int DATA_SECTION_OFFSET_POSITION = NUM_ELEMENTS_OFFSET + Integer.BYTES;
  private static final int HEADER_LENGTH = DATA_SECTION_OFFSET_POSITION + Integer.BYTES;

  private final PinotDataBuffer _dataBuffer;

  /**
   * The offset of the data section in the buffer/store. This info will be persisted in the header
   * so it has to be read from the buffer while initializing the store in read cases.
   */
  private final int _dataSectionStartOffSet;

  /**
   * Total number of values present in the store.
   */
  private final int _numElements;

  /**
   * Constructor to create a VarLengthBytesValueReaderWriter from a previously written buffer.
   */
  public VarLengthBytesValueReaderWriter(PinotDataBuffer dataBuffer) {
    _dataBuffer = dataBuffer;

    // To prepare ourselves to start reading the data, initialize the data offset.
    _numElements = dataBuffer.getInt(NUM_ELEMENTS_OFFSET);
    _dataSectionStartOffSet = dataBuffer.getInt(DATA_SECTION_OFFSET_POSITION);
  }

  /**
   * Constructor to create a new immutable store with the given data.
   */
  public VarLengthBytesValueReaderWriter(PinotDataBuffer dataBuffer, byte[][] byteArrays) {
    _dataBuffer = dataBuffer;
    _numElements = byteArrays.length;

    // For now, start writing the data section right after the header but if this store evolves,
    // we could decide to start the data section somewhere else.
    _dataSectionStartOffSet = HEADER_LENGTH;

    write(byteArrays);
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
          // Also verify that there is a valid numElements value and valid offset for data section.
          return buffer.getInt(NUM_ELEMENTS_OFFSET) >= 0 && buffer.getInt(DATA_SECTION_OFFSET_POSITION) > 0;
        }
      }
    }

    return false;
  }

  private void writeHeader() {
    for (int offset = 0; offset < MAGIC_BYTES.length; offset++) {
      _dataBuffer.putByte(offset, MAGIC_BYTES[offset]);
    }
    _dataBuffer.putInt(VERSION_OFFSET, VERSION);
    _dataBuffer.putInt(NUM_ELEMENTS_OFFSET, _numElements);
    _dataBuffer.putInt(DATA_SECTION_OFFSET_POSITION, _dataSectionStartOffSet);
  }

  private void write(byte[][] byteArrays) {
    writeHeader();

    // Then write the offset of each of the byte array in the data buffer.
    int nextOffset = _dataSectionStartOffSet;
    int nextArrayStartOffset = _dataSectionStartOffSet + Integer.BYTES * (byteArrays.length + 1);
    for (byte[] array : byteArrays) {
      _dataBuffer.putInt(nextOffset, nextArrayStartOffset);
      nextOffset += Integer.BYTES;
      nextArrayStartOffset += array.length;
    }

    // Write the additional offset to easily get the length of last array.
    _dataBuffer.putInt(nextOffset, nextArrayStartOffset);

    // Finally write the byte arrays.
    nextArrayStartOffset = _dataSectionStartOffSet + Integer.BYTES * (byteArrays.length + 1);
    for (byte[] array : byteArrays) {
      _dataBuffer.readFrom(nextArrayStartOffset, array);
      nextArrayStartOffset += array.length;
    }
  }

  public int getNumElements() {
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
    assert buffer.length >= numBytesPerValue;

    // Read the offset of the byte array first and then read the actual byte array.
    int offset = _dataBuffer.getInt(_dataSectionStartOffSet + Integer.BYTES * index);

    // To get the length of the byte array, we use the next byte array offset.
    int length = _dataBuffer.getInt(_dataSectionStartOffSet + Integer.BYTES * (index + 1)) - offset;

    assert numBytesPerValue >= length;
    _dataBuffer.copyTo(offset, buffer, 0, length);
    return StringUtil.decodeUtf8(buffer, 0, length);
  }

  @Override
  public String getPaddedString(int index, int numBytesPerValue, byte[] buffer) {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[] getBytes(int index, int numBytesPerValue) {
    // Read the offset of the byte array first and then read the actual byte array.
    int offset = _dataBuffer.getInt(_dataSectionStartOffSet + Integer.BYTES * index);

    // To get the length of the byte array, we use the next byte array offset.
    int length = _dataBuffer.getInt(_dataSectionStartOffSet + Integer.BYTES * (index + 1)) - offset;

    byte[] value = new byte[length];
    _dataBuffer.copyTo(offset, value);
    return value;
  }

  @Override
  public void close() {
    // NOTE: DO NOT close the PinotDataBuffer here because it is tracked by the caller and might be reused later. The
    // caller is responsible of closing the PinotDataBuffer.
  }
}
