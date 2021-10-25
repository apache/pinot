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
package org.apache.pinot.segment.local.segment.index.readers;

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.io.IOException;
import java.util.Arrays;
import org.apache.pinot.segment.local.io.util.FixedByteValueReaderWriter;
import org.apache.pinot.segment.local.io.util.ValueReader;
import org.apache.pinot.segment.local.io.util.VarLengthValueReader;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.utils.ByteArray;

import static java.nio.charset.StandardCharsets.UTF_8;


/**
 * Base implementation of immutable dictionary.
 */
@SuppressWarnings("rawtypes")
public abstract class BaseImmutableDictionary implements Dictionary {
  private final ValueReader _valueReader;
  private final int _length;
  private final int _numBytesPerValue;
  private final byte _paddingByte;

  protected BaseImmutableDictionary(PinotDataBuffer dataBuffer, int length, int numBytesPerValue, byte paddingByte) {
    if (VarLengthValueReader.isVarLengthValueBuffer(dataBuffer)) {
      VarLengthValueReader valueReader = new VarLengthValueReader(dataBuffer);
      _valueReader = valueReader;
      _length = valueReader.getNumValues();
      _paddingByte = 0;
    } else {
      Preconditions.checkState(dataBuffer.size() == (long) length * numBytesPerValue,
          "Buffer size mismatch: bufferSize = %s, numValues = %s, numByesPerValue = %s", dataBuffer.size(), length,
          numBytesPerValue);
      _valueReader = new FixedByteValueReaderWriter(dataBuffer);
      _length = length;
      _paddingByte = paddingByte;
    }
    _numBytesPerValue = numBytesPerValue;
  }

  /**
   * For virtual dictionary.
   */
  protected BaseImmutableDictionary(int length) {
    _valueReader = null;
    _length = length;
    _numBytesPerValue = -1;
    _paddingByte = 0;
  }

  @Override
  public boolean isSorted() {
    return true;
  }

  @Override
  public int length() {
    return _length;
  }

  @Override
  public int indexOf(String stringValue) {
    int index = insertionIndexOf(stringValue);
    return (index >= 0) ? index : Dictionary.NULL_VALUE_INDEX;
  }

  @Override
  public IntSet getDictIdsInRange(String lower, String upper, boolean includeLower, boolean includeUpper) {
    // This method should not be called for sorted dictionary.
    throw new UnsupportedOperationException();
  }

  @Override
  public int compare(int dictId1, int dictId2) {
    return Integer.compare(dictId1, dictId2);
  }

  @Override
  public Comparable getMinVal() {
    return (Comparable) get(0);
  }

  @Override
  public Comparable getMaxVal() {
    return (Comparable) get(_length - 1);
  }

  @Override
  public Object getSortedValues() {
    // This method is for the stats collection phase when sealing the consuming segment, so it is not required for
    // regular immutable dictionary within the immutable segment.
    throw new UnsupportedOperationException();
  }

  @Override
  public void close()
      throws IOException {
    if (_valueReader != null) {
      _valueReader.close();
    }
  }

  protected int binarySearch(int value) {
    int low = 0;
    int high = _length - 1;
    while (low <= high) {
      int mid = (low + high) >>> 1;
      int midValue = _valueReader.getInt(mid);
      if (midValue < value) {
        low = mid + 1;
      } else if (midValue > value) {
        high = mid - 1;
      } else {
        return mid;
      }
    }
    return -(low + 1);
  }

  protected int binarySearch(long value) {
    int low = 0;
    int high = _length - 1;
    while (low <= high) {
      int mid = (low + high) >>> 1;
      long midValue = _valueReader.getLong(mid);
      if (midValue < value) {
        low = mid + 1;
      } else if (midValue > value) {
        high = mid - 1;
      } else {
        return mid;
      }
    }
    return -(low + 1);
  }

  protected int binarySearch(float value) {
    int low = 0;
    int high = _length - 1;
    while (low <= high) {
      int mid = (low + high) >>> 1;
      float midValue = _valueReader.getFloat(mid);
      if (midValue < value) {
        low = mid + 1;
      } else if (midValue > value) {
        high = mid - 1;
      } else {
        return mid;
      }
    }
    return -(low + 1);
  }

  protected int binarySearch(double value) {
    int low = 0;
    int high = _length - 1;
    while (low <= high) {
      int mid = (low + high) >>> 1;
      double midValue = _valueReader.getDouble(mid);
      if (midValue < value) {
        low = mid + 1;
      } else if (midValue > value) {
        high = mid - 1;
      } else {
        return mid;
      }
    }
    return -(low + 1);
  }

  /**
   * WARNING: With non-zero padding byte, binary search result might not reflect the real insertion index for the value.
   * E.g. with padding byte 'b', if unpadded value "aa" is in the dictionary, and stored as "aab", then unpadded value
   * "a" will be mis-positioned after value "aa"; unpadded value "aab" will return positive value even if value "aab" is
   * not in the dictionary.
   * TODO: Clean up the segments with legacy non-zero padding byte, and remove the support for non-zero padding byte
   */
  protected int binarySearch(String value) {
    byte[] buffer = getBuffer();
    int low = 0;
    int high = _length - 1;
    if (_paddingByte == 0) {
      while (low <= high) {
        int mid = (low + high) >>> 1;
        String midValue = _valueReader.getUnpaddedString(mid, _numBytesPerValue, _paddingByte, buffer);
        int compareResult = midValue.compareTo(value);
        if (compareResult < 0) {
          low = mid + 1;
        } else if (compareResult > 0) {
          high = mid - 1;
        } else {
          return mid;
        }
      }
    } else {
      String paddedValue = padString(value);
      while (low <= high) {
        int mid = (low + high) >>> 1;
        String midValue = _valueReader.getPaddedString(mid, _numBytesPerValue, buffer);
        int compareResult = midValue.compareTo(paddedValue);
        if (compareResult < 0) {
          low = mid + 1;
        } else if (compareResult > 0) {
          high = mid - 1;
        } else {
          return mid;
        }
      }
    }
    return -(low + 1);
  }

  protected int binarySearch(byte[] value) {
    int low = 0;
    int high = _length - 1;

    while (low <= high) {
      int mid = (low + high) >>> 1;
      byte[] midValue = _valueReader.getBytes(mid, _numBytesPerValue);
      int compareResult = ByteArray.compare(midValue, value);
      if (compareResult < 0) {
        low = mid + 1;
      } else if (compareResult > 0) {
        high = mid - 1;
      } else {
        return mid;
      }
    }
    return -(low + 1);
  }

  protected String padString(String value) {
    byte[] valueBytes = value.getBytes(UTF_8);
    int length = valueBytes.length;
    String paddedValue;
    if (length >= _numBytesPerValue) {
      paddedValue = value;
    } else {
      byte[] paddedValueBytes = new byte[_numBytesPerValue];
      System.arraycopy(valueBytes, 0, paddedValueBytes, 0, length);
      Arrays.fill(paddedValueBytes, length, _numBytesPerValue, _paddingByte);
      paddedValue = new String(paddedValueBytes, UTF_8);
    }
    return paddedValue;
  }

  protected int getInt(int dictId) {
    return _valueReader.getInt(dictId);
  }

  protected long getLong(int dictId) {
    return _valueReader.getLong(dictId);
  }

  protected float getFloat(int dictId) {
    return _valueReader.getFloat(dictId);
  }

  protected double getDouble(int dictId) {
    return _valueReader.getDouble(dictId);
  }

  protected String getUnpaddedString(int dictId, byte[] buffer) {
    return _valueReader.getUnpaddedString(dictId, _numBytesPerValue, _paddingByte, buffer);
  }

  protected String getPaddedString(int dictId, byte[] buffer) {
    return _valueReader.getPaddedString(dictId, _numBytesPerValue, buffer);
  }

  protected byte[] getBytes(int dictId) {
    return _valueReader.getBytes(dictId, _numBytesPerValue);
  }

  protected byte[] getBuffer() {
    return new byte[_numBytesPerValue];
  }
}
