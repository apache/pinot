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
package org.apache.pinot.core.segment.index.readers;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.Arrays;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.core.io.util.FixedByteValueReaderWriter;
import org.apache.pinot.core.io.util.ValueReader;
import org.apache.pinot.core.io.util.VarLengthBytesValueReaderWriter;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;


public abstract class BaseImmutableDictionary extends BaseDictionary {
  private final ValueReader _valueReader;
  private final int _length;
  private final int _numBytesPerValue;
  private final byte _paddingByte;

  protected BaseImmutableDictionary(PinotDataBuffer dataBuffer, int length, int numBytesPerValue, byte paddingByte) {
    if (VarLengthBytesValueReaderWriter.isVarLengthBytesDictBuffer(dataBuffer)) {
      _valueReader = new VarLengthBytesValueReaderWriter(dataBuffer);
    } else {
      Preconditions.checkState(dataBuffer.size() == length * numBytesPerValue,
          "Buffer size mismatch: bufferSize = %s, numValues = %s, numByesPerValue = %s", dataBuffer.size(), length,
          numBytesPerValue);
      _valueReader = new FixedByteValueReaderWriter(dataBuffer);
    }
    _length = length;
    _numBytesPerValue = numBytesPerValue;
    _paddingByte = paddingByte;
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

  /**
   * Returns the insertion index of string representation of the value in the dictionary. Follows the same behavior as
   * in {@link Arrays#binarySearch(Object[], Object)}. This API is for range predicate evaluation.
   */
  public abstract int insertionIndexOf(String stringValue);

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
    return (index >= 0) ? index : NULL_VALUE_INDEX;
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
    byte[] valueBytes = StringUtil.encodeUtf8(value);
    int length = valueBytes.length;
    String paddedValue;
    if (length >= _numBytesPerValue) {
      paddedValue = value;
    } else {
      byte[] paddedValueBytes = new byte[_numBytesPerValue];
      System.arraycopy(valueBytes, 0, paddedValueBytes, 0, length);
      Arrays.fill(paddedValueBytes, length, _numBytesPerValue, _paddingByte);
      paddedValue = StringUtil.decodeUtf8(paddedValueBytes);
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
