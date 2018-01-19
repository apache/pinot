/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.segment.index.readers;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.core.io.util.FixedByteValueReaderWriter;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;


@SuppressWarnings("Duplicates")
public abstract class ImmutableDictionaryReader extends BaseDictionary {
  private static final Charset UTF_8 = Charset.forName("UTF-8");

  private final FixedByteValueReaderWriter _valueReader;
  private final int _length;
  private final int _numBytesPerValue;
  private final byte _paddingByte;

  protected ImmutableDictionaryReader(PinotDataBuffer dataBuffer, int length, int numBytesPerValue, byte paddingByte) {
    Preconditions.checkState(dataBuffer.size() == length * numBytesPerValue);
    _valueReader = new FixedByteValueReaderWriter(dataBuffer);
    _length = length;
    _numBytesPerValue = numBytesPerValue;
    _paddingByte = paddingByte;
  }

  /**
   * Returns the insertion index of object in the dictionary.
   * <ul>
   *   <li> If the object already exists, then returns the current index. </li>
   *   <li> If the object does not exist, then returns the index at which the object would be inserted, with -ve sign.
   *        Sign of index is inverted to indicate that the object was not found in the dictionary. </li>
   * </ul>
   * @param rawValue Object for which to find the insertion index
   * @return Insertion index of the object (as defined above).
   */
  public abstract int insertionIndexOf(Object rawValue);

  @Override
  public int length() {
    return _length;
  }

  public boolean isSorted() {
    return true;
  }

  @Override
  public void close() {
    _valueReader.close();
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
      String paddedValue = addPadding(value);
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

  protected String addPadding(String value) {
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

  protected byte[] getBuffer() {
    return new byte[_numBytesPerValue];
  }
}
