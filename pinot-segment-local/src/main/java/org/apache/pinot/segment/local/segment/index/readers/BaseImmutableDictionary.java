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
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.pinot.segment.local.io.util.FixedByteValueReaderWriter;
import org.apache.pinot.segment.local.io.util.ValueReader;
import org.apache.pinot.segment.local.io.util.VarLengthValueReader;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.readers.Vector;

import static java.nio.charset.StandardCharsets.UTF_8;


/**
 * Base implementation of immutable dictionary.
 */
@SuppressWarnings("rawtypes")
public abstract class BaseImmutableDictionary implements Dictionary {
  private final ValueReader _valueReader;
  private final int _length;
  private final int _numBytesPerValue;

  protected BaseImmutableDictionary(PinotDataBuffer dataBuffer, int length, int numBytesPerValue) {
    if (VarLengthValueReader.isVarLengthValueBuffer(dataBuffer)) {
      VarLengthValueReader valueReader = new VarLengthValueReader(dataBuffer);
      _valueReader = valueReader;
      _length = valueReader.getNumValues();
    } else {
      Preconditions.checkState(dataBuffer.size() == (long) length * numBytesPerValue,
          "Buffer size mismatch: bufferSize = %s, numValues = %s, numByesPerValue = %s", dataBuffer.size(), length,
          numBytesPerValue);
      _valueReader = new FixedByteValueReaderWriter(dataBuffer);
      _length = length;
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
    return normalizeIndex(insertionIndexOf(stringValue));
  }

  protected final int normalizeIndex(int index) {
    return index >= 0 ? index : NULL_VALUE_INDEX;
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

  protected int binarySearch(BigDecimal value) {
    int low = 0;
    int high = _length - 1;
    while (low <= high) {
      int mid = (low + high) >>> 1;
      BigDecimal midValue = _valueReader.getBigDecimal(mid, _numBytesPerValue);
      int compareResult = midValue.compareTo(value);
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

  protected int binarySearch(Vector value) {
    int low = 0;
    int high = _length - 1;
    while (low <= high) {
      int mid = (low + high) >>> 1;
      Vector midValue = _valueReader.getVector(mid, _numBytesPerValue);
      int compareResult = midValue.compareTo(value);
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

  protected int binarySearch(String value) {
    int low = 0;
    int high = _length - 1;
    byte[] utf8 = value.getBytes(UTF_8);
    while (low <= high) {
      int mid = (low + high) >>> 1;
      int compareResult = _valueReader.compareUtf8Bytes(mid, _numBytesPerValue, utf8);
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

  protected int binarySearch(byte[] value) {
    int low = 0;
    int high = _length - 1;
    while (low <= high) {
      int mid = (low + high) >>> 1;
      int compareResult = _valueReader.compareBytes(mid, _numBytesPerValue, value);
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

  protected BigDecimal getBigDecimal(int dictId) {
    return _valueReader.getBigDecimal(dictId, _numBytesPerValue);
  }

  protected Vector getVector(int dictId) {
    return _valueReader.getVector(dictId, _numBytesPerValue);
  }

  protected byte[] getUnpaddedBytes(int dictId, byte[] buffer) {
    return _valueReader.getUnpaddedBytes(dictId, _numBytesPerValue, buffer);
  }

  protected String getUnpaddedString(int dictId, byte[] buffer) {
    return _valueReader.getUnpaddedString(dictId, _numBytesPerValue, buffer);
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

  @Override
  public void getDictIds(List<String> sortedValues, IntSet dictIds, SortedBatchLookupAlgorithm algorithm) {
    switch (algorithm) {
      case DIVIDE_BINARY_SEARCH:
        getDictIdsDivideBinarySearch(sortedValues, 0, sortedValues.size(), 0, _length, dictIds);
        break;
      case SCAN:
        getDictIdsScan(sortedValues, dictIds);
        break;
      default:
        throw new IllegalStateException("Unsupported sorted batch lookup algorithm: " + algorithm);
    }
  }

  private void getDictIdsDivideBinarySearch(List<String> sortedValues, int startIndex, int endIndex, int startDictId,
      int endDictId, IntSet dictIds) {
    if (startIndex >= endIndex || startDictId >= endDictId) {
      return;
    }
    int midIndex = (startIndex + endIndex) >>> 1;
    String midValue = sortedValues.get(midIndex);
    int dictId = binarySearch(midValue, startDictId, endDictId);
    if (dictId >= 0) {
      dictIds.add(dictId);
      getDictIdsDivideBinarySearch(sortedValues, startIndex, midIndex, startDictId, dictId, dictIds);
      getDictIdsDivideBinarySearch(sortedValues, midIndex + 1, endIndex, dictId + 1, endDictId, dictIds);
    } else {
      dictId = -dictId - 1;
      getDictIdsDivideBinarySearch(sortedValues, startIndex, midIndex, startDictId, dictId, dictIds);
      getDictIdsDivideBinarySearch(sortedValues, midIndex + 1, endIndex, dictId, endDictId, dictIds);
    }
  }

  private int binarySearch(String value, int startDictId, int endDictId) {
    int low = startDictId;
    int high = endDictId - 1;
    byte[] utf8 = value.getBytes(UTF_8);
    while (low <= high) {
      int mid = (low + high) >>> 1;
      int compareResult = _valueReader.compareUtf8Bytes(mid, _numBytesPerValue, utf8);
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

  private void getDictIdsScan(List<String> sortedValues, IntSet dictIds) {
    int valueId = 0;
    int dictId = 0;
    byte[] utf8 = null;
    boolean needNewUtf8 = true;
    int numSortedValues = sortedValues.size();
    int dictLength = length();
    while (valueId < numSortedValues && dictId < dictLength) {
      if (needNewUtf8) {
        utf8 = sortedValues.get(valueId).getBytes(StandardCharsets.UTF_8);
      }
      int comparison = _valueReader.compareUtf8Bytes(dictId, _numBytesPerValue, utf8);
      if (comparison == 0) {
        dictIds.add(dictId);
        dictId++;
        valueId++;
        needNewUtf8 = true;
      } else if (comparison > 0) {
        valueId++;
        needNewUtf8 = true;
      } else {
        dictId++;
        needNewUtf8 = false;
      }
    }
  }
}
