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
package org.apache.pinot.segment.local.realtime.converter.stats;

import com.google.common.base.Utf8;
import java.math.BigDecimal;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.apache.pinot.spi.utils.ByteArray;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;


/// Column statistics for no-dictionary columns during commit-time compaction.
/// When commit-time compaction is enabled, only valid (non-deleted) documents should be considered.
@SuppressWarnings({"rawtypes", "unchecked"})
public class CompactedNoDictColumnStatistics extends MutableNoDictColumnStatistics {
  @Nullable
  private final Object _minValue;
  @Nullable
  private final Object _maxValue;
  private final int _minElementLength;
  private final int _maxElementLength;
  private final boolean _isSorted;
  private final int _totalEntries;
  private final int _maxMultiValues;
  private final int _maxRowLength;

  public CompactedNoDictColumnStatistics(DataSource dataSource, @Nullable int[] sortedDocIds, boolean isSortedColumn,
      RoaringBitmap validDocIds) {
    super(dataSource, sortedDocIds, isSortedColumn);

    DataType valueType = _forwardIndex.getStoredType();
    boolean isSingleValue = _forwardIndex.isSingleValue();
    boolean isVariableWidth = !valueType.isFixedWidth();

    // Single pass over valid documents to collect stats.
    // For SV columns, sort order is tracked inline: when sortedDocIds is provided, iterate in that order; when null,
    // iterate via the bitmap.
    // isSorted is initialized to false for sorted columns to skip the per-doc dictionary compare entirely.
    // MV columns are never sorted, so we always iterate via the bitmap regardless of sortedDocIds.
    Comparable minValue = null;
    Comparable maxValue = null;
    // For fixed-width types element length is constant; for variable-width it is tracked per entry
    int minElementLength = isVariableWidth ? Integer.MAX_VALUE : valueType.size();
    int maxElementLength = isVariableWidth ? 0 : valueType.size();
    boolean isSorted = !_isSortedColumn;
    Comparable prevValue = null;
    int totalEntries = 0;
    int maxMultiValues = -1;
    int maxRowLength = -1;

    if (isSingleValue) {
      if (_sortedDocIds != null) {
        // Iterate in sorted doc order, filtered to valid docs, to track sortedness inline
        for (int docId : _sortedDocIds) {
          if (!validDocIds.contains(docId)) {
            continue;
          }
          totalEntries++;
          Comparable value = readValue(docId, valueType);
          if (minValue == null || value.compareTo(minValue) < 0) {
            minValue = value;
          }
          if (maxValue == null || value.compareTo(maxValue) > 0) {
            maxValue = value;
          }
          if (isSorted) {
            if (prevValue != null && value.compareTo(prevValue) < 0) {
              isSorted = false;
            }
            prevValue = value;
          }
          if (isVariableWidth) {
            int length = getElementLength(value, valueType);
            if (length < minElementLength) {
              minElementLength = length;
            }
            if (length > maxElementLength) {
              maxElementLength = length;
            }
          }
        }
      } else {
        IntIterator iterator = validDocIds.getIntIterator();
        while (iterator.hasNext()) {
          int docId = iterator.next();
          totalEntries++;
          Comparable value = readValue(docId, valueType);
          if (minValue == null || value.compareTo(minValue) < 0) {
            minValue = value;
          }
          if (maxValue == null || value.compareTo(maxValue) > 0) {
            maxValue = value;
          }
          if (isSorted) {
            if (prevValue != null && value.compareTo(prevValue) < 0) {
              isSorted = false;
            }
            prevValue = value;
          }
          if (isVariableWidth) {
            int length = getElementLength(value, valueType);
            if (length < minElementLength) {
              minElementLength = length;
            }
            if (length > maxElementLength) {
              maxElementLength = length;
            }
          }
        }
      }
    } else {
      switch (valueType) {
        case INT: {
          int minInt = Integer.MAX_VALUE;
          int maxInt = Integer.MIN_VALUE;
          IntIterator iterator = validDocIds.getIntIterator();
          while (iterator.hasNext()) {
            int[] values = _forwardIndex.getIntMV(iterator.next());
            totalEntries += values.length;
            maxMultiValues = Math.max(maxMultiValues, values.length);
            for (int value : values) {
              if (value < minInt) {
                minInt = value;
              }
              if (value > maxInt) {
                maxInt = value;
              }
            }
          }
          if (totalEntries > 0) {
            minValue = minInt;
            maxValue = maxInt;
          }
          break;
        }
        case LONG: {
          long minLong = Long.MAX_VALUE;
          long maxLong = Long.MIN_VALUE;
          IntIterator iterator = validDocIds.getIntIterator();
          while (iterator.hasNext()) {
            long[] values = _forwardIndex.getLongMV(iterator.next());
            totalEntries += values.length;
            maxMultiValues = Math.max(maxMultiValues, values.length);
            for (long value : values) {
              if (value < minLong) {
                minLong = value;
              }
              if (value > maxLong) {
                maxLong = value;
              }
            }
          }
          if (totalEntries > 0) {
            minValue = minLong;
            maxValue = maxLong;
          }
          break;
        }
        case FLOAT: {
          float minFloat = Float.MAX_VALUE;
          float maxFloat = -Float.MAX_VALUE;
          IntIterator iterator = validDocIds.getIntIterator();
          while (iterator.hasNext()) {
            float[] values = _forwardIndex.getFloatMV(iterator.next());
            totalEntries += values.length;
            maxMultiValues = Math.max(maxMultiValues, values.length);
            for (float value : values) {
              if (value < minFloat) {
                minFloat = value;
              }
              if (value > maxFloat) {
                maxFloat = value;
              }
            }
          }
          if (totalEntries > 0) {
            minValue = minFloat;
            maxValue = maxFloat;
          }
          break;
        }
        case DOUBLE: {
          double minDouble = Double.MAX_VALUE;
          double maxDouble = -Double.MAX_VALUE;
          IntIterator iterator = validDocIds.getIntIterator();
          while (iterator.hasNext()) {
            double[] values = _forwardIndex.getDoubleMV(iterator.next());
            totalEntries += values.length;
            maxMultiValues = Math.max(maxMultiValues, values.length);
            for (double value : values) {
              if (value < minDouble) {
                minDouble = value;
              }
              if (value > maxDouble) {
                maxDouble = value;
              }
            }
          }
          if (totalEntries > 0) {
            minValue = minDouble;
            maxValue = maxDouble;
          }
          break;
        }
        case STRING: {
          String minString = null;
          String maxString = null;
          IntIterator iterator = validDocIds.getIntIterator();
          while (iterator.hasNext()) {
            String[] values = _forwardIndex.getStringMV(iterator.next());
            totalEntries += values.length;
            maxMultiValues = Math.max(maxMultiValues, values.length);
            int rowLength = 0;
            for (String value : values) {
              if (minString == null || value.compareTo(minString) < 0) {
                minString = value;
              }
              if (maxString == null || value.compareTo(maxString) > 0) {
                maxString = value;
              }
              int length = Utf8.encodedLength(value);
              if (length < minElementLength) {
                minElementLength = length;
              }
              if (length > maxElementLength) {
                maxElementLength = length;
              }
              rowLength += length;
            }
            maxRowLength = Math.max(maxRowLength, rowLength);
          }
          minValue = minString;
          maxValue = maxString;
          break;
        }
        case BYTES: {
          ByteArray minByteArray = null;
          ByteArray maxByteArray = null;
          IntIterator iterator = validDocIds.getIntIterator();
          while (iterator.hasNext()) {
            byte[][] values = _forwardIndex.getBytesMV(iterator.next());
            totalEntries += values.length;
            maxMultiValues = Math.max(maxMultiValues, values.length);
            int rowLength = 0;
            for (byte[] bytes : values) {
              ByteArray value = new ByteArray(bytes);
              if (minByteArray == null || value.compareTo(minByteArray) < 0) {
                minByteArray = value;
              }
              if (maxByteArray == null || value.compareTo(maxByteArray) > 0) {
                maxByteArray = value;
              }
              int length = bytes.length;
              if (length < minElementLength) {
                minElementLength = length;
              }
              if (length > maxElementLength) {
                maxElementLength = length;
              }
              rowLength += length;
            }
            maxRowLength = Math.max(maxRowLength, rowLength);
          }
          minValue = minByteArray;
          maxValue = maxByteArray;
          break;
        }
        default:
          throw new IllegalStateException("Unsupported value type: " + valueType);
      }
    }

    _minValue = minValue;
    _maxValue = maxValue;
    _minElementLength = minElementLength;
    _maxElementLength = maxElementLength;
    _totalEntries = totalEntries;
    _maxMultiValues = maxMultiValues;
    _maxRowLength = maxRowLength;

    if (_isSortedColumn) {
      _isSorted = true;
    } else if (!isSingleValue) {
      _isSorted = false;
    } else {
      _isSorted = isSorted;
    }
  }

  @Nullable
  @Override
  public Object getMinValue() {
    return _minValue;
  }

  @Nullable
  @Override
  public Object getMaxValue() {
    return _maxValue;
  }

  @Override
  public int getLengthOfShortestElement() {
    return _minElementLength;
  }

  @Override
  public int getLengthOfLargestElement() {
    return _maxElementLength;
  }

  @Override
  public boolean isSorted() {
    return _isSorted;
  }

  @Override
  public int getTotalNumberOfEntries() {
    return _totalEntries;
  }

  @Override
  public int getMaxNumberOfMultiValues() {
    return _maxMultiValues;
  }

  @Override
  public int getMaxRowLengthInBytes() {
    return _maxRowLength;
  }

  private Comparable readValue(int docId, DataType valueType) {
    switch (valueType) {
      case INT:
        return _forwardIndex.getInt(docId);
      case LONG:
        return _forwardIndex.getLong(docId);
      case FLOAT:
        return _forwardIndex.getFloat(docId);
      case DOUBLE:
        return _forwardIndex.getDouble(docId);
      case BIG_DECIMAL:
        return _forwardIndex.getBigDecimal(docId);
      case STRING:
        return _forwardIndex.getString(docId);
      case BYTES:
        return new ByteArray(_forwardIndex.getBytes(docId));
      default:
        throw new IllegalStateException("Unsupported value type: " + valueType);
    }
  }

  private static int getElementLength(Comparable value, DataType valueType) {
    switch (valueType) {
      case BIG_DECIMAL:
        return BigDecimalUtils.byteSize((BigDecimal) value);
      case STRING:
        return Utf8.encodedLength((String) value);
      case BYTES:
        return ((ByteArray) value).length();
      default:
        throw new IllegalStateException("Unsupported variable-width value type: " + valueType);
    }
  }
}
