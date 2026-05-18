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

import com.google.common.base.Preconditions;
import com.google.common.base.Utf8;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import java.math.BigDecimal;
import java.util.Arrays;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.mutable.MutableForwardIndex;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.apache.pinot.spi.utils.ByteArray;
import org.roaringbitmap.RoaringBitmap;


/// Column statistics for dictionary encoded columns during commit-time compaction.
/// When commit-time compaction is enabled, only valid (non-deleted) documents should be considered.
@SuppressWarnings("rawtypes")
public class CompactedColumnStatistics extends MutableColumnStatistics {
  private final int _totalDocs;
  private final Comparable _minValue;
  private final Comparable _maxValue;
  private final Object _uniqueValues;
  private final int _cardinality;
  private final int _minElementLength;
  private final int _maxElementLength;
  private final boolean _isAscii;
  private final boolean _isSorted;
  private final int _totalEntries;
  private final int _maxMultiValues;
  private final int _maxRowLength;

  public CompactedColumnStatistics(DataSource dataSource, @Nullable int[] sortedDocIds, boolean isSortedColumn,
      RoaringBitmap validDocIds) {
    super(dataSource, sortedDocIds, isSortedColumn);
    Preconditions.checkState(!validDocIds.isEmpty(), "Use EmptyColumnStatistics for empty column: %s",
        _fieldSpec.getName());
    _totalDocs = validDocIds.getCardinality();
    DataType storedType = getStoredType();
    boolean isSingleValue = isSingleValue();
    boolean isFixedWidth = storedType.isFixedWidth();
    boolean isVarBytesMV = !isSingleValue && !isFixedWidth;
    MutableForwardIndex forwardIndex = (MutableForwardIndex) dataSource.getForwardIndex();
    Preconditions.checkState(forwardIndex != null, "Failed to find forward index for column: %s", _fieldSpec.getName());
    Dictionary dictionary = _dictionary;

    // Single pass over valid documents to collect used dict IDs and entry counts.
    // For SV columns, sort order is tracked inline: when sortedDocIds is provided, iterate in that order; when null,
    // iterate via the bitmap.
    // isSorted is initialized to false for sorted columns to skip the per-doc dictionary compare entirely.
    // MV columns are never sorted, so we always iterate via the bitmap regardless of sortedDocIds.
    IntOpenHashSet usedDictIds = new IntOpenHashSet();
    boolean isSorted = !_isSortedColumn;
    int prevDictId = -1;
    int maxRowLength = 0;
    int totalEntries = 0;
    int maxMultiValues = 0;

    if (isSingleValue) {
      if (_sortedDocIds != null) {
        for (int docId : _sortedDocIds) {
          if (!validDocIds.contains(docId)) {
            continue;
          }
          int dictId = forwardIndex.getDictId(docId);
          totalEntries++;
          usedDictIds.add(dictId);
          if (isSorted) {
            if (prevDictId != -1 && dictionary.compare(prevDictId, dictId) > 0) {
              isSorted = false;
            }
            prevDictId = dictId;
          }
        }
      } else {
        org.roaringbitmap.IntIterator iterator = validDocIds.getIntIterator();
        while (iterator.hasNext()) {
          int docId = iterator.next();
          int dictId = forwardIndex.getDictId(docId);
          totalEntries++;
          usedDictIds.add(dictId);
          if (isSorted) {
            if (prevDictId != -1 && dictionary.compare(prevDictId, dictId) > 0) {
              isSorted = false;
            }
            prevDictId = dictId;
          }
        }
      }
    } else {
      org.roaringbitmap.IntIterator iterator = validDocIds.getIntIterator();
      while (iterator.hasNext()) {
        int docId = iterator.next();
        int[] dictIds = forwardIndex.getDictIdMV(docId);
        totalEntries += dictIds.length;
        maxMultiValues = Math.max(maxMultiValues, dictIds.length);
        for (int dictId : dictIds) {
          usedDictIds.add(dictId);
        }
        if (isVarBytesMV) {
          int rowLength = 0;
          for (int dictId : dictIds) {
            rowLength += dictionary.getValueSize(dictId);
          }
          maxRowLength = Math.max(maxRowLength, rowLength);
        }
      }
    }

    _cardinality = usedDictIds.size();
    _totalEntries = totalEntries;
    _maxMultiValues = maxMultiValues;

    // Build a typed array from the used dict IDs, sort it by natural value order, and extract min/max value and element
    // lengths from the sorted results.
    Comparable minValue = null;
    Comparable maxValue = null;
    // For fixed-width types element length is constant; for variable-width it is tracked per entry
    int minElementLength = isFixedWidth ? storedType.size() : Integer.MAX_VALUE;
    int maxElementLength = isFixedWidth ? storedType.size() : 0;
    boolean isAscii = false;
    switch (storedType) {
      case INT: {
        int[] values = new int[_cardinality];
        if (_cardinality > 0) {
          IntIterator iterator = usedDictIds.intIterator();
          for (int i = 0; iterator.hasNext(); i++) {
            values[i] = dictionary.getIntValue(iterator.nextInt());
          }
          Arrays.sort(values);
          minValue = values[0];
          maxValue = values[_cardinality - 1];
        }
        _uniqueValues = values;
        break;
      }
      case LONG: {
        long[] values = new long[_cardinality];
        if (_cardinality > 0) {
          IntIterator iterator = usedDictIds.intIterator();
          for (int i = 0; iterator.hasNext(); i++) {
            values[i] = dictionary.getLongValue(iterator.nextInt());
          }
          Arrays.sort(values);
          minValue = values[0];
          maxValue = values[_cardinality - 1];
        }
        _uniqueValues = values;
        break;
      }
      case FLOAT: {
        float[] values = new float[_cardinality];
        if (_cardinality > 0) {
          IntIterator iterator = usedDictIds.intIterator();
          for (int i = 0; iterator.hasNext(); i++) {
            values[i] = dictionary.getFloatValue(iterator.nextInt());
          }
          Arrays.sort(values);
          minValue = values[0];
          maxValue = values[_cardinality - 1];
        }
        _uniqueValues = values;
        break;
      }
      case DOUBLE: {
        double[] values = new double[_cardinality];
        if (_cardinality > 0) {
          IntIterator iterator = usedDictIds.intIterator();
          for (int i = 0; iterator.hasNext(); i++) {
            values[i] = dictionary.getDoubleValue(iterator.nextInt());
          }
          Arrays.sort(values);
          minValue = values[0];
          maxValue = values[_cardinality - 1];
        }
        _uniqueValues = values;
        break;
      }
      case BIG_DECIMAL: {
        BigDecimal[] values = new BigDecimal[_cardinality];
        if (_cardinality > 0) {
          IntIterator iterator = usedDictIds.intIterator();
          for (int i = 0; iterator.hasNext(); i++) {
            values[i] = dictionary.getBigDecimalValue(iterator.nextInt());
          }
          Arrays.sort(values);
          minValue = values[0];
          maxValue = values[_cardinality - 1];
          for (BigDecimal value : values) {
            int length = BigDecimalUtils.byteSize(value);
            minElementLength = Math.min(minElementLength, length);
            maxElementLength = Math.max(maxElementLength, length);
          }
        }
        _uniqueValues = values;
        break;
      }
      case STRING: {
        String[] values = new String[_cardinality];
        if (_cardinality > 0) {
          IntIterator iterator = usedDictIds.intIterator();
          for (int i = 0; iterator.hasNext(); i++) {
            values[i] = dictionary.getStringValue(iterator.nextInt());
          }
          Arrays.sort(values);
          minValue = values[0];
          maxValue = values[_cardinality - 1];
          isAscii = true;
          for (String value : values) {
            int length = Utf8.encodedLength(value);
            minElementLength = Math.min(minElementLength, length);
            maxElementLength = Math.max(maxElementLength, length);
            if (isAscii) {
              isAscii = length == value.length();
            }
          }
        }
        _uniqueValues = values;
        break;
      }
      case BYTES: {
        ByteArray[] values = new ByteArray[_cardinality];
        if (_cardinality > 0) {
          IntIterator iterator = usedDictIds.intIterator();
          for (int i = 0; iterator.hasNext(); i++) {
            values[i] = dictionary.getByteArrayValue(iterator.nextInt());
          }
          Arrays.sort(values);
          minValue = values[0];
          maxValue = values[_cardinality - 1];
          for (ByteArray value : values) {
            int length = value.length();
            minElementLength = Math.min(minElementLength, length);
            maxElementLength = Math.max(maxElementLength, length);
          }
        }
        _uniqueValues = values;
        break;
      }
      default:
        throw new IllegalStateException("Unsupported stored type: " + storedType);
    }
    _minValue = minValue;
    _maxValue = maxValue;
    _minElementLength = minElementLength;
    _maxElementLength = maxElementLength;
    _isAscii = isAscii;
    if (isSingleValue) {
      _maxRowLength = maxElementLength;
    } else if (isVarBytesMV) {
      _maxRowLength = maxRowLength;
    } else {
      _maxRowLength = maxMultiValues * storedType.size();
    }
    if (_isSortedColumn) {
      _isSorted = true;
    } else if (!isSingleValue) {
      _isSorted = false;
    } else {
      _isSorted = isSorted;
    }
  }

  @Override
  public int getTotalDocs() {
    return _totalDocs;
  }

  @Override
  public Comparable<?> getMinValue() {
    return _minValue;
  }

  @Override
  public Comparable<?> getMaxValue() {
    return _maxValue;
  }

  @Override
  public Object getUniqueValuesSet() {
    return _uniqueValues;
  }

  @Override
  public int getCardinality() {
    return _cardinality;
  }

  @Override
  public int getLengthOfShortestElement() {
    return _minElementLength;
  }

  @Override
  public int getLengthOfLongestElement() {
    return _maxElementLength;
  }

  @Override
  public boolean isAscii() {
    return _isAscii;
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
}
