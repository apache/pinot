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

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.mutable.MutableForwardIndex;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ByteArray;




/**
 * Column statistics for dictionary columns with raw (non-dictionary-encoded) forward indexes.
 * Reads raw values from forward index and maps them to dictionary IDs.
 *
 * This is used when:
 * - Column has a dictionary (dataSource.getDictionary() != null)
 * - Forward index is NOT dictionary-encoded (forwardIndex.isDictionaryEncoded() == false)
 * - Commit-time compaction is enabled
 *
 * Common scenarios:
 * - Multi-value columns where forward index stores raw values but dictionary exists for other operations
 * - Variable-length string columns optimized for sequential access
 * - Consuming segments where dictionary is built separately from forward index
 */
public class CompactedRawIndexDictColumnStatistics extends MutableColumnStatistics {
  private final Set<Integer> _usedDictIds;
  private final int _compactedCardinality;
  private final Object _compactedUniqueValues;

  public CompactedRawIndexDictColumnStatistics(DataSource dataSource, int[] sortedDocIds,
      ThreadSafeMutableRoaringBitmap validDocIds) {
    super(dataSource, sortedDocIds);

    String columnName = dataSource.getDataSourceMetadata().getFieldSpec().getName();
    Dictionary dictionary = dataSource.getDictionary();
    MutableForwardIndex forwardIndex = (MutableForwardIndex) dataSource.getForwardIndex();

    // Since forward index is not dictionary-encoded, we need to read raw values and map them to dictionary IDs
    _usedDictIds = new HashSet<>();
    int[] validDocIdsArray = validDocIds.getMutableRoaringBitmap().toArray();

    // Read raw values from valid documents and find corresponding dictionary IDs
    for (int docId : validDocIdsArray) {
      Object rawValue = getRawValue(forwardIndex, docId);
      if (rawValue != null) {
        // Find the dictionary ID for this raw value using type-specific lookup
        int dictId = getDictIdForValue(dictionary, rawValue, forwardIndex.getStoredType());
        if (dictId >= 0) {
          _usedDictIds.add(dictId);
        }
      }
    }

    _compactedCardinality = _usedDictIds.size();

    // Create compacted unique values array with type-safe sorting
    Object originalValues = dictionary.getSortedValues();
    Class<?> componentType = originalValues.getClass().getComponentType();
    // The cast is safe because dictionary values are always Comparable
    List<ValueWithOriginalId<Comparable<Object>>> usedValuesWithIds = new ArrayList<>();
    for (Integer dictId : _usedDictIds) {
      Comparable<Object> value = (Comparable<Object>) dictionary.get(dictId);
      usedValuesWithIds.add(new ValueWithOriginalId<>(value, dictId));
    }
    // Sort by values to ensure the compacted array is value-sorted
    usedValuesWithIds.sort(Comparator.comparing(a -> a._value));
    // Create a compacted array containing only the used dictionary values in sorted order by value
    Object compacted = Array.newInstance(componentType, _compactedCardinality);
    for (int i = 0; i < _compactedCardinality; i++) {
      Array.set(compacted, i, usedValuesWithIds.get(i)._value);
    }
    _compactedUniqueValues = compacted;
  }

  private Object getRawValue(MutableForwardIndex forwardIndex, int docId) {
    switch (forwardIndex.getStoredType()) {
      case INT:
        return forwardIndex.getInt(docId);
      case LONG:
        return forwardIndex.getLong(docId);
      case FLOAT:
        return forwardIndex.getFloat(docId);
      case DOUBLE:
        return forwardIndex.getDouble(docId);
      case STRING:
        return forwardIndex.getString(docId);
      case BYTES:
        return forwardIndex.getBytes(docId);
      case BIG_DECIMAL:
        return forwardIndex.getBigDecimal(docId);
      default:
        throw new IllegalStateException("Unsupported data type: " + forwardIndex.getStoredType());
    }
  }

  private int getDictIdForValue(Dictionary dictionary, Object value, DataType dataType) {
    switch (dataType) {
      case INT:
        return dictionary.indexOf((Integer) value);
      case LONG:
        return dictionary.indexOf((Long) value);
      case FLOAT:
        return dictionary.indexOf((Float) value);
      case DOUBLE:
        return dictionary.indexOf((Double) value);
      case STRING:
        return dictionary.indexOf((String) value);
      case BYTES:
        return dictionary.indexOf(new ByteArray((byte[]) value));
      case BIG_DECIMAL:
        return dictionary.indexOf((BigDecimal) value);
      default:
        throw new IllegalStateException("Unsupported data type: " + dataType);
    }
  }

  @Override
  public int getCardinality() {
    return _compactedCardinality;
  }

  @Override
  public Object getUniqueValuesSet() {
    return _compactedUniqueValues;
  }

  /**
   * Wrapper class to hold a value with its original dictionary ID for type-safe sorting.
   */
  class ValueWithOriginalId<T extends Comparable<? super T>> {
    final T _value;
    final int _originalId;

    ValueWithOriginalId(T value, int originalId) {
      _value = value;
      _originalId = originalId;
    }
  }
}
