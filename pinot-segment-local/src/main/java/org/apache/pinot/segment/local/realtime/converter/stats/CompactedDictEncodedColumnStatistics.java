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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.mutable.MutableForwardIndex;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.segment.spi.index.reader.Dictionary;


/**
 * Column statistics for dictionary columns with dictionary-encoded forward indexes.
 * Uses direct getDictId() calls for single-value columns and getDictIdMV() calls for multi-value columns
 * to find which dictionary entries are used by valid documents.
 *
 * This is used when:
 * - Column has a dictionary (dataSource.getDictionary() != null)
 * - Forward index is dictionary-encoded (forwardIndex.isDictionaryEncoded() == true)
 * - Commit-time compaction is enabled
 */
public class CompactedDictEncodedColumnStatistics extends MutableColumnStatistics {
  private final Set<Integer> _usedDictIds;
  private final int _compactedCardinality;
  private final DataSource _dataSource;
  private final Object _compactedUniqueValues;
  private int _maxNumberOfMultiValues = 1; // Track max multi-values for buffer allocation
  private int _totalNumberOfEntries = 0; // Total number of entries in the column
  private Object _minValue; // Track min value from valid documents
  private Object _maxValue; // Track max value from valid documents

  public CompactedDictEncodedColumnStatistics(DataSource dataSource, int[] sortedDocIds,
      ThreadSafeMutableRoaringBitmap validDocIds) {
    super(dataSource, sortedDocIds);
    _dataSource = dataSource;

    String columnName = dataSource.getDataSourceMetadata().getFieldSpec().getName();

    // Find which dictionary IDs are actually used by valid documents
    _usedDictIds = new HashSet<>();
    MutableForwardIndex forwardIndex = (MutableForwardIndex) dataSource.getForwardIndex();
    Dictionary dictionary = dataSource.getDictionary();

    // Iterate through valid document IDs
    int[] validDocIdsArray = validDocIds.getMutableRoaringBitmap().toArray();
    boolean isSingleValue = forwardIndex.isSingleValue();

    for (int docId : validDocIdsArray) {
      if (isSingleValue) {
        // Single-value column: use getDictId()
        int dictId = forwardIndex.getDictId(docId);
        _usedDictIds.add(dictId);
        _totalNumberOfEntries++; // Count each valid document

        // Track min/max values
        Object value = dictionary.get(dictId);
        updateMinMaxValue(value);
      } else {
        // Multi-value column: use getDictIdMV()
        int[] dictIds = forwardIndex.getDictIdMV(docId);
        _totalNumberOfEntries += dictIds.length; // Count all values in this document
        _maxNumberOfMultiValues = Math.max(_maxNumberOfMultiValues, dictIds.length);
        for (int dictId : dictIds) {
          _usedDictIds.add(dictId);

          // Track min/max values
          Object value = dictionary.get(dictId);
          updateMinMaxValue(value);
        }
      }
    }

    _compactedCardinality = _usedDictIds.size();

    // Create compacted unique values array with only used dictionary values
    Object originalValues = dictionary.getSortedValues();

    // Extract the used values and sort them by value (not by dictionary ID)
    List<ValueWithOriginalId<Comparable<Object>>> usedValuesWithIds = new ArrayList<>();
    for (Integer dictId : _usedDictIds) {
      Comparable<Object> value = (Comparable<Object>) dictionary.get(dictId);
      usedValuesWithIds.add(new ValueWithOriginalId<>(value, dictId));
    }

    // Sort by values to ensure the compacted array is value-sorted
    usedValuesWithIds.sort(Comparator.comparing(a -> a._value));

    // Create a compacted array containing only the used dictionary values in sorted order by value
    Class<?> componentType = originalValues.getClass().getComponentType();
    Object compacted = Array.newInstance(componentType, _compactedCardinality);

    for (int i = 0; i < _compactedCardinality; i++) {
      ValueWithOriginalId<Comparable<Object>> entry = usedValuesWithIds.get(i);
      Array.set(compacted, i, entry._value);
    }
    _compactedUniqueValues = compacted;
  }

  @Override
  public int getCardinality() {
    return _compactedCardinality;
  }

  @Override
  public Object getUniqueValuesSet() {
    return _compactedUniqueValues;
  }

  @Override
  public int getMaxNumberOfMultiValues() {
    return _maxNumberOfMultiValues;
  }

  @Override
  public int getTotalNumberOfEntries() {
    return _totalNumberOfEntries;
  }

  @Override
  public Object getMinValue() {
    return _minValue;
  }

  @Override
  public Object getMaxValue() {
    return _maxValue;
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

  private void updateMinMaxValue(Object value) {
    if (_minValue == null || ((Comparable) value).compareTo(_minValue) < 0) {
      _minValue = value;
    }
    if (_maxValue == null || ((Comparable) value).compareTo(_maxValue) > 0) {
      _maxValue = value;
    }
  }
}
