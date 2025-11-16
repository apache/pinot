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
import java.util.HashSet;
import java.util.Set;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.mutable.MutableForwardIndex;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.roaringbitmap.PeekableIntIterator;


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

    // Find which dictionary IDs are actually used by valid documents
    // AND collect the unique values at the same time to avoid duplicate dictionary lookups
    _usedDictIds = new HashSet<>();
    Set<Comparable<Object>> usedValues = new HashSet<>(); // Store unique values in valid docs (not sorted)
    MutableForwardIndex forwardIndex = (MutableForwardIndex) dataSource.getForwardIndex();
    Dictionary dictionary = dataSource.getDictionary();

    // Iterate through valid document IDs using bitmap iterator
    boolean isSingleValue = forwardIndex.isSingleValue();
    PeekableIntIterator iterator = validDocIds.getMutableRoaringBitmap().toRoaringBitmap().getIntIterator();
    while (iterator.hasNext()) {
      int docId = iterator.next();
      if (isSingleValue) {
        int dictId = forwardIndex.getDictId(docId);
        _totalNumberOfEntries++; // Count each valid document

        if (_usedDictIds.add(dictId)) {
          Comparable<Object> value = (Comparable<Object>) dictionary.get(dictId);
          usedValues.add(value);
          updateMinMaxValue(value);
        }
      } else {
        int[] dictIds = forwardIndex.getDictIdMV(docId);
        _totalNumberOfEntries += dictIds.length; // Count all values in this document
        _maxNumberOfMultiValues = Math.max(_maxNumberOfMultiValues, dictIds.length);
        for (int dictId : dictIds) {
          if (_usedDictIds.add(dictId)) {
            Comparable<Object> value = (Comparable<Object>) dictionary.get(dictId);
            usedValues.add(value);
            updateMinMaxValue(value);
          }
        }
      }
    }

    _compactedCardinality = _usedDictIds.size();

    // Iterate through the already-sorted original values and directly populate the compacted array by keeping the
    // used values
    Object originalValues = dictionary.getSortedValues();
    Class<?> componentType = originalValues.getClass().getComponentType();
    Object compacted = Array.newInstance(componentType, _compactedCardinality);

    int compactedIndex = 0;
    for (int i = 0; i < Array.getLength(originalValues); i++) {
      Object value = Array.get(originalValues, i);
      if (usedValues.contains(value)) {
        Array.set(compacted, compactedIndex++, value);
      }
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

  private void updateMinMaxValue(Object value) {
    if (_minValue == null || ((Comparable) value).compareTo(_minValue) < 0) {
      _minValue = value;
    }
    if (_maxValue == null || ((Comparable) value).compareTo(_maxValue) > 0) {
      _maxValue = value;
    }
  }
}
