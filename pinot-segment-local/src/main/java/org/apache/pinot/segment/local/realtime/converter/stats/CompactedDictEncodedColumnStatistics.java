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

  public CompactedDictEncodedColumnStatistics(DataSource dataSource, int[] sortedDocIds,
      ThreadSafeMutableRoaringBitmap validDocIds) {
    super(dataSource, sortedDocIds);
    _dataSource = dataSource;

    String columnName = dataSource.getDataSourceMetadata().getFieldSpec().getName();

    // Find which dictionary IDs are actually used by valid documents
    _usedDictIds = new HashSet<>();
    MutableForwardIndex forwardIndex = (MutableForwardIndex) dataSource.getForwardIndex();

    // Iterate through valid document IDs
    int[] validDocIdsArray = validDocIds.getMutableRoaringBitmap().toArray();
    boolean isSingleValue = forwardIndex.isSingleValue();

    for (int docId : validDocIdsArray) {
      if (isSingleValue) {
        // Single-value column: use getDictId()
        int dictId = forwardIndex.getDictId(docId);
        _usedDictIds.add(dictId);
      } else {
        // Multi-value column: use getDictIdMV()
        int[] dictIds = forwardIndex.getDictIdMV(docId);
        for (int dictId : dictIds) {
          _usedDictIds.add(dictId);
        }
      }
    }

    _compactedCardinality = _usedDictIds.size();

    // Create compacted unique values array with only used dictionary values
    Dictionary dictionary = dataSource.getDictionary();
    Object originalValues = dictionary.getSortedValues();

    // Extract the used values and sort them by value (not by dictionary ID)
    List<ValueWithOriginalId> usedValuesWithIds = new ArrayList<>();
    for (Integer dictId : _usedDictIds) {
      Object value = dictionary.get(dictId);
      usedValuesWithIds.add(new ValueWithOriginalId(value, dictId));
    }

    // Sort by values to ensure the compacted array is value-sorted
    usedValuesWithIds.sort((a, b) -> {
      @SuppressWarnings("unchecked")
      Comparable<Object> comparableA = (Comparable<Object>) a._value;
      return comparableA.compareTo(b._value);
    });

    // Create a compacted array containing only the used dictionary values in sorted order by value
    Class<?> componentType = originalValues.getClass().getComponentType();
    Object compacted = Array.newInstance(componentType, _compactedCardinality);

    for (int i = 0; i < _compactedCardinality; i++) {
      ValueWithOriginalId entry = usedValuesWithIds.get(i);
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

  /**
   * Helper class to store a value along with its original dictionary ID.
   */
  private static class ValueWithOriginalId {
    final Object _value;
    final int _originalDictId;

    ValueWithOriginalId(Object value, int originalDictId) {
      _value = value;
      _originalDictId = originalDictId;
    }
  }
}
