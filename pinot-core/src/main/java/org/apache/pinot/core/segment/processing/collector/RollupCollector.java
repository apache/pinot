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
package org.apache.pinot.core.segment.processing.collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * A Collector that rolls up the incoming records on unique dimensions + time columns, based on provided aggregation types for metrics.
 * By default will use the SUM aggregation on metrics.
 */
public class RollupCollector implements Collector {

  private final Map<Record, GenericRow> _collection = new HashMap<>();
  private final GenericRowSorter _sorter;

  private final int _keySize;
  private final int _valueSize;
  private final String[] _keyColumns;
  private final String[] _valueColumns;
  private final ValueAggregator[] _valueAggregators;

  public RollupCollector(CollectorConfig collectorConfig, Schema schema) {
    int keySize = 0;
    int valueSize = 0;
    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      if (!fieldSpec.isVirtualColumn()) {
        if (fieldSpec.getFieldType() == FieldSpec.FieldType.METRIC) {
          valueSize ++;
        } else {
          keySize ++;
        }
      }
    }
    _keySize = keySize;
    _valueSize = valueSize;
    _keyColumns = new String[_keySize];
    _valueColumns = new String[_valueSize];
    _valueAggregators = new ValueAggregator[_valueSize];

    Map<String, ValueAggregatorFactory.ValueAggregatorType> aggregatorTypeMap = collectorConfig.getAggregatorTypeMap();
    if (aggregatorTypeMap == null) {
      aggregatorTypeMap = Collections.emptyMap();
    }
    int valIdx = 0;
    int keyIdx = 0;
    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      if (!fieldSpec.isVirtualColumn()) {
        String name = fieldSpec.getName();
        if (fieldSpec.getFieldType() == FieldSpec.FieldType.METRIC) {
          _valueColumns[valIdx] = name;
          ValueAggregatorFactory.ValueAggregatorType aggregatorType =
              aggregatorTypeMap.getOrDefault(name, ValueAggregatorFactory.ValueAggregatorType.SUM);
          _valueAggregators[valIdx] =
              ValueAggregatorFactory.getValueAggregator(aggregatorType.toString(), fieldSpec.getDataType());
          valIdx++;
        } else {
          _keyColumns[keyIdx++] = name;
        }
      }
    }

    List<String> sortOrder = collectorConfig.getSortOrder();
    if (CollectionUtils.isNotEmpty(sortOrder)) {
      _sorter = new GenericRowSorter(sortOrder, schema);
    } else {
      _sorter = null;
    }
  }

  /**
   * If a row already exists in the collection (based on dimension + time columns), rollup the metric values, else add the row
   */
  @Override
  public void collect(GenericRow genericRow) {
    Object[] key = new Object[_keySize];
    for (int i = 0; i < _keySize; i++) {
      key[i] = genericRow.getValue(_keyColumns[i]);
    }
    Record keyRecord = new Record(key);
    GenericRow prev = _collection.get(keyRecord);
    if (prev == null) {
      _collection.put(keyRecord, genericRow);
    } else {
      for (int i = 0; i < _valueSize; i++) {
        String valueColumn = _valueColumns[i];
        Object aggregate = _valueAggregators[i].aggregate(prev.getValue(valueColumn), genericRow.getValue(valueColumn));
        prev.putValue(valueColumn, aggregate);
      }
    }
  }

  @Override
  public Iterator<GenericRow> iterator() {
    Iterator<GenericRow> iterator;
    if (_sorter != null) {
      List<GenericRow> sortedRows = new ArrayList<>(_collection.values());
      _sorter.sort(sortedRows);
      iterator = sortedRows.iterator();
    } else {
      iterator = _collection.values().iterator();
    }
    return iterator;
  }

  @Override
  public int size() {
    return _collection.size();
  }

  @Override
  public void reset() {
    _collection.clear();
  }

  /**
   * A representation for the keys of the generic row
   * Note that the dimensions can have multi-value columns, and hence the equals and hashCode need deep array operations
   */
  private static class Record {
    private final Object[] _keyParts;

    public Record(Object[] keyParts) {
      _keyParts = keyParts;
    }

    // NOTE: Not check class for performance concern
    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(Object o) {
      return Arrays.deepEquals(_keyParts, ((Record) o)._keyParts);
    }

    @Override
    public int hashCode() {
      return Arrays.deepHashCode(_keyParts);
    }
  }
}
