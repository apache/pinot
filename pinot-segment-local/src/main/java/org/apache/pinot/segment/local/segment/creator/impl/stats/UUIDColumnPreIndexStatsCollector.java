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
package org.apache.pinot.segment.local.segment.creator.impl.stats;

import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import java.util.Arrays;
import java.util.Set;
import org.apache.pinot.segment.spi.creator.StatsCollectorConfig;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.UUIDUtils;


/**
 * Extension of {@link AbstractColumnStatisticsCollector} for UUID column type.
 * UUIDs are stored as fixed 16-byte arrays.
 */
public class UUIDColumnPreIndexStatsCollector extends AbstractColumnStatisticsCollector {
  private static final int UUID_BYTE_SIZE = 16;

  private Set<ByteArray> _values = new ObjectOpenHashSet<>(INITIAL_HASH_SET_SIZE);
  private int _maxRowLength = 0;
  private ByteArray[] _sortedValues;
  private boolean _sealed = false;

  public UUIDColumnPreIndexStatsCollector(String column, StatsCollectorConfig statsCollectorConfig) {
    super(column, statsCollectorConfig);
  }

  @Override
  public void collect(Object entry) {
    assert !_sealed;

    if (entry instanceof Object[]) {
      // Multi-value UUID column
      Object[] values = (Object[]) entry;
      int rowLength = 0;
      for (Object obj : values) {
        ByteArray value = new ByteArray((byte[]) obj);
        if (value.length() != UUID_BYTE_SIZE) {
          throw new IllegalArgumentException(
              "UUID must be exactly 16 bytes, got " + value.length() + " bytes for column: " + _fieldSpec.getName());
        }
        _values.add(value);
        rowLength += UUID_BYTE_SIZE;
      }
      _maxNumberOfMultiValues = Math.max(_maxNumberOfMultiValues, values.length);
      _maxRowLength = Math.max(_maxRowLength, rowLength);
      updateTotalNumberOfEntries(values);
    } else {
      // Single-value UUID column
      ByteArray value = new ByteArray((byte[]) entry);
      if (value.length() != UUID_BYTE_SIZE) {
        throw new IllegalArgumentException(
            "UUID must be exactly 16 bytes, got " + value.length() + " bytes for column: " + _fieldSpec.getName());
      }
      addressSorted(value);
      if (_values.add(value)) {
        if (isPartitionEnabled()) {
          // Use UUID string representation for partition
          updatePartition(UUIDUtils.toStringSafe(value.getBytes()));
        }
        _maxRowLength = UUID_BYTE_SIZE;
      }
      _totalNumberOfEntries++;
    }
  }

  @Override
  public ByteArray getMinValue() {
    if (_sealed) {
      return _sortedValues[0];
    }
    throw new IllegalStateException("you must seal the collector first before asking for min value");
  }

  @Override
  public ByteArray getMaxValue() {
    if (_sealed) {
      return _sortedValues[_sortedValues.length - 1];
    }
    throw new IllegalStateException("you must seal the collector first before asking for max value");
  }

  @Override
  public ByteArray[] getUniqueValuesSet() {
    if (_sealed) {
      return _sortedValues;
    }
    throw new IllegalStateException("you must seal the collector first before asking for unique values set");
  }

  @Override
  public int getLengthOfShortestElement() {
    // UUID is always 16 bytes
    return UUID_BYTE_SIZE;
  }

  @Override
  public int getLengthOfLargestElement() {
    // UUID is always 16 bytes
    return UUID_BYTE_SIZE;
  }

  @Override
  public int getMaxRowLengthInBytes() {
    return _maxRowLength > 0 ? _maxRowLength : UUID_BYTE_SIZE;
  }

  @Override
  public int getCardinality() {
    return _sealed ? _sortedValues.length : _values.size();
  }

  @Override
  public void seal() {
    if (!_sealed) {
      _sortedValues = _values.toArray(new ByteArray[0]);
      _values = null;
      Arrays.sort(_sortedValues);
      _sealed = true;
    }
  }
}
