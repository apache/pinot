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

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import java.util.Arrays;
import org.apache.pinot.segment.spi.creator.StatsCollectorConfig;


public class LongColumnPreIndexStatsCollector extends AbstractColumnStatisticsCollector {
  private LongSet _values = new LongOpenHashSet(INITIAL_HASH_SET_SIZE);
  private long[] _sortedValues;
  private boolean _sealed = false;
  private long _prevValue = Long.MIN_VALUE;

  public LongColumnPreIndexStatsCollector(String column, StatsCollectorConfig statsCollectorConfig) {
    super(column, statsCollectorConfig);
  }

  @Override
  public void collect(Object entry) {
    assert !_sealed;

    if (entry instanceof Object[]) {
      Object[] values = (Object[]) entry;
      for (Object obj : values) {
        long value = (long) obj;
        _values.add(value);
      }

      _maxNumberOfMultiValues = Math.max(_maxNumberOfMultiValues, values.length);
      updateTotalNumberOfEntries(values);
    } else {
      long value = (long) entry;
      addressSorted(value);
      if (_values.add(value)) {
        if (isPartitionEnabled()) {
          updatePartition(Long.toString(value));
        }
      }

      _totalNumberOfEntries++;
    }
  }

  private void addressSorted(long entry) {
    if (_sorted) {
      _sorted = entry >= _prevValue;
      _prevValue = entry;
    }
  }

  @Override
  public Long getMinValue() {
    if (_sealed) {
      return _sortedValues[0];
    }
    throw new IllegalStateException("you must seal the collector first before asking for min value");
  }

  @Override
  public Long getMaxValue() {
    if (_sealed) {
      return _sortedValues[_sortedValues.length - 1];
    }
    throw new IllegalStateException("you must seal the collector first before asking for max value");
  }

  @Override
  public Object getUniqueValuesSet() {
    if (_sealed) {
      return _sortedValues;
    }
    throw new IllegalStateException("you must seal the collector first before asking for unique values set");
  }

  @Override
  public int getCardinality() {
    if (_sealed) {
      return _sortedValues.length;
    }
    throw new IllegalStateException("you must seal the collector first before asking for cardinality");
  }

  @Override
  public void seal() {
    if (!_sealed) {
      _sortedValues = _values.toLongArray();
      _values = null;
      Arrays.sort(_sortedValues);
      _sealed = true;
    }
  }
}
