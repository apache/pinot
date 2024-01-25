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

import it.unimi.dsi.fastutil.floats.FloatOpenHashSet;
import it.unimi.dsi.fastutil.floats.FloatSet;
import java.util.Arrays;
import org.apache.pinot.segment.spi.creator.StatsCollectorConfig;


public class FloatColumnPreIndexStatsCollector extends AbstractColumnStatisticsCollector {
  private FloatSet _values = new FloatOpenHashSet(INITIAL_HASH_SET_SIZE);
  private float[] _sortedValues;
  private boolean _sealed = false;
  private float _prevValue = Float.NEGATIVE_INFINITY;

  public FloatColumnPreIndexStatsCollector(String column, StatsCollectorConfig statsCollectorConfig) {
    super(column, statsCollectorConfig);
  }

  @Override
  public void collect(Object entry) {
    assert !_sealed;

    if (entry instanceof Object[]) {
      Object[] values = (Object[]) entry;
      for (Object obj : values) {
        float value = (float) obj;
        _values.add(value);
      }

      _maxNumberOfMultiValues = Math.max(_maxNumberOfMultiValues, values.length);
      updateTotalNumberOfEntries(values);
    } else {
      float value = (float) entry;
      addressSorted(value);
      if (_values.add(value)) {
        if (isPartitionEnabled()) {
          updatePartition(Float.toString(value));
        }
      }

      _totalNumberOfEntries++;
    }
  }

  private void addressSorted(float entry) {
    if (_sorted) {
      _sorted = entry >= _prevValue;
      _prevValue = entry;
    }
  }

  @Override
  public Float getMinValue() {
    if (_sealed) {
      return _sortedValues[0];
    }
    throw new IllegalStateException("you must seal the collector first before asking for min value");
  }

  @Override
  public Float getMaxValue() {
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
      _sortedValues = _values.toFloatArray();
      _values = null;
      Arrays.sort(_sortedValues);
      _sealed = true;
    }
  }
}
