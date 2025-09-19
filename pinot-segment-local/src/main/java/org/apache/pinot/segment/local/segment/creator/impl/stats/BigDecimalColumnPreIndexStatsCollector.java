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
import java.math.BigDecimal;
import java.util.Arrays;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.creator.StatsCollectorConfig;
import org.apache.pinot.spi.utils.BigDecimalUtils;


/**
 * Extension of {@link AbstractColumnStatisticsCollector} for BigDecimal column type.
 */
public class BigDecimalColumnPreIndexStatsCollector extends AbstractColumnStatisticsCollector {
  @Nullable
  private ObjectOpenHashSet<BigDecimal> _values;
  private int _minLength = Integer.MAX_VALUE;
  private int _maxLength = 0;
  private int _maxRowLength = 0;
  @Nullable
  private BigDecimal[] _sortedValues;
  private boolean _sealed = false;
  private BigDecimal _minValue;
  private BigDecimal _maxValue;

  public BigDecimalColumnPreIndexStatsCollector(String column, StatsCollectorConfig statsCollectorConfig) {
    super(column, statsCollectorConfig);
    if (_dictionaryEnabled) {
      _values = new ObjectOpenHashSet<>(INITIAL_HASH_SET_SIZE);
    }
  }

  @Override
  public void collect(Object entry) {
    assert !_sealed;

    if (entry instanceof Object[]) {
      throw new UnsupportedOperationException();
    } else {
      BigDecimal value = (BigDecimal) entry;
      int length = BigDecimalUtils.byteSize(value);
      addressSorted(value);
      boolean isNewValue = _dictionaryEnabled ? _values.add(value) : true;
      if (isNewValue) {
        if (isPartitionEnabled()) {
          updatePartition(value.toPlainString());
        }
        _minLength = Math.min(_minLength, length);
        _maxLength = Math.max(_maxLength, length);
        _maxRowLength = _maxLength;
        if (_minValue == null || value.compareTo(_minValue) < 0) {
          _minValue = value;
        }
        if (_maxValue == null || value.compareTo(_maxValue) > 0) {
          _maxValue = value;
        }
      }
      _totalNumberOfEntries++;
    }
  }

  @Override
  public BigDecimal getMinValue() {
    if (_sealed) {
      return _minValue;
    }
    throw new IllegalStateException("you must seal the collector first before asking for min value");
  }

  @Override
  public BigDecimal getMaxValue() {
    if (_sealed) {
      return _maxValue;
    }
    throw new IllegalStateException("you must seal the collector first before asking for max value");
  }

  @Override
  public BigDecimal[] getUniqueValuesSet() {
    if (_sealed) {
      return _dictionaryEnabled ? _sortedValues : null;
    }
    throw new IllegalStateException("you must seal the collector first before asking for unique values set");
  }

  @Override
  public int getLengthOfShortestElement() {
    return _minLength;
  }

  @Override
  public int getLengthOfLargestElement() {
    return _maxLength;
  }

  @Override
  public int getMaxRowLengthInBytes() {
    return _maxRowLength;
  }

  @Override
  public int getCardinality() {
    if (_dictionaryEnabled) {
      return _sealed ? _sortedValues.length : _values.size();
    }
    return _totalNumberOfEntries;
  }

  @Override
  public void seal() {
    if (!_sealed) {
      if (_dictionaryEnabled) {
        _sortedValues = _values.toArray(new BigDecimal[0]);
        _values = null;
        Arrays.sort(_sortedValues);
      }
      _sealed = true;
    }
  }
}
