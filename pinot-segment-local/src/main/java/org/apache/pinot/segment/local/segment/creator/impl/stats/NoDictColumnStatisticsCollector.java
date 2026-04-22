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

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.google.common.base.Utf8;
import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.creator.StatsCollectorConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Column statistics collector for no-dictionary columns that avoids storing unique values and thus reduces memory
 * Behavior:
 * - getUniqueValuesSet() returns null
 * - getCardinality() returns approximate cardinality using HLL++
 * - Doesn't handle cases where values are of different types (e.g. int and long). This is expected.
 *   Individual type collectors (e.g. IntColumnPreIndexStatsCollector) also don't handle this case.
 *   At this point in the Pinot process, the type consistency of a key should already be enforced.
 *   So if such a case is encountered, it will be raised as an exception during collect()
 * Doesn't handle MAP data type as MapColumnPreIndexStatsCollector is optimized for no-dictionary collection
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class NoDictColumnStatisticsCollector extends AbstractColumnStatisticsCollector {
  private static final Logger LOGGER = LoggerFactory.getLogger(NoDictColumnStatisticsCollector.class);
  // Track exact uniques up to a threshold to avoid small-N underestimation and test flakiness
  private static final int EXACT_UNIQUE_TRACKING_THRESHOLD = 2048;

  private final boolean _isFixedWidth;
  private Comparable _minValue;
  private Comparable _maxValue;
  private int _minLength = Integer.MAX_VALUE;
  private int _maxLength = 0;
  private boolean _isAscii;
  private int _maxRowLength = 0;
  private boolean _sealed = false;

  // HLL Plus generally returns approximate cardinality >= actual cardinality which is desired
  private final HyperLogLogPlus _hllPlus;
  // Single-threaded: simple Set is sufficient; set to null once threshold exceeded to cap memory
  private Set<Object> _exactUniques = new HashSet<>();

  public NoDictColumnStatisticsCollector(String column, StatsCollectorConfig statsCollectorConfig) {
    super(column, statsCollectorConfig);
    _isFixedWidth = _valueType.isFixedWidth();
    _isAscii = _valueType == DataType.STRING;
    // Use default p and sp; can be made configurable via StatsCollectorConfig later if needed
    _hllPlus = new HyperLogLogPlus(
        CommonConstants.Helix.DEFAULT_HYPERLOGLOG_PLUS_P,
        CommonConstants.Helix.DEFAULT_HYPERLOGLOG_PLUS_SP);
    LOGGER.info("Initialized NoDictColumnStatisticsCollector for column: {}", column);
  }

  @Override
  public void collect(Object entry) {
    assert !_sealed;
    if (entry instanceof Object[]) {
      Object[] values = (Object[]) entry;
      int rowLength = 0;
      for (Object value : values) {
        Comparable comparable = toComparable(value);
        updateMinMax(comparable);
        updateHllPlus(value);
        trackExactUnique(value);
        if (!_isFixedWidth) {
          int length = getValueLength(value);
          _minLength = Math.min(_minLength, length);
          _maxLength = Math.max(_maxLength, length);
          if (_isAscii) {
            _isAscii = length == ((String) value).length();
          }
          rowLength += length;
        }
      }
      _maxNumberOfMultiValues = Math.max(_maxNumberOfMultiValues, values.length);
      _maxRowLength = Math.max(_maxRowLength, rowLength);
      updateTotalNumberOfEntries(values);
    } else if (entry instanceof int[] || entry instanceof long[]
        || entry instanceof float[] || entry instanceof double[]) {
      // Native multi-value types don't require length calculation because they're not variable-length
      int length;
      if (entry instanceof int[]) {
        int[] values = (int[]) entry;
        for (int value : values) {
          updateMinMax(value);
          updateHllPlus(value);
          trackExactUnique(value);
        }
        length = values.length;
      } else if (entry instanceof long[]) {
        long[] values = (long[]) entry;
        for (long value : values) {
          updateMinMax(value);
          updateHllPlus(value);
          trackExactUnique(value);
        }
        length = values.length;
      } else if (entry instanceof float[]) {
        float[] values = (float[]) entry;
        for (float value : values) {
          updateMinMax(value);
          updateHllPlus(value);
          trackExactUnique(value);
        }
        length = values.length;
      } else {
        double[] values = (double[]) entry;
        for (double value : values) {
          updateMinMax(value);
          updateHllPlus(value);
          trackExactUnique(value);
        }
        length = values.length;
      }
      _maxNumberOfMultiValues = Math.max(_maxNumberOfMultiValues, length);
      updateTotalNumberOfEntries(length);
    } else {
      Comparable comparable = toComparable(entry);
      addressSorted(comparable);
      updateMinMax(comparable);
      updateHllPlus(entry);
      trackExactUnique(entry);
      if (!_isFixedWidth) {
        int length = getValueLength(entry);
        _minLength = Math.min(_minLength, length);
        _maxLength = Math.max(_maxLength, length);
        if (_isAscii) {
          _isAscii = length == ((String) entry).length();
        }
      }
      if (isPartitionEnabled()) {
        updatePartition(comparable.toString());
      }
      _totalNumberOfEntries++;
    }
  }

  private void updateMinMax(Comparable value) {
    if (_minValue == null || value.compareTo(_minValue) < 0) {
      _minValue = value;
    }
    if (_maxValue == null || value.compareTo(_maxValue) > 0) {
      _maxValue = value;
    }
  }

  private Comparable toComparable(Object value) {
    if (value instanceof byte[]) {
      return new ByteArray((byte[]) value);
    }
    if (value instanceof Comparable) {
      return (Comparable) value;
    }
    throw new IllegalStateException("Unsupported value type " + value.getClass());
  }

  private int getValueLength(Object value) {
    switch (_valueType) {
      case BIG_DECIMAL:
        return BigDecimalUtils.byteSize((BigDecimal) value);
      case STRING:
        return Utf8.encodedLength((String) value);
      case BYTES:
        return ((byte[]) value).length;
      default:
        throw new IllegalStateException("Unsupported variable-width value type: " + _valueType);
    }
  }

  @Nullable
  @Override
  public Comparable getMinValue() {
    if (_sealed) {
      return _minValue;
    }
    throw new IllegalStateException("you must seal the collector first before asking for min value");
  }

  @Nullable
  @Override
  public Comparable getMaxValue() {
    if (_sealed) {
      return _maxValue;
    }
    throw new IllegalStateException("you must seal the collector first before asking for max value");
  }

  @Nullable
  @Override
  public Object getUniqueValuesSet() {
    return null;
  }

  @Override
  public int getCardinality() {
    // If we are still tracking exact uniques, return exact cardinality
    if (_exactUniques != null) {
      return _exactUniques.size();
    }
    // Get approximate distinct count estimate using HLL++ with a 10% upward buffer
    long estimate = Math.round(_hllPlus.cardinality() * 1.1);
    // There are cases where approximation can overshoot the actual number of entries.
    // Returning a cardinality greater than total entries can break assumptions.
    return estimate > getTotalNumberOfEntries() ? getTotalNumberOfEntries() : (int) estimate;
  }

  @Override
  public int getLengthOfShortestElement() {
    if (_isFixedWidth) {
      return _valueType.size();
    } else {
      return _minLength != Integer.MAX_VALUE ? _minLength : 0;
    }
  }

  @Override
  public int getLengthOfLongestElement() {
    return _isFixedWidth ? _valueType.size() : _maxLength;
  }

  @Override
  public boolean isAscii() {
    return _isAscii;
  }

  @Override
  public int getMaxRowLengthInBytes() {
    if (_isFixedWidth) {
      int elementSize = _valueType.size();
      return isSingleValue() ? elementSize : _maxNumberOfMultiValues * elementSize;
    } else {
      return isSingleValue() ? _maxLength : _maxRowLength;
    }
  }

  @Override
  public void seal() {
    _sealed = true;
  }

  private void updateHllPlus(Object value) {
    _hllPlus.offer(value);
  }

  private void trackExactUnique(Object value) {
    if (_exactUniques == null) {
      return;
    }
    Object key;
    if (value instanceof byte[]) {
      key = new ByteArray((byte[]) value);
    } else if (value instanceof BigDecimal) {
      // Use string representation to avoid scale-related equality issues:
      // BigDecimals with different scales (e.g., 1.0 vs 1.00) are not equal by default,
      // but their string representations normalize the value for cardinality tracking.
      key = value.toString();
    } else {
      key = value;
    }
    _exactUniques.add(key);
    if (_exactUniques.size() > EXACT_UNIQUE_TRACKING_THRESHOLD) {
      // Drop exact tracking once the threshold is exceeded to avoid unbounded memory.
      _exactUniques = null;
    }
  }
}
