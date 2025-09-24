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

import com.dynatrace.hash4j.distinctcount.UltraLogLog;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.pinot.segment.local.utils.UltraLogLogUtils;
import org.apache.pinot.segment.spi.creator.StatsCollectorConfig;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.CommonConstants;


/**
 * Column statistics collector for no-dictionary columns that avoids storing unique values and thus reduces memory
 * Behavior:
 * - getUniqueValuesSet() throws NotImplementedException
 * - getCardinality() returns approximate cardinality using ULL
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class NoDictColumnStatisticsCollector extends AbstractColumnStatisticsCollector {
  private Comparable _minValue;
  private Comparable _maxValue;
  private int _minLength = Integer.MAX_VALUE;
  private int _maxLength = -1; // default return value is -1
  private int _maxRowLength = -1; // default return value is -1
  private boolean _sealed = false;
  private final UltraLogLog _ull;

  public NoDictColumnStatisticsCollector(String column, StatsCollectorConfig statsCollectorConfig) {
    super(column, statsCollectorConfig);
    // Use default p; can be made configurable via StatsCollectorConfig later if needed
    _ull = UltraLogLog.create(CommonConstants.Helix.DEFAULT_ULTRALOGLOG_P);
  }

  @Override
  public void collect(Object entry) {
    assert !_sealed;
    if (entry instanceof Object[]) {
      Object[] values = (Object[]) entry;
      int rowLength = 0;
      for (Object value : values) {
        if (value instanceof BigDecimal) {
          // BigDecimalColumnPreIndexStatsCollector doesn't support multi-value
          throw new UnsupportedOperationException();
        }
        updateMinMax(value);
        updateUll(value);
        int len = getValueLength(value);
        _minLength = Math.min(_minLength, len);
        _maxLength = Math.max(_maxLength, len);
        rowLength += len;
      }
      _maxNumberOfMultiValues = Math.max(_maxNumberOfMultiValues, values.length);
      _maxRowLength = Math.max(_maxRowLength, rowLength);
      updateTotalNumberOfEntries(values);
    } else if (entry instanceof int[] || entry instanceof long[]
        || entry instanceof float[] || entry instanceof double[]) {
      // Native multi-value types don't require length calculation
      int length;
      if (entry instanceof int[]) {
        int[] values = (int[]) entry;
        for (int value : values) {
          updateMinMax(value);
          updateUll(value);
        }
        length = values.length;
      } else if (entry instanceof long[]) {
        long[] values = (long[]) entry;
        for (long value : values) {
          updateMinMax(value);
          updateUll(value);
        }
        length = values.length;
      } else if (entry instanceof float[]) {
        float[] values = (float[]) entry;
        for (float value : values) {
          updateMinMax(value);
          updateUll(value);
        }
        length = values.length;
      } else {
        double[] values = (double[]) entry;
        for (double value : values) {
          updateMinMax(value);
          updateUll(value);
        }
        length = values.length;
      }
      _maxNumberOfMultiValues = Math.max(_maxNumberOfMultiValues, length);
      updateTotalNumberOfEntries(length);
    } else {
      Comparable value = toComparable(entry);
      addressSorted(value);
      updateMinMax(entry);
      updateUll(entry);
      int len = getValueLength(entry);
      _minLength = Math.min(_minLength, len);
      _maxLength = Math.max(_maxLength, len);
      if (isPartitionEnabled()) {
        updatePartition(value.toString());
      }
      _maxRowLength = Math.max(_maxRowLength, len);
      _totalNumberOfEntries++;
    }
  }

  private void updateMinMax(Object value) {
    Comparable comp = toComparable(value);
    if (_minValue == null || comp.compareTo(_minValue) < 0) {
      _minValue = comp;
    }
    if (_maxValue == null || comp.compareTo(_maxValue) > 0) {
      _maxValue = comp;
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
    if (value instanceof byte[]) {
      return ((byte[]) value).length;
    }
    if (value instanceof CharSequence) {
      return ((CharSequence) value).toString().getBytes(StandardCharsets.UTF_8).length;
    }
    if (value instanceof BigDecimal) {
      return BigDecimalUtils.byteSize((BigDecimal) value);
    }
    if (value instanceof Number) {
      return 8; // fixed-width approximation as it's not actually required for numeric fields which are of fixed length
    }
    throw new IllegalStateException("Unsupported value type " + value.getClass());
  }

  @Override
  public Object getMinValue() {
    if (_sealed) {
      return _minValue;
    }
    throw new IllegalStateException("you must seal the collector first before asking for min value");
  }

  @Override
  public Object getMaxValue() {
    if (_sealed) {
      return _maxValue;
    }
    throw new IllegalStateException("you must seal the collector first before asking for max value");
  }

  @Override
  public Object getUniqueValuesSet() {
    throw new NotImplementedException("getUniqueValuesSet is not supported in NoDictColumnStatisticsCollector");
  }

  @Override
  public int getLengthOfShortestElement() {
    return _minLength == Integer.MAX_VALUE ? -1 : _minLength;
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
    // Return approximate distinct count estimate
    long estimate = Math.round(_ull.getDistinctCountEstimate());
    // There are cases where ULL can overshoot the actual number of entries.
    // Returning a cardinality greater than total entries can break assumptions.
    return estimate > getTotalNumberOfEntries() ? getTotalNumberOfEntries() : (int) estimate;
  }

  @Override
  public void seal() {
    _sealed = true;
  }

  private void updateUll(Object value) {
    // Hash and add to ULL using shared utility to ensure deterministic canonicalization
    UltraLogLogUtils.hashObject(value).ifPresent(_ull::add);
  }
}
