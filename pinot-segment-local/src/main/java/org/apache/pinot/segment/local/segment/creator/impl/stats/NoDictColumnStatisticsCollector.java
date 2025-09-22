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

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import javax.ws.rs.NotSupportedException;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.pinot.segment.spi.creator.StatsCollectorConfig;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.apache.pinot.spi.utils.ByteArray;


/**
 * Column statistics collector for no-dictionary columns that avoids storing unique values and thus reduces memory
 * Behavior:
 * - getUniqueValuesSet() throws NotImplementedException
 * - getCardinality() returns total entries collected
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class NoDictColumnStatisticsCollector extends AbstractColumnStatisticsCollector {
  private Comparable _minValue;
  private Comparable _maxValue;
  private int _minLength = Integer.MAX_VALUE;
  private int _maxLength = 0;
  private int _maxRowLength = 0;
  private boolean _sealed = false;

  public NoDictColumnStatisticsCollector(String column, StatsCollectorConfig statsCollectorConfig) {
    super(column, statsCollectorConfig);
  }

  @Override
  public void collect(Object entry) {
    assert !_sealed;
    if (entry instanceof Object[]) {
      // todo - handle native double[], int[], long[] etc too.
      Object[] values = (Object[]) entry;
      int rowLength = 0;
      for (Object obj : values) {
        if (obj instanceof BigDecimal) {
          // BigDecimalColumnPreIndexStatsCollector doesn't support multi-value
          throw new UnsupportedOperationException();
        }
        updateMinMax(obj);
        int len = valueLength(obj);
        _minLength = Math.min(_minLength, len);
        _maxLength = Math.max(_maxLength, len);
        rowLength += len;
      }
      _maxNumberOfMultiValues = Math.max(_maxNumberOfMultiValues, values.length);
      _maxRowLength = Math.max(_maxRowLength, rowLength);
      updateTotalNumberOfEntries(values);
    } else {
      addressSorted(toComparable(entry));
      updateMinMax(entry);
      if (isPartitionEnabled()) {
        updatePartition(entry.toString());
      }
      int len = valueLength(entry);
      _minLength = Math.min(_minLength, len);
      _maxLength = Math.max(_maxLength, len);
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

  private int valueLength(Object value) {
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
      return 8; // fixed-width approximation; not used for fixed types' length assertions generally
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
    return _totalNumberOfEntries;
  }

  @Override
  public void seal() {
    _sealed = true;
  }
}


