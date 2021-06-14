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


/**
 * Extension of {@link AbstractColumnStatisticsCollector} for byte[] column type.
 */
public class BytesColumnPredIndexStatsCollector extends AbstractColumnStatisticsCollector {
  private final Set<ByteArray> _values = new ObjectOpenHashSet<>(INITIAL_HASH_SET_SIZE);

  private int _minLength = Integer.MAX_VALUE;
  private int _maxLength = 0;
  private long _totalLength = 0;
  private ByteArray[] _sortedValues;
  private boolean _sealed = false;

  public BytesColumnPredIndexStatsCollector(String column, StatsCollectorConfig statsCollectorConfig) {
    super(column, statsCollectorConfig);
  }

  @Override
  public void collect(Object entry) {
    ByteArray value = new ByteArray((byte[]) entry);
    addressSorted(value);
    updatePartition(value);
    _values.add(value);

    int length = value.length();
    _minLength = Math.min(_minLength, length);
    _maxLength = Math.max(_maxLength, length);
    _totalLength += length;

    totalNumberOfEntries++;
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
    return _minLength;
  }

  @Override
  public int getLengthOfLargestElement() {
    if (_sealed) {
      return _maxLength;
    }
    throw new IllegalStateException("you must seal the collector first before asking for longest value");
  }

  @Override
  public long getTotalLengthOfAllElements() {
    if (_sealed) {
      return _totalLength;
    }
    throw new IllegalStateException("you must seal the collector first before asking for total length of all values");
  }

  @Override
  public int getCardinality() {
    if (_sealed) {
      return _sortedValues.length;
    }
    throw new IllegalStateException("you must seal the collector first before asking for cardinality");
  }

  @Override
  public boolean hasNull() {
    return false;
  }

  @Override
  public void seal() {
    _sortedValues = _values.toArray(new ByteArray[_values.size()]);
    Arrays.sort(_sortedValues);
    _sealed = true;
  }
}
