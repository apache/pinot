/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.segment.creator.impl.stats;

import com.linkedin.pinot.common.utils.primitive.ByteArray;
import com.linkedin.pinot.core.segment.creator.StatsCollectorConfig;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;


/**
 * Extension of {@link AbstractColumnStatisticsCollector} for byte[] column type.
 */
public class BytesColumnPredIndexStatsCollector extends AbstractColumnStatisticsCollector {
  private ByteArray _min = null;
  private ByteArray _max = null;

  private int _lengthOfShortestEntry = Integer.MAX_VALUE;
  private int _lengthOfLongestEntry = 0;

  private final Set<ByteArray> _rawBytesSet;
  private final Set<ByteArray> _aggregateBytesSet;

  private ByteArray[] _sortedBytesList;
  private boolean _sealed = false;

  public BytesColumnPredIndexStatsCollector(String column, StatsCollectorConfig statsCollectorConfig) {
    super(column, statsCollectorConfig);
    _rawBytesSet = new HashSet<>(INITIAL_HASH_SET_SIZE);
    _aggregateBytesSet = new HashSet<>(INITIAL_HASH_SET_SIZE);
  }

  /**
   * Collect statistics for the given entry.
   * - Add it to the passed in set (which could be raw or aggregated)
   * - Update maximum number of values for Multi-valued entries
   * - Update Total number of entries
   * - Check if entry is sorted.
   *
   * @param entry Entry value
   * @param set Set containing entries
   */
  private void collectEntry(byte[] entry, Set<ByteArray> set) {
    ByteArray value = new ByteArray(entry);
    addressSorted(value);
    updatePartition(value);
    set.add(value);

    int valueLength = value.length();
    _lengthOfShortestEntry = Math.min(_lengthOfShortestEntry, valueLength);
    _lengthOfLongestEntry = Math.max(_lengthOfLongestEntry, valueLength);

    totalNumberOfEntries++;
  }

  /**
   * {@inheritDoc}
   * @param entry Entry to be collected
   * @param isAggregated True for aggregated, False for raw.
   */
  @Override
  public void collect(Object entry, boolean isAggregated) {
    assert entry instanceof byte[];
    if (isAggregated) {
      collectEntry((byte[]) entry, _aggregateBytesSet);
    } else {
      collectEntry((byte[]) entry, _rawBytesSet);
    }
  }

  /**
   * {@inheritDoc}
   * @param entry Entry to be collected
   */
  @Override
  public void collect(Object entry) {
    collect(entry, false /* isAggregated */);
  }

  @Override
  public ByteArray getMinValue() {
    if (_sealed) {
      return _min;
    }
    throw new IllegalStateException("you must seal the collector first before asking for min value");
  }

  @Override
  public ByteArray getMaxValue() {
    if (_sealed) {
      return _max;
    }
    throw new IllegalStateException("you must seal the collector first before asking for max value");
  }

  @Override
  public ByteArray[] getUniqueValuesSet() {
    if (_sealed) {
      return _sortedBytesList;
    }
    throw new IllegalStateException("you must seal the collector first before asking for unique values set");
  }

  @Override
  public int getLengthOfShortestElement() {
    return _lengthOfShortestEntry;
  }

  @Override
  public int getLengthOfLargestElement() {
    if (_sealed) {
      return _lengthOfLongestEntry;
    }
    throw new IllegalStateException("you must seal the collector first before asking for longest value");
  }

  @Override
  public int getCardinality() {
    if (_sealed) {
      return _sortedBytesList.length;
    }
    throw new IllegalStateException("you must seal the collector first before asking for cardinality");
  }

  @Override
  public boolean hasNull() {
    return false;
  }

  @Override
  public void seal() {
    _sortedBytesList = _rawBytesSet.toArray(new ByteArray[_rawBytesSet.size()]);
    Arrays.sort(_sortedBytesList);

    // Update min/max based on raw docs.
    _min = _sortedBytesList[0];
    _max = _sortedBytesList[_sortedBytesList.length - 1];

    // Merge the raw and aggregated docs, so stats for dictionary creation are collected correctly.
    if (!_aggregateBytesSet.isEmpty()) {
      _rawBytesSet.addAll(_aggregateBytesSet);
      _sortedBytesList = _rawBytesSet.toArray(new ByteArray[_rawBytesSet.size()]);
      Arrays.sort(_sortedBytesList);
    }

    _sealed = true;
  }
}
