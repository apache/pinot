/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectSet;
import java.util.Arrays;


/**
 * Extension of {@link AbstractColumnStatisticsCollector} for byte[] column type.
 */
public class BytesColumnPredIndexStatsCollector extends AbstractColumnStatisticsCollector {
  private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

  private byte[] _min = EMPTY_BYTE_ARRAY;
  private byte[] _max = EMPTY_BYTE_ARRAY;

  private int _lengthOfShortestEntry = Integer.MAX_VALUE;
  private int _lengthOfLongestEntry = 0;

  private final ObjectSet<ByteArray> _rawBytesSet;
  private final ObjectSet<ByteArray> _aggregateBytesSet;

  private ByteArray[] _sortedBytesList;
  private boolean _sealed = false;

  public BytesColumnPredIndexStatsCollector(String column, StatsCollectorConfig statsCollectorConfig) {
    super(column, statsCollectorConfig);
    _rawBytesSet = new ObjectOpenHashSet<>(INITIAL_HASH_SET_SIZE);
    _aggregateBytesSet = new ObjectOpenHashSet<>(INITIAL_HASH_SET_SIZE);
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
  private void collectEntry(Object entry, ObjectSet<ByteArray> set) {
    if (entry instanceof Object[]) {
      for (final Object e : (Object[]) entry) {
        byte[] value = (byte[]) e;
        set.add(new ByteArray(value));

        _lengthOfShortestEntry = Math.min(_lengthOfShortestEntry, value.length);
        _lengthOfLongestEntry = Math.max(_lengthOfLongestEntry, value.length);
      }

      if (maxNumberOfMultiValues < ((Object[]) entry).length) {
        maxNumberOfMultiValues = ((Object[]) entry).length;
      }
      updateTotalNumberOfEntries((Object[]) entry);
    } else {
      ByteArray value;
      if (entry != null) {
        value = (ByteArray) entry;
      } else {
        value = (ByteArray) fieldSpec.getDefaultNullValue();
      }

      addressSorted(value);
      updatePartition(value);
      set.add(value);

      int valueLength = value.length();
      _lengthOfShortestEntry = Math.min(_lengthOfShortestEntry, valueLength);
      _lengthOfLongestEntry = Math.max(_lengthOfLongestEntry, valueLength);

      totalNumberOfEntries++;
    }
  }

  /**
   * {@inheritDoc}
   * @param entry Entry to be collected
   * @param isAggregated True for aggregated, False for raw.
   */
  @Override
  public void collect(Object entry, boolean isAggregated) {
    if (isAggregated) {
      collectEntry(entry, _aggregateBytesSet);
    } else {
      collectEntry(entry, _rawBytesSet);
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
  public byte[] getMinValue() {
    if (_sealed) {
      return _min;
    }
    throw new IllegalStateException("you must seal the collector first before asking for min value");
  }

  @Override
  public byte[] getMaxValue() {
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
    _sealed = true;
    _sortedBytesList = new ByteArray[_rawBytesSet.size()];
    _rawBytesSet.toArray(_sortedBytesList);

    Arrays.sort(_sortedBytesList);

    if (_sortedBytesList.length == 0) {
      _min = null;
      _max = null;
      return;
    }

    // Update min/max based on raw docs.
    _min = _sortedBytesList[0].getBytes();
    _max = _sortedBytesList[_sortedBytesList.length - 1].getBytes();

    // Merge the raw and aggregated docs, so stats for dictionary creation are collected correctly.
    int numAggregated = _aggregateBytesSet.size();
    if (numAggregated > 0) {
      _rawBytesSet.addAll(_aggregateBytesSet);
      _sortedBytesList = new ByteArray[_rawBytesSet.size()];
      _rawBytesSet.toArray(_sortedBytesList);
      Arrays.sort(_sortedBytesList);
    }
  }
}
