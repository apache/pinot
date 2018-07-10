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

import com.linkedin.pinot.core.segment.creator.StatsCollectorConfig;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import java.util.Arrays;


public class LongColumnPreIndexStatsCollector extends AbstractColumnStatisticsCollector {
  private Long min = null;
  private Long max = null;
  private final LongSet rawLongSet;
  private final LongSet aggregatedLongSet;
  private long[] sortedLongList;
  private boolean sealed = false;

  public LongColumnPreIndexStatsCollector(String column, StatsCollectorConfig statsCollectorConfig) {
    super(column, statsCollectorConfig);
    rawLongSet = new LongOpenHashSet(INITIAL_HASH_SET_SIZE);
    aggregatedLongSet = new LongOpenHashSet(INITIAL_HASH_SET_SIZE);
  }

  /**
   * Collect statistics for the given entry.
   * - Add it to the passed in set (which could be raw or aggregated)
   * - Update maximum number of values for Multi-valued entries
   * - Update Total number of entries
   * - Check if entry is sorted.
   * @param entry
   * @param set
   */
  private void collectEntry(Object entry, LongSet set) {
    if (entry instanceof Object[]) {
      for (final Object e : (Object[]) entry) {
        set.add(((Number) e).longValue());
      }
      if (maxNumberOfMultiValues < ((Object[]) entry).length) {
        maxNumberOfMultiValues = ((Object[]) entry).length;
      }
      updateTotalNumberOfEntries((Object[]) entry);
    } else {
      long value = ((Number) entry).longValue();
      addressSorted(value);
      updatePartition(value);
      set.add(value);
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
      collectEntry(entry, aggregatedLongSet);
    } else {
      collectEntry(entry, rawLongSet);
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
  public Long getMinValue() {
    if (sealed) {
      return min;
    }
    throw new IllegalStateException("you must seal the collector first before asking for min value");
  }

  @Override
  public Long getMaxValue() {
    if (sealed) {
      return max;
    }
    throw new IllegalStateException("you must seal the collector first before asking for min value");
  }

  @Override
  public Object getUniqueValuesSet() {
    if (sealed) {
      return sortedLongList;
    }
    throw new IllegalStateException("you must seal the collector first before asking for min value");
  }

  @Override
  public int getCardinality() {
    if (sealed) {
      return sortedLongList.length;
    }
    throw new IllegalStateException("you must seal the collector first before asking for min value");
  }

  @Override
  public boolean hasNull() {
    return false;
  }

  @Override
  public void seal() {
    sealed = true;
    sortedLongList = new long[rawLongSet.size()];
    rawLongSet.toArray(sortedLongList);

    Arrays.sort(sortedLongList);

    if (sortedLongList.length == 0) {
      min = null;
      max = null;
      return;
    }

    // Update min/max based on raw docs.
    min = sortedLongList[0];
    max = sortedLongList[sortedLongList.length - 1];

    // Merge the raw and aggregated docs, so stats for dictionary creation are collected correctly.
    int numAggregated = aggregatedLongSet.size();
    if (numAggregated > 0) {
      rawLongSet.addAll(aggregatedLongSet);
      sortedLongList = new long[rawLongSet.size()];
      rawLongSet.toArray(sortedLongList);
      Arrays.sort(sortedLongList);
    }
  }
}
