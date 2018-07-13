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
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.util.Arrays;


public class IntColumnPreIndexStatsCollector extends AbstractColumnStatisticsCollector {
  private Integer min = null;
  private Integer max = null;
  private final IntSet rawIntSet;
  private final IntSet aggregatedIntSet;
  private int[] sortedIntList;
  private boolean sealed = false;

  public IntColumnPreIndexStatsCollector(String column, StatsCollectorConfig statsCollectorConfig) {
    super(column, statsCollectorConfig);
    rawIntSet = new IntOpenHashSet(INITIAL_HASH_SET_SIZE);
    aggregatedIntSet = new IntOpenHashSet(INITIAL_HASH_SET_SIZE);
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
  private void collectEntry(Object entry, IntSet set) {
    if (entry instanceof Object[]) {
      for (Object e : (Object[]) entry) {
        set.add(((Number) e).intValue());
      }
      if (maxNumberOfMultiValues < ((Object[]) entry).length) {
        maxNumberOfMultiValues = ((Object[]) entry).length;
      }
      updateTotalNumberOfEntries((Object[]) entry);
    } else {

      int value = ((Number) entry).intValue();
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
      collectEntry(entry, aggregatedIntSet);
    } else {
      collectEntry(entry, rawIntSet);
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
  public Integer getMinValue() {
    if (sealed) {
      return min;
    }
    throw new IllegalStateException("you must seal the collector first before asking for min value");
  }

  @Override
  public Integer getMaxValue() {
    if (sealed) {
      return max;
    }
    throw new IllegalStateException("you must seal the collector first before asking for max value");
  }

  @Override
  public Object getUniqueValuesSet() {
    if (sealed) {
      return sortedIntList;
    }
    throw new IllegalStateException("you must seal the collector first before asking for unique value set");
  }

  @Override
  public int getCardinality() {
    if (sealed) {
      return sortedIntList.length;
    }
    throw new IllegalStateException("you must seal the collector first before asking for cardinality");
  }

  @Override
  public boolean hasNull() {
    return false;
  }

  @Override
  public void seal() {
    sealed = true;
    sortedIntList = new int[rawIntSet.size()];
    rawIntSet.toArray(sortedIntList);

    Arrays.sort(sortedIntList);

    if (sortedIntList.length == 0) {
      min = null;
      max = null;
      return;
    }

    // Update min/max based on raw docs.
    min = sortedIntList[0];
    max = sortedIntList[sortedIntList.length - 1];

    // Merge the raw and aggregated docs, so stats for dictionary creation are collected correctly.
    int numAggregated = aggregatedIntSet.size();
    if (numAggregated > 0) {
      rawIntSet.addAll(aggregatedIntSet);
      sortedIntList = new int[rawIntSet.size()];
      rawIntSet.toArray(sortedIntList);
      Arrays.sort(sortedIntList);
    }
  }
}
