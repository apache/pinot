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
import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;
import it.unimi.dsi.fastutil.doubles.DoubleSet;
import java.util.Arrays;


public class DoubleColumnPreIndexStatsCollector extends AbstractColumnStatisticsCollector {
  private Double min = null;
  private Double max = null;
  private final DoubleSet rawDoubleSet;
  private final DoubleSet aggregatedDoubleSet;
  private double[] sortedDoubleList;
  private boolean sealed = false;

  public DoubleColumnPreIndexStatsCollector(String column, StatsCollectorConfig statsCollectorConfig) {
    super(column, statsCollectorConfig);
    rawDoubleSet = new DoubleOpenHashSet(INITIAL_HASH_SET_SIZE);
    aggregatedDoubleSet = new DoubleOpenHashSet(INITIAL_HASH_SET_SIZE);
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
  private void collectEntry(Object entry, DoubleSet set) {
    if (entry instanceof Object[]) {
      for (final Object e : (Object[]) entry) {
        set.add(((Number) e).doubleValue());
      }
      if (maxNumberOfMultiValues < ((Object[]) entry).length) {
        maxNumberOfMultiValues = ((Object[]) entry).length;
      }
      updateTotalNumberOfEntries((Object[]) entry);
    } else {
      double value = ((Number) entry).doubleValue();
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
      collectEntry(entry, aggregatedDoubleSet);
    } else {
      collectEntry(entry, rawDoubleSet);
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
  public Double getMinValue() {
    if (sealed) {
      return min;
    }
    throw new IllegalStateException("you must seal the collector first before asking for min value");
  }

  @Override
  public Double getMaxValue() {
    if (sealed) {
      return max;
    }
    throw new IllegalStateException("you must seal the collector first before asking for min value");
  }

  @Override
  public Object getUniqueValuesSet() {
    if (sealed) {
      return sortedDoubleList;
    }
    throw new IllegalStateException("you must seal the collector first before asking for min value");
  }

  @Override
  public int getCardinality() {
    if (sealed) {
      return sortedDoubleList.length;
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
    sortedDoubleList = new double[rawDoubleSet.size()];
    rawDoubleSet.toArray(sortedDoubleList);

    Arrays.sort(sortedDoubleList);

    if (sortedDoubleList.length == 0) {
      min = null;
      max = null;
      return;
    }

    // Update the min-max values based on raw docs.
    min = sortedDoubleList[0];
    max = sortedDoubleList[sortedDoubleList.length - 1];

    // Merge the raw and aggregated docs, so stats for dictionary creation are collected correctly.
    int numAggregated = aggregatedDoubleSet.size();
    if (numAggregated > 0) {
      rawDoubleSet.addAll(aggregatedDoubleSet);
      sortedDoubleList = new double[rawDoubleSet.size()];
      rawDoubleSet.toArray(sortedDoubleList);
      Arrays.sort(sortedDoubleList);
    }
  }
}
