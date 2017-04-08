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

import com.linkedin.pinot.core.segment.creator.StatsCollectorConfig;
import it.unimi.dsi.fastutil.floats.FloatOpenHashSet;
import it.unimi.dsi.fastutil.floats.FloatSet;
import java.util.Arrays;


public class FloatColumnPreIndexStatsCollector extends AbstractColumnStatisticsCollector {
  private Float min = Float.MAX_VALUE;
  private Float max = Float.MIN_VALUE;
  private final FloatSet rawFloatSet;
  private final FloatSet aggregatedFloatSet;
  private float[] sortedFloatList;
  private boolean sealed = false;

  public FloatColumnPreIndexStatsCollector(String column, StatsCollectorConfig statsCollectorConfig) {
    super(column, statsCollectorConfig);
    rawFloatSet = new FloatOpenHashSet(INITIAL_HASH_SET_SIZE);
    aggregatedFloatSet = new FloatOpenHashSet(INITIAL_HASH_SET_SIZE);
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
  private void collectEntry(Object entry, FloatSet set) {
    if (entry instanceof Object[]) {
      for (final Object e : (Object[]) entry) {
        set.add(((Number) e).floatValue());
      }
      if (maxNumberOfMultiValues < ((Object[]) entry).length) {
        maxNumberOfMultiValues = ((Object[]) entry).length;
      }
      updateTotalNumberOfEntries((Object[]) entry);
    } else {
      float value = ((Number) entry).floatValue();
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
      collectEntry(entry, aggregatedFloatSet);
    } else {
      collectEntry(entry, rawFloatSet);
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
  public Float getMinValue() {
    if (sealed) {
      return min;
    }
    throw new IllegalStateException("you must seal the collector first before asking for min value");
  }

  @Override
  public Float getMaxValue() {
    if (sealed) {
      return max;
    }
    throw new IllegalStateException("you must seal the collector first before asking for min value");
  }

  @Override
  public Object getUniqueValuesSet() {
    if (sealed) {
      return sortedFloatList;
    }
    throw new IllegalStateException("you must seal the collector first before asking for min value");
  }

  @Override
  public int getCardinality() {
    if (sealed) {
      return sortedFloatList.length;
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
    sortedFloatList = new float[rawFloatSet.size()];
    rawFloatSet.toArray(sortedFloatList);

    Arrays.sort(sortedFloatList);

    if (sortedFloatList.length == 0) {
      min = null;
      max = null;
      return;
    }

    // Update min/max based on raw docs.
    min = sortedFloatList[0];
    max = sortedFloatList[sortedFloatList.length - 1];

    // Merge the raw and aggregated docs, so stats for dictionary creation are collected correctly.
    int numAggregated = aggregatedFloatSet.size();
    if (numAggregated > 0) {
      rawFloatSet.addAll(aggregatedFloatSet);
      sortedFloatList = new float[rawFloatSet.size()];
      rawFloatSet.toArray(sortedFloatList);
      Arrays.sort(sortedFloatList);
    }
  }
}
