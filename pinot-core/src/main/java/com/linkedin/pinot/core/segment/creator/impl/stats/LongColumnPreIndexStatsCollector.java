/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import java.util.Arrays;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.segment.creator.AbstractColumnStatisticsCollector;


/**
 * Nov 7, 2014
 */

public class LongColumnPreIndexStatsCollector extends AbstractColumnStatisticsCollector {

  private Long min = null;
  private Long max = null;
  private final LongSet longSet;
  private long[] sortedLongList;
  private boolean hasNull = false;
  private boolean sealed = false;

  public LongColumnPreIndexStatsCollector(FieldSpec spec) {
    super(spec);
    longSet = new LongOpenHashSet(1000);
  }

  @Override
  public void collect(Object entry) {

    if (entry instanceof Object[]) {
      for (final Object e : (Object[]) entry) {
        longSet.add(((Number) e).longValue());
      }
      if (maxNumberOfMultiValues < ((Object[]) entry).length) {
        maxNumberOfMultiValues = ((Object[]) entry).length;
      }
      updateTotalNumberOfEntries((Object[]) entry);
      return;
    }

    long value = ((Number) entry).longValue();
    addressSorted(value);
    longSet.add(value);

  }

  @Override
  public Long getMinValue() throws Exception {
    if (sealed) {
      return min;
    }
    throw new IllegalAccessException("you must seal the collector first before asking for min value");
  }

  @Override
  public Long getMaxValue() throws Exception {
    if (sealed) {
      return max;
    }
    throw new IllegalAccessException("you must seal the collector first before asking for min value");
  }

  @Override
  public Object getUniqueValuesSet() throws Exception {
    if (sealed) {
      return sortedLongList;
    }
    throw new IllegalAccessException("you must seal the collector first before asking for min value");
  }

  @Override
  public int getCardinality() throws Exception {
    if (sealed) {
      return longSet.size();
    }
    throw new IllegalAccessException("you must seal the collector first before asking for min value");
  }

  @Override
  public boolean hasNull() {
    return false;
  }

  @Override
  public void seal() {
    sealed = true;
    sortedLongList = new long[longSet.size()];
    longSet.toArray(sortedLongList);

    Arrays.sort(sortedLongList);

    if (sortedLongList.length == 0) {
      min = null;
      max = null;
      return;
    }

    min = sortedLongList[0];
    if (sortedLongList.length == 0) {
      max = sortedLongList[0];
    } else {
      max = sortedLongList[sortedLongList.length - 1];
    }

  }
}
