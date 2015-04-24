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
package com.linkedin.pinot.core.segment.creator.impl;

import it.unimi.dsi.fastutil.ints.IntAVLTreeSet;

import java.util.Arrays;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.segment.creator.AbstractColumnStatisticsCollector;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Nov 7, 2014
 */

public class IntColumnPreIndexStatsCollector extends AbstractColumnStatisticsCollector {

  private Integer min = null;
  private Integer max = null;
  private final IntAVLTreeSet intAVLTreeSet;
  private boolean hasNull = false;
  private Integer[] sortedIntList;
  private boolean sealed = false;

  public IntColumnPreIndexStatsCollector(FieldSpec spec) {
    super(spec);
    intAVLTreeSet = new IntAVLTreeSet();
  }

  @Override
  public void collect(Object entry) {
    if (entry instanceof Object[]) {
      for (Object e : (Object[]) entry) {
        intAVLTreeSet.add(((Integer) e).intValue());
      }
      if (maxNumberOfMultiValues < ((Object[]) entry).length) {
        maxNumberOfMultiValues = ((Object[]) entry).length;
      }
      updateTotalNumberOfEntries((Object[]) entry);
      return;
    }

    intAVLTreeSet.add(((Integer) entry).intValue());
    addressSorted(entry);

  }

  @Override
  public Integer getMinValue() throws Exception {
    if (sealed) {
      return min;
    }
    throw new IllegalAccessException("you must seal the collector first before asking for min value");
  }

  @Override
  public Integer getMaxValue() throws Exception {
    if (sealed) {
      return max;
    }
    throw new IllegalAccessException("you must seal the collector first before asking for min value");
  }

  @Override
  public Object[] getUniqueValuesSet() throws Exception {
    if (sealed) {
      return sortedIntList;
    }
    throw new IllegalAccessException("you must seal the collector first before asking for min value");
  }

  @Override
  public int getCardinality() throws Exception {
    if (sealed) {
      return intAVLTreeSet.size();
    }
    throw new IllegalAccessException("you must seal the collector first before asking for min value");
  }

  @Override
  public boolean hasNull() {
    return hasNull;
  }

  @Override
  public void seal() {
    sealed = true;
    sortedIntList = new Integer[intAVLTreeSet.size()];
    intAVLTreeSet.toArray(sortedIntList);
    Arrays.sort(sortedIntList);
    if (sortedIntList.length == 0) {
      min = null;
      max = null;
      return;
    }

    min = sortedIntList[0];
    if (sortedIntList.length == 0) {
      max = sortedIntList[0];
    } else {
      max = sortedIntList[sortedIntList.length - 1];
    }
  }
}
