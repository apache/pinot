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

import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;
import it.unimi.dsi.fastutil.doubles.DoubleSet;
import java.util.Arrays;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.segment.creator.AbstractColumnStatisticsCollector;


/**
 * Nov 7, 2014
 */

public class DoubleColumnPreIndexStatsCollector extends AbstractColumnStatisticsCollector {

  private Double min = null;
  private Double max = null;
  private final DoubleSet doubleSet;
  private double[] sortedDoubleList;
  private boolean hasNull = false;
  private boolean sealed = false;

  public DoubleColumnPreIndexStatsCollector(FieldSpec spec) {
    super(spec);
    doubleSet = new DoubleOpenHashSet(1000);
  }

  @Override
  public void collect(Object entry) {
    if (entry instanceof Object[]) {
      for (final Object e : (Object[]) entry) {
        doubleSet.add(((Number) e).doubleValue());
      }
      if (maxNumberOfMultiValues < ((Object[]) entry).length) {
        maxNumberOfMultiValues = ((Object[]) entry).length;
      }
      updateTotalNumberOfEntries((Object[]) entry);
      return;
    }
    double value = ((Number) entry).doubleValue();
    addressSorted(value);
    doubleSet.add(value);
  }

  @Override
  public Double getMinValue() throws Exception {
    if (sealed) {
      return min;
    }
    throw new IllegalAccessException("you must seal the collector first before asking for min value");
  }

  @Override
  public Double getMaxValue() throws Exception {
    if (sealed) {
      return max;
    }
    throw new IllegalAccessException("you must seal the collector first before asking for min value");
  }

  @Override
  public Object getUniqueValuesSet() throws Exception {
    if (sealed) {
      return sortedDoubleList;
    }
    throw new IllegalAccessException("you must seal the collector first before asking for min value");
  }

  @Override
  public int getCardinality() throws Exception {
    if (sealed) {
      return doubleSet.size();
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
    sortedDoubleList = new double[doubleSet.size()];
    doubleSet.toArray(sortedDoubleList);

    Arrays.sort(sortedDoubleList);

    if (sortedDoubleList.length == 0) {
      min = null;
      max = null;
      return;
    }

    min = sortedDoubleList[0];
    if (sortedDoubleList.length == 0) {
      max = sortedDoubleList[0];
    } else {
      max = sortedDoubleList[sortedDoubleList.length - 1];
    }

  }

  public static void main(String[] args) {
    int a = 1;
    float b = 2;
    long c = 3;
    double d = 4;
    Integer a1 = 1;
    Float b1 = (float) 2;
    Long c1 = (long) 3;
    Double d1 = (double) 4;
    System.out.println((Integer) a1);
    System.out.println(((Number) ((Object) a1)).doubleValue());
    System.out.println(((Number) ((Object) b1)).doubleValue());
    System.out.println(((Number) ((Object) a1)).doubleValue());

  }
}
