package com.linkedin.pinot.core.segment.creator.impl;

import it.unimi.dsi.fastutil.ints.IntAVLTreeSet;

import java.util.Arrays;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.segment.creator.AbstractColumnPreIndexStatsCollector;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Nov 7, 2014
 */

public class IntColumnPreIndexStatsCollector extends AbstractColumnPreIndexStatsCollector {

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
    if (entry == null) {
      hasNull = true;
      return;
    }

    if (entry instanceof Object[]) {
      for (final Object e : (Object[])entry) {
        intAVLTreeSet.add(((Integer)e).intValue());
      }
      if (maxNumberOfMultiValues < ((Object[])entry).length) {
        maxNumberOfMultiValues = ((Object[])entry).length;
      }
      updateTotalNumberOfEntries((Object[])entry);
      return;
    }

    intAVLTreeSet.add(((Integer)entry).intValue());
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
