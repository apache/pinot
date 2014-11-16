package com.linkedin.pinot.core.chunk.creator.impl;

import it.unimi.dsi.fastutil.doubles.DoubleAVLTreeSet;

import java.util.Arrays;
import java.util.Set;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.chunk.creator.AbstractColumnPreIndexStatsCollector;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Nov 7, 2014
 */

public class DoubleColumnPreIndexStatsCollector extends AbstractColumnPreIndexStatsCollector {

  private Double min = null;
  private Double max = null;
  private final DoubleAVLTreeSet doubleSet;
  private Double[] sortedDoubleList;
  private boolean hasNull = false;
  private boolean sealed = false;
  
  public DoubleColumnPreIndexStatsCollector(FieldSpec spec) {
    super(spec);
    doubleSet = new DoubleAVLTreeSet();
  }

  @Override
  public void collect(Object entry) {
    if (entry == null) {
      hasNull = true;
      return;
    }
    
    if (entry instanceof Object[]) {
      for (Object e : (Object[])entry) {
        doubleSet.add(((Double)e).doubleValue());
      }
      updateTotalNumberOfEntries((Object[])entry);
      return;
    }
    
    addressSorted(entry);
    doubleSet.add(((Double) entry).doubleValue());
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
  public Object[] getUniqueValuesSet() throws Exception {
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
    return hasNull;
  }

  @Override
  public void seal() {
    sealed = true;
    sortedDoubleList = new Double[doubleSet.size()];
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
}
