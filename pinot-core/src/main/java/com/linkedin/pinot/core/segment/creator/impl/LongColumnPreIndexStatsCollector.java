package com.linkedin.pinot.core.segment.creator.impl;

import it.unimi.dsi.fastutil.longs.LongAVLTreeSet;

import java.util.Arrays;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.segment.creator.AbstractColumnPreIndexStatsCollector;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Nov 7, 2014
 */

public class LongColumnPreIndexStatsCollector extends AbstractColumnPreIndexStatsCollector {

  private Long min = null;
  private Long max = null;
  private final LongAVLTreeSet longSet;
  private Long[] sortedLongList;
  private boolean hasNull = false;
  private boolean sealed = false;

  public LongColumnPreIndexStatsCollector(FieldSpec spec) {
    super(spec);
    longSet = new LongAVLTreeSet();
  }

  @Override
  public void collect(Object entry) {
    if (entry == null) {
      hasNull = true;
      return;
    }

    if (entry instanceof Object[]) {
      for (final Object e : (Object[])entry) {
        longSet.add(((Long)e).longValue());
      }
      if (maxNumberOfMultiValues < ((Object[])entry).length) {
        maxNumberOfMultiValues = ((Object[])entry).length;
      }
      updateTotalNumberOfEntries((Object[])entry);
      return;
    }

    addressSorted(entry);
    longSet.add(((Long) entry).longValue());
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
  public Object[] getUniqueValuesSet() throws Exception {
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
    return hasNull;
  }

  @Override
  public void seal() {
    sealed = true;
    sortedLongList = new Long[longSet.size()];
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
