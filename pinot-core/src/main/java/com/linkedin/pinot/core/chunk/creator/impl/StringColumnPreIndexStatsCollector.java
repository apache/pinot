package com.linkedin.pinot.core.chunk.creator.impl;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.chunk.creator.AbstractColumnPreIndexStatsCollector;
import com.linkedin.pinot.core.indexsegment.columnar.creator.V1Constants;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Nov 7, 2014
 */

public class StringColumnPreIndexStatsCollector extends AbstractColumnPreIndexStatsCollector {

  private String min = V1Constants.Str.NULL_STRING;
  private String max = V1Constants.Str.NULL_STRING;
  private final int longestStringLength = 0;
  private final Set<String> stringSet;
  private String[] sortedStringList;
  private boolean hasNull = false;
  private boolean sealed = false;

  public StringColumnPreIndexStatsCollector(FieldSpec spec) {
    super(spec);
    stringSet = new HashSet<String>();
  }

  @Override
  public void collect(Object entry) {
    if (entry == null) {
      hasNull = true;
      return;
    }

    if (entry instanceof Object[]) {
      for (final Object e : (Object[])entry) {
        stringSet.add(((String)e));
      }
      if (maxNumberOfMultiValues < ((Object[])entry).length) {
        maxNumberOfMultiValues = ((Object[])entry).length;
      }
      updateTotalNumberOfEntries((Object[])entry);
      return;
    }

    addressSorted(entry);
    stringSet.add((String)entry);
  }

  @Override
  public String getMinValue() throws Exception {
    if (sealed) {
      return min;
    }
    throw new IllegalAccessException("you must seal the collector first before asking for min value");
  }

  @Override
  public String getMaxValue() throws Exception {
    if (sealed) {
      return max;
    }
    throw new IllegalAccessException("you must seal the collector first before asking for min value");
  }

  @Override
  public Object[] getUniqueValuesSet() throws Exception {
    if (sealed) {
      return sortedStringList;
    }
    throw new IllegalAccessException("you must seal the collector first before asking for min value");
  }

  @Override
  public int getLengthOfLargestElement() throws Exception {
    if (sealed) {
      return longestStringLength;
    }
    throw new IllegalAccessException("you must seal the collector first before asking for longest value");
  }

  @Override
  public int getCardinality() throws Exception {
    if (sealed) {
      return stringSet.size();
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
    sortedStringList = new String[stringSet.size()];
    stringSet.toArray(sortedStringList);
    Arrays.sort(sortedStringList);
    if (sortedStringList.length == 0) {
      min = null;
      max = null;
      return;
    }

    min = sortedStringList[0];
    if (sortedStringList.length == 0) {
      max = sortedStringList[0];
    } else {
      max = sortedStringList[sortedStringList.length - 1];
    }

  }

}
