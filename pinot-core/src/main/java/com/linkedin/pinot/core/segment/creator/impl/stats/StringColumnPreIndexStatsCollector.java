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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.segment.creator.AbstractColumnStatisticsCollector;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;


/**
 * Nov 7, 2014
 */

public class StringColumnPreIndexStatsCollector extends AbstractColumnStatisticsCollector {

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

    if (entry instanceof Object[]) {
      for (final Object e : (Object[]) entry) {
        stringSet.add(e.toString());
      }
      if (maxNumberOfMultiValues < ((Object[]) entry).length) {
        maxNumberOfMultiValues = ((Object[]) entry).length;
      }
      updateTotalNumberOfEntries((Object[]) entry);
      return;
    }

    String value;
    if (entry != null) {
      value = entry.toString();
    } else {
      value = fieldSpec.getDefaultNullValue().toString();
    }
    addressSorted(value);
    stringSet.add(value);
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
    throw new IllegalAccessException("you must seal the collector first before asking for max value");
  }

  @Override
  public Object[] getUniqueValuesSet() throws Exception {
    if (sealed) {
      return sortedStringList;
    }
    throw new IllegalAccessException("you must seal the collector first before asking for unique values set");
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
    throw new IllegalAccessException("you must seal the collector first before asking for cardinality");
  }

  @Override
  public boolean hasNull() {
    return false;
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
