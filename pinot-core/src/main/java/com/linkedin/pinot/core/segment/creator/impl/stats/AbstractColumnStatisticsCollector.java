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

import com.linkedin.pinot.core.segment.creator.ColumnStatistics;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;


/**
 * This class in initialized per column and all the data is
 * sent to it before actual indexes are created
 * the job of this class is to collect
 * unique elements
 * record cardinality
 * compute min
 * compute max
 * see if column isSorted
 */

public abstract class AbstractColumnStatisticsCollector implements ColumnStatistics {
  protected static final int INITIAL_HASH_SET_SIZE = 1000;

  private Object previousValue = null;
  protected final FieldSpec fieldSpec;
  private boolean isSorted = true;
  protected int totalNumberOfEntries = 0;
  protected int maxNumberOfMultiValues = 0;

  void updateTotalNumberOfEntries(Object[] entries) {
    totalNumberOfEntries += entries.length;
  }

  public int getTotalNumberOfEntries() {
    return totalNumberOfEntries;
  }

  AbstractColumnStatisticsCollector(FieldSpec spec) {
    fieldSpec = spec;
    addressNull(previousValue, fieldSpec.getDataType());
    previousValue = null;
  }

  public int getMaxNumberOfMultiValues() {
    return maxNumberOfMultiValues;
  }

  void addressSorted(Object entry) {
    if (isSorted) {
      if (previousValue != null) {
        if (!entry.equals(previousValue) && previousValue != null) {
          final Comparable prevValue = (Comparable) previousValue;
          final Comparable origin = (Comparable) entry;
          if (origin.compareTo(prevValue) < 0) {
            isSorted = false;
          }
        }
      }
      previousValue = entry;
    }
  }

  @Override
  public boolean isSorted() {
    return fieldSpec.isSingleValueField() && isSorted;
  }

  /**
   * Collect statistics for given the entry.
   * Entry is expected to be 'raw', and not pre-aggregated (for star-tree).
   * @param entry Entry to be collected
   */
  public abstract void collect(Object entry);

  /**
   * Collected statistics for the given entry.
   *
   * @param entry Entry to be collected
   * @param isAggregated True for aggregated, False for raw.
   */
  public abstract void collect(Object entry, boolean isAggregated);

  @Override
  public int getLengthOfLargestElement() {
    return -1;
  }

  public abstract void seal();

  Object addressNull(Object entry, DataType e) {
    if (entry == null) {
      if (e == DataType.STRING) {
        entry = V1Constants.Str.NULL_STRING;
      } else if (e == DataType.BOOLEAN) {
        entry = V1Constants.Str.NULL_BOOLEAN;
      } else if (e == DataType.DOUBLE) {
        entry = V1Constants.Numbers.NULL_DOUBLE;
      } else if (e == DataType.FLOAT) {
        entry = V1Constants.Numbers.NULL_FLOAT;
      } else if (e == DataType.LONG) {
        entry = V1Constants.Numbers.NULL_LONG;
      } else if (e == DataType.INT) {
        entry = V1Constants.Numbers.NULL_INT;
      }
    }
    return entry;
  }
}
