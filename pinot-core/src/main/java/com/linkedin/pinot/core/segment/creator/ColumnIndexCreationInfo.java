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
package com.linkedin.pinot.core.segment.creator;

import org.apache.commons.lang3.ArrayUtils;


/**
 */

public class ColumnIndexCreationInfo {
  private final boolean createDictionary;
  private final Object min;
  private final Object max;
  private final Object sortedUniqueElementsArray;
  private final ForwardIndexType forwardIndexType;
  private final InvertedIndexType invertedIndexType;
  private final boolean isSorted;
  private final boolean hasNulls;
  private final int totalNumberOfEntries;
  private final int maxNumberOfMutiValueElements;


  public ColumnIndexCreationInfo(boolean createDictionary, Object min, Object max, Object sortedArray, ForwardIndexType forwardIndexType,
      InvertedIndexType invertedIndexType, boolean isSortedColumn, boolean hasNulls) {
    this.createDictionary = createDictionary;
    this.min = min;
    this.max = max;
    sortedUniqueElementsArray = sortedArray;
    this.forwardIndexType = forwardIndexType;
    this.invertedIndexType = invertedIndexType;
    isSorted = isSortedColumn;
    this.hasNulls = hasNulls;
    totalNumberOfEntries = 0;
    maxNumberOfMutiValueElements = 0;
  }

  public ColumnIndexCreationInfo(boolean createDictionary, Object min, Object max, Object sortedArray, ForwardIndexType forwardIndexType,
      InvertedIndexType invertedIndexType, boolean isSortedColumn, boolean hasNulls, int totalNumberOfEntries,
      int maxNumberOfMultiValueElements) {
    this.createDictionary = createDictionary;
    this.min = min;
    this.max = max;
    sortedUniqueElementsArray = sortedArray;
    this.forwardIndexType = forwardIndexType;
    this.invertedIndexType = invertedIndexType;
    isSorted = isSortedColumn;
    this.hasNulls = hasNulls;
    this.totalNumberOfEntries = totalNumberOfEntries;
    maxNumberOfMutiValueElements = maxNumberOfMultiValueElements;

  }

  public int getMaxNumberOfMutiValueElements() {
    return maxNumberOfMutiValueElements;
  }

  public boolean isCreateDictionary() {
    return createDictionary;
  }

  public Object getMin() {
    return min;
  }

  public Object getMax() {
    return max;
  }

  public Object getSortedUniqueElementsArray() {
    return sortedUniqueElementsArray;
  }

  public ForwardIndexType getForwardIndexType() {
    return forwardIndexType;
  }

  public InvertedIndexType getInvertedIndexType() {
    return invertedIndexType;
  }

  public boolean isSorted() {
    return isSorted;
  }

  public boolean hasNulls() {
    return hasNulls;
  }

  public int getTotalNumberOfEntries() {
    return totalNumberOfEntries;
  }

  public int getDistinctValueCount() {
    return ArrayUtils.getLength(sortedUniqueElementsArray);
  }
}
