package com.linkedin.pinot.core.segment.creator;

/**
 * @author Dhaval Patel<dpatel@linkedin.com> Nov 9, 2014
 */

public class ColumnIndexCreationInfo {
  private final boolean createDictionary;
  private final Object min;
  private final Object max;
  private final Object[] sortedUniqueElementsArray;
  private final ForwardIndexType forwardIndexType;
  private final InvertedIndexType invertedIndexType;
  private final boolean isSorted;
  private final boolean hasNulls;
  private final int totalNumberOfEntries;
  private final int maxNumberOfMutiValueElements;

  public ColumnIndexCreationInfo(boolean createDictionary, Object min, Object max, Object[] sortedArray, ForwardIndexType forwardIndexType,
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

  public ColumnIndexCreationInfo(boolean createDictionary, Object min, Object max, Object[] sortedArray, ForwardIndexType forwardIndexType,
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

  public Object[] getSortedUniqueElementsArray() {
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
}
