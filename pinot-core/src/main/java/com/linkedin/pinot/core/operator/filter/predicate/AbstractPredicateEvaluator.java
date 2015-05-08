package com.linkedin.pinot.core.operator.filter.predicate;

import java.util.Arrays;


public class AbstractPredicateEvaluator implements PredicateEvaluator {

  protected int[] matchingIds;

  public AbstractPredicateEvaluator() {

  }

  @Override
  public boolean apply(int dicId) {
    return Arrays.binarySearch(matchingIds, dicId) >= 0;
  }

  @Override
  public boolean apply(int[] dicIds) {
    if (dicIds.length < matchingIds.length) {
      for (int i = 0; i < dicIds.length; i++) {
        int index = Arrays.binarySearch(matchingIds, dicIds[i]);
        if (index >= 0) {
          return true;
        }
      }
    } else {
      for (int i = 0; i < matchingIds.length; i++) {
        int index = Arrays.binarySearch(dicIds, matchingIds[i]);
        if (index >= 0) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public int[] getDictionaryIds() {
    return matchingIds;
  }
}
