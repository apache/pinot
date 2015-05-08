package com.linkedin.pinot.core.operator.filter.predicate;

import java.util.Arrays;

import com.linkedin.pinot.core.common.predicate.EqPredicate;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;


public class EqualsPredicateEvaluator implements PredicateEvaluator {

  private int[] equalsMatchDicId;
  private int index;

  public EqualsPredicateEvaluator(EqPredicate predicate, Dictionary dictionary) {
    index = dictionary.indexOf(predicate.getEqualsValue());
    if (index >= 0) {
      equalsMatchDicId = new int[1];
      equalsMatchDicId[0] = index;
    } else {
      equalsMatchDicId = new int[0];
    }
  }

  @Override
  public boolean apply(int dicId) {
    return (dicId == index);
  }

  @Override
  public boolean apply(int[] dicIds) {
    if (index < 0) {
      return false;
    }
    Arrays.sort(dicIds);
    int i = Arrays.binarySearch(dicIds, index);
    return i >= 0;
  }

  @Override
  public int[] getDictionaryIds() {
    return equalsMatchDicId;
  }
}
