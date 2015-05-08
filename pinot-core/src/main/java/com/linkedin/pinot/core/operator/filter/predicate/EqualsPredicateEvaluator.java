package com.linkedin.pinot.core.operator.filter.predicate;

import java.util.Arrays;

import com.linkedin.pinot.core.common.predicate.EqPredicate;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;


public class EqualsPredicateEvaluator implements PredicateEvaluator {

  private int equalsMatchDicId;

  public EqualsPredicateEvaluator(EqPredicate predicate, Dictionary dictionary) {
    equalsMatchDicId = dictionary.indexOf(predicate.getEqualsValue());
  }

  @Override
  public boolean apply(int dicId) {
    return (dicId == equalsMatchDicId);
  }

  @Override
  public boolean apply(int[] dicIds) {
    Arrays.sort(dicIds);
    int index = Arrays.binarySearch(dicIds, equalsMatchDicId);
    return index >= 0;
  }

  @Override
  public int[] getDictionaryIds() {
    return new int[] { equalsMatchDicId };
  }

}
