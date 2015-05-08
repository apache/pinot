package com.linkedin.pinot.core.operator.filter.predicate;

import java.util.ArrayList;
import java.util.List;

import com.linkedin.pinot.core.common.predicate.NEqPredicate;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;


public class NotEqualsPredicateEvaluator extends AbstractPredicateEvaluator {

  public NotEqualsPredicateEvaluator(NEqPredicate predicate, Dictionary dictionary) {
    final int neq = dictionary.indexOf(predicate.getNotEqualsValue());
    List<Integer> temp = new ArrayList<Integer>();

    for (int i = 0; i < dictionary.length(); i++) {
      if (i != neq) {
        temp.add(i);
      }
    }

    matchingIds = new int[temp.size()];
    for (int i = 0; i < matchingIds.length; i++) {
      matchingIds[i] = temp.get(i);
    }
  }
}
