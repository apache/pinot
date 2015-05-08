package com.linkedin.pinot.core.operator.filter.predicate;

import java.util.ArrayList;
import java.util.List;

import com.linkedin.pinot.core.common.predicate.InPredicate;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;


public class InPredicateEvaluator extends AbstractPredicateEvaluator {

  public InPredicateEvaluator(InPredicate predicate, Dictionary dictionary) {
    List<Integer> dictIds = new ArrayList<Integer>();
    final String[] inValues = predicate.getInRange();
    for (final String value : inValues) {
      final int index = dictionary.indexOf(value);
      if (index >= 0) {
        dictIds.add(index);
      }
    }
    matchingIds = new int[dictIds.size()];
    for (int i = 0; i < matchingIds.length; i++) {
      matchingIds[i] = dictIds.get(i);
    }
  }
}
