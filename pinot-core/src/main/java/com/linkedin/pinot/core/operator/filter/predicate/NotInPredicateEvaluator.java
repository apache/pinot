package com.linkedin.pinot.core.operator.filter.predicate;

import java.util.ArrayList;
import java.util.List;

import com.linkedin.pinot.core.common.predicate.NotInPredicate;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;


public class NotInPredicateEvaluator extends AbstractPredicateEvaluator {

  public NotInPredicateEvaluator(NotInPredicate predicate, Dictionary dictionary) {
    List<Integer> dictIds = new ArrayList<Integer>();
    final String[] notInValues = predicate.getNotInRange();
    final List<Integer> notInIds = new ArrayList<Integer>();
    for (final String notInValue : notInValues) {
      notInIds.add(new Integer(dictionary.indexOf(notInValue)));
    }
    for (int i = 0; i < dictionary.length(); i++) {
      if (!notInIds.contains(new Integer(i))) {
        dictIds.add(i);
      }
    }
    matchingIds = new int[dictIds.size()];
    for (int i = 0; i < matchingIds.length; i++) {
      matchingIds[i] = dictIds.get(i);
    }
  }
}
