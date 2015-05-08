package com.linkedin.pinot.core.operator.filter.predicate;

import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.common.predicate.EqPredicate;
import com.linkedin.pinot.core.common.predicate.InPredicate;
import com.linkedin.pinot.core.common.predicate.NEqPredicate;
import com.linkedin.pinot.core.common.predicate.NotInPredicate;
import com.linkedin.pinot.core.common.predicate.RangePredicate;
import com.linkedin.pinot.core.realtime.impl.dictionary.MutableDictionaryReader;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import com.linkedin.pinot.core.segment.index.readers.ImmutableDictionaryReader;


public class PredicateEvaluatorProvider {

  public static PredicateEvaluator getPredicateFunctionFor(Predicate predicate, Dictionary dictionary) {
    switch (predicate.getType()) {
      case EQ:
        return new EqualsPredicateEvaluator((EqPredicate) predicate, dictionary);
      case NEQ:
        return new NotEqualsPredicateEvaluator((NEqPredicate) predicate, dictionary);
      case IN:
        return new InPredicateEvaluator((InPredicate) predicate, dictionary);
      case NOT_IN:
        return new NotInPredicateEvaluator((NotInPredicate) predicate, dictionary);
      case RANGE:
        if (dictionary instanceof ImmutableDictionaryReader) {
          return new RangeOfflineDictionaryPredicateEvaluator((RangePredicate) predicate,
              (ImmutableDictionaryReader) dictionary);
        } else {
          return new RangeRealtimeDictionaryPredicateEvaluator((RangePredicate) predicate,
              (MutableDictionaryReader) dictionary);
        }
      case REGEX:
        throw new UnsupportedOperationException("regex is not supported");
      default:
        throw new UnsupportedOperationException("UnKnown predicate type");
    }
  }
}
