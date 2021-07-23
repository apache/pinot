package org.apache.pinot.core.startree;

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;


public class PredicateEvaluatorsWithType {
  private final FilterContext.Type _filterContextType;
  private final List<PredicateEvaluator> _predicateEvaluators;

  public PredicateEvaluatorsWithType(FilterContext.Type filterContextType) {
    _filterContextType = filterContextType;

    _predicateEvaluators = new ArrayList<>();
  }

  public void addPredicateEvaluator(PredicateEvaluator predicateEvaluator) {
    assert predicateEvaluator != null;

    _predicateEvaluators.add(predicateEvaluator);
  }

  public List<PredicateEvaluator> getPredicateEvaluators() {
    return _predicateEvaluators;
  }

  public FilterContext.Type getFilterContextType() {
    return _filterContextType;
  }
}
