package org.apache.pinot.core.plan;

public class FilterPlanNodeStateVisitor implements PlanNodeVisitor<Boolean> {
  private final boolean _isFilterPredicate;

  public FilterPlanNodeStateVisitor(boolean isFilterPredicate) {
    _isFilterPredicate = isFilterPredicate;
  }

  @Override
  public Boolean getMetadata() {
    return _isFilterPredicate;
  }
}
