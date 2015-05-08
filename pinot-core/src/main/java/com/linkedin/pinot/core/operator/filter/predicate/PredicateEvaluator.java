package com.linkedin.pinot.core.operator.filter.predicate;

public interface PredicateEvaluator {

  /**
   *
   * @param dicId
   * @return
   */
  public boolean apply(int dicId);

  /**
   *
   * @param dicIds
   * @return
   */
  public boolean apply(int[] dicIds);

  /**
   *
   * @return
   */
  public int[] getDictionaryIds();
}
