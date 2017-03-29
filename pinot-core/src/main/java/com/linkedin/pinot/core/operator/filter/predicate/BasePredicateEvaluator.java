package com.linkedin.pinot.core.operator.filter.predicate;


public abstract class BasePredicateEvaluator implements PredicateEvaluator {

  private static final String EXCEPTION_MESSAGE = "Incorrect method called on base class.";

  @Override
  public boolean apply(int dictionaryId) {
    throw new UnsupportedOperationException(EXCEPTION_MESSAGE);
  }

  @Override
  public boolean apply(int[] dictionaryIds) {
    throw new UnsupportedOperationException(EXCEPTION_MESSAGE);
  }

  @Override
  public boolean apply(int[] dictionaryIds, int length) {
    throw new UnsupportedOperationException(EXCEPTION_MESSAGE);
  }

  @Override
  public int[] getMatchingDictionaryIds() {
    throw new UnsupportedOperationException(EXCEPTION_MESSAGE);
  }

  @Override
  public int[] getNonMatchingDictionaryIds() {
    throw new UnsupportedOperationException(EXCEPTION_MESSAGE);
  }

  @Override
  public boolean alwaysFalse() {
    return false;
  }

  /**
   * STRING
   */
  @Override
  public boolean apply(String value) {
    throw new UnsupportedOperationException(EXCEPTION_MESSAGE);
  }

  @Override
  public boolean apply(String[] values) {
    throw new UnsupportedOperationException(EXCEPTION_MESSAGE);
  }

  @Override
  public boolean apply(String[] values, int length) {
    throw new UnsupportedOperationException(EXCEPTION_MESSAGE);
  }

  /**
   * LONG
   */
  @Override
  public boolean apply(long value) {
    throw new UnsupportedOperationException(EXCEPTION_MESSAGE);
  }

  @Override
  public boolean apply(long[] values) {
    throw new UnsupportedOperationException(EXCEPTION_MESSAGE);
  }

  @Override
  public boolean apply(long[] values, int length) {
    throw new UnsupportedOperationException(EXCEPTION_MESSAGE);
  }
}
