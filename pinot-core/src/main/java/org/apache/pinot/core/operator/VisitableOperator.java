package org.apache.pinot.core.operator;

/**
 * Defines the interface for an operator which
 * is visitable.
 */
public interface VisitableOperator {
  <T> void accept(T v);
}
