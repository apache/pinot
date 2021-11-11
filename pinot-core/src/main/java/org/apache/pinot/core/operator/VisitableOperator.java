package org.apache.pinot.core.operator;

public interface VisitableOperator {
  <T> void accept(T v);
}
