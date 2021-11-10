package org.apache.pinot.core.plan;

public interface PlanNodeVisitor<T> {
  T getMetadata();
}
