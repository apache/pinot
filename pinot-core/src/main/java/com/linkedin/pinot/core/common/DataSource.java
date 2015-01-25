package com.linkedin.pinot.core.common;



public interface DataSource extends Operator {

  boolean setPredicate(Predicate predicate);
}
