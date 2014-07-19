package com.linkedin.pinot.index.operator;

import com.linkedin.pinot.index.common.Operator;
import com.linkedin.pinot.index.common.Predicate;


public interface DataSource extends Operator {

	boolean setPredicate(Predicate predicate);
}
