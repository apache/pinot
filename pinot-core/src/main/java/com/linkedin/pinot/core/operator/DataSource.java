package com.linkedin.pinot.core.operator;

import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.common.Predicate;


public interface DataSource extends Operator {

	boolean setPredicate(Predicate predicate);
}
