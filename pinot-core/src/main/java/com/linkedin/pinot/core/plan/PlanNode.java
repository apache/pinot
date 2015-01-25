package com.linkedin.pinot.core.plan;

import com.linkedin.pinot.core.common.Operator;


public interface PlanNode {

  Operator run() throws Exception;

  void showTree(String prefix);
}
