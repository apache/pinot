package com.linkedin.pinot.index.plan;

import com.linkedin.pinot.index.common.Operator;


public interface PlanNode {

  Operator run();

  void showTree(String prefix);
}
