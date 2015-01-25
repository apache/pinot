package com.linkedin.pinot.core.plan;

import com.linkedin.pinot.common.utils.DataTable;


public abstract class Plan {

  public abstract void print();

  /**
   * Root node of the plan
   *
   * @return
   */
  public abstract PlanNode getRoot();

  public abstract void execute() throws Exception;

  public abstract DataTable getInstanceResponse();
}
