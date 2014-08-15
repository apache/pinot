package com.linkedin.pinot.core.plan;

import com.linkedin.pinot.common.response.InstanceResponse;


public abstract class Plan {

  public abstract void print();

  /**
   * Root node of the plan
   * 
   * @return
   */
  public abstract PlanNode getRoot();

  public abstract InstanceResponse execute();

  public abstract InstanceResponse getInstanceResponse();
}
