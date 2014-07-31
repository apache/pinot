package com.linkedin.pinot.core.query.plan.maker;

import com.linkedin.pinot.common.query.request.Query;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.plan.PlanNode;


public interface PlanMaker {

  public PlanNode makePlan(IndexSegment indexSegment, Query query);
}
