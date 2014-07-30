package com.linkedin.pinot.query.plan.maker;

import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.plan.PlanNode;
import com.linkedin.pinot.query.request.Query;


public interface PlanMaker {

  public PlanNode makePlan(IndexSegment indexSegment, Query query);
}
