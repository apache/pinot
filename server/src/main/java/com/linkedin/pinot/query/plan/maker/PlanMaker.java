package com.linkedin.pinot.query.plan.maker;

import com.linkedin.pinot.index.plan.PlanNode;
import com.linkedin.pinot.index.segment.IndexSegment;
import com.linkedin.pinot.query.request.Query;


public interface PlanMaker {

  public PlanNode makePlan(IndexSegment indexSegment, Query query);
}
