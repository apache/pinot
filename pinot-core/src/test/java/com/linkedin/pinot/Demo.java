package com.linkedin.pinot;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.plan.FilterPlanNode;


public class Demo {
  public static void main(String[] args) {
    String queryString = "";
    //parse query string
    BrokerRequest query = new BrokerRequest();
    IndexSegment segment = null;
    FilterPlanNode filterPlan = new FilterPlanNode(segment, query);
    filterPlan.run();
  }
}
