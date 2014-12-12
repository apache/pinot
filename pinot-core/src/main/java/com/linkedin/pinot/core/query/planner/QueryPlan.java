package com.linkedin.pinot.core.query.planner;

import com.linkedin.pinot.common.request.BrokerRequest;


/**
 * QueryPlan is a DAG to represent the sequence to process segments.
 * The DAG related operation is not thread safe. Only one PlanExecutor will process QueryPlan.
 * 
 */
public class QueryPlan {

  private BrokerRequest _brokerRequest = null;
  private JobVertex _virtualRoot = null;

  private void setVirtualRoot(JobVertex virtualRoot) {
    _virtualRoot = virtualRoot;
    _virtualRoot.setIndexSegmentList(null);

  }

  public QueryPlan() {
    setVirtualRoot(new JobVertex(null));
  }

  public QueryPlan(BrokerRequest brokerRequest, JobVertex root) {
    setBrokerRequest(brokerRequest);
    setVirtualRoot(root);
  }

  public JobVertex getVirtualRoot() {
    return _virtualRoot;
  }

  public BrokerRequest getBrokerRequest() {
    return _brokerRequest;
  }

  public void setBrokerRequest(BrokerRequest brokerRequest) {
    _brokerRequest = brokerRequest;
  }

}
