package com.linkedin.pinot.core.query.planner;

import com.linkedin.pinot.common.request.BrokerRequest;


/**
 * QueryPlanCreator will take the dependency relationship for JobVertexes then create an immutable DAG to represent the
 * sequence to process segments.
 * 
 */
public class QueryPlanCreator {

  private JobVertex _virtualRoot = null;

  private BrokerRequest _brokerRequest = null;

  public BrokerRequest getQuery() {
    return _brokerRequest;
  }

  public void setQuery(BrokerRequest brokerRequest) {
    _brokerRequest = brokerRequest;
  }

  private void setVirtualRoot(JobVertex virtualRoot) {
    _virtualRoot = virtualRoot;
    _virtualRoot.setIndexSegmentList(null);
  }

  public QueryPlanCreator(BrokerRequest brokerRequest) {
    setVirtualRoot(new JobVertex(null));
    setQuery(brokerRequest);
  }

  public void addJobVertexWithDependency(JobVertex jobVertexFrom, JobVertex jobVertexTo) {
    if (jobVertexFrom == null) {
      _virtualRoot.addSuccessor(jobVertexTo);
      jobVertexTo.addParent(_virtualRoot);
    } else {
      jobVertexFrom.addSuccessor(jobVertexTo);
      jobVertexTo.addParent(jobVertexFrom);
    }
  }

  public QueryPlan buildQueryPlan() {
    return new QueryPlan(_brokerRequest, _virtualRoot);
  }

}
