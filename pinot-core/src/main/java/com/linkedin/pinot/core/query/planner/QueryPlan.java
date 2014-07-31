package com.linkedin.pinot.core.query.planner;

import com.linkedin.pinot.common.query.request.Query;


/**
 * QueryPlan is a DAG to represent the sequence to process segments.
 * The DAG related operation is not thread safe. Only one PlanExecutor will process QueryPlan.
 * 
 */
public class QueryPlan {

  private Query _query = null;
  private JobVertex _virtualRoot = null;

  private void setVirtualRoot(JobVertex virtualRoot) {
    _virtualRoot = virtualRoot;
    _virtualRoot.setIndexSegmentList(null);

  }

  public QueryPlan() {
    setVirtualRoot(new JobVertex(null));
  }

  public QueryPlan(Query query, JobVertex root) {
    setQuery(_query);
    setVirtualRoot(root);
  }

  public JobVertex getVirtualRoot() {
    return _virtualRoot;
  }

  public Query getQuery() {
    return _query;
  }

  public void setQuery(Query query) {
    _query = query;
  }

}
