package com.linkedin.pinot.query.planner;

import com.linkedin.pinot.query.request.Query;


/**
 * QueryPlanCreator will take the dependency relationship for JobVertexes then create an immutable DAG to represent the
 * sequence to process segments.
 * 
 */
public class QueryPlanCreator {

  private JobVertex _virtualRoot = null;

  private Query _query = null;

  public Query getQuery() {
    return _query;
  }

  public void setQuery(Query query) {
    _query = query;
  }

  private void setVirtualRoot(JobVertex virtualRoot) {
    _virtualRoot = virtualRoot;
    _virtualRoot.setIndexSegmentList(null);
  }

  public QueryPlanCreator(Query query) {
    setVirtualRoot(new JobVertex(null));
    setQuery(query);
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
    return new QueryPlan(_query, _virtualRoot);
  }

}
