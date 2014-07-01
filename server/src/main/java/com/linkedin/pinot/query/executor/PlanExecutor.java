package com.linkedin.pinot.query.executor;

import com.linkedin.pinot.query.planner.QueryPlan;
import com.linkedin.pinot.query.request.Query;
import com.linkedin.pinot.query.response.InstanceResponse;


/**
 * Given a query and query plan, different strategy may apply based on the query type.
 * 
 * @author Xiang Fu <xiafu@linkedin.com>
 *
 */
public interface PlanExecutor {
  public InstanceResponse ProcessQueryBasedOnPlan(final Query query, QueryPlan queryPlan) throws Exception;
}
