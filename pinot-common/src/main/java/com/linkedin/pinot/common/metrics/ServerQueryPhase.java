package com.linkedin.pinot.common.metrics;

import com.linkedin.pinot.common.Utils;


/**
* Enumeration containing all the query phases executed by the server.
*
* @author jfim
*/
public enum ServerQueryPhase implements AbstractMetrics.QueryPhase {
  TOTAL_QUERY_TIME, SEGMENT_PRUNING, BUILD_QUERY_PLAN, QUERY_PLAN_EXECUTION;

  private final String queryPhaseName;

  ServerQueryPhase() { queryPhaseName = Utils.toCamelCase(name().toLowerCase()); }

  public String getQueryPhaseName() {
    return queryPhaseName;
  }
}
