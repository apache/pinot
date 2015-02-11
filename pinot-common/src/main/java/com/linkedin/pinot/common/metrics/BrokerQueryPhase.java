package com.linkedin.pinot.common.metrics;

import com.linkedin.pinot.common.Utils;


/**
* Enumeration containing all the query phases executed by the Pinot broker.
*
* @author jfim
*/
public enum BrokerQueryPhase implements AbstractMetrics.QueryPhase {
  REQUEST_COMPILATION,
  QUERY_EXECUTION,
  QUERY_ROUTING,
  SCATTER_GATHER,
  DESERIALIZATION,
  REDUCE;

  private final String queryPhaseName;

  BrokerQueryPhase() {
    queryPhaseName = Utils.toCamelCase(name().toLowerCase());
  }

  public String getQueryPhaseName() {
    return queryPhaseName;
  }
}
