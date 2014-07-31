package com.linkedin.pinot.core.query.config;

import org.apache.commons.configuration.Configuration;


/**
 * Config for QueryPlanner.
 * 
 * @author xiafu
 *
 */
public class QueryPlannerConfig {

  private Configuration _queryPlannerConfig;

  public QueryPlannerConfig(Configuration queryPlannerConfig) {
    _queryPlannerConfig = queryPlannerConfig;
  }

  public String getQueryPlannerType() {
    return _queryPlannerConfig.getString("type");
  }
}
