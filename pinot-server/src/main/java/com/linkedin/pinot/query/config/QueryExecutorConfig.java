package com.linkedin.pinot.query.config;

import org.apache.commons.configuration.Configuration;


/**
 * Config for QueryExecutor.
 * 
 * @author xiafu
 *
 */
public class QueryExecutorConfig {

  public static String QUERY_EXECUTOR_CONFIG_PREFIX = "query.executor";
  public static String PRUNER = "pruner";
  public static String QUERY_PLANNER = "queryPlanner";

  private Configuration _queryExecutorConfig = null;

  public QueryExecutorConfig(Configuration serverConf) {
    _queryExecutorConfig = serverConf.subset(QUERY_EXECUTOR_CONFIG_PREFIX);
  }

  public Configuration getConfig() {
    return _queryExecutorConfig;
  }

  public SegmentPrunerConfig getPrunerConfig() {
    return new SegmentPrunerConfig(_queryExecutorConfig.subset(PRUNER));
  }

  public QueryPlannerConfig getQueryPlannerConfig() {
    return new QueryPlannerConfig(_queryExecutorConfig.subset(QUERY_PLANNER));
  }
}
