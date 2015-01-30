package com.linkedin.pinot.core.query.config;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;


/**
 * Config for QueryExecutor.
 * 
 * @author xiafu
 *
 */
public class QueryExecutorConfig {

  // Prefix key of Query Pruner
  public static final String QUERY_PRUNER = "pruner";
  // Prefix key of Query Planner
  public static final String QUERY_PLANNER = "queryPlanner";
  // Prefix key of TimeOut
  public static final String TIME_OUT = "timeout";

  private static final String[] REQUIRED_KEYS = {};

  private Configuration _queryExecutorConfig = null;
  private SegmentPrunerConfig _segmentPrunerConfig;
  private QueryPlannerConfig _queryPlannerConfig;
  private final long _timeOutMs;

  public QueryExecutorConfig(Configuration config) throws ConfigurationException {
    _queryExecutorConfig = config;
    checkRequiredKeys();
    _segmentPrunerConfig = new SegmentPrunerConfig(_queryExecutorConfig.subset(QUERY_PRUNER));
    _queryPlannerConfig = new QueryPlannerConfig(_queryExecutorConfig.subset(QUERY_PLANNER));
    _timeOutMs = _queryExecutorConfig.getLong(TIME_OUT, -1);
  }

  private void checkRequiredKeys() throws ConfigurationException {
    for (String keyString : REQUIRED_KEYS) {
      if (!_queryExecutorConfig.containsKey(keyString)) {
        throw new ConfigurationException("Cannot find required key : " + keyString);
      }
    }
  }

  public Configuration getConfig() {
    return _queryExecutorConfig;
  }

  public SegmentPrunerConfig getPrunerConfig() {
    return _segmentPrunerConfig;
  }

  public QueryPlannerConfig getQueryPlannerConfig() {
    return _queryPlannerConfig;
  }

  public long getTimeOut() {
    return _timeOutMs;
  }
}
