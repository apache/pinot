package com.linkedin.pinot.core.query.config;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;


/**
 * Config for QueryPlanner.
 * 
 * @author xiafu
 *
 */
public class QueryPlannerConfig {

  private Configuration _queryPlannerConfig;
  private static String[] REQUIRED_KEYS = {};

  public QueryPlannerConfig(Configuration queryPlannerConfig) throws ConfigurationException {
    _queryPlannerConfig = queryPlannerConfig;
    checkRequiredKeys();
  }

  private void checkRequiredKeys() throws ConfigurationException {
    for (String keyString : REQUIRED_KEYS) {
      if (!_queryPlannerConfig.containsKey(keyString)) {
        throw new ConfigurationException("Cannot find required key : " + keyString);
      }
    }
  }

  public String getQueryPlannerType() {
    return _queryPlannerConfig.getString("type");
  }
}
