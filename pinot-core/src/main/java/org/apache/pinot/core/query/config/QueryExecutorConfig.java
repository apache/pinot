/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.query.config;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.pinot.spi.env.PinotConfiguration;


/**
 * Config for QueryExecutor.
 *
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

  private PinotConfiguration _queryExecutorConfig = null;
  private SegmentPrunerConfig _segmentPrunerConfig;
  private QueryPlannerConfig _queryPlannerConfig;
  private final long _timeOutMs;

  public QueryExecutorConfig(PinotConfiguration config) throws ConfigurationException {
    _queryExecutorConfig = config;
    checkRequiredKeys();
    _segmentPrunerConfig = new SegmentPrunerConfig(_queryExecutorConfig.subset(QUERY_PRUNER));
    _queryPlannerConfig = new QueryPlannerConfig(_queryExecutorConfig.subset(QUERY_PLANNER));
    _timeOutMs = _queryExecutorConfig.getProperty(TIME_OUT, -1);
  }

  private void checkRequiredKeys()
      throws ConfigurationException {
    for (String keyString : REQUIRED_KEYS) {
      if (!_queryExecutorConfig.containsKey(keyString)) {
        throw new ConfigurationException("Cannot find required key : " + keyString);
      }
    }
  }

  public PinotConfiguration getConfig() {
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
