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
 * Config for QueryPlanner.
 *
 *
 */
public class QueryPlannerConfig {

  private PinotConfiguration _queryPlannerConfig;
  private static final String[] REQUIRED_KEYS = {};

  public QueryPlannerConfig(PinotConfiguration queryPlannerConfig)
      throws ConfigurationException {
    _queryPlannerConfig = queryPlannerConfig;
    checkRequiredKeys();
  }

  private void checkRequiredKeys()
      throws ConfigurationException {
    for (String keyString : REQUIRED_KEYS) {
      if (!_queryPlannerConfig.containsKey(keyString)) {
        throw new ConfigurationException("Cannot find required key : " + keyString);
      }
    }
  }

  public String getQueryPlannerType() {
    return _queryPlannerConfig.getProperty("type");
  }
}
