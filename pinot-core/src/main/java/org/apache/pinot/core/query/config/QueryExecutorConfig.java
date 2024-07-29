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

import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Server;


/**
 * Config for QueryExecutor.
 */
public class QueryExecutorConfig {
  public static final String QUERY_PRUNER = "pruner";
  public static final String PLAN_MAKER_CLASS = "plan.maker.class";
  public static final String TIME_OUT = "timeout";

  private final PinotConfiguration _config;

  public QueryExecutorConfig(PinotConfiguration config)
      throws ConfigurationException {
    _config = config;
  }

  public PinotConfiguration getConfig() {
    return _config;
  }

  public SegmentPrunerConfig getPrunerConfig() {
    return new SegmentPrunerConfig(_config.subset(QUERY_PRUNER));
  }

  public String getPlanMakerClass() {
    return _config.getProperty(PLAN_MAKER_CLASS, Server.DEFAULT_QUERY_EXECUTOR_PLAN_MAKER_CLASS);
  }

  public long getTimeOut() {
    return _config.getProperty(TIME_OUT, Server.DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS);
  }
}
