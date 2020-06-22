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
package org.apache.pinot.server.conf;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.spi.env.PinotConfiguration;


/**
 * The config used for Server.
 */
public class ServerConf {
  private static final String PINOT_ = "pinot.";
  private static final String PINOT_SERVER_INSTANCE = "pinot.server.instance";
  private static final String PINOT_SERVER_METRICS = "pinot.server.metrics";
  private static final String PINOT_SERVER_METRICS_PREFIX = "pinot.server.metrics.prefix";
  private static final String PINOT_SERVER_TABLE_LEVEL_METRICS = "pinot.server.enableTableLevelMetrics";
  private static final String PINOT_SERVER_QUERY = "pinot.server.query.executor";
  private static final String PINOT_SERVER_REQUEST = "pinot.server.request";
  private static final String PINOT_SERVER_NETTY = "pinot.server.netty";
  private static final String PINOT_SERVER_INSTANCE_DATA_MANAGER_CLASS = "pinot.server.instance.data.manager.class";
  private static final String PINOT_SERVER_QUERY_EXECUTOR_CLASS = "pinot.server.query.executor.class";
  private static final String PINOT_SERVER_TRANSFORM_FUNCTIONS = "pinot.server.transforms";

  private static final String PINOT_QUERY_SCHEDULER_PREFIX = "pinot.query.scheduler";

  private PinotConfiguration _serverConf;

  public ServerConf(PinotConfiguration serverConfig) {
    _serverConf = serverConfig;
  }

  public void init(PinotConfiguration serverConfig) {
    _serverConf = serverConfig;
  }

  public PinotConfiguration getInstanceDataManagerConfig() {
    return _serverConf.subset(PINOT_SERVER_INSTANCE);
  }

  public PinotConfiguration getQueryExecutorConfig() {
    return _serverConf.subset(PINOT_SERVER_QUERY);
  }

  public PinotConfiguration getRequestConfig() {
    return _serverConf.subset(PINOT_SERVER_REQUEST);
  }

  public PinotConfiguration getMetricsConfig() {
    return _serverConf.subset(PINOT_SERVER_METRICS);
  }

  public NettyServerConfig getNettyConfig()
      throws ConfigurationException {
    return new NettyServerConfig(_serverConf.subset(PINOT_SERVER_NETTY));
  }

  public PinotConfiguration getConfig(String component) {
    return _serverConf.subset(PINOT_ + component);
  }

  public String getInstanceDataManagerClassName() {
    return _serverConf.getProperty(PINOT_SERVER_INSTANCE_DATA_MANAGER_CLASS);
  }

  public String getQueryExecutorClassName() {
    return _serverConf.getProperty(PINOT_SERVER_QUERY_EXECUTOR_CLASS);
  }

  public PinotConfiguration getSchedulerConfig() {
    return _serverConf.subset(PINOT_QUERY_SCHEDULER_PREFIX);
  }

  /**
   * Returns a list of transform function names as defined in the config
   * @return List of transform functions
   */
  public List<String> getTransformFunctions() {
    return _serverConf.getProperty(PINOT_SERVER_TRANSFORM_FUNCTIONS, Arrays.asList());
  }

  public boolean emitTableLevelMetrics() {
    return _serverConf.getProperty(PINOT_SERVER_TABLE_LEVEL_METRICS, true);
  }

  public String getMetricsPrefix() {
    return _serverConf.getProperty(PINOT_SERVER_METRICS_PREFIX, CommonConstants.Server.DEFAULT_METRICS_PREFIX);
  }
}
