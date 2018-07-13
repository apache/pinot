/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.server.conf;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;


/**
 * The config used for Server.
 */
public class ServerConf {

  private static final String PINOT_ = "pinot.";
  private static final String PINOT_SERVER_INSTANCE = "pinot.server.instance";
  private static final String PINOT_SERVER_METRICS = "pinot.server.metrics";
  private static final String PINOT_SERVER_TABLE_LEVEL_METRICS = "pinot.server.enableTableLevelMetrics";
  // List of metrics to always send table level metrics even if table level metrics is disabled
  private static final String PINOT_SERVER_TABLE_LEVEL_METRICS_LIST = "pinot.server.tablelevel.metrics.whitelist";
  private static final String PINOT_SERVER_QUERY = "pinot.server.query.executor";
  private static final String PINOT_SERVER_REQUEST = "pinot.server.request";
  private static final String PINOT_SERVER_NETTY = "pinot.server.netty";
  private static final String PINOT_SERVER_INSTANCE_DATA_MANAGER_CLASS = "pinot.server.instance.data.manager.class";
  private static final String PINOT_SERVER_QUERY_EXECUTOR_CLASS = "pinot.server.query.executor.class";
  private static final String PINOT_SERVER_TRANSFORM_FUNCTIONS = "pinot.server.transforms";

  private static final String PINOT_QUERY_SCHEDULER_PREFIX = "pinot.query.scheduler";

  private Configuration _serverConf;

  public ServerConf(Configuration serverConfig) {
    _serverConf = serverConfig;
  }

  public void init(Configuration serverConfig) {
    _serverConf = serverConfig;
  }

  public Configuration getInstanceDataManagerConfig() {
    return _serverConf.subset(PINOT_SERVER_INSTANCE);
  }

  public Configuration getQueryExecutorConfig() {
    return _serverConf.subset(PINOT_SERVER_QUERY);
  }

  public Configuration getRequestConfig() {
    return _serverConf.subset(PINOT_SERVER_REQUEST);
  }

  public Configuration getMetricsConfig() {
    return _serverConf.subset(PINOT_SERVER_METRICS);
  }

  public NettyServerConfig getNettyConfig() throws ConfigurationException {
    return new NettyServerConfig(_serverConf.subset(PINOT_SERVER_NETTY));
  }

  public Configuration getConfig(String component) {
    return _serverConf.subset(PINOT_ + component);
  }

  public String getInstanceDataManagerClassName() {
    return _serverConf.getString(PINOT_SERVER_INSTANCE_DATA_MANAGER_CLASS);
  }

  public String getQueryExecutorClassName() {
    return _serverConf.getString(PINOT_SERVER_QUERY_EXECUTOR_CLASS);
  }

  public Configuration getSchedulerConfig() {
    return _serverConf.subset(PINOT_QUERY_SCHEDULER_PREFIX);
  }

  /**
   * Returns an array of transform function names as defined in the config
   * @return String array of transform functions
   */
  public String[] getTransformFunctions() {
    return _serverConf.getStringArray(PINOT_SERVER_TRANSFORM_FUNCTIONS);
  }

  public boolean emitTableLevelMetrics() {
    return _serverConf.getBoolean(PINOT_SERVER_TABLE_LEVEL_METRICS, true);
  }
}
