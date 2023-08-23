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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Helix;
import org.apache.pinot.spi.utils.CommonConstants.Server;

import static org.apache.pinot.spi.utils.CommonConstants.Server.*;


/**
 * The config used for Server.
 */
public class ServerConf {

  private final PinotConfiguration _serverConf;

  public ServerConf(PinotConfiguration serverConfig) {
    _serverConf = serverConfig;
  }

  public PinotConfiguration getInstanceDataManagerConfig() {
    return _serverConf.subset(INSTANCE_DATA_MANAGER_CONFIG_PREFIX);
  }

  public PinotConfiguration getQueryExecutorConfig() {
    return _serverConf.subset(QUERY_EXECUTOR_CONFIG_PREFIX);
  }

  public PinotConfiguration getMetricsConfig() {
    return _serverConf.subset(METRICS_CONFIG_PREFIX);
  }

  public boolean isNettyServerEnabled() {
    return _serverConf.getProperty(Server.CONFIG_OF_NETTY_SERVER_ENABLED, Server.DEFAULT_NETTY_SERVER_ENABLED);
  }

  public int getNettyPort() {
    return _serverConf.getProperty(Helix.KEY_OF_SERVER_NETTY_PORT, Helix.DEFAULT_SERVER_NETTY_PORT);
  }

  public boolean isNettyTlsServerEnabled() {
    return _serverConf.getProperty(Server.CONFIG_OF_NETTYTLS_SERVER_ENABLED, Server.DEFAULT_NETTYTLS_SERVER_ENABLED);
  }

  public int getNettyTlsPort() {
    return _serverConf.getProperty(Helix.KEY_OF_SERVER_NETTYTLS_PORT, Helix.DEFAULT_SERVER_NETTYTLS_PORT);
  }

  public boolean isEnableGrpcServer() {
    return _serverConf.getProperty(Server.CONFIG_OF_ENABLE_GRPC_SERVER, Server.DEFAULT_ENABLE_GRPC_SERVER);
  }

  public boolean isGrpcTlsServerEnabled() {
    return _serverConf.getProperty(Server.CONFIG_OF_GRPCTLS_SERVER_ENABLED, Server.DEFAULT_GRPCTLS_SERVER_ENABLED);
  }

  public boolean isMultiStageServerEnabled() {
    return _serverConf.getProperty(Helix.CONFIG_OF_MULTI_STAGE_ENGINE_ENABLED,
        Helix.DEFAULT_MULTI_STAGE_ENGINE_ENABLED);
  }

  public boolean isEnableSwagger() {
    return _serverConf.getProperty(CONFIG_OF_SWAGGER_SERVER_ENABLED, DEFAULT_SWAGGER_SERVER_ENABLED);
  }

  public int getGrpcPort() {
    return _serverConf.getProperty(Server.CONFIG_OF_GRPC_PORT, Server.DEFAULT_GRPC_PORT);
  }

  public int getMultiStageServicePort() {
    return _serverConf.getProperty(CommonConstants.MultiStageQueryRunner.KEY_OF_QUERY_SERVER_PORT,
        CommonConstants.MultiStageQueryRunner.DEFAULT_QUERY_SERVER_PORT);
  }

  public int getMultiStageMailboxPort() {
    return _serverConf.getProperty(CommonConstants.MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_PORT,
        CommonConstants.MultiStageQueryRunner.DEFAULT_QUERY_RUNNER_PORT);
  }

  public String getInstanceDataManagerClassName() {
    return _serverConf.getProperty(CONFIG_OF_INSTANCE_DATA_MANAGER_CLASS, DEFAULT_DATA_MANAGER_CLASS);
  }

  public String getQueryExecutorClassName() {
    return _serverConf.getProperty(CONFIG_OF_QUERY_EXECUTOR_CLASS, DEFAULT_QUERY_EXECUTOR_CLASS);
  }

  public PinotConfiguration getSchedulerConfig() {
    return _serverConf.subset(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX);
  }

  /**
   * Returns a list of transform function names as defined in the config
   * @return List of transform functions
   */
  public List<String> getTransformFunctions() {
    return _serverConf.getProperty(CONFIG_OF_TRANSFORM_FUNCTIONS, Collections.emptyList());
  }

  public boolean emitTableLevelMetrics() {
    return _serverConf.getProperty(CONFIG_OF_ENABLE_TABLE_LEVEL_METRICS, DEFAULT_ENABLE_TABLE_LEVEL_METRICS);
  }

  public Collection<String> getAllowedTablesForEmittingMetrics() {
    return _serverConf.getProperty(CONFIG_OF_ALLOWED_TABLES_FOR_EMITTING_METRICS, Collections.emptyList());
  }

  public String getMetricsPrefix() {
    return _serverConf.getProperty(PINOT_SERVER_METRICS_PREFIX, Server.DEFAULT_METRICS_PREFIX);
  }

  public PinotConfiguration getPinotConfig() {
    return _serverConf;
  }
}
