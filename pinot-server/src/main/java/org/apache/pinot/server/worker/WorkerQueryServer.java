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
package org.apache.pinot.server.worker;

import org.apache.helix.HelixManager;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.query.runtime.QueryRunner;
import org.apache.pinot.query.service.server.QueryServer;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.NetUtils;


public class WorkerQueryServer {
  private final int _queryServicePort;
  private final PinotConfiguration _configuration;
  private final HelixManager _helixManager;

  private QueryServer _queryWorkerService;
  private QueryRunner _queryRunner;
  private InstanceDataManager _instanceDataManager;
  private ServerMetrics _serverMetrics;

  public WorkerQueryServer(PinotConfiguration configuration, InstanceDataManager instanceDataManager,
      HelixManager helixManager, ServerMetrics serverMetrics) {
    _configuration = toWorkerQueryConfig(configuration);
    _helixManager = helixManager;
    _instanceDataManager = instanceDataManager;
    _serverMetrics = serverMetrics;
    _queryServicePort =
        _configuration.getProperty(CommonConstants.MultiStageQueryRunner.KEY_OF_QUERY_SERVER_PORT,
            CommonConstants.MultiStageQueryRunner.DEFAULT_QUERY_SERVER_PORT);
    _queryRunner = new QueryRunner();
    _queryRunner.init(_configuration, _instanceDataManager, _helixManager, _serverMetrics);
    _queryWorkerService = new QueryServer(_queryServicePort, _queryRunner, _configuration);
  }

  private static PinotConfiguration toWorkerQueryConfig(PinotConfiguration configuration) {
    PinotConfiguration newConfig = new PinotConfiguration(configuration.toMap());
    String hostname = newConfig.getProperty(CommonConstants.MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_HOSTNAME);
    if (hostname == null) {
      String instanceId =
          newConfig.getProperty(CommonConstants.Helix.KEY_OF_SERVER_NETTY_HOST, NetUtils.getHostnameOrAddress());
      hostname = instanceId.startsWith(CommonConstants.Helix.PREFIX_OF_SERVER_INSTANCE) ? instanceId.substring(
          CommonConstants.Helix.SERVER_INSTANCE_PREFIX_LENGTH) : instanceId;
      newConfig.addProperty(CommonConstants.MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_HOSTNAME, hostname);
    }
    int runnerPort = newConfig.getProperty(
        CommonConstants.MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_PORT,
        CommonConstants.MultiStageQueryRunner.DEFAULT_QUERY_RUNNER_PORT);
    if (runnerPort == -1) {
      runnerPort =
          newConfig.getProperty(CommonConstants.Server.CONFIG_OF_GRPC_PORT, CommonConstants.Server.DEFAULT_GRPC_PORT);
      newConfig.addProperty(CommonConstants.MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_PORT, runnerPort);
    }
    int servicePort =
        newConfig.getProperty(CommonConstants.MultiStageQueryRunner.KEY_OF_QUERY_SERVER_PORT,
            CommonConstants.MultiStageQueryRunner.DEFAULT_QUERY_SERVER_PORT);
    if (servicePort == -1) {
      servicePort = newConfig.getProperty(CommonConstants.Helix.KEY_OF_SERVER_NETTY_PORT,
          CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT);
      newConfig.addProperty(CommonConstants.MultiStageQueryRunner.KEY_OF_QUERY_SERVER_PORT, servicePort);
    }
    return newConfig;
  }

  public int getPort() {
    return _queryServicePort;
  }

  public void start() {
    try {
      _queryWorkerService.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void shutDown() {
    _queryWorkerService.shutdown();
  }
}
