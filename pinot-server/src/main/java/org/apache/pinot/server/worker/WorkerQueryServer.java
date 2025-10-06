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

import javax.annotation.Nullable;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.query.runtime.QueryRunner;
import org.apache.pinot.query.service.server.QueryServer;
import org.apache.pinot.server.starter.helix.SendStatsPredicate;
import org.apache.pinot.spi.accounting.ThreadAccountant;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Helix;
import org.apache.pinot.spi.utils.CommonConstants.MultiStageQueryRunner;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.apache.pinot.spi.utils.NetUtils;


public class WorkerQueryServer {
  private final int _queryServicePort;
  private final QueryServer _queryWorkerService;

  public WorkerQueryServer(PinotConfiguration serverConf, InstanceDataManager instanceDataManager,
      @Nullable TlsConfig tlsConfig, ThreadAccountant threadAccountant, SendStatsPredicate sendStats) {
    serverConf = toWorkerQueryConfig(serverConf);
    _queryServicePort = serverConf.getProperty(MultiStageQueryRunner.KEY_OF_QUERY_SERVER_PORT,
        MultiStageQueryRunner.DEFAULT_QUERY_SERVER_PORT);
    QueryRunner queryRunner = new QueryRunner();
    queryRunner.init(serverConf, instanceDataManager, tlsConfig, sendStats::isSendStats);
    _queryWorkerService =
        new QueryServer(serverConf, instanceDataManager.getInstanceId(), _queryServicePort, queryRunner, tlsConfig,
            threadAccountant);
  }

  private static PinotConfiguration toWorkerQueryConfig(PinotConfiguration configuration) {
    PinotConfiguration newConfig = new PinotConfiguration(configuration.toMap());
    String hostname = newConfig.getProperty(MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_HOSTNAME);
    if (hostname == null) {
      String instanceId = newConfig.getProperty(Helix.KEY_OF_SERVER_NETTY_HOST, NetUtils.getHostnameOrAddress());
      hostname = instanceId.startsWith(Helix.PREFIX_OF_SERVER_INSTANCE) ? instanceId.substring(
          Helix.SERVER_INSTANCE_PREFIX_LENGTH) : instanceId;
      newConfig.addProperty(MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_HOSTNAME, hostname);
    }
    int runnerPort = newConfig.getProperty(MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_PORT,
        MultiStageQueryRunner.DEFAULT_QUERY_RUNNER_PORT);
    if (runnerPort == -1) {
      runnerPort = newConfig.getProperty(Server.CONFIG_OF_GRPC_PORT, Server.DEFAULT_GRPC_PORT);
      newConfig.addProperty(MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_PORT, runnerPort);
    }
    int servicePort = newConfig.getProperty(MultiStageQueryRunner.KEY_OF_QUERY_SERVER_PORT,
        MultiStageQueryRunner.DEFAULT_QUERY_SERVER_PORT);
    if (servicePort == -1) {
      servicePort = newConfig.getProperty(Helix.KEY_OF_SERVER_NETTY_PORT, Helix.DEFAULT_SERVER_NETTY_PORT);
      newConfig.addProperty(MultiStageQueryRunner.KEY_OF_QUERY_SERVER_PORT, servicePort);
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
