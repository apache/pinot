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
package org.apache.pinot.benchmark;

import java.util.concurrent.TimeUnit;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.pinot.benchmark.api.PinotClusterManager;
import org.apache.pinot.benchmark.api.data.DataPreparationManager;
import org.apache.pinot.benchmark.api.retentions.TableRetentionManager;
import org.apache.pinot.benchmark.common.PinotBenchServiceApplication;
import org.apache.pinot.benchmark.common.utils.PinotClusterClient;
import org.apache.pinot.benchmark.common.utils.PinotClusterLocator;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PinotBenchmarkServiceStarter {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotBenchmarkServiceStarter.class);

  private final String _clusterName;
  private final String _pinotBenchInstanceId;
  private final String _zkServers;
  private final PinotBenchConf _config;
  private PinotClusterLocator _pinotClusterLocator;
  private final PinotClusterClient _pinotClusterClient;
  private final PinotClusterManager _pinotClusterManager;
  private final DataPreparationManager _dataPreparationManager;
  private final TableRetentionManager _tableRetentionManager;
  private final PinotBenchServiceApplication _pinotBenchServiceApplication;

  private HelixManager _helixManager;

  public PinotBenchmarkServiceStarter(PinotBenchConf conf) {
    _config = conf;
    String host = _config.getPinotBenchHost();
    int port = _config.getPinotBenchPort();
    _clusterName = _config.getClusterName();
    _pinotBenchInstanceId = host + ":" + port;
    _zkServers = _config.getZkStr();

    _pinotClusterClient = new PinotClusterClient();
    _pinotClusterLocator = new PinotClusterLocator(_config);
    _pinotClusterManager = new PinotClusterManager(_config, _pinotClusterClient, _pinotClusterLocator);
    _dataPreparationManager = new DataPreparationManager(_config, _pinotClusterClient, _pinotClusterLocator);
    _tableRetentionManager = new TableRetentionManager(_config, _pinotClusterManager);
    _pinotBenchServiceApplication = new PinotBenchServiceApplication();
  }

  public void start()
      throws Exception {
    _helixManager =
        HelixManagerFactory.getZKHelixManager(_clusterName, _pinotBenchInstanceId, InstanceType.SPECTATOR, _zkServers);
    _helixManager.connect();

    LOGGER.info("Starting Pinot cluster manager");
    _pinotClusterManager.start(_helixManager);

    LOGGER.info("Starting data preparation manager");
    _dataPreparationManager.start(_helixManager);

    LOGGER.info("Starting table retention manager");
    _tableRetentionManager.start(_helixManager);

    _pinotBenchServiceApplication.registerBinder(new AbstractBinder() {
      @Override
      protected void configure() {
        bind(_pinotClusterManager).to(PinotClusterManager.class);
        bind(_dataPreparationManager).to(DataPreparationManager.class);
      }
    });

    LOGGER.info("Starting Pinot benchmark service.");
    _pinotBenchServiceApplication.start(_config.getPinotBenchPort());
  }

  public void stop() {
    LOGGER.info("Stopping Pinot benchmark service...");
    _pinotBenchServiceApplication.stop();

    _tableRetentionManager.stop();
  }

  public static void main(String[] args)
      throws InterruptedException {
    PinotBenchConf conf = new PinotBenchConf();
    conf.setPerfControllerHost("localhost");
    conf.setPerfControllerPort(9010);
    conf.setPinotBenchHost("localhost");
    conf.setPinotBenchPort(9008);
    conf.setClusterName("PinotCluster");
    conf.setZkStr("localhost:2181");
    conf.setTableRetentionManagerInitialDelayInSeconds(30L);
    conf.setTableRetentionManagerFrequencyInSeconds(TimeUnit.HOURS.toSeconds(6L));

    conf.setProdControllerHost("localhost");
    conf.setProdControllerPort(9000);

    PinotBenchmarkServiceStarter starter = new PinotBenchmarkServiceStarter(conf);

    try {
      LOGGER.info("Start starter.");
      starter.start();
    } catch (Exception e) {
      LOGGER.error("Error starting", e);
    }

    int i = 0;
    while (i++ < 600L) {
      Thread.sleep(1000L);
    }

    LOGGER.info("Stopping");
    starter.stop();
  }
}
