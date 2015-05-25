/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.server.starter.helix;

import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.NetUtil;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentMetadataLoader;
import com.linkedin.pinot.server.conf.ServerConf;
import com.linkedin.pinot.server.starter.ServerInstance;
import com.yammer.metrics.core.MetricsRegistry;


/**
 * Single server helix starter. Will start automatically with an untagged box.
 * Will auto join current cluster as a participant.
 *
 *
 * @author xiafu
 *
 */
public class HelixServerStarter {

  private static final Logger LOGGER = LoggerFactory.getLogger(HelixServerStarter.class);

  protected final HelixManager _helixManager;
  private final Configuration _pinotHelixProperties;
  private HelixAdmin _helixAdmin;

  private ServerConf _serverConf;
  private ServerInstance _serverInstance;

  public HelixServerStarter(String helixClusterName, String zkServer, Configuration pinotHelixProperties)
      throws Exception {

    _pinotHelixProperties = pinotHelixProperties;
    final String instanceId =
        pinotHelixProperties.getString(
            "instanceId",
            CommonConstants.Helix.PREFIX_OF_SERVER_INSTANCE
                + pinotHelixProperties.getString(CommonConstants.Helix.KEY_OF_SERVER_NETTY_HOST,
                    NetUtil.getHostAddress())
                + "_"
                + pinotHelixProperties.getInt(CommonConstants.Helix.KEY_OF_SERVER_NETTY_PORT,
                    CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT));

    pinotHelixProperties.addProperty("pinot.server.instance.id", instanceId);
    startServerInstance(pinotHelixProperties);
    _helixManager =
        HelixManagerFactory.getZKHelixManager(helixClusterName, instanceId, InstanceType.PARTICIPANT, zkServer);
    final StateMachineEngine stateMachineEngine = _helixManager.getStateMachineEngine();
    final StateModelFactory<?> stateModelFactory =
        new SegmentOnlineOfflineStateModelFactory(helixClusterName, instanceId, _serverInstance.getInstanceDataManager(),
            new ColumnarSegmentMetadataLoader(), pinotHelixProperties);
    stateMachineEngine.registerStateModelFactory(SegmentOnlineOfflineStateModelFactory.getStateModelDef(),
        stateModelFactory);
    _helixManager.connect();
    _helixAdmin = _helixManager.getClusterManagmentTool();
    addInstanceTagIfNeeded(helixClusterName, instanceId);
  }

  private void addInstanceTagIfNeeded(String clusterName, String instanceName) {
    InstanceConfig instanceConfig = _helixAdmin.getInstanceConfig(clusterName, instanceName);
    List<String> instanceTags = instanceConfig.getTags();
    if (instanceTags == null || instanceTags.size() == 0) {
      _helixAdmin.addInstanceTag(clusterName, instanceName, CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE);
    }
  }

  private void startServerInstance(Configuration moreConfigurations) throws Exception {
    _serverConf = getInstanceServerConfig(moreConfigurations);
    if (_serverInstance == null) {
      LOGGER.info("Trying to create a new ServerInstance!");
      _serverInstance = new ServerInstance();
      LOGGER.info("Trying to initial ServerInstance!");
      _serverInstance.init(_serverConf, new MetricsRegistry());
      LOGGER.info("Trying to start ServerInstance!");
      _serverInstance.start();
    }
  }

  private ServerConf getInstanceServerConfig(Configuration moreConfigurations) {
    return DefaultHelixStarterServerConfig.getDefaultHelixServerConfig(moreConfigurations);
  }

  public void stop() {
    _helixManager.disconnect();
    _serverInstance.shutDown();
  }

  public static void main(String[] args) throws Exception {
    final Configuration configuration = new PropertiesConfiguration();
    final int port = 8003;
    configuration.addProperty(CommonConstants.Helix.KEY_OF_SERVER_NETTY_PORT, port);
    long currentTimeMillis = System.currentTimeMillis();
    configuration.addProperty("pinot.server.instance.dataDir", "/tmp/PinotServer/test" + port + "/index");
    configuration.addProperty("pinot.server.instance.segmentTarDir", "/tmp/PinotServer/test" + port + "/segmentTar");
    final HelixServerStarter pinotHelixStarter =
        new HelixServerStarter("pinotControllerV2", "localhost:2121", configuration);
  }
}
