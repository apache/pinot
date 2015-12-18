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

import com.linkedin.pinot.common.Utils;
import com.linkedin.pinot.common.utils.MmapUtils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metrics.ServerMeter;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.ControllerTenantNameBuilder;
import com.linkedin.pinot.common.utils.NetUtil;
import com.linkedin.pinot.common.utils.ZkUtils;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentMetadataLoader;
import com.linkedin.pinot.server.conf.ServerConf;
import com.linkedin.pinot.server.starter.ServerInstance;
import com.yammer.metrics.core.MetricsRegistry;


/**
 * Single server helix starter. Will start automatically with an untagged box.
 * Will auto join current cluster as a participant.
 *
 *
 *
 */
public class HelixServerStarter {

  private static final Logger LOGGER = LoggerFactory.getLogger(HelixServerStarter.class);

  protected final HelixManager _helixManager;
  private final Configuration _pinotHelixProperties;
  private HelixAdmin _helixAdmin;

  private ServerConf _serverConf;
  private ServerInstance _serverInstance;

  private final String _helixClusterName;
  private final String _instanceId;

  public HelixServerStarter(String helixClusterName, String zkServer, Configuration pinotHelixProperties)
      throws Exception {

    _helixClusterName = helixClusterName;
    _pinotHelixProperties = pinotHelixProperties;

    _instanceId =
        pinotHelixProperties.getString(
            "instanceId",
            CommonConstants.Helix.PREFIX_OF_SERVER_INSTANCE
                + pinotHelixProperties.getString(CommonConstants.Helix.KEY_OF_SERVER_NETTY_HOST,
                    NetUtil.getHostAddress())
                + "_"
                + pinotHelixProperties.getInt(CommonConstants.Helix.KEY_OF_SERVER_NETTY_PORT,
                    CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT));

    pinotHelixProperties.addProperty("pinot.server.instance.id", _instanceId);
    startServerInstance(pinotHelixProperties);

    // Replace all white-spaces from list of zkServers.
    String zkServers = zkServer.replaceAll("\\s+", "");
    _helixManager =
        HelixManagerFactory.getZKHelixManager(helixClusterName, _instanceId, InstanceType.PARTICIPANT, zkServers);
    final StateMachineEngine stateMachineEngine = _helixManager.getStateMachineEngine();
    _helixManager.connect();
    ZkHelixPropertyStore<ZNRecord> zkPropertyStore = ZkUtils.getZkPropertyStore(_helixManager, helixClusterName);
    final StateModelFactory<?> stateModelFactory =
        new SegmentOnlineOfflineStateModelFactory(helixClusterName, _instanceId,
            _serverInstance.getInstanceDataManager(), new ColumnarSegmentMetadataLoader(), pinotHelixProperties,
            zkPropertyStore);
    stateMachineEngine.registerStateModelFactory(SegmentOnlineOfflineStateModelFactory.getStateModelDef(),
        stateModelFactory);
    _helixAdmin = _helixManager.getClusterManagmentTool();
    addInstanceTagIfNeeded(helixClusterName, _instanceId);
    setShuttingDownStatus(false);

    _serverInstance.getServerMetrics().addCallbackGauge(
        "helix.connected", () -> _helixManager.isConnected() ? 1L : 0L);

    _helixManager.addPreConnectCallback(() ->
        _serverInstance.getServerMetrics().addMeteredValue(null, ServerMeter.HELIX_ZOOKEEPER_RECONNECTS, 1L));

    _serverInstance.getServerMetrics().addCallbackGauge(
        "memory.directByteBufferUsage", MmapUtils::getDirectByteBufferUsage);

    _serverInstance.getServerMetrics().addCallbackGauge(
        "memory.mmapBufferUsage", MmapUtils::getMmapBufferUsage);

    _serverInstance.getServerMetrics().addCallbackGauge(
        "memory.mmapBufferCount", MmapUtils::getMmapBufferCount);
  }

  private void setShuttingDownStatus(boolean shuttingDown) {
    HelixConfigScope scope =
        new HelixConfigScopeBuilder(ConfigScopeProperty.PARTICIPANT, _helixClusterName).forParticipant(_instanceId)
            .build();
    Map<String, String> propToUpdate = new HashMap<String, String>();
    propToUpdate.put(CommonConstants.Helix.IS_SHUTDOWN_IN_PROGRESS, String.valueOf(shuttingDown));
    _helixAdmin.setConfig(scope, propToUpdate);
  }

  private void addInstanceTagIfNeeded(String clusterName, String instanceName) {
    InstanceConfig instanceConfig = _helixAdmin.getInstanceConfig(clusterName, instanceName);
    List<String> instanceTags = instanceConfig.getTags();
    if (instanceTags == null || instanceTags.size() == 0) {
      if (ZKMetadataProvider.getClusterTenantIsolationEnabled(_helixManager.getHelixPropertyStore())) {
        _helixAdmin.addInstanceTag(clusterName, instanceName,
            TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(ControllerTenantNameBuilder.DEFAULT_TENANT_NAME));
        _helixAdmin.addInstanceTag(clusterName, instanceName,
            TableNameBuilder.REALTIME_TABLE_NAME_BUILDER.forTable(ControllerTenantNameBuilder.DEFAULT_TENANT_NAME));
      } else {
        _helixAdmin.addInstanceTag(clusterName, instanceName, CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE);
      }
    }
  }

  private void startServerInstance(Configuration moreConfigurations) throws Exception {
    Utils.logVersions();

    _serverConf = getInstanceServerConfig(moreConfigurations);
    setupHelixSystemProperties(moreConfigurations);

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

  private void setupHelixSystemProperties(Configuration conf) {
    System.setProperty("helixmanager.flappingTimeWindow",
        conf.getString(CommonConstants.Server.CONFIG_OF_HELIX_FLAPPING_TIMEWINDOW_MS));
  }

  public void stop() {
    setShuttingDownStatus(true);
    try {
      Thread.sleep(5000);
    } catch (Exception e) {
      LOGGER.error("error trying to sleep waiting for external view to change : ", e);
    }
    _helixManager.disconnect();
    _serverInstance.shutDown();
  }

  public static HelixServerStarter startDefault() throws Exception {
    final Configuration configuration = new PropertiesConfiguration();
    final int port = 8003;
    configuration.addProperty(CommonConstants.Helix.KEY_OF_SERVER_NETTY_PORT, port);
    long currentTimeMillis = System.currentTimeMillis();
    configuration.addProperty("pinot.server.instance.dataDir", "/tmp/PinotServer/test" + port + "/index");
    configuration.addProperty("pinot.server.instance.segmentTarDir", "/tmp/PinotServer/test" + port + "/segmentTar");
    final HelixServerStarter pinotHelixStarter = new HelixServerStarter("quickstart", "localhost:2122", configuration);
    return pinotHelixStarter;
  }

  public static void main(String[] args) throws Exception {
    /*
    // Another way to start a server via IDE
    final int port = 3800;
    final String serverFQDN = "server.host.foo";
    final String server = "Server_" + serverFQDN + "_" + port;
    final Configuration configuration = new PropertiesConfiguration();
    configuration.addProperty("pinot.server.instance.dataDir", "/tmp/PinotServer/test" + port + "/index");
    configuration.addProperty("pinot.server.instance.segmentTarDir", "/tmp/PinotServer/test" + port + "/segmentTar");
    configuration.addProperty("instanceId",  server);
    final HelixServerStarter pinotHelixStarter = new HelixServerStarter("pinotDevDeploy", "localhost:2181", configuration);
    */
    startDefault();
  }
}
