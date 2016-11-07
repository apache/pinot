/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.PreConnectCallback;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Message;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.linkedin.pinot.common.Utils;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metrics.ServerMeter;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.ControllerTenantNameBuilder;
import com.linkedin.pinot.common.utils.MmapUtils;
import com.linkedin.pinot.common.utils.NetUtil;
import com.linkedin.pinot.common.utils.ZkUtils;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentMetadataLoader;
import com.linkedin.pinot.server.conf.ServerConf;
import com.linkedin.pinot.server.realtime.ControllerLeaderLocator;
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
  private AdminApiApplication _adminApiApplication;

  public HelixServerStarter(String helixClusterName, String zkServer, Configuration pinotHelixProperties)
      throws Exception {
    LOGGER.info("Starting Pinot server");
    _helixClusterName = helixClusterName;
    _pinotHelixProperties = pinotHelixProperties;
    String hostname = pinotHelixProperties.getString(CommonConstants.Helix.KEY_OF_SERVER_NETTY_HOST,
        NetUtil.getHostAddress());
    _instanceId =
        pinotHelixProperties.getString(
            "instanceId",
            CommonConstants.Helix.PREFIX_OF_SERVER_INSTANCE
                + hostname
                + "_"
                + pinotHelixProperties.getInt(CommonConstants.Helix.KEY_OF_SERVER_NETTY_PORT,
                    CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT));

    pinotHelixProperties.addProperty("pinot.server.instance.id", _instanceId);
    startServerInstance(pinotHelixProperties);

    LOGGER.info("Connecting Helix components");
    // Replace all white-spaces from list of zkServers.
    String zkServers = zkServer.replaceAll("\\s+", "");
    _helixManager =
        HelixManagerFactory.getZKHelixManager(helixClusterName, _instanceId, InstanceType.PARTICIPANT, zkServers);
    final StateMachineEngine stateMachineEngine = _helixManager.getStateMachineEngine();
    _helixManager.connect();
    ZkHelixPropertyStore<ZNRecord> zkPropertyStore = ZkUtils.getZkPropertyStore(_helixManager, helixClusterName);

    SegmentFetcherAndLoader fetcherAndLoader = new SegmentFetcherAndLoader(_serverInstance.getInstanceDataManager(),
        new ColumnarSegmentMetadataLoader(), zkPropertyStore, pinotHelixProperties, _instanceId);

    // Register state model factory
    final StateModelFactory<?> stateModelFactory =
        new SegmentOnlineOfflineStateModelFactory(helixClusterName, _instanceId,
            _serverInstance.getInstanceDataManager(),  zkPropertyStore, fetcherAndLoader);
    stateMachineEngine.registerStateModelFactory(SegmentOnlineOfflineStateModelFactory.getStateModelName(),
        stateModelFactory);
    _helixAdmin = _helixManager.getClusterManagmentTool();
    addInstanceTagIfNeeded(helixClusterName, _instanceId);
    // Start restlet server for admin API endpoint
    int adminApiPort = pinotHelixProperties.getInt(CommonConstants.Server.CONFIG_OF_ADMIN_API_PORT,
        Integer.parseInt(CommonConstants.Server.DEFAULT_ADMIN_API_PORT));
    _adminApiApplication = new AdminApiApplication(_serverInstance);
    _adminApiApplication.start(adminApiPort);
    updateInstanceConfigInHelix(adminApiPort, false/*shutDownStatus*/);

    // Register message handler factory
    SegmentMessageHandlerFactory messageHandlerFactory = new SegmentMessageHandlerFactory(fetcherAndLoader);
    _helixManager.getMessagingService().registerMessageHandlerFactory(Message.MessageType.USER_DEFINE_MSG.toString(),
        messageHandlerFactory);

    _serverInstance.getServerMetrics()
        .addCallbackGauge("helix.connected", new Callable<Long>() {
              @Override
              public Long call()
                  throws Exception {
                return _helixManager.isConnected() ? 1L : 0L;
              }
            });

    _helixManager.addPreConnectCallback(new PreConnectCallback() {
          @Override
          public void onPreConnect() {
            _serverInstance.getServerMetrics().addMeteredGlobalValue(ServerMeter.HELIX_ZOOKEEPER_RECONNECTS, 1L);
          }
        });

    ControllerLeaderLocator.create(_helixManager);

    LOGGER.info("Pinot server ready");

    // Create metrics for mmap stuff
    _serverInstance.getServerMetrics().addCallbackGauge(
        "memory.directByteBufferUsage", new Callable<Long>() {
              @Override
              public Long call() throws Exception {
                return MmapUtils.getDirectByteBufferUsage();
              }
            });

    _serverInstance.getServerMetrics().addCallbackGauge(
        "memory.mmapBufferUsage", new Callable<Long>() {
              @Override
              public Long call() throws Exception {
                return MmapUtils.getMmapBufferUsage();
              }
         });

    _serverInstance.getServerMetrics().addCallbackGauge(
        "memory.mmapBufferCount", new Callable<Long>() {
              @Override
              public Long call() throws Exception {
                return MmapUtils.getMmapBufferCount();
              }
            });


  }

  private void updateInstanceConfigInHelix(int adminApiPort, boolean shuttingDown) {
    Map<String, String> propToUpdate = new HashMap<String, String>();
    propToUpdate.put(CommonConstants.Helix.IS_SHUTDOWN_IN_PROGRESS, String.valueOf(shuttingDown));
    propToUpdate.put(CommonConstants.Helix.Instance.ADMIN_PORT_KEY, String.valueOf(adminApiPort));
    updateInstanceConfigInHelix(propToUpdate);
  }

  private void setShuttingDownStatus(boolean shuttingDownStatus) {
    Map<String, String> propToUpdate = new HashMap<String, String>();
    propToUpdate.put(CommonConstants.Helix.IS_SHUTDOWN_IN_PROGRESS, String.valueOf(shuttingDownStatus));
    updateInstanceConfigInHelix(propToUpdate);
  }

  private void updateInstanceConfigInHelix(Map<String, String> props) {
    HelixConfigScope scope =
        new HelixConfigScopeBuilder(ConfigScopeProperty.PARTICIPANT, _helixClusterName)
            .forParticipant(_instanceId)
            .build();
    _helixAdmin.setConfig(scope, props);
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
      _serverInstance = new ServerInstance();
      _serverInstance.init(_serverConf, new MetricsRegistry());
      _serverInstance.start();
      LOGGER.info("Started server instance");
    }
  }

  private ServerConf getInstanceServerConfig(Configuration moreConfigurations) {
    return DefaultHelixStarterServerConfig.getDefaultHelixServerConfig(moreConfigurations);
  }

  private void setupHelixSystemProperties(Configuration conf) {
    // [PINOT-2435] [PINOT-3927] Disable helix detection of flapping connection
    // Helix will shutdown and effectively remove the instance from cluster if
    // it detects flapping while the process continues to run
    // Helix ignores the value if it is <= 0. Hence, setting time window to small value
    // and number of connection failures within that window to high value
    System.setProperty(CommonConstants.Helix.HELIX_MANAGER_FLAPPING_TIME_WINDOW_KEY,
        conf.getString(CommonConstants.Helix.CONFIG_OF_HELIX_FLAPPING_TIMEWINDOW_MS,
            CommonConstants.Helix.DEFAULT_HELIX_FLAPPING_TIMEWINDOW_MS));

    System.setProperty(CommonConstants.Helix.HELIX_MANAGER_MAX_DISCONNECT_THRESHOLD_KEY,
        conf.getString(CommonConstants.Helix.CONFIG_OF_HELIX_MAX_DISCONNECT_THRESHOLD,
            CommonConstants.Helix.DEFAULT_HELIX_FLAPPING_MAX_DISCONNECT_THRESHOLD));
  }

  public void stop() {
    _adminApiApplication.stop();
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
    final HelixServerStarter pinotHelixStarter = new HelixServerStarter("quickstart", "localhost:2191", configuration);
    return pinotHelixStarter;
  }

  public static void main(String[] args) throws Exception {
    /*
    // Another way to start a server via IDE
    if (args.length < 1) {
      throw new RuntimeException("Usage: cmd <port>");
    }
    for (int i = 0; i < args.length; i++) {
      final int port = Integer.valueOf(args[i]);
      final String serverFQDN = "localhost";
      final String server = "Server_" + serverFQDN + "_" + port;
      final Configuration configuration = new PropertiesConfiguration();
      configuration.addProperty("pinot.server.instance.dataDir", "/tmp/PinotServer/test" + port + "/index");
      configuration.addProperty("pinot.server.instance.segmentTarDir", "/tmp/PinotServer/test" + port + "/segmentTar");
      configuration.addProperty("instanceId", server);
      final HelixServerStarter pinotHelixStarter = new HelixServerStarter("PinotPerfTestCluster", "localhost:2191", configuration);
    }
    */
    startDefault();
  }
}
