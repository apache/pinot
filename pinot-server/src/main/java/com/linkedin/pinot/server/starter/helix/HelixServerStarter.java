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

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Uninterruptibles;
import com.linkedin.pinot.common.Utils;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metrics.ServerMeter;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.ControllerTenantNameBuilder;
import com.linkedin.pinot.common.utils.MmapUtils;
import com.linkedin.pinot.common.utils.NetUtil;
import com.linkedin.pinot.common.utils.ServiceStatus;
import com.linkedin.pinot.server.conf.ServerConf;
import com.linkedin.pinot.server.realtime.ControllerLeaderLocator;
import com.linkedin.pinot.server.realtime.ServerSegmentCompletionProtocolHandler;
import com.linkedin.pinot.server.starter.ServerInstance;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationUtils;
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


/**
 * Single server helix starter. Will start automatically with an untagged box.
 * Will auto join current cluster as a participant.
 *
 *
 *
 */
public class HelixServerStarter {
  private static final Logger LOGGER = LoggerFactory.getLogger(HelixServerStarter.class);

  private final String _helixClusterName;
  private final Configuration _helixServerConfig;
  private final String _instanceId;
  private final long _maxQueryTimeMs;
  private final HelixManager _helixManager;
  private final HelixAdmin _helixAdmin;
  private final ServerInstance _serverInstance;
  private final AdminApiApplication _adminApiApplication;

  public HelixServerStarter(String helixClusterName, String zkServer, Configuration helixServerConfig)
      throws Exception {
    LOGGER.info("Starting Pinot server");
    _helixClusterName = helixClusterName;

    // Make a clone so that changes to the config won't propagate to the caller
    _helixServerConfig = ConfigurationUtils.cloneConfiguration(helixServerConfig);

    if (_helixServerConfig.containsKey(CommonConstants.Server.CONFIG_OF_INSTANCE_ID)) {
      _instanceId = _helixServerConfig.getString(CommonConstants.Server.CONFIG_OF_INSTANCE_ID);
    } else {
      String host =
          _helixServerConfig.getString(CommonConstants.Helix.KEY_OF_SERVER_NETTY_HOST, NetUtil.getHostAddress());
      int port = _helixServerConfig.getInt(CommonConstants.Helix.KEY_OF_SERVER_NETTY_PORT,
          CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT);
      _instanceId = CommonConstants.Helix.PREFIX_OF_SERVER_INSTANCE + host + "_" + port;
      _helixServerConfig.addProperty(CommonConstants.Server.CONFIG_OF_INSTANCE_ID, _instanceId);
    }

    _maxQueryTimeMs = _helixServerConfig.getLong(CommonConstants.Server.CONFIG_OF_QUERY_EXECUTOR_TIMEOUT,
        CommonConstants.Server.DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS);

    LOGGER.info("Connecting Helix components");
    setupHelixSystemProperties(_helixServerConfig);
    // Replace all white-spaces from list of zkServers.
    String zkServers = zkServer.replaceAll("\\s+", "");
    _helixManager =
        HelixManagerFactory.getZKHelixManager(helixClusterName, _instanceId, InstanceType.PARTICIPANT, zkServers);
    final StateMachineEngine stateMachineEngine = _helixManager.getStateMachineEngine();
    _helixManager.connect();
    _helixAdmin = _helixManager.getClusterManagmentTool();
    addInstanceTagIfNeeded(helixClusterName, _instanceId);
    ZkHelixPropertyStore<ZNRecord> propertyStore = _helixManager.getHelixPropertyStore();


    LOGGER.info("Starting server instance");
    Utils.logVersions();
    ServerConf serverInstanceConfig = DefaultHelixStarterServerConfig.getDefaultHelixServerConfig(_helixServerConfig);
    // Need to do this before we start receiving state transitions.
    ServerSegmentCompletionProtocolHandler.init(_helixServerConfig.subset(CommonConstants.Server.PREFIX_OF_CONFIG_OF_SEGMENT_UPLOADER));
    _serverInstance = new ServerInstance();
    _serverInstance.init(serverInstanceConfig, propertyStore);
    _serverInstance.start();

    // Register state model factory
    SegmentFetcherAndLoader fetcherAndLoader =
        new SegmentFetcherAndLoader(_serverInstance.getInstanceDataManager(), propertyStore, _helixServerConfig);
    StateModelFactory<?> stateModelFactory =
        new SegmentOnlineOfflineStateModelFactory(_instanceId, _serverInstance.getInstanceDataManager(),
            fetcherAndLoader, propertyStore);
    stateMachineEngine.registerStateModelFactory(SegmentOnlineOfflineStateModelFactory.getStateModelName(),
        stateModelFactory);

    // Start restlet server for admin API endpoint
    int adminApiPort = _helixServerConfig.getInt(CommonConstants.Server.CONFIG_OF_ADMIN_API_PORT,
        CommonConstants.Server.DEFAULT_ADMIN_API_PORT);
    _adminApiApplication = new AdminApiApplication(_serverInstance);
    _adminApiApplication.start(adminApiPort);
    updateInstanceConfigInHelix(adminApiPort, false/*shutDownStatus*/);

    // Register message handler factory
    SegmentMessageHandlerFactory messageHandlerFactory =
        new SegmentMessageHandlerFactory(fetcherAndLoader, _serverInstance.getInstanceDataManager());
    _helixManager.getMessagingService()
        .registerMessageHandlerFactory(Message.MessageType.USER_DEFINE_MSG.toString(), messageHandlerFactory);

    _serverInstance.getServerMetrics().addCallbackGauge("helix.connected", new Callable<Long>() {
      @Override
      public Long call() throws Exception {
        return _helixManager.isConnected() ? 1L : 0L;
      }
    });

    _helixManager.addPreConnectCallback(new PreConnectCallback() {
      @Override
      public void onPreConnect() {
        _serverInstance.getServerMetrics().addMeteredGlobalValue(ServerMeter.HELIX_ZOOKEEPER_RECONNECTS, 1L);
      }
    });

    // Register the service status handler
    ServiceStatus.setServiceStatusCallback(new ServiceStatus.MultipleCallbackServiceStatusCallback(ImmutableList.of(
        new ServiceStatus.IdealStateAndCurrentStateMatchServiceStatusCallback(_helixManager, _helixClusterName,
            _instanceId),
        new ServiceStatus.IdealStateAndExternalViewMatchServiceStatusCallback(_helixManager, _helixClusterName,
            _instanceId))));

    ControllerLeaderLocator.create(_helixManager);

    LOGGER.info("Pinot server ready");

    // Create metrics for mmap stuff
    _serverInstance.getServerMetrics().addCallbackGauge("memory.directByteBufferUsage", new Callable<Long>() {
      @Override
      public Long call() throws Exception {
        return MmapUtils.getDirectByteBufferUsage();
      }
    });

    _serverInstance.getServerMetrics().addCallbackGauge("memory.mmapBufferUsage", new Callable<Long>() {
      @Override
      public Long call() throws Exception {
        return MmapUtils.getMmapBufferUsage();
      }
    });

    _serverInstance.getServerMetrics().addCallbackGauge("memory.mmapBufferCount", new Callable<Long>() {
      @Override
      public Long call() throws Exception {
        return MmapUtils.getMmapBufferCount();
      }
    });

    _serverInstance.getServerMetrics().addCallbackGauge("memory.allocationFailureCount", new Callable<Long>() {
      @Override
      public Long call() throws Exception {
        return (long) MmapUtils.getAllocationFailureCount();
      }
    });
  }

  private void updateInstanceConfigInHelix(int adminApiPort, boolean shuttingDown) {
    Map<String, String> propToUpdate = new HashMap<>();
    propToUpdate.put(CommonConstants.Helix.IS_SHUTDOWN_IN_PROGRESS, String.valueOf(shuttingDown));
    propToUpdate.put(CommonConstants.Helix.Instance.ADMIN_PORT_KEY, String.valueOf(adminApiPort));
    updateInstanceConfigInHelix(propToUpdate);
  }

  private void setShuttingDownStatus(boolean shuttingDownStatus) {
    Map<String, String> propToUpdate = new HashMap<>();
    propToUpdate.put(CommonConstants.Helix.IS_SHUTDOWN_IN_PROGRESS, String.valueOf(shuttingDownStatus));
    updateInstanceConfigInHelix(propToUpdate);
  }

  private void updateInstanceConfigInHelix(Map<String, String> props) {
    HelixConfigScope scope =
        new HelixConfigScopeBuilder(ConfigScopeProperty.PARTICIPANT, _helixClusterName).forParticipant(_instanceId)
            .build();
    _helixAdmin.setConfig(scope, props);
  }

  private void addInstanceTagIfNeeded(String clusterName, String instanceName) {
    InstanceConfig instanceConfig = _helixAdmin.getInstanceConfig(clusterName, instanceName);
    List<String> instanceTags = instanceConfig.getTags();
    if (instanceTags == null || instanceTags.size() == 0) {
      if (ZKMetadataProvider.getClusterTenantIsolationEnabled(_helixManager.getHelixPropertyStore())) {
        _helixAdmin.addInstanceTag(clusterName, instanceName,
            TableNameBuilder.OFFLINE.tableNameWithType(ControllerTenantNameBuilder.DEFAULT_TENANT_NAME));
        _helixAdmin.addInstanceTag(clusterName, instanceName,
            TableNameBuilder.REALTIME.tableNameWithType(ControllerTenantNameBuilder.DEFAULT_TENANT_NAME));
      } else {
        _helixAdmin.addInstanceTag(clusterName, instanceName, CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE);
      }
    }
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
    if (_helixServerConfig.getBoolean(CommonConstants.Server.CONFIG_OF_ENABLE_SHUTDOWN_DELAY, true)) {
      Uninterruptibles.sleepUninterruptibly(_maxQueryTimeMs, TimeUnit.MILLISECONDS);
    }
    _helixManager.disconnect();
    _serverInstance.shutDown();
  }

  /**
   * This method is for reference purpose only.
   */
  public static HelixServerStarter startDefault() throws Exception {
    Configuration configuration = new PropertiesConfiguration();
    int port = 8003;
    configuration.addProperty(CommonConstants.Helix.KEY_OF_SERVER_NETTY_PORT, port);
    configuration.addProperty("pinot.server.instance.dataDir", "/tmp/PinotServer/test" + port + "/index");
    configuration.addProperty("pinot.server.instance.segmentTarDir", "/tmp/PinotServer/test" + port + "/segmentTar");
    return new HelixServerStarter("quickstart", "localhost:2191", configuration);
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
