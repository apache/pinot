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

import com.linkedin.pinot.server.api.restlet.PinotAdminEndpointApplication;
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
import org.restlet.Component;
import org.restlet.Context;
import org.restlet.data.Protocol;
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
  private Component _adminApiComponent = null;

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

    SegmentFetcherAndLoader fetcherAndLoader = new SegmentFetcherAndLoader(_serverInstance.getInstanceDataManager(),
        new ColumnarSegmentMetadataLoader(), zkPropertyStore, pinotHelixProperties, _instanceId);

    // Register state model factory
    final StateModelFactory<?> stateModelFactory =
        new SegmentOnlineOfflineStateModelFactory(helixClusterName, _instanceId,
            _serverInstance.getInstanceDataManager(),  zkPropertyStore, fetcherAndLoader);
    stateMachineEngine.registerStateModelFactory(SegmentOnlineOfflineStateModelFactory.getStateModelDef(),
        stateModelFactory);
    _helixAdmin = _helixManager.getClusterManagmentTool();
    addInstanceTagIfNeeded(helixClusterName, _instanceId);
    setShuttingDownStatus(false);

    // Register message handler factory
    SegmentMessageHandlerFactory messageHandlerFactory = new SegmentMessageHandlerFactory(fetcherAndLoader);
    _helixManager.getMessagingService().registerMessageHandlerFactory(Message.MessageType.USER_DEFINE_MSG.toString(),
        messageHandlerFactory);

    _serverInstance.getServerMetrics()
        .addCallbackGauge("helix.connected",
            new Callable<Long>() {
              @Override
              public Long call() throws Exception {
                return _helixManager.isConnected() ? 1L : 0L;
              }
            });

    _helixManager.addPreConnectCallback(
        new PreConnectCallback() {
          @Override
          public void onPreConnect() {
            _serverInstance.getServerMetrics().addMeteredGlobalValue(ServerMeter.HELIX_ZOOKEEPER_RECONNECTS, 1L);
          }
        });

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

    // Start restlet server for admin API endpoint
    try {
      int adminApiPort = pinotHelixProperties.getInt(CommonConstants.Server.CONFIG_OF_ADMIN_API_PORT,
          Integer.parseInt(CommonConstants.Server.DEFAULT_ADMIN_API_PORT));

      if (0 < adminApiPort) {
        _adminApiComponent = new Component();

        _adminApiComponent.getServers().add(Protocol.HTTP, adminApiPort);
        _adminApiComponent.getClients().add(Protocol.FILE);
        _adminApiComponent.getClients().add(Protocol.JAR);
        _adminApiComponent.getClients().add(Protocol.WAR);

        PinotAdminEndpointApplication adminEndpointApplication = new PinotAdminEndpointApplication();
        final Context applicationContext = _adminApiComponent.getContext().createChildContext();
        adminEndpointApplication.setContext(applicationContext);
        _adminApiComponent.getDefaultHost().attach(adminEndpointApplication);
        LOGGER.info("Will start admin API endpoint on port {}", adminApiPort);
      } else {
        LOGGER.warn("Not starting admin API endpoint due to invalid port number {}", adminApiPort);
      }
    } catch (Exception e) {
      LOGGER.warn("Not starting admin API endpoint due to exception", e);
    }
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
      if (_adminApiComponent != null) {
        LOGGER.info("Trying to start admin API endpoint");
        _adminApiComponent.start();
      }
    }
  }

  private ServerConf getInstanceServerConfig(Configuration moreConfigurations) {
    return DefaultHelixStarterServerConfig.getDefaultHelixServerConfig(moreConfigurations);
  }

  private void setupHelixSystemProperties(Configuration conf) {
    System.setProperty("helixmanager.flappingTimeWindow",
        conf.getString(CommonConstants.Server.CONFIG_OF_HELIX_FLAPPING_TIMEWINDOW_MS,
            CommonConstants.Server.DEFAULT_HELIX_FLAPPING_TIMEWINDOW_MS));
  }

  public void stop() {
    if (_adminApiComponent != null) {
      try {
        _adminApiComponent.stop();
      } catch (Exception e) {
        LOGGER.warn("Caught exception while stopping admin API endpoint", e);
      }
    }

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
