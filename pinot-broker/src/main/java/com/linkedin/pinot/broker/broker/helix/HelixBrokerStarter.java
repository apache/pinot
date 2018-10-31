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
package com.linkedin.pinot.broker.broker.helix;

import com.google.common.collect.ImmutableList;
import com.linkedin.pinot.broker.broker.AccessControlFactory;
import com.linkedin.pinot.broker.broker.BrokerServerBuilder;
import com.linkedin.pinot.broker.queryquota.TableQueryQuotaManager;
import com.linkedin.pinot.broker.routing.HelixExternalViewBasedRouting;
import com.linkedin.pinot.common.config.TagNameUtils;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metrics.BrokerMeter;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.NetUtil;
import com.linkedin.pinot.common.utils.ServiceStatus;
import com.linkedin.pinot.common.utils.StringUtil;
import com.yammer.metrics.core.MetricsRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.PreConnectCallback;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Message;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Helix Broker Startable
 *
 *
 */
public class HelixBrokerStarter {

  private static final String PROPERTY_STORE = "PROPERTYSTORE";

  private final HelixManager _spectatorHelixManager;
  private final HelixManager _helixManager;
  private final HelixAdmin _helixAdmin;
  private final Configuration _pinotHelixProperties;
  private final HelixExternalViewBasedRouting _helixExternalViewBasedRouting;
  private final BrokerServerBuilder _brokerServerBuilder;
  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final LiveInstancesChangeListenerImpl _liveInstancesListener;
  private final MetricsRegistry _metricsRegistry;
  private final TableQueryQuotaManager _tableQueryQuotaManager;
  private final TimeboundaryRefreshMessageHandlerFactory _tbiMessageHandler;

  // Set after broker is started, which is actually in the constructor.
  private AccessControlFactory _accessControlFactory;

  private static final Logger LOGGER = LoggerFactory.getLogger(HelixBrokerStarter.class);

  private static final String ROUTING_TABLE_PARAMS_SUBSET_KEY =
      "pinot.broker.routing.table";

  public HelixBrokerStarter(String helixClusterName, String zkServer, Configuration pinotHelixProperties)
      throws Exception {
    this(null, helixClusterName, zkServer, pinotHelixProperties);
  }

  public HelixBrokerStarter(String brokerHost, String helixClusterName, String zkServer, Configuration pinotHelixProperties)
      throws Exception {
    LOGGER.info("Starting Pinot broker");

    _liveInstancesListener = new LiveInstancesChangeListenerImpl(helixClusterName);

    _pinotHelixProperties = DefaultHelixBrokerConfig.getDefaultBrokerConf(pinotHelixProperties);

    if (brokerHost == null) {
      brokerHost = NetUtil.getHostAddress();
    }

    final String brokerId =
        _pinotHelixProperties.getString(
                CommonConstants.Helix.Instance.INSTANCE_ID_KEY,
                CommonConstants.Helix.PREFIX_OF_BROKER_INSTANCE
                        + brokerHost
                        + "_"
                        + _pinotHelixProperties.getInt(CommonConstants.Helix.KEY_OF_BROKER_QUERY_PORT,
                    CommonConstants.Helix.DEFAULT_BROKER_QUERY_PORT));

    _pinotHelixProperties.addProperty(CommonConstants.Broker.CONFIG_OF_BROKER_ID, brokerId);
    setupHelixSystemProperties();

    // Remove all white-spaces from the list of zkServers (if any).
    String zkServers = zkServer.replaceAll("\\s+", "");

    LOGGER.info("Connecting Helix components");
    // Connect spectator Helix manager.
    _spectatorHelixManager =
        HelixManagerFactory.getZKHelixManager(helixClusterName, brokerId, InstanceType.SPECTATOR, zkServers);
    _spectatorHelixManager.connect();
    _helixAdmin = _spectatorHelixManager.getClusterManagmentTool();
    _propertyStore = _spectatorHelixManager.getHelixPropertyStore();
    _helixExternalViewBasedRouting = new HelixExternalViewBasedRouting(_propertyStore, _spectatorHelixManager,
        pinotHelixProperties.subset(ROUTING_TABLE_PARAMS_SUBSET_KEY));
    _tableQueryQuotaManager = new TableQueryQuotaManager(_spectatorHelixManager);
    _brokerServerBuilder = startBroker(_pinotHelixProperties);
    _metricsRegistry = _brokerServerBuilder.getMetricsRegistry();
    ClusterChangeMediator clusterChangeMediator =
        new ClusterChangeMediator(_helixExternalViewBasedRouting, _tableQueryQuotaManager, _brokerServerBuilder.getBrokerMetrics());
    _spectatorHelixManager.addExternalViewChangeListener(clusterChangeMediator);
    _spectatorHelixManager.addInstanceConfigChangeListener(clusterChangeMediator);
    _spectatorHelixManager.addLiveInstanceChangeListener(_liveInstancesListener);

    // Connect participant Helix manager.
    _helixManager =
        HelixManagerFactory.getZKHelixManager(helixClusterName, brokerId, InstanceType.PARTICIPANT, zkServers);
    StateMachineEngine stateMachineEngine = _helixManager.getStateMachineEngine();
    StateModelFactory<?> stateModelFactory =
        new BrokerResourceOnlineOfflineStateModelFactory(_spectatorHelixManager, _propertyStore,
            _helixExternalViewBasedRouting, _tableQueryQuotaManager);
    stateMachineEngine.registerStateModelFactory(BrokerResourceOnlineOfflineStateModelFactory.getStateModelDef(),
        stateModelFactory);
    _helixManager.connect();
    _tbiMessageHandler = new TimeboundaryRefreshMessageHandlerFactory
            (_helixExternalViewBasedRouting,
                    _pinotHelixProperties.getLong(
                            CommonConstants.Broker.CONFIG_OF_BROKER_REFRESH_TIMEBOUNDARY_INFO_SLEEP_INTERVAL,
                            CommonConstants.Broker.DEFAULT_BROKER_REFRESH_TIMEBOUNDARY_INFO_SLEEP_INTERVAL_MS));
    _helixManager.getMessagingService().registerMessageHandlerFactory(
            Message.MessageType.USER_DEFINE_MSG.toString(), _tbiMessageHandler);

    addInstanceTagIfNeeded(helixClusterName, brokerId);

    // Register the service status handler
    ServiceStatus.setServiceStatusCallback(
        new ServiceStatus.MultipleCallbackServiceStatusCallback(ImmutableList.of(
            new ServiceStatus.IdealStateAndCurrentStateMatchServiceStatusCallback(_helixManager, helixClusterName, brokerId),
            new ServiceStatus.IdealStateAndExternalViewMatchServiceStatusCallback(_helixManager, helixClusterName, brokerId)
        )));

    _brokerServerBuilder.getBrokerMetrics().addCallbackGauge(
        "helix.connected", new Callable<Long>() {
          @Override
          public Long call() throws Exception {
            return _helixManager.isConnected() ? 1L : 0L;
          }
        });

    _helixManager.addPreConnectCallback(
        new PreConnectCallback() {
          @Override
          public void onPreConnect() {
            _brokerServerBuilder.getBrokerMetrics()
            .addMeteredGlobalValue(BrokerMeter.HELIX_ZOOKEEPER_RECONNECTS, 1L);
          }
        });
  }

  private void setupHelixSystemProperties() {
    final String helixFlappingTimeWindowPropName = "helixmanager.flappingTimeWindow";
    System.setProperty(helixFlappingTimeWindowPropName,
        _pinotHelixProperties.getString(DefaultHelixBrokerConfig.HELIX_FLAPPING_TIME_WINDOW_NAME,
            DefaultHelixBrokerConfig.DEFAULT_HELIX_FLAPPING_TIMEIWINDWOW_MS));
  }

  private void addInstanceTagIfNeeded(String clusterName, String instanceName) {
    InstanceConfig instanceConfig = _helixAdmin.getInstanceConfig(clusterName, instanceName);
    List<String> instanceTags = instanceConfig.getTags();
    if (instanceTags == null || instanceTags.isEmpty()) {
      if (ZKMetadataProvider.getClusterTenantIsolationEnabled(_propertyStore)) {
        _helixAdmin.addInstanceTag(clusterName, instanceName,
            TagNameUtils.getBrokerTagForTenant(TagNameUtils.DEFAULT_TENANT_NAME));
      } else {
        _helixAdmin.addInstanceTag(clusterName, instanceName, CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE);
      }
    }
  }

  private BrokerServerBuilder startBroker(Configuration config) {
    if (config == null) {
      config = DefaultHelixBrokerConfig.getDefaultBrokerConf();
    }
    BrokerServerBuilder brokerServerBuilder = new BrokerServerBuilder(config, _helixExternalViewBasedRouting,
        _helixExternalViewBasedRouting.getTimeBoundaryService(), _liveInstancesListener, _tableQueryQuotaManager);
    _accessControlFactory = brokerServerBuilder.getAccessControlFactory();
    _helixExternalViewBasedRouting.setBrokerMetrics(brokerServerBuilder.getBrokerMetrics());
    _tableQueryQuotaManager.setBrokerMetrics(brokerServerBuilder.getBrokerMetrics());
    brokerServerBuilder.start();

    LOGGER.info("Pinot broker ready and listening on port {} for API requests",
        config.getProperty("pinot.broker.client.queryPort"));

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          brokerServerBuilder.stop();
        } catch (final Exception e) {
          LOGGER.error("Caught exception while running shutdown hook", e);
        }
      }
    });
    return brokerServerBuilder;
  }

  public AccessControlFactory getAccessControlFactory() {
    return _accessControlFactory;
  }

  /**
   * The zk string format should be 127.0.0.1:3000,127.0.0.1:3001/app/a which applies
   * the /helixClusterName/PROPERTY_STORE after chroot to all servers.
   * Expected output for this method is:
   * 127.0.0.1:3000/app/a/helixClusterName/PROPERTY_STORE,127.0.0.1:3001/app/a/helixClusterName/PROPERTY_STORE
   *
   * @param zkServers
   * @param helixClusterName
   * @return the full property store path
   *
   * @see org.apache.zookeeper.ZooKeeper#ZooKeeper(String, int, org.apache.zookeeper.Watcher)
   */
  public static String getZkAddressForBroker(String zkServers, String helixClusterName) {
    List tokens = new ArrayList<String>();
    String[] zkSplit = zkServers.split("/", 2);
    String zkHosts = zkSplit[0];
    String zkPathSuffix = StringUtil.join("/", helixClusterName, PROPERTY_STORE);
    if (zkSplit.length > 1) {
      zkPathSuffix = zkSplit[1] + "/" + zkPathSuffix;
    }
    for (String token : zkHosts.split(",")) {
      tokens.add(StringUtil.join("/", StringUtils.chomp(token, "/"), zkPathSuffix));
    }
    return StringUtils.join(tokens, ",");
  }

  public HelixManager getSpectatorHelixManager() {
    return _spectatorHelixManager;
  }

  public HelixExternalViewBasedRouting getHelixExternalViewBasedRouting() {
    return _helixExternalViewBasedRouting;
  }

  public BrokerServerBuilder getBrokerServerBuilder() {
    return _brokerServerBuilder;
  }

  public static HelixBrokerStarter startDefault() throws Exception {
    Configuration configuration = new PropertiesConfiguration();
    int port = 5001;
    configuration.addProperty(CommonConstants.Helix.KEY_OF_BROKER_QUERY_PORT, port);
    configuration.addProperty("pinot.broker.timeoutMs", 500 * 1000L);

    final HelixBrokerStarter pinotHelixBrokerStarter =
        new HelixBrokerStarter(null, "quickstart", "localhost:2122", configuration);
    return pinotHelixBrokerStarter;
  }

  public void shutdown() {
    LOGGER.info("Shutting down");

    if (_helixManager != null) {
      LOGGER.info("Disconnecting Helix manager");
      _helixManager.disconnect();
    }

    if (_spectatorHelixManager != null) {
      LOGGER.info("Disconnecting spectator Helix manager");
      _spectatorHelixManager.disconnect();
    }
    if (_tbiMessageHandler != null) {
      LOGGER.info("Shutting down timeboundary info refresh message handler");
      _tbiMessageHandler.shutdown();
    }
  }

  public MetricsRegistry getMetricsRegistry() {
    return _metricsRegistry;
  }

  public static void main(String[] args) throws Exception {
    startDefault();
  }
}
