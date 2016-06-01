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
package com.linkedin.pinot.broker.broker.helix;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.broker.broker.BrokerServerBuilder;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metrics.BrokerMeter;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.ControllerTenantNameBuilder;
import com.linkedin.pinot.common.utils.NetUtil;
import com.linkedin.pinot.common.utils.StringUtil;
import com.linkedin.pinot.routing.HelixExternalViewBasedRouting;
import com.linkedin.pinot.routing.builder.RoutingTableBuilder;
import com.linkedin.pinot.routing.builder.RoutingTableBuilderFactory;


/**
 * Helix Broker Startable
 *
 *
 */
public class HelixBrokerStarter {

  private static final String TABLES_KEY = "tables";
  private static final String PROPERTY_STORE = "PROPERTYSTORE";

  private final HelixManager _helixManager;
  private final HelixAdmin _helixAdmin;
  private final ZkClient _zkClient;
  private final Configuration _pinotHelixProperties;
  private final HelixBrokerRoutingTable _helixBrokerRoutingTable;
  private final HelixExternalViewBasedRouting _helixExternalViewBasedRouting;
  private final BrokerServerBuilder _brokerServerBuilder;
  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final LiveInstancesChangeListenerImpl _liveInstancesListener;

  private static final Logger LOGGER = LoggerFactory.getLogger("HelixBrokerStarter");

  private static final String DEFAULT_OFFLINE_ROUTING_TABLE_BUILDER_KEY =
      "pinot.broker.routing.table.builder.default.offline";
  private static final String DEFAULT_REALTIME_ROUTING_TABLE_BUILDER_KEY =
      "pinot.broker.routing.table.builder.default.realtime";
  private static final String ROUTING_TABLE_BUILDER_KEY = "pinot.broker.routing.table.builder";

  public HelixBrokerStarter(String helixClusterName, String zkServer, Configuration pinotHelixProperties)
      throws Exception {
    _liveInstancesListener = new LiveInstancesChangeListenerImpl(helixClusterName);

    _pinotHelixProperties = DefaultHelixBrokerConfig.getDefaultBrokerConf(pinotHelixProperties);
    final String brokerId =
        _pinotHelixProperties.getString(
                "instanceId",
                CommonConstants.Helix.PREFIX_OF_BROKER_INSTANCE
                        + NetUtil.getHostAddress()
                        + "_"
                        + _pinotHelixProperties.getInt(CommonConstants.Helix.KEY_OF_BROKER_QUERY_PORT,
                    CommonConstants.Helix.DEFAULT_BROKER_QUERY_PORT));

    _pinotHelixProperties.addProperty("pinot.broker.id", brokerId);
    setupHelixSystemProperties();
    RoutingTableBuilder defaultOfflineRoutingTableBuilder =
        getRoutingTableBuilder(_pinotHelixProperties.subset(DEFAULT_OFFLINE_ROUTING_TABLE_BUILDER_KEY));
    RoutingTableBuilder defaultRealtimeRoutingTableBuilder =
        getRoutingTableBuilder(_pinotHelixProperties.subset(DEFAULT_REALTIME_ROUTING_TABLE_BUILDER_KEY));
    Map<String, RoutingTableBuilder> tableToRoutingTableBuilderMap =
        getTableToRoutingTableBuilderMap(_pinotHelixProperties.subset(ROUTING_TABLE_BUILDER_KEY));

    // Remove all white-spaces from the list of zkServers (if any).
    String zkServers = zkServer.replaceAll("\\s+", "");

    _zkClient =
        new ZkClient(getZkAddressForBroker(zkServers, helixClusterName),
            ZkClient.DEFAULT_SESSION_TIMEOUT, ZkClient.DEFAULT_CONNECTION_TIMEOUT, new ZNRecordSerializer());
    _propertyStore = new ZkHelixPropertyStore<ZNRecord>(new ZkBaseDataAccessor<ZNRecord>(_zkClient), "/", null);
    _helixExternalViewBasedRouting =
        new HelixExternalViewBasedRouting(defaultOfflineRoutingTableBuilder, defaultRealtimeRoutingTableBuilder,
            tableToRoutingTableBuilderMap, _propertyStore);

    // _brokerServerBuilder = startBroker();
    _brokerServerBuilder = startBroker(_pinotHelixProperties);
    _helixManager =
        HelixManagerFactory.getZKHelixManager(helixClusterName, brokerId, InstanceType.PARTICIPANT, zkServers);
    final StateMachineEngine stateMachineEngine = _helixManager.getStateMachineEngine();
    final StateModelFactory<?> stateModelFactory =
        new BrokerResourceOnlineOfflineStateModelFactory(_helixManager, _helixExternalViewBasedRouting);
    stateMachineEngine.registerStateModelFactory(BrokerResourceOnlineOfflineStateModelFactory.getStateModelDef(),
        stateModelFactory);
    _helixManager.connect();
    _helixAdmin = _helixManager.getClusterManagmentTool();
    _helixBrokerRoutingTable = new HelixBrokerRoutingTable(_helixExternalViewBasedRouting, brokerId, _helixManager);
    addInstanceTagIfNeeded(helixClusterName, brokerId);
    _helixManager.addExternalViewChangeListener(_helixBrokerRoutingTable);
    _helixManager.addInstanceConfigChangeListener(_helixBrokerRoutingTable);
    _helixManager.addLiveInstanceChangeListener(_liveInstancesListener);

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
      if (ZKMetadataProvider.getClusterTenantIsolationEnabled(_helixManager.getHelixPropertyStore())) {
        _helixAdmin.addInstanceTag(clusterName, instanceName,
            ControllerTenantNameBuilder.getBrokerTenantNameForTenant(ControllerTenantNameBuilder.DEFAULT_TENANT_NAME));
      } else {
        _helixAdmin.addInstanceTag(clusterName, instanceName, CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE);
      }
    }
  }

  private Map<String, RoutingTableBuilder> getTableToRoutingTableBuilderMap(Configuration routingTableBuilderConfig) {
    if (routingTableBuilderConfig.containsKey(TABLES_KEY)) {
      String[] tables = routingTableBuilderConfig.getStringArray(TABLES_KEY);
      if ((tables != null) && (tables.length > 0)) {
        Map<String, RoutingTableBuilder> routingTableBuilderMap = new HashMap<String, RoutingTableBuilder>();
        for (String table : tables) {
          RoutingTableBuilder routingTableBuilder = getRoutingTableBuilder(routingTableBuilderConfig.subset(table));
          if (routingTableBuilder == null) {
            LOGGER.error("RoutingTableBuilder is null for table : " + table);
          } else {
            routingTableBuilderMap.put(table, routingTableBuilder);
          }
        }
        return routingTableBuilderMap;
      }
    }
    return null;
  }

  private RoutingTableBuilder getRoutingTableBuilder(Configuration routingTableBuilderConfig) {
    RoutingTableBuilder routingTableBuilder = null;
    try {
      String routingTableBuilderKey = routingTableBuilderConfig.getString("class", null);
      if (routingTableBuilderKey != null) {
        routingTableBuilder = RoutingTableBuilderFactory.get(routingTableBuilderKey);
        routingTableBuilder.init(routingTableBuilderConfig);
      }
    } catch (Exception e) {
      LOGGER.warn("Caught exception while building routing table", e);
    }
    return routingTableBuilder;
  }

  private BrokerServerBuilder startBroker(Configuration config) throws Exception {
    if (config == null) {
      config = DefaultHelixBrokerConfig.getDefaultBrokerConf();
    }
    final BrokerServerBuilder brokerServerBuilder =
        new BrokerServerBuilder(config, _helixExternalViewBasedRouting,
            _helixExternalViewBasedRouting.getTimeBoundaryService(), _liveInstancesListener);
    brokerServerBuilder.buildNetwork();
    brokerServerBuilder.buildHTTP();
    brokerServerBuilder.start();

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

  private String getZkAddressForBroker(String zkServers, String helixClusterName) {
    List tokens = new ArrayList<String>();

    for (String token : zkServers.split(",")) {
      tokens.add(StringUtil.join("/", StringUtils.chomp(token, "/"), helixClusterName, PROPERTY_STORE));
    }

    return StringUtils.join(tokens, ",");
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
        new HelixBrokerStarter("quickstart", "localhost:2122", configuration);
    return pinotHelixBrokerStarter;
  }

  public void shutdown() {
    LOGGER.info("Shutting down");

    if (_helixManager != null) {
      LOGGER.info("Disconnecting Helix Manager");
      _helixManager.disconnect();
    }

    if (_zkClient != null) {
      LOGGER.info("Closing ZK Client");
      _zkClient.close();
    }
  }

  public static void main(String[] args) throws Exception {
    startDefault();
  }
}
