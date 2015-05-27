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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
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

  private final HelixManager _helixManager;
  private final HelixAdmin _helixAdmin;
  private final Configuration _pinotHelixProperties;
  private final HelixBrokerRoutingTable _helixBrokerRoutingTable;
  private final HelixExternalViewBasedRouting _helixExternalViewBasedRouting;
  private final BrokerServerBuilder _brokerServerBuilder;
  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;

  private static final Logger LOGGER = LoggerFactory.getLogger("HelixBrokerStarter");

  private static final String DEFAULT_OFFLINE_ROUTING_TABLE_BUILDER_KEY = "pinot.broker.routing.table.builder.default.offline";
  private static final String DEFAULT_REALTIME_ROUTING_TABLE_BUILDER_KEY = "pinot.broker.routing.table.builder.default.realtime";
  private static final String ROUTING_TABLE_BUILDER_KEY = "pinot.broker.routing.table.builder";

  public HelixBrokerStarter(String helixClusterName, String zkServer, Configuration pinotHelixProperties)
      throws Exception {
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
    RoutingTableBuilder defaultOfflineRoutingTableBuilder =
        getRoutingTableBuilder(_pinotHelixProperties.subset(DEFAULT_OFFLINE_ROUTING_TABLE_BUILDER_KEY));
    RoutingTableBuilder defaultRealtimeRoutingTableBuilder =
        getRoutingTableBuilder(_pinotHelixProperties.subset(DEFAULT_REALTIME_ROUTING_TABLE_BUILDER_KEY));
    Map<String, RoutingTableBuilder> resourceToRoutingTableBuilderMap =
        getResourceToRoutingTableBuilderMap(_pinotHelixProperties.subset(ROUTING_TABLE_BUILDER_KEY));
    ZkClient zkClient =
        new ZkClient(StringUtil.join("/", StringUtils.chomp(zkServer, "/"), helixClusterName, "PROPERTYSTORE"),
            ZkClient.DEFAULT_SESSION_TIMEOUT, ZkClient.DEFAULT_CONNECTION_TIMEOUT, new ZNRecordSerializer());
    _propertyStore = new ZkHelixPropertyStore<ZNRecord>(new ZkBaseDataAccessor<ZNRecord>(zkClient), "/", null);
    _helixExternalViewBasedRouting =
        new HelixExternalViewBasedRouting(defaultOfflineRoutingTableBuilder, defaultRealtimeRoutingTableBuilder, resourceToRoutingTableBuilderMap, _propertyStore);

    // _brokerServerBuilder = startBroker();
    _brokerServerBuilder = startBroker(_pinotHelixProperties);
    _helixManager =
        HelixManagerFactory.getZKHelixManager(helixClusterName, brokerId, InstanceType.PARTICIPANT, zkServer);
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

  }

  private void addInstanceTagIfNeeded(String clusterName, String instanceName) {
    InstanceConfig instanceConfig = _helixAdmin.getInstanceConfig(clusterName, instanceName);
    List<String> instanceTags = instanceConfig.getTags();
    if (instanceTags == null || instanceTags.isEmpty()) {
      if (ZKMetadataProvider.getClusterTenantIsolationEnabled(_helixManager.getHelixPropertyStore())) {
        _helixAdmin.addInstanceTag(clusterName, instanceName, ControllerTenantNameBuilder.getBrokerTenantNameForTenant(ControllerTenantNameBuilder.DEFAULT_TENANT_NAME));
      } else {
        _helixAdmin.addInstanceTag(clusterName, instanceName, CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE);
      }
    }
  }

  private Map<String, RoutingTableBuilder> getResourceToRoutingTableBuilderMap(Configuration routingTableBuilderConfig) {
    String[] resources = routingTableBuilderConfig.getStringArray("resources");
    if ((resources != null) && (resources.length > 0)) {
      Map<String, RoutingTableBuilder> routingTableBuilderMap = new HashMap<String, RoutingTableBuilder>();
      for (String resource : resources) {
        RoutingTableBuilder routingTableBuilder = getRoutingTableBuilder(routingTableBuilderConfig.subset(resource));
        if (routingTableBuilder == null) {
          LOGGER.error("RoutingTableBuilder is null for resource : " + resource);
        } else {
          routingTableBuilderMap.put(resource, routingTableBuilder);
        }
      }
      return routingTableBuilderMap;
    } else {
      return null;
    }
  }

  private RoutingTableBuilder getRoutingTableBuilder(Configuration routingTableBuilderConfig) {
    String routingTableBuilderKey = routingTableBuilderConfig.getString("class", null);
    RoutingTableBuilder routingTableBuilder = RoutingTableBuilderFactory.get(routingTableBuilderKey);
    if (routingTableBuilder == null) {
      return null;
    }
    routingTableBuilder.init(routingTableBuilderConfig);
    return routingTableBuilder;
  }

  private BrokerServerBuilder startBroker(Configuration config) throws Exception {
    if (config == null) {
      config = DefaultHelixBrokerConfig.getDefaultBrokerConf();
    }
    final BrokerServerBuilder brokerServerBuilder = new BrokerServerBuilder(config, _helixExternalViewBasedRouting,
        _helixExternalViewBasedRouting.getTimeBoundaryService());
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

  public HelixExternalViewBasedRouting getHelixExternalViewBasedRouting() {
    return _helixExternalViewBasedRouting;
  }

  public BrokerServerBuilder getBrokerServerBuilder() {
    return _brokerServerBuilder;
  }

  public static void main(String[] args) throws Exception {

    Configuration configuration = new PropertiesConfiguration();
    int port = 5001;
    configuration.addProperty(CommonConstants.Helix.KEY_OF_BROKER_QUERY_PORT, port);
    configuration.addProperty("pinot.broker.time.out", 500 * 1000L);

    final HelixBrokerStarter pinotHelixBrokerStarter =
        new HelixBrokerStarter("pinotControllerV2", "localhost:2121", configuration);
    Thread.sleep(1000);
  }
}
