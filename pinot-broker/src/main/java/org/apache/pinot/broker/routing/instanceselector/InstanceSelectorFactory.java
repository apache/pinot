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
package org.apache.pinot.broker.routing.instanceselector;

import com.google.common.annotations.VisibleForTesting;
import java.time.Clock;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.broker.routing.adaptiveserverselector.AdaptiveServerSelector;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class InstanceSelectorFactory {
  private InstanceSelectorFactory() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(InstanceSelectorFactory.class);

  public static final String LEGACY_REPLICA_GROUP_OFFLINE_ROUTING = "PartitionAwareOffline";
  public static final String LEGACY_REPLICA_GROUP_REALTIME_ROUTING = "PartitionAwareRealtime";

  @VisibleForTesting
  public static InstanceSelector getInstanceSelector(TableConfig tableConfig,
      ZkHelixPropertyStore<ZNRecord> propertyStore, BrokerMetrics brokerMetrics, PinotConfiguration brokerConfig,
      Set<String> enabledInstances, Map<String, ServerInstance> enabledServerMap, IdealState idealState,
      ExternalView externalView, Set<String> onlineSegments) {
    return getInstanceSelector(tableConfig, propertyStore, brokerMetrics, null, Clock.systemUTC(), brokerConfig,
        enabledInstances, enabledServerMap, idealState, externalView, onlineSegments);
  }

  public static InstanceSelector getInstanceSelector(TableConfig tableConfig,
      ZkHelixPropertyStore<ZNRecord> propertyStore, BrokerMetrics brokerMetrics,
      @Nullable AdaptiveServerSelector adaptiveServerSelector, PinotConfiguration brokerConfig,
      Set<String> enabledInstances, Map<String, ServerInstance> enabledServerMap, IdealState idealState,
      ExternalView externalView, Set<String> onlineSegments) {
    return getInstanceSelector(tableConfig, propertyStore, brokerMetrics, adaptiveServerSelector, Clock.systemUTC(),
        brokerConfig, enabledInstances, enabledServerMap, idealState, externalView, onlineSegments);
  }

  public static InstanceSelector getInstanceSelector(TableConfig tableConfig,
      ZkHelixPropertyStore<ZNRecord> propertyStore, BrokerMetrics brokerMetrics,
      @Nullable AdaptiveServerSelector adaptiveServerSelector, Clock clock, PinotConfiguration brokerConfig,
      Set<String> enabledInstances, Map<String, ServerInstance> enabledServerMap, IdealState idealState,
      ExternalView externalView, Set<String> onlineSegments) {
    String tableNameWithType = tableConfig.getTableName();
    RoutingConfig routingConfig = tableConfig.getRoutingConfig();
    boolean useFixedReplica = brokerConfig.getProperty(CommonConstants.Broker.CONFIG_OF_USE_FIXED_REPLICA,
        CommonConstants.Broker.DEFAULT_USE_FIXED_REPLICA);
    if (routingConfig != null && routingConfig.getUseFixedReplica() != null) {
      useFixedReplica = routingConfig.getUseFixedReplica();
    }
    long newSegmentExpirationTimeInSeconds =
        brokerConfig.getProperty(CommonConstants.Broker.CONFIG_OF_NEW_SEGMENT_EXPIRATION_SECONDS,
        CommonConstants.Broker.DEFAULT_VALUE_OF_NEW_SEGMENT_EXPIRATION_SECONDS);
    boolean emitSinglePoolSegmentsMetric = brokerConfig.getProperty(
            CommonConstants.Broker.CONFIG_OF_BROKER_ENABLE_SINGLE_POOL_SEGMENTS_METRIC,
            CommonConstants.Broker.DEFAULT_ENABLE_SINGLE_POOL_SEGMENTS_METRIC);
    InstanceSelectorConfig config = new InstanceSelectorConfig(useFixedReplica, newSegmentExpirationTimeInSeconds,
            emitSinglePoolSegmentsMetric);

    InstanceSelector instanceSelector = null;
    if (routingConfig != null) {
      if ((tableConfig.getTableType() == TableType.OFFLINE && LEGACY_REPLICA_GROUP_OFFLINE_ROUTING.equalsIgnoreCase(
          routingConfig.getRoutingTableBuilderName())) || (tableConfig.getTableType() == TableType.REALTIME
          && LEGACY_REPLICA_GROUP_REALTIME_ROUTING.equalsIgnoreCase(routingConfig.getRoutingTableBuilderName()))) {
        LOGGER.info("Using ReplicaGroupInstanceSelector for table: {}", tableNameWithType);
        instanceSelector = new ReplicaGroupInstanceSelector();
      }

      if (routingConfig.getInstanceSelectorType() != null) {
        switch (routingConfig.getInstanceSelectorType()) {
          case RoutingConfig.REPLICA_GROUP_INSTANCE_SELECTOR_TYPE: {
            LOGGER.info("Using ReplicaGroupInstanceSelector for table: {}", tableNameWithType);
            instanceSelector = new ReplicaGroupInstanceSelector();
            break;
          }
          case RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE: {
            LOGGER.info("Using StrictReplicaGroupInstanceSelector for table: {}", tableNameWithType);
            instanceSelector = new StrictReplicaGroupInstanceSelector();
            break;
          }
          case RoutingConfig.MULTI_STAGE_REPLICA_GROUP_SELECTOR_TYPE: {
            LOGGER.info("Using {} for table: {}", routingConfig.getInstanceSelectorType(), tableNameWithType);
            instanceSelector = new MultiStageReplicaGroupSelector();
            break;
          }
          case RoutingConfig.BALANCED_INSTANCE_SELECTOR_TYPE: {
            LOGGER.info("Using BalancedInstanceSelector for table: {}", tableNameWithType);
            instanceSelector = new BalancedInstanceSelector();
            break;
          }
          default: {
            // Try to load custom instance selector
            try {
              instanceSelector = PluginManager.get().createInstance(routingConfig.getInstanceSelectorType());
              break;
            } catch (Exception e) {
              LOGGER.error("Failed to create instance selector for table: {}", tableNameWithType, e);
              throw new RuntimeException(e);
            }
          }
        }
      }
    }

    if (instanceSelector == null) {
      instanceSelector = new BalancedInstanceSelector();
    }

    instanceSelector.init(tableConfig, propertyStore, brokerMetrics, adaptiveServerSelector, clock,
        config, enabledInstances, enabledServerMap, idealState, externalView, onlineSegments);
    return instanceSelector;
  }
}
