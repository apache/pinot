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
import javax.annotation.Nullable;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.broker.routing.adaptiveserverselector.AdaptiveServerSelector;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.env.PinotConfiguration;
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
      ZkHelixPropertyStore<ZNRecord> propertyStore, BrokerMetrics brokerMetrics, PinotConfiguration config) {
    return getInstanceSelector(tableConfig, propertyStore, brokerMetrics, null, Clock.systemUTC(), config);
  }

  public static InstanceSelector getInstanceSelector(TableConfig tableConfig,
      ZkHelixPropertyStore<ZNRecord> propertyStore, BrokerMetrics brokerMetrics,
      @Nullable AdaptiveServerSelector adaptiveServerSelector, PinotConfiguration brokerConfig) {
    return getInstanceSelector(tableConfig, propertyStore, brokerMetrics, adaptiveServerSelector, Clock.systemUTC(),
        brokerConfig);
  }

  public static InstanceSelector getInstanceSelector(TableConfig tableConfig,
      ZkHelixPropertyStore<ZNRecord> propertyStore, BrokerMetrics brokerMetrics,
      @Nullable AdaptiveServerSelector adaptiveServerSelector, Clock clock, PinotConfiguration brokerConfig) {
    String tableNameWithType = tableConfig.getTableName();
    RoutingConfig routingConfig = tableConfig.getRoutingConfig();
    boolean useConsistentRouting = brokerConfig.getProperty(CommonConstants.Broker.CONFIG_OF_USE_CONSISTENT_ROUTING,
        CommonConstants.Broker.DEFAULT_USE_CONSISTENT_ROUTING);
    if (routingConfig != null) {
      if (RoutingConfig.REPLICA_GROUP_INSTANCE_SELECTOR_TYPE.equalsIgnoreCase(routingConfig.getInstanceSelectorType())
          || (tableConfig.getTableType() == TableType.OFFLINE && LEGACY_REPLICA_GROUP_OFFLINE_ROUTING.equalsIgnoreCase(
          routingConfig.getRoutingTableBuilderName())) || (tableConfig.getTableType() == TableType.REALTIME
          && LEGACY_REPLICA_GROUP_REALTIME_ROUTING.equalsIgnoreCase(routingConfig.getRoutingTableBuilderName()))) {
        LOGGER.info("Using ReplicaGroupInstanceSelector for table: {}", tableNameWithType);
        return new ReplicaGroupInstanceSelector(tableNameWithType, propertyStore, brokerMetrics, adaptiveServerSelector,
            clock, useConsistentRouting);
      }
      if (RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE.equalsIgnoreCase(
          routingConfig.getInstanceSelectorType())) {
        LOGGER.info("Using StrictReplicaGroupInstanceSelector for table: {}", tableNameWithType);
        return new StrictReplicaGroupInstanceSelector(tableNameWithType, propertyStore, brokerMetrics,
            adaptiveServerSelector, clock, useConsistentRouting);
      }
      if (RoutingConfig.MULTI_STAGE_REPLICA_GROUP_SELECTOR_TYPE.equalsIgnoreCase(
          routingConfig.getInstanceSelectorType())) {
        LOGGER.info("Using {} for table: {}", routingConfig.getInstanceSelectorType(), tableNameWithType);
        return new MultiStageReplicaGroupSelector(tableNameWithType, propertyStore, brokerMetrics,
            adaptiveServerSelector, clock, useConsistentRouting);
      }
    }
    return new BalancedInstanceSelector(tableNameWithType, propertyStore, brokerMetrics, adaptiveServerSelector, clock,
        useConsistentRouting);
  }
}
