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

import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class InstanceSelectorFactory {
  private InstanceSelectorFactory() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(InstanceSelectorFactory.class);

  public static final String LEGACY_REPLICA_GROUP_OFFLINE_ROUTING = "PartitionAwareOffline";
  public static final String LEGACY_REPLICA_GROUP_REALTIME_ROUTING = "PartitionAwareRealtime";

  public static InstanceSelector getInstanceSelector(TableConfig tableConfig, BrokerMetrics brokerMetrics) {
    String tableNameWithType = tableConfig.getTableName();
    RoutingConfig routingConfig = tableConfig.getRoutingConfig();
    if (routingConfig != null) {
      if (RoutingConfig.REPLICA_GROUP_INSTANCE_SELECTOR_TYPE.equalsIgnoreCase(routingConfig.getInstanceSelectorType())
          || (tableConfig.getTableType() == TableType.OFFLINE
              && LEGACY_REPLICA_GROUP_OFFLINE_ROUTING.equalsIgnoreCase(routingConfig.getRoutingTableBuilderName()))
          || (tableConfig.getTableType() == TableType.REALTIME
              && LEGACY_REPLICA_GROUP_REALTIME_ROUTING.equalsIgnoreCase(routingConfig.getRoutingTableBuilderName()))) {
        LOGGER.info("Using ReplicaGroupInstanceSelector for table: {}", tableNameWithType);
        return new ReplicaGroupInstanceSelector(tableNameWithType, brokerMetrics);
      }
      if (RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE
          .equalsIgnoreCase(routingConfig.getInstanceSelectorType())) {
        LOGGER.info("Using StrictReplicaGroupInstanceSelector for table: {}", tableNameWithType);
        return new StrictReplicaGroupInstanceSelector(tableNameWithType, brokerMetrics);
      }
    }
    return new BalancedInstanceSelector(tableNameWithType, brokerMetrics);
  }
}
