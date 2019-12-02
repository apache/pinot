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
package org.apache.pinot.broker.routing;

import org.apache.commons.configuration.Configuration;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.broker.routing.builder.BalancedRandomRoutingTableBuilder;
import org.apache.pinot.broker.routing.builder.DefaultOfflineRoutingTableBuilder;
import org.apache.pinot.broker.routing.builder.DefaultRealtimeRoutingTableBuilder;
import org.apache.pinot.broker.routing.builder.HighLevelConsumerBasedRoutingTableBuilder;
import org.apache.pinot.broker.routing.builder.LowLevelConsumerRoutingTableBuilder;
import org.apache.pinot.broker.routing.builder.PartitionAwareOfflineRoutingTableBuilder;
import org.apache.pinot.broker.routing.builder.PartitionAwareRealtimeRoutingTableBuilder;
import org.apache.pinot.broker.routing.builder.RoutingTableBuilder;
import org.apache.pinot.common.config.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.CommonConstants.Helix.TableType;
import org.apache.pinot.core.realtime.stream.StreamConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RoutingTableBuilderFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(RoutingTableBuilderFactory.class);

  private Configuration _configuration;

  private ZkHelixPropertyStore<ZNRecord> _propertyStore;

  enum RoutingTableBuilderName {
    DefaultOffline,
    DefaultRealtime,
    BalancedRandom,
    // This should ideally be LowLevel and HighLevel. But we cannot rename these, else all tables which reference these
    // in the configs will break. We will keep these prefixed with "Kafka", but they are intended to work for any stream
    KafkaLowLevel,
    KafkaHighLevel,
    PartitionAwareOffline,
    PartitionAwareRealtime
  }

  public RoutingTableBuilderFactory(Configuration configuration, ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _configuration = configuration;
    _propertyStore = propertyStore;
  }

  public RoutingTableBuilder createRoutingTableBuilder(TableConfig tableConfig, BrokerMetrics brokerMetrics) {
    String builderName = null;
    if (tableConfig.getRoutingConfig() != null) {
      builderName = tableConfig.getRoutingConfig().getRoutingTableBuilderName();
    }
    RoutingTableBuilderName buildNameEnum = null;

    if (builderName != null) {
      try {
        buildNameEnum = RoutingTableBuilderName.valueOf(builderName);
      } catch (Exception e) {
        LOGGER.error("Unable to create routing table builder with name:{} for table:{}", builderName,
            tableConfig.getTableName());
        buildNameEnum = null;
      }
    }
    // use appropriate default if builderName is not specified or we fail to recognize the builderName
    if (buildNameEnum == null) {
      if (tableConfig.getTableType() == TableType.OFFLINE) {
        buildNameEnum = RoutingTableBuilderName.DefaultOffline;
      } else if (tableConfig.getTableType() == TableType.REALTIME) {
        buildNameEnum = RoutingTableBuilderName.DefaultRealtime;
      } else {
        buildNameEnum = RoutingTableBuilderName.BalancedRandom;
      }
    }
    RoutingTableBuilder builder = null;
    switch (buildNameEnum) {
      case BalancedRandom:
        builder = new BalancedRandomRoutingTableBuilder();
        break;
      case DefaultOffline:
        builder = new DefaultOfflineRoutingTableBuilder();
        break;
      case DefaultRealtime:
        builder = new DefaultRealtimeRoutingTableBuilder();
        break;
      case KafkaHighLevel:
        builder = new HighLevelConsumerBasedRoutingTableBuilder();
        break;
      case KafkaLowLevel:
        builder = new LowLevelConsumerRoutingTableBuilder();
        break;
      case PartitionAwareOffline:
        SegmentsValidationAndRetentionConfig validationConfig = tableConfig.getValidationConfig();
        String segmentAssignmentStrategy = validationConfig.getSegmentAssignmentStrategy();

        // Check that the replica group aware segment assignment strategy is used.
        boolean isSegmentAssignmentStrategyCorrect =
            (CommonConstants.Helix.DataSource.SegmentAssignmentStrategyType.valueOf(segmentAssignmentStrategy)
                == CommonConstants.Helix.DataSource.SegmentAssignmentStrategyType.ReplicaGroupSegmentAssignmentStrategy);

        // Check that replica group strategy config is correctly set
        boolean hasReplicaGroupStrategyConfig = (validationConfig.getReplicaGroupStrategyConfig() != null);
        if (isSegmentAssignmentStrategyCorrect && hasReplicaGroupStrategyConfig) {
          builder = new PartitionAwareOfflineRoutingTableBuilder();
        } else {
          builder = new DefaultOfflineRoutingTableBuilder();
        }
        break;
      case PartitionAwareRealtime:
        // Check that the table uses LL consumer.
        StreamConfig streamConfig = new StreamConfig(tableConfig.getTableName(),
            tableConfig.getIndexingConfig().getStreamConfigs());

        if (streamConfig.getConsumerTypes().size() == 1 && streamConfig.hasLowLevelConsumerType()) {
          builder = new PartitionAwareRealtimeRoutingTableBuilder();
        } else {
          builder = new DefaultRealtimeRoutingTableBuilder();
        }
        break;
    }

    // TODO: Need to set dynamic routing flag based on table config
    builder.init(_configuration, tableConfig, _propertyStore, brokerMetrics);
    return builder;
  }
}
