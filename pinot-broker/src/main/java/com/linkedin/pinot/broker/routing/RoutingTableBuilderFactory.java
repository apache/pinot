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
package com.linkedin.pinot.broker.routing;

import com.linkedin.pinot.broker.routing.builder.PartitionAwareOfflineRoutingTableBuilder;
import com.linkedin.pinot.broker.routing.builder.PartitionAwareRealtimeRoutingTableBuilder;
import com.linkedin.pinot.common.config.SegmentsValidationAndRetentionConfig;
import com.linkedin.pinot.common.metadata.stream.KafkaStreamMetadata;
import com.linkedin.pinot.common.utils.CommonConstants;
import org.apache.commons.configuration.Configuration;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.broker.routing.builder.BalancedRandomRoutingTableBuilder;
import com.linkedin.pinot.broker.routing.builder.DefaultOfflineRoutingTableBuilder;
import com.linkedin.pinot.broker.routing.builder.DefaultRealtimeRoutingTableBuilder;
import com.linkedin.pinot.broker.routing.builder.KafkaHighLevelConsumerBasedRoutingTableBuilder;
import com.linkedin.pinot.broker.routing.builder.KafkaLowLevelConsumerRoutingTableBuilder;
import com.linkedin.pinot.broker.routing.builder.RoutingTableBuilder;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;


public class RoutingTableBuilderFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(RoutingTableBuilderFactory.class);

  private Configuration _configuration;

  private ZkHelixPropertyStore<ZNRecord> _propertyStore;

  enum RoutingTableBuilderName {
    DefaultOffline,
    DefaultRealtime,
    BalancedRandom,
    KafkaLowLevel,
    KafkaHighLevel,
    PartitionAwareOffline,
    PartitionAwareRealtime
  }

  public RoutingTableBuilderFactory(Configuration configuration, ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _configuration = configuration;
    _propertyStore = propertyStore;
  }

  public RoutingTableBuilder createRoutingTableBuilder(TableConfig tableConfig) {
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
        builder = new KafkaHighLevelConsumerBasedRoutingTableBuilder();
        break;
      case KafkaLowLevel:
        builder = new KafkaLowLevelConsumerRoutingTableBuilder();
        break;
      case PartitionAwareOffline:
        SegmentsValidationAndRetentionConfig validationConfig = tableConfig.getValidationConfig();
        String segmentAssignmentStrategy = validationConfig.getSegmentAssignmentStrategy();

        // Check that the replica group aware segment assignment strategy is used.
        boolean isSegmentAssignmentStrategyCorrect =
            (CommonConstants.Helix.DataSource.SegmentAssignmentStrategyType.valueOf(segmentAssignmentStrategy)
                == CommonConstants.Helix.DataSource.SegmentAssignmentStrategyType.ReplicaGroupSegmentAssignmentStrategy);

        // Check that replica group strategy config is correctly set
        boolean hasReplicaGroupStrategyConfig = (validationConfig != null);

        // Check that the table push type is not 'refresh'.
        boolean isNotRefreshPush = (validationConfig.getSegmentPushType() != null) &&
            !validationConfig.getSegmentPushType().equalsIgnoreCase("REFRESH");

        if (isSegmentAssignmentStrategyCorrect && hasReplicaGroupStrategyConfig && isNotRefreshPush) {
          builder = new PartitionAwareOfflineRoutingTableBuilder();
        } else {
          builder = new DefaultOfflineRoutingTableBuilder();
        }
        break;
      case PartitionAwareRealtime:
        // Check that the table uses LLC kafka consumer.
        KafkaStreamMetadata streamMetadata = new KafkaStreamMetadata(tableConfig.getIndexingConfig().getStreamConfigs());

        if (streamMetadata.getConsumerTypes().size() == 1 && streamMetadata.getConsumerTypes().get(0)
            == CommonConstants.Helix.DataSource.Realtime.Kafka.ConsumerType.simple) {
          builder = new PartitionAwareRealtimeRoutingTableBuilder();
        } else {
          builder = new DefaultRealtimeRoutingTableBuilder();
        }
        break;
    }
    builder.init(_configuration, tableConfig, _propertyStore);
    return builder;
  }
}
