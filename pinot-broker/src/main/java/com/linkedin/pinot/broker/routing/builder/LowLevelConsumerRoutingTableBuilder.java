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
package com.linkedin.pinot.broker.routing.builder;

import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.metrics.BrokerMetrics;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.LLCUtils;
import com.linkedin.pinot.common.utils.SegmentName;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import org.apache.commons.configuration.Configuration;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Routing table builder for the low level consumer.
 */
public class LowLevelConsumerRoutingTableBuilder extends GeneratorBasedRoutingTableBuilder {
  private static final Logger LOGGER = LoggerFactory.getLogger(LowLevelConsumerRoutingTableBuilder.class);

  private int _targetNumServersPerQuery = 8;

  @Override
  public void init(Configuration configuration, TableConfig tableConfig, ZkHelixPropertyStore<ZNRecord> propertyStore,
      BrokerMetrics brokerMetrics) {
    super.init(configuration, tableConfig, propertyStore, brokerMetrics);
    // TODO jfim This is a broker-level configuration for now, until we refactor the configuration of the routing table to allow per-table routing settings
    if (configuration.containsKey("realtimeTargetServerCountPerQuery")) {
      final String targetServerCountPerQuery = configuration.getString("realtimeTargetServerCountPerQuery");
      try {
        _targetNumServersPerQuery = Integer.parseInt(targetServerCountPerQuery);
        LOGGER.info("Using realtime target server count of {}", _targetNumServersPerQuery);
      } catch (Exception e) {
        LOGGER.warn(
            "Could not get the realtime target server count per query from configuration value {}, keeping default value {}",
            targetServerCountPerQuery, _targetNumServersPerQuery, e);
      }
    } else {
      LOGGER.info("Using default value for realtime target server count of {}", _targetNumServersPerQuery);
    }
  }

  @Override
  protected Map<String, List<String>> computeSegmentToServersMapFromExternalView(ExternalView externalView,
      List<InstanceConfig> instanceConfigs) {
    // We build the segment to servers mapping here. What we want to do is to make sure that we uphold
    // the guarantees clients expect (no duplicate records, eventual consistency) and spreading the load as equally as
    // possible between the servers.
    //
    // Each partition contains a fraction of the data, so we need to make sure that we query all partitions.
    // Because in certain unlikely degenerate scenarios, we can consume overlapping data until segments are flushed (at
    // which point the overlapping data is discarded during the reconciliation process with the controller), we need to
    // ensure that the query that is sent has only one partition in CONSUMING state in order to avoid duplicate records.
    //
    // The upstream code in BaseRoutingTableGenerator will generate routing tables based on taking a subset of servers
    // if the cluster is large enough as well as ensure that the best routing tables are used for routing.

    Map<String, List<String>> segmentToServersMap = new HashMap<>();

    // 1. Gather all segments and group them by partition, sorted by sequence number
    Map<String, SortedSet<SegmentName>> sortedSegmentsByStreamPartition =
        LLCUtils.sortSegmentsByStreamPartition(externalView.getPartitionSet());

    // 2. Ensure that for each partition, we have at most one Helix partition (Pinot segment) in consuming state
    Map<String, SegmentName> allowedSegmentInConsumingStateByPartition =
        LowLevelRoutingTableBuilderUtil.getAllowedConsumingStateSegments(externalView,
            sortedSegmentsByStreamPartition);

    // 3. Sort all the segments to be used during assignment in ascending order of replicas

    // PriorityQueue throws IllegalArgumentException when given a size of zero
    RoutingTableInstancePruner instancePruner = new RoutingTableInstancePruner(instanceConfigs);

    for (Map.Entry<String, SortedSet<SegmentName>> entry : sortedSegmentsByStreamPartition.entrySet()) {
      String partitionId = entry.getKey();
      SortedSet<SegmentName> segmentNames = entry.getValue();

      // The only segment name which is allowed to be in CONSUMING state or null
      SegmentName validConsumingSegment = allowedSegmentInConsumingStateByPartition.get(partitionId);

      for (SegmentName segmentName : segmentNames) {
        List<String> validServers = new ArrayList<>();
        String segmentNameStr = segmentName.getSegmentName();
        Map<String, String> externalViewState = externalView.getStateMap(segmentNameStr);

        for (Map.Entry<String, String> instanceAndStateEntry : externalViewState.entrySet()) {
          String instance = instanceAndStateEntry.getKey();
          String state = instanceAndStateEntry.getValue();

          // Skip pruned replicas (shutting down or otherwise disabled)
          if (instancePruner.isInactive(instance)) {
            continue;
          }

          // Replicas in ONLINE state are always allowed
          if (state.equalsIgnoreCase(
              CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.ONLINE)) {
            validServers.add(instance);
            continue;
          }

          // Replicas in CONSUMING state are only allowed on the last segment
          if (state.equalsIgnoreCase(
              CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.CONSUMING)
              && segmentName.equals(validConsumingSegment)) {
            validServers.add(instance);
          }
        }

        if (!validServers.isEmpty()) {
          segmentToServersMap.put(segmentNameStr, validServers);
        } else {
          handleNoServingHost(segmentNameStr);
        }

        // If this segment is the segment allowed in CONSUMING state, don't process segments after it in that partition
        if (segmentName.equals(validConsumingSegment)) {
          break;
        }
      }
    }
    return segmentToServersMap;
  }

  @Override
  int getTargetNumServersPerQuery() {
    return _targetNumServersPerQuery;
  }
}
