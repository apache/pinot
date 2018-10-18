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

import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.SegmentName;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;


/**
 * Util class for routing table builders
 */
public class RoutingTableBuilderUtil {

  private RoutingTableBuilderUtil() {
  }

  /**
   * Compute the map of allowed 'consuming' segments for each partition. This function is used when computing low level
   * kafka routing table.
   *
   * @param externalView helix external view
   * @param sortedSegmentsByKafkaPartition map of Kafka partition to sorted set of segment names.
   * @return map of allowed consuming segment for each partition for routing.
   */
  public static Map<String, SegmentName> getAllowedConsumingStateSegments(ExternalView externalView,
      Map<String, SortedSet<SegmentName>> sortedSegmentsByKafkaPartition) {
    Map<String, SegmentName> allowedSegmentInConsumingStateByKafkaPartition = new HashMap<>();
    for (String kafkaPartition : sortedSegmentsByKafkaPartition.keySet()) {
      SortedSet<SegmentName> sortedSegmentsForKafkaPartition = sortedSegmentsByKafkaPartition.get(kafkaPartition);
      SegmentName lastAllowedSegmentInConsumingState = null;

      for (SegmentName segmentName : sortedSegmentsForKafkaPartition) {
        Map<String, String> helixPartitionState = externalView.getStateMap(segmentName.getSegmentName());
        boolean allInConsumingState = true;
        int replicasInConsumingState = 0;

        // Only keep the segment if all replicas have it in CONSUMING state
        for (String externalViewState : helixPartitionState.values()) {
          // Ignore ERROR state
          if (externalViewState.equalsIgnoreCase(
              CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.ERROR)) {
            continue;
          }

          // Not all segments are in CONSUMING state, therefore don't consider the last segment assignable to CONSUMING
          // replicas
          if (externalViewState.equalsIgnoreCase(
              CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.ONLINE)) {
            allInConsumingState = false;
            break;
          }

          // Otherwise count the replica as being in CONSUMING state
          if (externalViewState.equalsIgnoreCase(
              CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.CONSUMING)) {
            replicasInConsumingState++;
          }
        }

        // If all replicas have this segment in consuming state (and not all of them are in ERROR state), then pick this
        // segment to be the last allowed segment to be in CONSUMING state
        if (allInConsumingState && 0 < replicasInConsumingState) {
          lastAllowedSegmentInConsumingState = segmentName;
          break;
        }
      }

      if (lastAllowedSegmentInConsumingState != null) {
        allowedSegmentInConsumingStateByKafkaPartition.put(kafkaPartition, lastAllowedSegmentInConsumingState);
      }
    }
    return allowedSegmentInConsumingStateByKafkaPartition;
  }

  /**
   * Compute the mapping of segment to servers based on the given external view and a list of instance configs
   *
   * @param externalView an external view
   * @param instanceConfigs a list of instance config
   * @return a mapping of segment to servers
   */
  public static Map<String, List<String>> computeSegmentToServersMapForOfflineTable(ExternalView externalView,
      List<InstanceConfig> instanceConfigs) {
    Map<String, List<String>> segmentToServersMap = new HashMap<>();
    RoutingTableInstancePruner instancePruner = new RoutingTableInstancePruner(instanceConfigs);
    for (String segmentName : externalView.getPartitionSet()) {
      // List of servers that are active and are serving the segment
      List<String> servers = new ArrayList<>();
      for (Map.Entry<String, String> entry : externalView.getStateMap(segmentName).entrySet()) {
        String serverName = entry.getKey();
        if (entry.getValue().equals(CommonConstants.Helix.StateModel.SegmentOnlineOfflineStateModel.ONLINE)
            && !instancePruner.isInactive(serverName)) {
          servers.add(serverName);
        }
      }
      segmentToServersMap.put(segmentName, servers);
    }
    return segmentToServersMap;
  }
}
