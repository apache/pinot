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
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import org.apache.helix.model.ExternalView;


/**
 * Util class for low level routing table builder.
 */
public class LowLevelRoutingTableBuilderUtil {

  /**
   * Compute the map of allowed 'consuming' segments for each partition.
   *
   * @param externalView helix external view
   * @param sortedSegmentsByPartition map of partition to sorted set of segment names.
   * @return map of allowed consuming segment for each partition for routing.
   */
  public static Map<String, SegmentName> getAllowedConsumingStateSegments(ExternalView externalView,
      Map<String, SortedSet<SegmentName>> sortedSegmentsByPartition) {
    Map<String, SegmentName> allowedSegmentInConsumingStateByPartition = new HashMap<>();
    for (String partition : sortedSegmentsByPartition.keySet()) {
      SortedSet<SegmentName> sortedSegmentsForPartition = sortedSegmentsByPartition.get(partition);
      SegmentName lastAllowedSegmentInConsumingState = null;

      for (SegmentName segmentName : sortedSegmentsForPartition) {
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
        allowedSegmentInConsumingStateByPartition.put(partition, lastAllowedSegmentInConsumingState);
      }
    }
    return allowedSegmentInConsumingStateByPartition;
  }
}
