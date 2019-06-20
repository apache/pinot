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
package org.apache.pinot.broker.routing.builder;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import org.apache.helix.model.ExternalView;
import org.apache.pinot.common.utils.CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel;
import org.apache.pinot.common.utils.SegmentName;


/**
 * Util class for low level routing table builder.
 */
public class LowLevelRoutingTableBuilderUtil {

  /**
   * Compute the map of allowed CONSUMING segments for each partition for routing purpose.
   * <p>Within a partition, we only allow querying the first CONSUMING segment (segment with instances in CONSUMING
   * state) for the following reasons:
   * <ul>
   *   <li>
   *     Within a partition, there could be multiple CONSUMING segments (typically caused by the delay of CONSUMING to
   *     ONLINE state transition). We should only query the first CONSUMING segment because it might contain records
   *     that overlapped with the records in the next segment (over-consumed).
   *   </li>
   *   <li>
   *     If the instance states for a segment are partial CONSUMING (instances can be ONLINE if they have finished
   *     the CONSUMING to ONLINE state transition; instances can be OFFLINE if they encountered error while consuming
   *     and controller set the IdealState to OFFLINE; instances can be ERROR if they encountered error during the state
   *     transition), we count the segment as CONSUMING segment. If we don't count the segment as CONSUMING segment,
   *     then this segment is not allowed to be in the CONSUMING state for routing purpose, and we will not route
   *     queries to this segment if there is no ONLINE instances, or route all queries to the ONLINE instances which can
   *     potentially overwhelm instances.
   *   </li>
   *   <li>
   *     It is possible that the latest CONSUMING segment is not allowed for routing purpose and we won't query it, but
   *     it should only last for a short period of time. Once the older CONSUMING segment becomes ONLINE (all instances
   *     finished the CONSUMING to ONLINE state transition), the latest CONSUMING segment will become the first
   *     CONSUMING segment and will be allowed for routing purpose.
   *   </li>
   * </ul>
   *
   * @param externalView External view for the real-time table
   * @param sortedSegmentsByPartition Map from partition to segments
   * @return Map from partition to allowed CONSUMING segment for routing purpose
   */
  public static Map<String, SegmentName> getAllowedConsumingStateSegments(ExternalView externalView,
      Map<String, SortedSet<SegmentName>> sortedSegmentsByPartition) {
    Map<String, SegmentName> allowedConsumingSegments = new HashMap<>();
    for (Map.Entry<String, SortedSet<SegmentName>> entry : sortedSegmentsByPartition.entrySet()) {
      String partitionId = entry.getKey();
      SortedSet<SegmentName> sortedSegmentsForPartition = entry.getValue();
      for (SegmentName segmentName : sortedSegmentsForPartition) {
        if (externalView.getStateMap(segmentName.getSegmentName())
            .containsValue(RealtimeSegmentOnlineOfflineStateModel.CONSUMING)) {
          allowedConsumingSegments.put(partitionId, segmentName);
          break;
        }
      }
    }
    return allowedConsumingSegments;
  }
}
