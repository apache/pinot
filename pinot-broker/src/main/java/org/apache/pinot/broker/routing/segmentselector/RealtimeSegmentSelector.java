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
package org.apache.pinot.broker.routing.segmentselector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.helix.model.ExternalView;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.utils.CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel;
import org.apache.pinot.common.utils.HLCSegmentName;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.SegmentName;


/**
 * Segment selector for real-time table which handles the following scenarios:
 * <ul>
 *   <li>When HLC and LLC segments coexist (during LLC migration), select only HLC segments or LLC segments</li>
 *   <li>For HLC segments, only select segments in one group</li>
 *   <li>
 *     For LLC segments, only select the first CONSUMING segment for each partition to avoid duplicate data because in
 *     certain unlikely degenerate scenarios, we can consume overlapping data until segments are flushed (at which point
 *     the overlapping data is discarded during the reconciliation process with the controller).
 *   </li>
 * </ul>
 */
public class RealtimeSegmentSelector implements SegmentSelector {
  public static final String ROUTING_OPTIONS_KEY = "routingOptions";
  public static final String FORCE_HLC = "FORCE_HLC";

  private final AtomicLong _requestId = new AtomicLong();
  private volatile List<List<String>> _hlcSegments;
  private volatile List<String> _llcSegments;

  @Override
  public void init(ExternalView externalView, Set<String> onlineSegments) {
    onExternalViewChange(externalView, onlineSegments);
  }

  @Override
  public void onExternalViewChange(ExternalView externalView, Set<String> onlineSegments) {
    // Group HLC segments by their group id
    // NOTE: Use TreeMap so that group ids are sorted and the result is deterministic
    Map<String, List<String>> groupIdToHLCSegmentsMap = new TreeMap<>();

    List<String> completedLLCSegments = new ArrayList<>();
    // Store the first CONSUMING segment for each partition
    Map<Integer, LLCSegmentName> partitionIdToFirstConsumingLLCSegmentMap = new HashMap<>();

    // Iterate over the external view instead of the online segments so that the map lookups are performed on the
    // HashSet instead of the TreeSet for performance. For LLC segments, we need the external view to figure out whether
    // the segments are in CONSUMING state. For the goal of segment selector, we should not exclude segments not in the
    // external view, but it is okay to exclude them as there is no way to route them without instance states in
    // external view.
    // - New added segment might only exist in ideal state
    // - New removed segment might only exist in external view
    for (Map.Entry<String, Map<String, String>> entry : externalView.getRecord().getMapFields().entrySet()) {
      String segment = entry.getKey();
      if (!onlineSegments.contains(segment)) {
        continue;
      }

      // TODO: for new added segments, before all replicas are up, consider not selecting them to avoid causing
      //       hotspot servers

      Map<String, String> instanceStateMap = entry.getValue();
      if (SegmentName.isHighLevelConsumerSegmentName(segment)) {
        HLCSegmentName hlcSegmentName = new HLCSegmentName(segment);
        groupIdToHLCSegmentsMap.computeIfAbsent(hlcSegmentName.getGroupId(), k -> new ArrayList<>()).add(segment);
      } else {
        if (instanceStateMap.containsValue(RealtimeSegmentOnlineOfflineStateModel.CONSUMING)) {
          // Keep the first CONSUMING segment for each partition
          LLCSegmentName llcSegmentName = new LLCSegmentName(segment);
          partitionIdToFirstConsumingLLCSegmentMap.compute(llcSegmentName.getPartitionId(), (k, consumingSegment) -> {
            if (consumingSegment == null) {
              return llcSegmentName;
            } else {
              if (llcSegmentName.getSequenceNumber() < consumingSegment.getSequenceNumber()) {
                return llcSegmentName;
              } else {
                return consumingSegment;
              }
            }
          });
        } else {
          completedLLCSegments.add(segment);
        }
      }
    }

    int numHLCGroups = groupIdToHLCSegmentsMap.size();
    if (numHLCGroups != 0) {
      List<List<String>> hlcSegments = new ArrayList<>(numHLCGroups);
      for (List<String> hlcSegmentsForGroup : groupIdToHLCSegmentsMap.values()) {
        hlcSegments.add(Collections.unmodifiableList(hlcSegmentsForGroup));
      }
      _hlcSegments = hlcSegments;
    } else {
      _hlcSegments = null;
    }

    if (!completedLLCSegments.isEmpty() || !partitionIdToFirstConsumingLLCSegmentMap.isEmpty()) {
      List<String> llcSegments =
          new ArrayList<>(completedLLCSegments.size() + partitionIdToFirstConsumingLLCSegmentMap.size());
      llcSegments.addAll(completedLLCSegments);
      for (LLCSegmentName llcSegmentName : partitionIdToFirstConsumingLLCSegmentMap.values()) {
        llcSegments.add(llcSegmentName.getSegmentName());
      }
      _llcSegments = Collections.unmodifiableList(llcSegments);
    } else {
      _llcSegments = null;
    }
  }

  @Override
  public List<String> select(BrokerRequest brokerRequest) {
    if (_hlcSegments == null && _llcSegments == null) {
      return Collections.emptyList();
    }
    if (_hlcSegments == null) {
      return selectLLCSegments();
    }
    if (_llcSegments == null) {
      return selectHLCSegments();
    }

    // Handle HLC and LLC coexisting scenario, select HLC segments only if it is forced in the routing options
    Map<String, String> debugOptions = brokerRequest.getDebugOptions();
    if (debugOptions != null) {
      String routingOptions = debugOptions.get(ROUTING_OPTIONS_KEY);
      if (routingOptions != null && routingOptions.toUpperCase().contains(FORCE_HLC)) {
        return selectHLCSegments();
      }
    }
    return selectLLCSegments();
  }

  private List<String> selectHLCSegments() {
    List<List<String>> hlcSegments = _hlcSegments;
    return hlcSegments.get((int) (_requestId.getAndIncrement() % hlcSegments.size()));
  }

  private List<String> selectLLCSegments() {
    return _llcSegments;
  }
}
