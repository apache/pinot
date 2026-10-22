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

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.pinot.broker.routing.adaptiveserverselector.ServerSelectionContext;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;


/// Instance selector for strict replica-group routing strategy.
///
/// The strict replica-group routing strategy always routes same-partition segments to the same instance. During
/// routing state construction, [#updateSegmentMapsForUpsertTable(IdealState, ExternalView, Set, Map)] removes from
/// every segment in a partition any replica that is unavailable for any old segment in that partition. Consequently,
/// all same-partition segments have identical, ordered candidate identities.
///
/// Adaptive routing relies on two parent-selector contracts:
/// - [#selectWithContext(List, int, SegmentStates, ServerSelectionContext)] obtains one ranking snapshot before
///   iterating the query's segments; it must not re-rank per segment.
/// - [#selectServers(List, int, SegmentStates, Map, ServerSelectionContext)] resolves equal-ranked candidates using
///   its strictly-less-than (`rank < bestRank`) comparison. It consequently retains the first candidate in the shared
///   ordered candidate list.
///
/// Since same-partition old segments have identical ordered candidates, these contracts ensure they select the same
/// instance. Changing either contract requires revisiting strict replica-group routing. Different partitions may
/// independently choose different replicas.
///
/// New segments do not exclude a candidate when that segment is unavailable; they remain optional so that the broker or
/// server can skip them if necessary.
public class StrictReplicaGroupInstanceSelector extends ReplicaGroupInstanceSelector {

  @Override
  void updateSegmentMaps(IdealState idealState, ExternalView externalView, Set<String> onlineSegments,
      Map<String, Long> newSegmentCreationTimeMap) {
    super.updateSegmentMapsForUpsertTable(idealState, externalView, onlineSegments, newSegmentCreationTimeMap);
  }

  @Override
  public InstanceMapping select(List<String> segments, int requestId,
      SegmentStates segmentStates, Map<String, String> queryOptions) {
    ServerSelectionContext ctx = new ServerSelectionContext(queryOptions, _config);
    if (_adaptiveServerSelector != null && _priorityPoolInstanceSelector != null) {
      if (ctx.isUseFixedReplica()) {
        // Fixed-replica routing configured on the broker or table disables adaptive routing in InstanceSelectorFactory.
        // This fixed replica config can still be set per query, in which case we'll prefer that over adaptive routing.
        return selectServers(segments, requestId, segmentStates, null, ctx);
      }
      if (QueryOptionsUtils.getNumReplicaGroupsToQuery(ctx.getQueryOptions()) != null) {
        // This option intentionally fans segments across replica groups, so preserve the non-adaptive behavior.
        // This contradicts the behaviour specified by StrictReplicaGroup, so we should eventually reject / ignore
        // this combination.
        return selectServers(segments, requestId, segmentStates, null, ctx);
      }
    }
    return selectWithContext(segments, requestId, segmentStates, ctx);
  }

  // Same-partition old segments share one identical, identically-ordered candidate list by construction
  // (updateSegmentMapsForUpsertTable), so they are either all healthy or all unavailable, and selectServers() picks
  // the same index into that list for every segment of a request. During fail-open, healthy segments keep their
  // normal candidates (selectServers() never reads the fallback candidates), so a healthy partition is never routed
  // onto an unhealthy server, and is routed exactly as in the normal pass under round robin or fixed-replica routing.
  // (With adaptive selection, fail-open falls back to round robin; with numReplicaGroupsToQuery, the now-routed
  // unavailable segments shift the replica offset. Either can move a healthy partition to another healthy server.)
  // This flag is only needed for new segments of an unavailable partition: they have no enabled candidates and aren't
  // counted as unavailable, so tracking their fail-open candidates lets them follow their old siblings onto the same
  // fail-open instance instead of being skipped.
  @Override
  protected boolean shouldTrackFailOpenCandidatesForHealthySegments() {
    return true;
  }
}
