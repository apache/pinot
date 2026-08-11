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
/// Adaptive routing preserves that guarantee without explicit partition or mirror-set metadata. The inherited
/// selector takes one ranking snapshot for the query and deterministically chooses the best candidate from each
/// segment's ordered list. Identical filtered candidate lists, the same ranking snapshot, and deterministic list-order
/// tie-breaking therefore produce identical selections for every segment in a partition. Different partitions may
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
        throw new IllegalArgumentException(
            "useFixedReplica cannot be used when adaptive routing is enabled for StrictReplicaGroupInstanceSelector");
      }
      if (QueryOptionsUtils.getNumReplicaGroupsToQuery(ctx.getQueryOptions()) != null) {
        // This option intentionally fans segments across replica groups, so preserve the non-adaptive behavior.
        return selectServers(segments, requestId, segmentStates, null, ctx);
      }
    }
    return selectWithContext(segments, requestId, segmentStates, ctx);
  }
}
