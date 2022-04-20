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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.core.util.QueryOptionsUtils;


/**
 * Instance selector for replica-group routing strategy.
 * <p>The selection algorithm will always evenly distribute the traffic to all replicas of each segment, and will select
 * the same index of the enabled instances for all segments with the same number of replicas. The algorithm is very
 * light-weight and will do best effort to select the least servers for the request.
 * <p>The algorithm relies on the mirror segment assignment from replica-group segment assignment strategy. With mirror
 * segment assignment, any server in one replica-group will always have a corresponding server in other replica-groups
 * that have the same segments assigned. For an example, if S1 is a server in replica-group 1, and it has mirror server
 * S2 in replica-group 2 and S3 in replica-group 3. All segments assigned to S1 will also be assigned to S2 and S3. In
 * stable scenario (external view matches ideal state), all segments assigned to S1 will have the same enabled instances
 * of [S1, S2, S3] sorted (in alphabetical order). If we always pick the same index of enabled instances for all
 * segments, only one of S1, S2, S3 will be picked, so it is guaranteed that we pick the least server instances for the
 * request (there is no guarantee on choosing servers from the same replica-group though). In transitioning/error
 * scenario (external view does not match ideal state), there is no guarantee on picking the least server instances, but
 * the traffic is guaranteed to be evenly distributed to all available instances to avoid overwhelming hotspot servers.
 *<p> If the query option NUM_REPLICA_GROUPS_TO_QUERY is provided, the servers to be picked will be from different
 * replica groups such that segments are evenly distributed amongst the provided value of NUM_REPLICA_GROUPS_TO_QUERY.
 * Thus in case of [S1, S2, S3] if NUM_REPLICA_GROUPS_TO_QUERY = 2, the ReplicaGroup S1 and ReplicaGroup S2 will be
 * selected such that half the segments will come from S1 and other half from S2. If NUM_REPLICA_GROUPS_TO_QUERY value
 * is much greater than available servers, then ReplicaGroupInstanceSelector will behave similar to
 * BalancedInstanceSelector.
 */
public class ReplicaGroupInstanceSelector extends BaseInstanceSelector {

  public ReplicaGroupInstanceSelector(String tableNameWithType, BrokerMetrics brokerMetrics) {
    super(tableNameWithType, brokerMetrics);
  }

  @Override
  Map<String, String> select(List<String> segments, int requestId,
      Map<String, List<String>> segmentToEnabledInstancesMap, Map<String, String> queryOptions) {
    Map<String, String> segmentToSelectedInstanceMap = new HashMap<>(HashUtil.getHashMapCapacity(segments.size()));
    int replicaOffset = 0;
    Integer replicaGroup = QueryOptionsUtils.getNumReplicaGroupsToQuery(queryOptions);
    int numReplicaGroupsToQuery = replicaGroup == null ? 1 : replicaGroup;
    for (String segment : segments) {
      List<String> enabledInstances = segmentToEnabledInstancesMap.get(segment);
      // NOTE: enabledInstances can be null when there is no enabled instances for the segment, or the instance selector
      // has not been updated (we update all components for routing in sequence)
      if (enabledInstances != null) {
        int numEnabledInstances = enabledInstances.size();
        int instanceToSelect = (requestId + replicaOffset) % numEnabledInstances;
        segmentToSelectedInstanceMap.put(segment, enabledInstances.get(instanceToSelect));
        if (numReplicaGroupsToQuery > numEnabledInstances) {
          numReplicaGroupsToQuery = numEnabledInstances;
        }
        replicaOffset = (replicaOffset + 1) % numReplicaGroupsToQuery;
      }
    }
    return segmentToSelectedInstanceMap;
  }
}
