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

package com.linkedin.pinot.broker.routing.builder;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.helix.model.ExternalView;

import com.linkedin.pinot.broker.routing.RoutingTableLookupRequest;
import com.linkedin.pinot.broker.routing.ServerToSegmentSetMap;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.transport.common.SegmentIdSet;


/**
 * Utility class to share common utility methods between routing table builders.
 */
public abstract class AbstractRoutingTableBuilder implements RoutingTableBuilder {
  
  private List<ServerToSegmentSetMap> _routingTables;

  protected Random _random = new Random();

  //by default we will set it to false. Implementations can override this
  private boolean _isEmpty= false;

  /**
   * Picks a random replica, inversely weighted by the number of segments already assigned to each replica. See a longer
   * description of the probabilities used in
   * {@link KafkaLowLevelConsumerRoutingTableBuilder#computeRoutingTableFromExternalView(String, ExternalView, List)}
   *
   * @param validReplicaSet The list of valid replicas from which to pick a replica
   * @param instanceToSegmentSetMap A map containing the segments already assigned to each replica for routing
   * @param random The random number generator to use
   * @return The instance name of a replica that was chosen
   */
  protected String pickWeightedRandomReplica(Set<String> validReplicaSet,
      Map<String, Set<String>> instanceToSegmentSetMap, Random random) {

    // No replicas?
    if (validReplicaSet.isEmpty()) {
      return null;
    }

    // Only one valid replica?
    if (validReplicaSet.size() == 1) {
      return validReplicaSet.iterator().next();
    }

    // Find maximum segment count assigned to a replica
    String[] replicas = validReplicaSet.toArray(new String[validReplicaSet.size()]);
    int[] replicaSegmentCounts = new int[validReplicaSet.size()];

    int maxSegmentCount = 0;
    for (int i = 0; i < replicas.length; i++) {
      String replica = replicas[i];
      int replicaSegmentCount = 0;

      if (instanceToSegmentSetMap.containsKey(replica)) {
        replicaSegmentCount = instanceToSegmentSetMap.get(replica).size();
      }

      replicaSegmentCounts[i] = replicaSegmentCount;

      if (maxSegmentCount < replicaSegmentCount) {
        maxSegmentCount = replicaSegmentCount;
      }
    }

    // Compute replica weights
    int[] replicaWeights = new int[validReplicaSet.size()];
    int totalReplicaWeights = 0;
    for (int i = 0; i < replicas.length; i++) {
      int replicaWeight = maxSegmentCount - replicaSegmentCounts[i];
      replicaWeights[i] = replicaWeight;
      totalReplicaWeights += replicaWeight;
    }

    // If all replicas are equal, just pick a random replica
    if (totalReplicaWeights == 0) {
      return replicas[random.nextInt(replicas.length)];
    }

    // Pick the proper replica given their respective weights
    int randomValue = random.nextInt(totalReplicaWeights);
    int i = 0;
    while(replicaWeights[i] == 0 || replicaWeights[i] <= randomValue) {
      randomValue -= replicaWeights[i];
      ++i;
    }

    return replicas[i];
  }
  
  @Override
  public Map<ServerInstance, SegmentIdSet> findServers(RoutingTableLookupRequest request) {
    return _routingTables.get(_random.nextInt(_routingTables.size())).getRouting();
  }
  
  @Override
  public List<ServerToSegmentSetMap> getRoutingTables() {
    return _routingTables;
  }
  
  protected void setRoutingTables(List<ServerToSegmentSetMap> routingTables){
    _routingTables = routingTables;
  }

  protected void setIsEmpty(boolean isEmpty){
    _isEmpty = isEmpty;
  }
  
   @Override
  public boolean isPartitionAware() {
    return false;
  }
}
