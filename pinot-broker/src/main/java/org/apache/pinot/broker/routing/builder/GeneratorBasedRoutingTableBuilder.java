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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.core.transport.ServerInstance;


/**
 * Routing table builder that uses a random routing table generator to create multiple routing tables. See a more
 * detailed explanation of the algorithm in {@link LowLevelConsumerRoutingTableBuilder} and
 * {@link LargeClusterRoutingTableBuilder}.
 */
public abstract class GeneratorBasedRoutingTableBuilder extends BaseRoutingTableBuilder {

  /** Number of routing tables to keep */
  private static final int ROUTING_TABLE_COUNT = 500;

  /** Number of routing tables to generate during the optimization phase */
  private static final int ROUTING_TABLE_GENERATION_COUNT = 1000;

  /**
   * Generates a routing table, decorated with a metric.
   *
   * @return A pair of a routing table and its associated metric.
   */
  private Pair<Map<ServerInstance, List<String>>, Float> generateRoutingTableWithMetric(
      Map<String, List<ServerInstance>> segmentToServersMap) {
    Map<ServerInstance, List<String>> routingTable = generateRoutingTable(segmentToServersMap);
    int segmentCount = 0;
    int serverCount = 0;

    // Compute the number of segments and servers (for the average part of the variance)
    for (List<String> segmentsForServer : routingTable.values()) {
      int segmentCountForServer = segmentsForServer.size();
      segmentCount += segmentCountForServer;
      serverCount++;
    }

    // Compute the variance of the number of segments allocated per server
    float averageSegmentCount = ((float) segmentCount) / serverCount;
    float variance = 0.0f;
    for (List<String> segmentsForServer : routingTable.values()) {
      int segmentCountForServer = segmentsForServer.size();
      float difference = segmentCountForServer - averageSegmentCount;
      variance += difference * difference;
    }

    return new ImmutablePair<>(routingTable, variance);
  }

  private Map<ServerInstance, List<String>> generateRoutingTable(
      Map<String, List<ServerInstance>> segmentToServersMap) {
    if (segmentToServersMap.isEmpty()) {
      return Collections.emptyMap();
    }

    // Construct the map from server to list of segments
    Map<ServerInstance, List<String>> serverToSegmentsMap = new HashMap<>();
    for (Map.Entry<String, List<ServerInstance>> entry : segmentToServersMap.entrySet()) {
      List<ServerInstance> servers = entry.getValue();
      for (ServerInstance serverInstance : servers) {
        serverToSegmentsMap.computeIfAbsent(serverInstance, k -> new ArrayList<>()).add(entry.getKey());
      }
    }

    int numSegments = segmentToServersMap.size();
    List<ServerInstance> servers = new ArrayList<>(serverToSegmentsMap.keySet());
    int numServers = servers.size();

    // Set of segments that have no instance serving them
    Set<String> segmentsNotHandledByServers = new HashSet<>(segmentToServersMap.keySet());

    // Set of servers in this routing table
    int targetNumServersPerQuery = getTargetNumServersPerQuery();
    Set<ServerInstance> serversInRoutingTable = new HashSet<>(targetNumServersPerQuery);

    if (numServers <= targetNumServersPerQuery) {
      // If there are not enough instances, add them all
      serversInRoutingTable.addAll(servers);
      segmentsNotHandledByServers.clear();
    } else {
      // Otherwise add _targetNumServersPerQuery instances
      while (serversInRoutingTable.size() < targetNumServersPerQuery) {
        ServerInstance randomServer = servers.get(_random.nextInt(numServers));
        if (!serversInRoutingTable.contains(randomServer)) {
          serversInRoutingTable.add(randomServer);
          segmentsNotHandledByServers.removeAll(serverToSegmentsMap.get(randomServer));
        }
      }
    }

    // If there are segments that have no instance that can serve them, add a server to serve them
    while (!segmentsNotHandledByServers.isEmpty()) {
      String segmentNotHandledByServers = segmentsNotHandledByServers.iterator().next();

      // Pick a random server that can serve this segment
      List<ServerInstance> serversForSegment = segmentToServersMap.get(segmentNotHandledByServers);
      ServerInstance randomServer = serversForSegment.get(_random.nextInt(serversForSegment.size()));
      serversInRoutingTable.add(randomServer);
      segmentsNotHandledByServers.removeAll(serverToSegmentsMap.get(randomServer));
    }

    // Sort all the segments to be used during assignment in ascending order of replicas
    PriorityQueue<Pair<String, List<ServerInstance>>> segmentToReplicaSetQueue =
        new PriorityQueue<>(numSegments, Comparator.comparingInt(pair -> pair.getRight().size()));

    for (Map.Entry<String, List<ServerInstance>> entry : segmentToServersMap.entrySet()) {
      // Servers for the segment is the intersection of all servers for this segment and the servers that we have in
      // this routing table
      List<ServerInstance> serversForSegment = new ArrayList<>(entry.getValue());
      serversForSegment.retainAll(serversInRoutingTable);

      segmentToReplicaSetQueue.add(new ImmutablePair<>(entry.getKey(), serversForSegment));
    }

    // Assign each segment to a server
    Map<ServerInstance, List<String>> routingTable = new HashMap<>();
    Pair<String, List<ServerInstance>> segmentServersPair;
    while ((segmentServersPair = segmentToReplicaSetQueue.poll()) != null) {
      assignSegmentToLeastAssignedServer(segmentServersPair.getLeft(), segmentServersPair.getRight(), routingTable);
    }
    return routingTable;
  }

  /*
    The weighted random selection logic for reference
    This can be used to replace getServerWithLeastSegmentsAssigned()

    private String pickWeightedRandomReplica(Set<String> validReplicaSet,
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
    */

  @Override
  protected List<Map<ServerInstance, List<String>>> computeRoutingTablesFromSegmentToServersMap(
      Map<String, List<ServerInstance>> segmentToServersMap) {
    // The default routing table algorithm tries to balance all available segments across all servers, so that each
    // server is hit on every query. This works fine with small clusters (say less than 20 servers) but for larger
    // clusters, this adds up to significant overhead (one request must be enqueued for each server, processed,
    // returned, deserialized, aggregated, etc.).
    //
    // For large clusters, we want to avoid hitting every server, as this also has an adverse effect on client tail
    // latency. This is due to the fact that a query cannot return until it has received a response from each server,
    // and the greater the number of servers that are hit, the more likely it is that one of the servers will be a
    // straggler (eg. due to contention for query processing threads, GC, etc.). We also want to balance the segments
    // within any given routing table so that each server in the routing table has approximately the same number of
    // segments to process.
    //
    // To do so, we have a routing table generator that generates routing tables by picking a random subset of servers.
    // With this set of servers, we check if the set of segments served by these servers is complete. If the set of
    // segments served does not cover all of the segments, we compute the list of missing segments and pick a random
    // server that serves these missing segments until we have complete coverage of all the segments.
    //
    // We then order the segments in ascending number of replicas within our server set, in order to allocate the
    // segments with fewer replicas first. This ensures that segments that are 'easier' to allocate are more likely to
    // end up on a server with fewer segments.
    //
    // Then, we pick a server with least segments already assigned for each segment. This ensures that we build a
    // routing table that's as even as possible.
    //
    // The algorithm to generate a routing table is thus:
    // 1. Compute the inverse external view, a mapping of servers to segments
    // 2. For each routing table to generate:
    //   a) Pick _targetNumServersPerQuery distinct servers
    //   b) Check if the server set covers all the segments; if not, add additional servers until it does
    //   c) Order the segments in our server set in ascending order of number of replicas present in our server set
    //   d) For each segment, pick a server with least segments already assigned
    //   e) Return that routing table
    //
    // Given that we can generate routing tables at will, we then generate many routing tables and use them to optimize
    // according to two criteria: the variance in workload per server for any individual table as well as the variance
    // in workload per server across all the routing tables. To do so, we generate an initial set of routing tables
    // according to a per-routing table metric and discard the worst routing tables.

    PriorityQueue<Pair<Map<ServerInstance, List<String>>, Float>> topRoutingTables =
        new PriorityQueue<>(ROUTING_TABLE_COUNT, (left, right) -> {
          // Float.compare sorts in ascending order and we want a max heap, so we need to return the negative
          // of the comparison
          return -Float.compare(left.getValue(), right.getValue());
        });

    for (int i = 0; i < ROUTING_TABLE_COUNT; i++) {
      topRoutingTables.add(generateRoutingTableWithMetric(segmentToServersMap));
    }

    // Generate routing more tables and keep the ROUTING_TABLE_COUNT top ones
    for (int i = 0; i < (ROUTING_TABLE_GENERATION_COUNT - ROUTING_TABLE_COUNT); ++i) {
      Pair<Map<ServerInstance, List<String>>, Float> newRoutingTable =
          generateRoutingTableWithMetric(segmentToServersMap);
      Pair<Map<ServerInstance, List<String>>, Float> worstRoutingTable = topRoutingTables.peek();

      // If the new routing table is better than the worst one, keep it
      if (newRoutingTable.getRight() < worstRoutingTable.getRight()) {
        topRoutingTables.poll();
        topRoutingTables.add(newRoutingTable);
      }
    }

    // Return the best routing tables
    List<Map<ServerInstance, List<String>>> routingTables = new ArrayList<>(topRoutingTables.size());
    while (!topRoutingTables.isEmpty()) {
      routingTables.add(topRoutingTables.poll().getKey());
    }

    return routingTables;
  }

  /**
   * Returns the number of target servers per query
   */
  abstract int getTargetNumServersPerQuery();
}
