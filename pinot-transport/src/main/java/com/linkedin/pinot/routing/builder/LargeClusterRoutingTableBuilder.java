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

package com.linkedin.pinot.routing.builder;

import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.routing.ServerToSegmentSetMap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Routing table builder for large offline clusters (over 20-30 servers) that avoids having each request go to every server.
 */
public class LargeClusterRoutingTableBuilder extends AbstractRoutingTableBuilder {
  private static final Logger LOGGER = LoggerFactory.getLogger(LargeClusterRoutingTableBuilder.class);

  /** Number of servers to hit for each query (this is a soft limit, not a hard limit) */
  private int TARGET_SERVER_COUNT_PER_QUERY = 20;

  /** Number of routing tables to keep */
  private static final int ROUTING_TABLE_COUNT = 500;

  /** Number of routing tables to generate during the optimization phase */
  private static final int ROUTING_TABLE_GENERATION_COUNT = 1000;

  private final Random random;

  public LargeClusterRoutingTableBuilder() {
    random = new Random();
  }

  LargeClusterRoutingTableBuilder(Random random) {
    this.random = random;
  }

  private class RoutingTableGenerator {
    private Map<String, Set<String>> segmentToInstanceMap = new HashMap<>();
    private Map<String, Set<String>> instanceToSegmentMap = new HashMap<>();
    private Set<String> segmentsWithAtLeastOneOnlineReplica = new HashSet<>();
    private Set<String> allValidInstancesSet;
    private String[] instanceArray;
    private Map<String, String[]> segmentToInstanceArrayMap;

    private void init(ExternalView externalView, List<InstanceConfig> instanceConfigList) {
      RoutingTableInstancePruner pruner = new RoutingTableInstancePruner(instanceConfigList);

      // Compute the inverse of the external view
      for (String segment : externalView.getPartitionSet()) {
        Set<String> instancesForSegment = new HashSet<>();
        segmentToInstanceMap.put(segment, instancesForSegment);

        for (Map.Entry<String, String> instanceAndState : externalView.getStateMap(segment).entrySet()) {
          String instance = instanceAndState.getKey();
          String state = instanceAndState.getValue();

          // Only consider partitions that are ONLINE
          if (!CommonConstants.Helix.StateModel.SegmentOnlineOfflineStateModel.ONLINE.equals(state)) {
            continue;
          }

          // Skip instances that are disabled
          if (pruner.isInactive(instance)) {
            continue;
          }

          // Add to the instance -> segments map
          Set<String> segmentsForInstance = instanceToSegmentMap.get(instance);

          if (segmentsForInstance == null) {
            segmentsForInstance = new HashSet<>();
            instanceToSegmentMap.put(instance, segmentsForInstance);
          }

          segmentsForInstance.add(segment);

          // Add to the segment -> instances map
          instancesForSegment.add(instance);

          // Add to the valid segments map
          segmentsWithAtLeastOneOnlineReplica.add(segment);
        }
      }

      allValidInstancesSet = instanceToSegmentMap.keySet();
      instanceArray = allValidInstancesSet.toArray(new String[allValidInstancesSet.size()]);

      segmentToInstanceArrayMap = new HashMap<>();
    }

    private Map<String, Set<String>> generateRoutingTable() {
      // List of segments that have no instance serving them
      Set<String> segmentsNotHandledByServers = new HashSet<>(segmentsWithAtLeastOneOnlineReplica);

      // List of servers in this routing table
      Set<String> instancesInRoutingTable = new HashSet<>(TARGET_SERVER_COUNT_PER_QUERY);

      // If there are not enough instances, add them all
      if (instanceArray.length <= TARGET_SERVER_COUNT_PER_QUERY) {
        instancesInRoutingTable.addAll(allValidInstancesSet);
        segmentsNotHandledByServers.clear();
      } else {
        // Otherwise add TARGET_SERVER_COUNT_PER_QUERY instances
        while (instancesInRoutingTable.size() < TARGET_SERVER_COUNT_PER_QUERY) {
          String randomInstance = instanceArray[random.nextInt(instanceArray.length)];
          instancesInRoutingTable.add(randomInstance);
          segmentsNotHandledByServers.removeAll(instanceToSegmentMap.get(randomInstance));
        }
      }

      // If there are segments that have no instance that can serve them, add a server to serve them
      while (!segmentsNotHandledByServers.isEmpty()) {
        // Get the instances in array format
        String segmentNotHandledByServers = segmentsNotHandledByServers.iterator().next();

        String[] instancesArrayForThisSegment = segmentToInstanceArrayMap.get(segmentNotHandledByServers);

        if (instancesArrayForThisSegment == null) {
          Set<String> instanceSet = segmentToInstanceMap.get(segmentNotHandledByServers);
          instancesArrayForThisSegment = instanceSet.toArray(new String[instanceSet.size()]);
          segmentToInstanceArrayMap.put(segmentNotHandledByServers, instancesArrayForThisSegment);
        }

        // Pick a random instance that can serve this segment
        String instance = instancesArrayForThisSegment[random.nextInt(instancesArrayForThisSegment.length)];
        instancesInRoutingTable.add(instance);
        segmentsNotHandledByServers.removeAll(instanceToSegmentMap.get(instance));
      }

      // Sort all the segments to be used during assignment in ascending order of replicas
      int segmentCount = Math.max(segmentsWithAtLeastOneOnlineReplica.size(), 1);
      PriorityQueue<Pair<String, Set<String>>> segmentToReplicaSetQueue = new PriorityQueue<>(segmentCount,
          new Comparator<Pair<String, Set<String>>>() {
            @Override
            public int compare(Pair<String, Set<String>> firstPair, Pair<String, Set<String>> secondPair) {
              return Integer.compare(firstPair.getRight().size(), secondPair.getRight().size());
            }
          });

      for (String segment : segmentsWithAtLeastOneOnlineReplica) {
        // Instances for this segment is the intersection of all instances for this segment and the instances that we
        // have in this routing table
        Set<String> instancesForThisSegment = new HashSet<>(segmentToInstanceMap.get(segment));
        instancesForThisSegment.retainAll(instancesInRoutingTable);

        segmentToReplicaSetQueue.add(new ImmutablePair<>(segment, instancesForThisSegment));
      }

      // Create the routing table from the segment -> instances priority queue
      Map<String, Set<String>> instanceToSegmentSetMap = new HashMap<>();
      int[] replicas = new int[10];
      while(!segmentToReplicaSetQueue.isEmpty()) {
        Pair<String, Set<String>> segmentAndReplicaSet = segmentToReplicaSetQueue.poll();
        String segment = segmentAndReplicaSet.getKey();
        Set<String> replicaSet = segmentAndReplicaSet.getValue();
        replicas[replicaSet.size() - 1]++;

        String instance = pickWeightedRandomReplica(replicaSet, instanceToSegmentSetMap, random);
        if (instance != null) {
          Set<String> segmentsAssignedToInstance = instanceToSegmentSetMap.get(instance);

          if (segmentsAssignedToInstance == null) {
            segmentsAssignedToInstance = new HashSet<>();
            instanceToSegmentSetMap.put(instance, segmentsAssignedToInstance);
          }

          segmentsAssignedToInstance.add(segment);
        } else {
          LOGGER.error("null replica while trying to find replicas for segment {}, this shouldn't happen", segment);
        }
      }

      return instanceToSegmentSetMap;
    }
  }

  @Override
  public void init(Configuration configuration) {
    // TODO jfim This is a broker-level configuration for now, until we refactor the configuration of the routing table to allow per-table routing settings
    if (configuration.containsKey("targetServerCountPerQuery")) {
      final String targetServerCountPerQuery = configuration.getString("targetServerCountPerQuery");
      try {
        TARGET_SERVER_COUNT_PER_QUERY = Integer.parseInt(targetServerCountPerQuery);
      } catch (Exception e) {
        LOGGER.warn(
            "Could not get the target server count per query from configuration value {}, keeping default value {}",
            targetServerCountPerQuery, TARGET_SERVER_COUNT_PER_QUERY, e);
      }
    }
  }

  @Override
  public List<ServerToSegmentSetMap> computeRoutingTableFromExternalView(String tableName, ExternalView externalView,
      List<InstanceConfig> instanceConfigList) {
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
    // end up on a replica with fewer segments.
    //
    // Then, we pick a random replica for each segment, iterating from fewest replicas to most replicas, inversely
    // weighted by the number of segments already assigned to that replica. This ensures that we build a routing table
    // that's as even as possible.
    //
    // The algorithm to generate a routing table is thus:
    // 1. Compute the inverse external view, a mapping of servers to segments
    // 2. For each routing table to generate:
    //   a) Pick TARGET_SERVER_COUNT_PER_QUERY distinct servers
    //   b) Check if the server set covers all the segments; if not, add additional servers until it does.
    //   c) Order the segments in our server set in ascending order of number of replicas present in our server set
    //   d) For each segment, pick a random replica with proper weighting
    //   e) Return that routing table
    //
    // Given that we can generate routing tables at will, we then generate many routing tables and use them to optimize
    // according to two criteria: the variance in workload per server for any individual table as well as the variance
    // in workload per server across all the routing tables. To do so, we generate an initial set of routing tables
    // according to a per-routing table metric and discard the worst routing tables.

    RoutingTableGenerator routingTableGenerator = new RoutingTableGenerator();
    routingTableGenerator.init(externalView, instanceConfigList);

    PriorityQueue<Pair<Map<String, Set<String>>, Float>> topRoutingTables = new PriorityQueue<>(ROUTING_TABLE_COUNT, new Comparator<Pair<Map<String, Set<String>>, Float>>() {
      @Override
      public int compare(Pair<Map<String, Set<String>>, Float> left, Pair<Map<String, Set<String>>, Float> right) {
        // Float.compare sorts in ascending order and we want a max heap, so we need to return the negative of the comparison
        return -Float.compare(left.getValue(), right.getValue());
      }
    });

    for (int i = 0; i < ROUTING_TABLE_COUNT; i++) {
      topRoutingTables.add(generateRoutingTableWithMetric(routingTableGenerator));
    }

    // Generate routing more tables and keep the ROUTING_TABLE_COUNT top ones
    for(int i = 0; i < (ROUTING_TABLE_GENERATION_COUNT - ROUTING_TABLE_COUNT); ++i) {
      Pair<Map<String, Set<String>>, Float> newRoutingTable = generateRoutingTableWithMetric(routingTableGenerator);
      Pair<Map<String, Set<String>>, Float> worstRoutingTable = topRoutingTables.peek();

      // If the new routing table is better than the worst one, keep it
      if (newRoutingTable.getRight() < worstRoutingTable.getRight()) {
        topRoutingTables.poll();
        topRoutingTables.add(newRoutingTable);
      }
    }

    // Return the best routing tables
    List<ServerToSegmentSetMap> routingTables = new ArrayList<>(topRoutingTables.size());
    while(!topRoutingTables.isEmpty()) {
      Pair<Map<String, Set<String>>, Float> routingTableWithMetric = topRoutingTables.poll();
      routingTables.add(new ServerToSegmentSetMap(routingTableWithMetric.getKey()));
    }

    return routingTables;
  }

  private Pair<Map<String, Set<String>>, Float> generateRoutingTableWithMetric(RoutingTableGenerator routingTableGenerator) {
    Map<String, Set<String>> routingTable = routingTableGenerator.generateRoutingTable();
    int segmentCount = 0;
    int serverCount = 0;

    // Compute the number of segments and servers (for the average part of the variance)
    for (Set<String> segmentsForServer : routingTable.values()) {
      int segmentCountForServer = segmentsForServer.size();
      segmentCount += segmentCountForServer;
      serverCount++;
    }

    // Compute the variance of the number of segments allocated per server
    float averageSegmentCount = ((float) segmentCount) / serverCount;
    float variance = 0.0f;
    for (Set<String> segmentsForServer : routingTable.values()) {
      int segmentCountForServer = segmentsForServer.size();
      float difference = segmentCountForServer - averageSegmentCount;
      variance += difference * difference;
    }

    return new ImmutablePair<>(routingTable, variance);
  }
}
