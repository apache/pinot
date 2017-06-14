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

import com.linkedin.pinot.broker.routing.RoutingTableLookupRequest;
import com.linkedin.pinot.broker.routing.ServerToSegmentSetMap;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.transport.common.SegmentIdSet;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Routing table builder that uses a random routing table generator to create multiple routing tables. See a more
 * detailed explanation of the algorithm in {@link KafkaLowLevelConsumerRoutingTableBuilder} and
 * {@link LargeClusterRoutingTableBuilder}.
 */
public abstract class GeneratorBasedRoutingTableBuilder extends AbstractRoutingTableBuilder {
  private static final Logger LOGGER = LoggerFactory.getLogger(GeneratorBasedRoutingTableBuilder.class);

  /** Number of routing tables to keep */
  protected static final int ROUTING_TABLE_COUNT = 500;

  /** Number of routing tables to generate during the optimization phase */
  protected static final int ROUTING_TABLE_GENERATION_COUNT = 1000;

  protected List<ServerToSegmentSetMap> _routingTables = new ArrayList<>();

  protected Random _random = new Random();
  /**
   * Generates a routing table, decorated with a metric.
   *
   * @param routingTableGenerator The routing table generator to use to generate routing tables.
   * @return A pair of a routing table and its associated metric.
   */
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

  interface RoutingTableGenerator {
    void init(ExternalView externalView, List<InstanceConfig> instanceConfigList);
    Map<String, Set<String>> generateRoutingTable();
  }

  abstract class BaseRoutingTableGenerator implements RoutingTableGenerator {
    private final int TARGET_SERVER_COUNT_PER_QUERY;
    private final Random random;

    protected BaseRoutingTableGenerator(int target_server_count_per_query, Random random) {
      TARGET_SERVER_COUNT_PER_QUERY = target_server_count_per_query;
      this.random = random;
    }

    /**
     * Set of segments to assign during routing table generation.
     */
    protected abstract Set<String> getSegmentSet();

    /**
     * Array of instance names to use during assignment.
     */
    protected abstract String[] getInstanceArray();

    /**
     * Set of instances to use during assignment.
     */
    protected abstract Set<String> getInstanceSet();

    /**
     * Map from instance name to segment set.
     */
    protected abstract Map<String,Set<String>> getInstanceToSegmentMap();

    /**
     * Map from segment name to instance name array.
     */
    protected abstract Map<String, String[]> getSegmentToInstanceArrayMap();

    /**
     * Map from segment name to instance set.
     */
    protected abstract Map<String,Set<String>> getSegmentToInstanceMap();

    public Map<String, Set<String>> generateRoutingTable() {
      Set<String> segmentSet = getSegmentSet();
      Set<String> instanceSet = getInstanceSet();
      String[] instanceArray = getInstanceArray();
      Map<String, Set<String>> instanceToSegmentMap = getInstanceToSegmentMap();
      Map<String, String[]> segmentToInstanceArrayMap = getSegmentToInstanceArrayMap();
      Map<String, Set<String>> segmentToInstanceMap = getSegmentToInstanceMap();

      // List of segments that have no instance serving them
      Set<String> segmentsNotHandledByServers = new HashSet<>(segmentSet);

      // List of servers in this routing table
      Set<String> instancesInRoutingTable = new HashSet<>(TARGET_SERVER_COUNT_PER_QUERY);

      // If there are not enough instances, add them all
      if (instanceArray.length <= TARGET_SERVER_COUNT_PER_QUERY) {
        instancesInRoutingTable.addAll(instanceSet);
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
          Set<String> segmentInstanceSet = segmentToInstanceMap.get(segmentNotHandledByServers);
          instancesArrayForThisSegment = segmentInstanceSet.toArray(new String[segmentInstanceSet.size()]);
          segmentToInstanceArrayMap.put(segmentNotHandledByServers, instancesArrayForThisSegment);
        }

        // Pick a random instance that can serve this segment
        String instance = instancesArrayForThisSegment[random.nextInt(instancesArrayForThisSegment.length)];
        instancesInRoutingTable.add(instance);
        segmentsNotHandledByServers.removeAll(instanceToSegmentMap.get(instance));
      }

      // Sort all the segments to be used during assignment in ascending order of replicas
      int segmentCount = Math.max(segmentSet.size(), 1);
      PriorityQueue<Pair<String, Set<String>>> segmentToReplicaSetQueue = new PriorityQueue<>(segmentCount,
          new Comparator<Pair<String, Set<String>>>() {
            @Override
            public int compare(Pair<String, Set<String>> firstPair, Pair<String, Set<String>> secondPair) {
              return Integer.compare(firstPair.getRight().size(), secondPair.getRight().size());
            }
          });

      for (String segment : segmentSet) {
        // Instances for this segment is the intersection of all instances for this segment and the instances that we
        // have in this routing table
        Set<String> instancesForThisSegment = new HashSet<>(segmentToInstanceMap.get(segment));
        instancesForThisSegment.retainAll(instancesInRoutingTable);

        segmentToReplicaSetQueue.add(new ImmutablePair<>(segment, instancesForThisSegment));
      }

      // Create the routing table from the segment -> instances priority queue
      Map<String, Set<String>> instanceToSegmentSetMap = new HashMap<>();
      while(!segmentToReplicaSetQueue.isEmpty()) {
        Pair<String, Set<String>> segmentAndReplicaSet = segmentToReplicaSetQueue.poll();
        String segment = segmentAndReplicaSet.getKey();
        Set<String> replicaSet = segmentAndReplicaSet.getValue();

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

  protected abstract RoutingTableGenerator buildRoutingTableGenerator();

  @Override
  public void computeRoutingTableFromExternalView(String tableName, ExternalView externalView,
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

    RoutingTableGenerator routingTableGenerator = buildRoutingTableGenerator();
    routingTableGenerator.init(externalView, instanceConfigList);

    PriorityQueue<Pair<Map<String, Set<String>>, Float>>
        topRoutingTables = new PriorityQueue<>(ROUTING_TABLE_COUNT, new Comparator<Pair<Map<String, Set<String>>, Float>>() {
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
    _routingTables = routingTables;
  }
  
  @Override
  public Map<ServerInstance, SegmentIdSet> findServers(RoutingTableLookupRequest request) {
    return _routingTables.get(_random.nextInt(_routingTables.size())).getRouting();
  }

  @Override
  public List<ServerToSegmentSetMap> getRoutingTables() {
    return _routingTables;
  }

}
