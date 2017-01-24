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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.LLCSegmentName;
import com.linkedin.pinot.common.utils.SegmentName;
import com.linkedin.pinot.routing.ServerToSegmentSetMap;


/**
 * Routing table builder for the Kafka low level consumer.
 */
public class KafkaLowLevelConsumerRoutingTableBuilder extends GeneratorBasedRoutingTableBuilder {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaLowLevelConsumerRoutingTableBuilder.class);
  private final Random _random = new Random();
  private int TARGET_SERVER_COUNT_PER_QUERY = 8;

  @Override
  public void init(Configuration configuration) {
    // TODO jfim This is a broker-level configuration for now, until we refactor the configuration of the routing table to allow per-table routing settings
    if (configuration.containsKey("realtimeTargetServerCountPerQuery")) {
      final String targetServerCountPerQuery = configuration.getString("realtimeTargetServerCountPerQuery");
      try {
        TARGET_SERVER_COUNT_PER_QUERY = Integer.parseInt(targetServerCountPerQuery);
      } catch (Exception e) {
        LOGGER.warn(
            "Could not get the target server count per query from configuration value {}, keeping default value {}",
            targetServerCountPerQuery, TARGET_SERVER_COUNT_PER_QUERY, e);
      }
    } else {
      LOGGER.info("Using default value for target server count of {}", TARGET_SERVER_COUNT_PER_QUERY);
    }
  }

  @Override
  protected RoutingTableGenerator buildRoutingTableGenerator() {
    return new KafkaLowLevelConsumerRoutingTableGenerator();
  }

  private class KafkaLowLevelConsumerRoutingTableGenerator extends BaseRoutingTableGenerator {
    // We build the routing table based off the external view here. What we want to do is to make sure that we uphold
    // the guarantees clients expect (no duplicate records, eventual consistency) and spreading the load as equally as
    // possible between the servers.
    //
    // Each Kafka partition contains a fraction of the data, so we need to make sure that we query all partitions.
    // Because in certain unlikely degenerate scenarios, we can consume overlapping data until segments are flushed (at
    // which point the overlapping data is discarded during the reconciliation process with the controller), we need to
    // ensure that the query that is sent has only one partition in CONSUMING state in order to avoid duplicate records.
    //
    // Because we also want to want to spread the load as equally as possible between servers, we use a weighted random
    // replica selection that favors picking replicas with fewer segments assigned to them, thus having an approximately
    // equal distribution of load between servers.
    //
    // For example, given three replicas with 1, 2 and 3 segments assigned to each, the replica with one segment should
    // have a weight of 2, which is the maximum segment count minus the segment count for that replica. Thus, each
    // replica other than the replica(s) with the maximum segment count should have a chance of getting a segment
    // assigned to it. This corresponds to alternative three below:
    //
    // Alternative 1 (weight is sum of segment counts - segment count in that replica):
    // (6 - 1) = 5 -> P(0.4166)
    // (6 - 2) = 4 -> P(0.3333)
    // (6 - 3) = 3 -> P(0.2500)
    //
    // Alternative 2 (weight is max of segment counts - segment count in that replica + 1):
    // (3 - 1) + 1 = 3 -> P(0.5000)
    // (3 - 2) + 1 = 2 -> P(0.3333)
    // (3 - 3) + 1 = 1 -> P(0.1666)
    //
    // Alternative 3 (weight is max of segment counts - segment count in that replica):
    // (3 - 1) = 2 -> P(0.6666)
    // (3 - 2) = 1 -> P(0.3333)
    // (3 - 3) = 0 -> P(0.0000)
    //
    // Of those three weighting alternatives, the third one has the smallest standard deviation of the number of
    // segments assigned per replica, so it corresponds to the weighting strategy used for segment assignment. Empirical
    // testing shows that for 20 segments and three replicas, the standard deviation of each alternative is respectively
    // 2.112, 1.496 and 0.853.
    //
    // This algorithm works as follows:
    // 1. Gather all segments and group them by Kafka partition, sorted by sequence number
    // 2. Ensure that for each partition, we have at most one partition in consuming state
    // 3. Sort all the segments to be used during assignment in ascending order of replicas
    // 4. For each segment to be used during assignment, pick a random replica, weighted by the number of segments
    //    assigned to each replica.
    //
    // The upstream code in BaseRoutingTableGenerator will generate routing tables based on taking a subset of servers
    // if the cluster is large enough as well as ensure that the best routing tables are used for routing.

    private Set<String> segmentSet = new HashSet<>();
    private Set<String> instanceSet = new HashSet<>();
    private Map<String, Set<String>> segmentToInstanceMap = new HashMap<>();
    private Map<String, String[]> segmentToInstanceArrayMap = new HashMap<>();
    private Map<String, Set<String>> instanceToSegmentMap = new HashMap<>();
    private String[] instanceArray;

    protected KafkaLowLevelConsumerRoutingTableGenerator() {
      super(TARGET_SERVER_COUNT_PER_QUERY, _random);
    }

    @Override
    public void init(ExternalView externalView, List<InstanceConfig> instanceConfigList) {
      // 1. Gather all segments and group them by Kafka partition, sorted by sequence number
      Map<String, SortedSet<SegmentName>> sortedSegmentsByKafkaPartition = new HashMap<String, SortedSet<SegmentName>>();
      for (String helixPartitionName : externalView.getPartitionSet()) {
        // Ignore segments that are not low level consumer segments
        if (!SegmentName.isLowLevelConsumerSegmentName(helixPartitionName)) {
          continue;
        }

        final LLCSegmentName segmentName = new LLCSegmentName(helixPartitionName);
        String kafkaPartitionName = segmentName.getPartitionRange();
        SortedSet<SegmentName> segmentsForPartition = sortedSegmentsByKafkaPartition.get(kafkaPartitionName);

        // Create sorted set if necessary
        if (segmentsForPartition == null) {
          segmentsForPartition = new TreeSet<SegmentName>();

          sortedSegmentsByKafkaPartition.put(kafkaPartitionName, segmentsForPartition);
        }

        segmentsForPartition.add(segmentName);
      }

      // 2. Ensure that for each Kafka partition, we have at most one Helix partition (Pinot segment) in consuming state
      Map<String, SegmentName> allowedSegmentInConsumingStateByKafkaPartition = new HashMap<String, SegmentName>();
      for (String kafkaPartition : sortedSegmentsByKafkaPartition.keySet()) {
        SortedSet<SegmentName> sortedSegmentsForKafkaPartition = sortedSegmentsByKafkaPartition.get(kafkaPartition);
        SegmentName lastAllowedSegmentInConsumingState = null;

        for (SegmentName segmentName : sortedSegmentsForKafkaPartition) {
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
          allowedSegmentInConsumingStateByKafkaPartition.put(kafkaPartition, lastAllowedSegmentInConsumingState);
        }
      }

      // 3. Sort all the segments to be used during assignment in ascending order of replicas

      // PriorityQueue throws IllegalArgumentException when given a size of zero
      RoutingTableInstancePruner instancePruner = new RoutingTableInstancePruner(instanceConfigList);

      for (Map.Entry<String, SortedSet<SegmentName>> entry : sortedSegmentsByKafkaPartition.entrySet()) {
        String kafkaPartition = entry.getKey();
        SortedSet<SegmentName> segmentNames = entry.getValue();

        // The only segment name which is allowed to be in CONSUMING state or null
        SegmentName validConsumingSegment = allowedSegmentInConsumingStateByKafkaPartition.get(kafkaPartition);

        for (SegmentName segmentName : segmentNames) {
          Set<String> validReplicas = new HashSet<String>();
          String segmentNameStr = segmentName.getSegmentName();
          Map<String, String> externalViewState = externalView.getStateMap(segmentNameStr);

          for (Map.Entry<String, String> instanceAndStateEntry : externalViewState.entrySet()) {
            String instance = instanceAndStateEntry.getKey();
            String state = instanceAndStateEntry.getValue();

            // Skip pruned replicas (shutting down or otherwise disabled)
            if (instancePruner.isInactive(instance)) {
              continue;
            }

            // Replicas in ONLINE state are always allowed
            if (state.equalsIgnoreCase(CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.ONLINE)) {
              validReplicas.add(instance);
              continue;
            }

            // Replicas in CONSUMING state are only allowed on the last segment
            if (state.equalsIgnoreCase(CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.CONSUMING)
                && segmentName.equals(validConsumingSegment)) {
              validReplicas.add(instance);
            }
          }

          if (!validReplicas.isEmpty()) {
            segmentSet.add(segmentNameStr);
            instanceSet.addAll(validReplicas);
            segmentToInstanceMap.put(segmentNameStr, validReplicas);
            segmentToInstanceArrayMap.put(segmentNameStr, validReplicas.toArray(new String[validReplicas.size()]));

            for (String validReplica : validReplicas) {
              Set<String> segmentsForReplica = instanceToSegmentMap.get(validReplica);

              if (segmentsForReplica == null) {
                segmentsForReplica = new HashSet<>();
                instanceToSegmentMap.put(validReplica, segmentsForReplica);
              }

              segmentsForReplica.add(segmentNameStr);
            }
          }

          // If this segment is the segment allowed in CONSUMING state, don't process segments after it in that Kafka
          // partition
          if (segmentName.equals(validConsumingSegment)) {
            break;
          }
        }
      }

      instanceArray = instanceSet.toArray(new String[instanceSet.size()]);
    }

    @Override
    protected Set<String> getSegmentSet() {
      return segmentSet;
    }

    @Override
    protected String[] getInstanceArray() {
      return instanceArray;
    }

    @Override
    protected Set<String> getInstanceSet() {
      return instanceSet;
    }

    @Override
    protected Map<String, Set<String>> getInstanceToSegmentMap() {
      return instanceToSegmentMap;
    }

    @Override
    protected Map<String, String[]> getSegmentToInstanceArrayMap() {
      return segmentToInstanceArrayMap;
    }

    @Override
    protected Map<String, Set<String>> getSegmentToInstanceMap() {
      return segmentToInstanceMap;
    }
  }
}
