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
public class LargeClusterRoutingTableBuilder extends GeneratorBasedRoutingTableBuilder {
  private static final Logger LOGGER = LoggerFactory.getLogger(LargeClusterRoutingTableBuilder.class);

  /** Number of servers to hit for each query (this is a soft limit, not a hard limit) */
  private int TARGET_SERVER_COUNT_PER_QUERY = 20;

  private final Random random;

  public LargeClusterRoutingTableBuilder() {
    random = new Random();
  }

  LargeClusterRoutingTableBuilder(Random random) {
    this.random = random;
  }

  @Override
  protected RoutingTableGenerator buildRoutingTableGenerator() {
    return new LargeClusterOfflineRoutingTableGenerator();
  }

  private class LargeClusterOfflineRoutingTableGenerator extends BaseRoutingTableGenerator {
    private Map<String, Set<String>> segmentToInstanceMap = new HashMap<>();
    private Map<String, Set<String>> instanceToSegmentMap = new HashMap<>();
    private Set<String> segmentsWithAtLeastOneOnlineReplica = new HashSet<>();
    private Set<String> allValidInstancesSet;
    private String[] instanceArray;
    private Map<String, String[]> segmentToInstanceArrayMap;

    private LargeClusterOfflineRoutingTableGenerator() {
      super(TARGET_SERVER_COUNT_PER_QUERY, random);
    }

    public void init(ExternalView externalView, List<InstanceConfig> instanceConfigList) {
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

    @Override
    protected Set<String> getSegmentSet() {
      return segmentsWithAtLeastOneOnlineReplica;
    }

    @Override
    protected String[] getInstanceArray() {
      return instanceArray;
    }

    @Override
    protected Set<String> getInstanceSet() {
      return allValidInstancesSet;
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

  @Override
  public void init(Configuration configuration) {
    // TODO jfim This is a broker-level configuration for now, until we refactor the configuration of the routing table to allow per-table routing settings
    if (configuration.containsKey("offlineTargetServerCountPerQuery")) {
      final String targetServerCountPerQuery = configuration.getString("offlineTargetServerCountPerQuery");
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
}
