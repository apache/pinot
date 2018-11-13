/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.controller.helix.core.relocation;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.MinMaxPriorityQueue;
import com.linkedin.pinot.common.config.RealtimeTagConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.utils.helix.HelixHelper;
import com.linkedin.pinot.common.utils.retry.RetryPolicies;
import com.linkedin.pinot.common.utils.time.TimeUtils;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.PinotHelixSegmentOnlineOfflineStateModelGenerator;
import com.linkedin.pinot.controller.helix.core.periodictask.ControllerPeriodicTask;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.collections.MapUtils;
import org.apache.helix.HelixManager;
import org.apache.helix.model.IdealState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Manager to relocate completed segments to completed servers
 * Segment relocation will be done by this manager, instead of directly moving segments to completed servers on completion,
 * so that we don't get segment downtime when a segment is in transition
 *
 * We only relocate segments for realtime tables, and only if tenant config indicates that relocation is required
 * A segment will be relocated, one replica at a time, once all of its replicas are in ONLINE state and all/some are on servers other than completed servers
 */
public class RealtimeSegmentRelocator extends ControllerPeriodicTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(RealtimeSegmentRelocator.class);

  public RealtimeSegmentRelocator(PinotHelixResourceManager pinotHelixResourceManager, ControllerConf config) {
    super("RealtimeSegmentRelocator", getRunFrequencySeconds(config.getRealtimeSegmentRelocatorFrequency()),
        pinotHelixResourceManager);
  }

  @Override
  public void process(List<String> allTableNames) {
    runRelocation(allTableNames);
  }

  /**
   * Check all tables. Perform relocation of segments if table is realtime and relocation is required
   * TODO: Model this to implement {@link com.linkedin.pinot.controller.helix.core.rebalance.RebalanceSegmentStrategy} interface
   * https://github.com/linkedin/pinot/issues/2609
   * @param allTableNames List of all the table names
   */
  private void runRelocation(List<String> allTableNames) {
    for (final String tableNameWithType : allTableNames) {
      // Only consider realtime tables.
      if (!TableNameBuilder.REALTIME.tableHasTypeSuffix(tableNameWithType)) {
        continue;
      }
      try {
        LOGGER.info("Starting relocation of segments for table: {}", tableNameWithType);

        TableConfig tableConfig = _pinotHelixResourceManager.getRealtimeTableConfig(tableNameWithType);
        final RealtimeTagConfig realtimeTagConfig = new RealtimeTagConfig(tableConfig);
        if (!realtimeTagConfig.isRelocateCompletedSegments()) {
          LOGGER.info("Skipping relocation of segments for {}", tableNameWithType);
          continue;
        }

        Function<IdealState, IdealState> updater = new Function<IdealState, IdealState>() {
          @Nullable
          @Override
          public IdealState apply(@Nullable IdealState idealState) {
            if (!idealState.isEnabled()) {
              LOGGER.info("Skipping relocation of segments for {} since ideal state is disabled", tableNameWithType);
              return null;
            }
            relocateSegments(realtimeTagConfig, idealState);
            return idealState;
          }
        };

        HelixHelper.updateIdealState(_pinotHelixResourceManager.getHelixZkManager(), tableNameWithType, updater,
            RetryPolicies.exponentialBackoffRetryPolicy(5, 1000, 2.0f));
      } catch (Exception e) {
        LOGGER.error("Exception in relocating realtime segments of table {}", tableNameWithType, e);
      }
    }
  }

  /**
   * Given a realtime tag config and an ideal state, relocate the segments
   * which are completed but not yet moved to completed servers, one replica at a time
   * @param  realtimeTagConfig
   * @param idealState
   */
  protected void relocateSegments(RealtimeTagConfig realtimeTagConfig, IdealState idealState) {

    final HelixManager helixManager = _pinotHelixResourceManager.getHelixZkManager();

    final List<String> completedServers = getInstancesWithTag(helixManager, realtimeTagConfig.getCompletedServerTag());
    if (completedServers.isEmpty()) {
      throw new IllegalStateException(
          "Found no realtime completed servers with tag " + realtimeTagConfig.getCompletedServerTag());
    }

    if (completedServers.size() < Integer.valueOf(idealState.getReplicas())) {
      throw new IllegalStateException(
          "Number of completed servers: " + completedServers.size() + " is less than num replicas: "
              + idealState.getReplicas());
    }
    // TODO: use segment assignment strategy to decide where to place relocated segment

    // create map of completed server name to num segments it holds.
    // This will help us decide which completed server to choose for replacing a consuming server
    Map<String, Integer> completedServerToNumSegments = new HashMap<>(completedServers.size());
    completedServers.forEach(server -> completedServerToNumSegments.put(server, 0));

    for (String segmentName : idealState.getPartitionSet()) {
      Map<String, String> instanceStateMap = idealState.getInstanceStateMap(segmentName);
      for (String instance : instanceStateMap.keySet()) {
        if (completedServers.contains(instance)) {
          completedServerToNumSegments.put(instance, completedServerToNumSegments.get(instance) + 1);
        }
      }
    }
    Comparator<Map.Entry<String, Integer>> comparator = Comparator.comparingInt(Map.Entry::getValue);
    MinMaxPriorityQueue<Map.Entry<String, Integer>> completedServersQueue =
        MinMaxPriorityQueue.orderedBy(comparator).maximumSize(completedServers.size()).create();
    completedServersQueue.addAll(completedServerToNumSegments.entrySet());

    // get new mapping for segments that need relocation
    createNewIdealState(realtimeTagConfig, idealState, completedServers, completedServersQueue);
  }

  @VisibleForTesting
  protected List<String> getInstancesWithTag(HelixManager helixManager, String instanceTag) {
    return HelixHelper.getInstancesWithTag(helixManager, instanceTag);
  }

  /**
   * Given an ideal state find the segments that need to relocate a replica to completed servers,
   * and create a new instance state map for those segments
   *
   * @param realtimeTagConfig
   * @param idealState
   * @param completedServers
   * @param completedServersQueue
   * @return
   */
  private void createNewIdealState(final RealtimeTagConfig realtimeTagConfig, IdealState idealState,
      final List<String> completedServers, MinMaxPriorityQueue<Map.Entry<String, Integer>> completedServersQueue) {

    // TODO: we are scanning the entire segments list every time. This is unnecessary because only the latest segments will need relocation
    // Can we do something to avoid this?
    // 1. Time boundary: scan only last day whereas runFrequency = hourly
    // 2. For each partition, scan in descending order, and stop when the first segment not needing relocation is found
    for (String segmentName : idealState.getPartitionSet()) {
      final Map<String, String> instanceStateMap = idealState.getInstanceStateMap(segmentName);
      Map<String, String> newInstanceStateMap =
          createNewInstanceStateMap(realtimeTagConfig, segmentName, instanceStateMap, completedServers,
              completedServersQueue);
      if (MapUtils.isNotEmpty(newInstanceStateMap)) {
        idealState.setInstanceStateMap(segmentName, newInstanceStateMap);
      }
    }
  }

  /**
   * Given the instance state map of a segment, relocate one replica to a completed server if needed
   * Relocation should be done only if all replicas are ONLINE and at least one replica is not on the completed servers
   *
   * @param realtimeTagConfig
   * @param instanceStateMap
   * @param completedServers
   * @param completedServersQueue
   * @return
   */
  private Map<String, String> createNewInstanceStateMap(final RealtimeTagConfig realtimeTagConfig,
      final String segmentName, final Map<String, String> instanceStateMap, final List<String> completedServers,
      MinMaxPriorityQueue<Map.Entry<String, Integer>> completedServersQueue) {

    Map<String, String> newInstanceStateMap = null;

    // proceed only if all segments are ONLINE
    for (String state : instanceStateMap.values()) {
      if (!state.equals(PinotHelixSegmentOnlineOfflineStateModelGenerator.ONLINE_STATE)) {
        return newInstanceStateMap;
      }
    }

    // if an instance is found in the instance state map which is not a completed server,
    // replace the instance with one from the completed servers queue
    for (String instance : instanceStateMap.keySet()) {
      if (!completedServers.contains(instance)) {
        // Decide best strategy to pick completed server.
        // 1. pick random from list of completed servers
        // 2. pick completed server with minimum segments, based on ideal state of this resource
        // 3. pick completed server with minimum segment, based on ideal state of all resources in this tenant
        // 4. use SegmentAssignmentStrategy

        // TODO: Using 2 for now. We should use 4. However the current interface and implementations cannot be used as is.
        // We should refactor the SegmentAssignmentStrategy interface suitably and reuse it here

        Map.Entry<String, Integer> chosenServer = null;
        List<Map.Entry<String, Integer>> polledServers = new ArrayList<>(1);
        while (!completedServersQueue.isEmpty()) {
          Map.Entry<String, Integer> server = completedServersQueue.pollFirst();
          if (instanceStateMap.keySet().contains(server.getKey())) {
            polledServers.add(server);
          } else {
            chosenServer = server;
            break;
          }
        }
        completedServersQueue.addAll(polledServers);
        if (chosenServer == null) {
          throw new IllegalStateException("Could not find server to relocate segment");
        }

        newInstanceStateMap = new HashMap<>(instanceStateMap.size());
        newInstanceStateMap.putAll(instanceStateMap);
        newInstanceStateMap.remove(instance);
        newInstanceStateMap.put(chosenServer.getKey(), PinotHelixSegmentOnlineOfflineStateModelGenerator.ONLINE_STATE);

        chosenServer.setValue(chosenServer.getValue() + 1);
        completedServersQueue.add(chosenServer);
        LOGGER.info("Relocating segment {} from server {} to completed server {} (tag {})", segmentName, instance,
            chosenServer, realtimeTagConfig.getCompletedServerTag());
        break;
      }
    }
    return newInstanceStateMap;
  }

  private static long getRunFrequencySeconds(String timeStr) {
    long seconds;
    try {
      Long millis = TimeUtils.convertPeriodToMillis(timeStr);
      seconds = millis / 1000;
    } catch (Exception e) {
      throw new RuntimeException("Invalid time spec '" + timeStr + "' (Valid examples: '3h', '4h30m', '30m')", e);
    }
    return seconds;
  }
}
