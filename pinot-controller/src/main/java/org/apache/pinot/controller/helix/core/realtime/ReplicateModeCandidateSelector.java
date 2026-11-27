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
package org.apache.pinot.controller.helix.core.realtime;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.assignment.InstancePartitionsUtils;
import org.apache.pinot.common.assignment.PartitionAssignmentUtils;
import org.apache.pinot.common.assignment.ReplicateModeAssignmentUtils;
import org.apache.pinot.common.restlet.resources.DiskUsageInfo;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignmentUtils;
import org.apache.pinot.controller.validation.ResourceUtilizationInfo;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceAssignmentConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.utils.CommonConstants.Helix;


/// Selects a recorded replicate-mode replica from existing placement, liveness and configuration observations.
/// This class has immutable configuration and performs no RPCs or assignment changes. Calls may run concurrently
/// with stable caller-provided snapshots; a selection must be revalidated before a later recovery action executes.
public final class ReplicateModeCandidateSelector {
  private final boolean _checkDiskUtilization;
  private final double _diskUtilizationThreshold;
  private final long _diskUsageMaxAgeMs;

  ReplicateModeCandidateSelector(ControllerConf controllerConf) {
    long resourceCheckFrequencySeconds = controllerConf.getResourceUtilizationCheckerFrequency();
    boolean diskCheckerEnabled = controllerConf.isAllResourceUtilizationCheckersEnabled()
        || controllerConf.isDiskUtilizationCheckerEnabled();
    _checkDiskUtilization = controllerConf.isResourceUtilizationCheckEnabled()
        && diskCheckerEnabled && resourceCheckFrequencySeconds > 0;
    _diskUtilizationThreshold = controllerConf.getDiskUtilizationThreshold();
    _diskUsageMaxAgeMs = TimeUnit.SECONDS.toMillis(resourceCheckFrequencySeconds);
  }

  private ReplicateModeCandidateSelector() {
    _checkDiskUtilization = false;
    _diskUtilizationThreshold = 0;
    _diskUsageMaxAgeMs = 0;
  }

  /// Shares placement, liveness and instance eligibility with manual rebalance. Resource capacity checks remain
  /// with the rebalance pre-checker, rather than introducing a second set of Controller configuration defaults.
  public static List<String> getEligibleServersForRebalance(CandidateContext context, int partitionId,
      Collection<String> affectedSegments, long nowMs) {
    return new ReplicateModeCandidateSelector().select(context, partitionId, affectedSegments, Set.of(), Map.of(),
        null, nowMs).eligibleServers();
  }

  /// A caller-provided observation snapshot. The selector does not mutate its configuration or metadata objects.
  public record CandidateContext(TableConfig tableConfig, IdealState idealState, InstancePartitions instancePartitions,
                          Map<String, InstanceConfig> instanceConfigs, Set<String> liveInstances) {
  }

  /// Eligible candidates are sorted; rejection reasons describe only recorded replicas. Unknown resource usage is
  /// not a resource-health guarantee and does not by itself exclude an otherwise eligible server.
  record Selection(List<String> eligibleServers, @Nullable String selectedServer, Map<String, String> rejectedServers) {
    Selection {
      eligibleServers = List.copyOf(eligibleServers);
      rejectedServers = Map.copyOf(rejectedServers);
    }
  }

  Selection select(CandidateContext context, int partitionId, Collection<String> affectedSegments,
      Set<String> excludedServers, Map<String, Integer> leaderCounts, @Nullable String preferredServer, long nowMs) {
    Set<String> placementInstances = getPlacementInstances(context, partitionId);
    String requiredTag = getRequiredTag(context.tableConfig());
    List<String> eligible = new ArrayList<>();
    Map<String, String> rejected = new TreeMap<>();
    for (String server : new TreeSet<>(ReplicateModeAssignmentUtils.getReplicas(context.idealState(), partitionId))) {
      String rejection = getRejection(context, server, affectedSegments, excludedServers, placementInstances,
          requiredTag, nowMs);
      if (rejection == null) {
        eligible.add(server);
      } else {
        rejected.put(server, rejection);
      }
    }

    String selected = null;
    if (preferredServer != null && eligible.contains(preferredServer)) {
      selected = preferredServer;
    } else if (!eligible.isEmpty()) {
      int startIndex = Math.floorMod(partitionId, eligible.size());
      int minCount = Integer.MAX_VALUE;
      for (int i = 0; i < eligible.size(); i++) {
        String candidate = eligible.get((startIndex + i) % eligible.size());
        int count = leaderCounts.getOrDefault(candidate, 0);
        if (selected == null || count < minCount) {
          selected = candidate;
          minCount = count;
        }
      }
    }
    return new Selection(eligible, selected, rejected);
  }

  private static Set<String> getPlacementInstances(CandidateContext context, int partitionId) {
    if (SegmentAssignmentUtils.isStrictReplicaGroupRouting(context.tableConfig())) {
      List<String> pinnedInstances = PartitionAssignmentUtils.getInstances(context.idealState(), partitionId);
      if (pinnedInstances != null && !pinnedInstances.isEmpty()) {
        // Native pinned assignment remains authoritative until rebalance, even if instance partitions have changed.
        return new HashSet<>(pinnedInstances);
      }
    }
    Set<String> instances = new HashSet<>();
    context.instancePartitions().getPartitionToInstancesMap().values().forEach(instances::addAll);
    return instances;
  }

  @Nullable
  private static String getRequiredTag(TableConfig tableConfig) {
    if (InstancePartitionsUtils.shouldFetchPreConfiguredInstancePartitions(tableConfig,
        InstancePartitionsType.CONSUMING)) {
      // Explicit instance partitions bypass tag-based instance assignment in the native placement path.
      return null;
    }
    Map<String, InstanceAssignmentConfig> assignmentConfigs = tableConfig.getInstanceAssignmentConfigMap();
    InstanceAssignmentConfig consumingConfig =
        assignmentConfigs != null ? assignmentConfigs.get(InstancePartitionsType.CONSUMING.name()) : null;
    return consumingConfig != null
        ? consumingConfig.getTagPoolConfig().getTag()
        : TagNameUtils.extractConsumingServerTag(tableConfig.getTenantConfig());
  }

  @Nullable
  private String getRejection(CandidateContext context, String server, Collection<String> affectedSegments,
      Set<String> excludedServers, Set<String> placementInstances, @Nullable String requiredTag, long nowMs) {
    if (excludedServers.contains(server)) {
      return "Server is excluded from this recovery";
    }
    if (!placementInstances.contains(server)) {
      return "Server is outside the current consuming placement";
    }
    if (!context.liveInstances().contains(server)) {
      return "Server is not live";
    }
    InstanceConfig instanceConfig = context.instanceConfigs().get(server);
    if (instanceConfig == null) {
      return "Server has no instance configuration";
    }
    if (!instanceConfig.getInstanceEnabled()) {
      return "Server is disabled";
    }
    if (instanceConfig.getRecord().getBooleanField(Helix.IS_SHUTDOWN_IN_PROGRESS, false)) {
      return "Server shutdown is in progress";
    }
    if (instanceConfig.getRecord().getBooleanField(Helix.QUERIES_DISABLED, false)) {
      return "Server queries are disabled";
    }
    if (requiredTag != null && !instanceConfig.containsTag(requiredTag)) {
      return "Server does not have the consuming tag: " + requiredTag;
    }
    for (String segment : affectedSegments) {
      if (!instanceConfig.getInstanceEnabledForPartition(context.tableConfig().getTableName(), segment)) {
        return "Server is disabled for segment: " + segment;
      }
    }
    if (_checkDiskUtilization && exceedsDiskUtilizationThreshold(server, nowMs)) {
      return "Server disk utilization exceeds the configured threshold";
    }
    return null;
  }

  private boolean exceedsDiskUtilizationThreshold(String server, long nowMs) {
    DiskUsageInfo usage = ResourceUtilizationInfo.getDiskUsageInfo(server);
    if (usage == null) {
      return false;
    }
    long updated = usage.getLastUpdatedTimeInEpochMs();
    // Missing, invalid and stale samples are unknown. A stopped resource checker cannot supply a fresh observation.
    if (updated <= 0 || updated > nowMs || nowMs - updated > _diskUsageMaxAgeMs || usage.getTotalSpaceBytes() <= 0
        || usage.getUsedSpaceBytes() < 0) {
      return false;
    }
    return usage.getUsedSpaceBytes() > usage.getTotalSpaceBytes() * _diskUtilizationThreshold;
  }
}
