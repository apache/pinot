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
package org.apache.pinot.controller.helix.core.relocation;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.helix.ClusterMessagingService;
import org.apache.helix.Criteria;
import org.apache.helix.InstanceType;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.assignment.InstanceAssignmentConfigUtils;
import org.apache.pinot.common.messages.SegmentReloadMessage;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.tier.Tier;
import org.apache.pinot.common.tier.TierSegmentSelector;
import org.apache.pinot.common.utils.config.TierConfigUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.periodictask.ControllerPeriodicTask;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceResult;
import org.apache.pinot.controller.helix.core.rebalance.TableRebalancer;
import org.apache.pinot.controller.util.TableTierReader;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TierConfig;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.apache.pinot.spi.utils.RebalanceConfigConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Periodic task to run rebalancer in background to
 * 1. relocate COMPLETED segments to tag overrides
 * 2. relocate ONLINE segments to tiers if tier configs are set
 * Allow at most one replica unavailable during rebalance. Not applicable for HLC tables.
 */
public class SegmentRelocator extends ControllerPeriodicTask<Void> {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentRelocator.class);

  private final ExecutorService _executorService;
  private final HttpConnectionManager _connectionManager;
  private final boolean _enableLocalTierMigration;
  private final int _timeoutMs;

  public SegmentRelocator(PinotHelixResourceManager pinotHelixResourceManager,
      LeadControllerManager leadControllerManager, ControllerConf config, ControllerMetrics controllerMetrics,
      ExecutorService executorService, HttpConnectionManager connectionManager) {
    super(SegmentRelocator.class.getSimpleName(), config.getSegmentRelocatorFrequencyInSeconds(),
        config.getSegmentRelocatorInitialDelayInSeconds(), pinotHelixResourceManager, leadControllerManager,
        controllerMetrics);
    _executorService = executorService;
    _connectionManager = connectionManager;
    _enableLocalTierMigration = config.enableSegmentRelocatorLocalTierMigration();
    _timeoutMs = config.getServerAdminRequestTimeoutSeconds() * 1000;
  }

  @Override
  protected void processTable(String tableNameWithType) {
    TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(tableNameWithType);
    Preconditions.checkState(tableConfig != null, "Failed to find table config for table: {}", tableNameWithType);

    // Segment relocation doesn't apply to HLC
    boolean isRealtimeTable = TableNameBuilder.isRealtimeTableResource(tableNameWithType);
    if (isRealtimeTable && new StreamConfig(tableNameWithType,
        IngestionConfigUtils.getStreamConfigMap(tableConfig)).hasHighLevelConsumerType()) {
      return;
    }

    boolean relocate = false;
    if (TierConfigUtils.shouldRelocateToTiers(tableConfig)) {
      relocate = true;
      LOGGER.info("Relocating segments to tiers for table: {}", tableNameWithType);
    }
    if (isRealtimeTable && InstanceAssignmentConfigUtils.shouldRelocateCompletedSegments(tableConfig)) {
      relocate = true;
      LOGGER.info("Relocating COMPLETED segments for table: {}", tableNameWithType);
    }
    if (_enableLocalTierMigration) {
      relocate = true;
      LOGGER.info("Migrating segment tiers on servers locally for table: {}", tableNameWithType);
    }
    if (!relocate) {
      LOGGER.debug("No need to relocate segments of table: {}", tableNameWithType);
      return;
    }

    // Allow at most one replica unavailable during relocation
    Configuration rebalanceConfig = new BaseConfiguration();
    rebalanceConfig.addProperty(RebalanceConfigConstants.MIN_REPLICAS_TO_KEEP_UP_FOR_NO_DOWNTIME, -1);
    // Run rebalance asynchronously
    _executorService.submit(() -> {
      try {
        // Relocating segments to new tiers needs two sequential actions: table rebalance and local tier migration.
        // Table rebalance moves segments to the new ideal servers, which can change for a segment when its target
        // tier is updated. New servers can put segments onto the right tier when loading the segments. After that,
        // all segments are put on the right servers. If any segments are not on their target tier, the server local
        // tier migration is triggered for them, basically asking the hosting servers to reload them.
        //
        // We assume segment target tier is not changed between the two actions, so update target tier here as well,
        // instead of using a separate task.
        // TODO: can add some sanity checks on the server side when reloading segments to be more defensive, e.g. only
        //       migrating segment to new tier when the hosting server is in the server pool configured for that tier.
        updateTargetTier(tableNameWithType);

        RebalanceResult rebalance =
            new TableRebalancer(_pinotHelixResourceManager.getHelixZkManager()).rebalance(tableConfig, rebalanceConfig);
        switch (rebalance.getStatus()) {
          case NO_OP:
            LOGGER.info("All segments are already relocated for table: {}", tableNameWithType);
            migrateToTargetTier(tableNameWithType);
            break;
          case DONE:
            LOGGER.info("Finished relocating segments for table: {}", tableNameWithType);
            migrateToTargetTier(tableNameWithType);
            break;
          default:
            LOGGER.error("Relocation failed for table: {}", tableNameWithType);
            break;
        }
      } catch (Throwable t) {
        LOGGER.error("Caught exception/error while rebalancing table: {}", tableNameWithType, t);
      }
    });
  }

  /**
   * Calculate the target tier the segment belongs to and set it in segment ZK metadata as goal state, which can be
   * checked by servers when loading the segment to put it onto the target storage tier.
   */
  private void updateTargetTier(String tableNameWithType) {
    if (!_enableLocalTierMigration) {
      LOGGER.debug("Skipping updating target tier for segments of table: {}", tableNameWithType);
      return;
    }
    TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(tableNameWithType);
    Preconditions.checkState(tableConfig != null, "Failed to find table config for table: {}", tableNameWithType);
    List<TierConfig> tierCfgs = tableConfig.getTierConfigsList();
    List<Tier> sortedTiers = tierCfgs == null ? Collections.emptyList()
        : TierConfigUtils.getSortedTiers(tierCfgs, _pinotHelixResourceManager.getHelixZkManager());
    LOGGER.info("Updating target tiers for segments of table: {} with tierConfigs: {}", tableNameWithType, sortedTiers);
    for (String segmentName : _pinotHelixResourceManager.getSegmentsFor(tableNameWithType, true)) {
      updateSegmentTargetTier(tableNameWithType, segmentName, sortedTiers, _pinotHelixResourceManager);
    }
  }

  /**
   * Migrate segment tiers on their hosting servers locally. Once table is balanced, i.e. segments are on their ideal
   * servers, we check if any segment needs to move to a new tier on its hosting servers, i.e. doing local tier
   * migration for the segments.
   */
  private void migrateToTargetTier(String tableNameWithType) {
    if (!_enableLocalTierMigration) {
      LOGGER.debug("Skipping migrating segments of table: {} to new tiers on hosting servers", tableNameWithType);
      return;
    }
    LOGGER.info("Migrating segments of table: {} to new tiers on hosting servers", tableNameWithType);
    try {
      TableTierReader.TableTierDetails tableTiers =
          new TableTierReader(_executorService, _connectionManager, _pinotHelixResourceManager).getTableTierDetails(
              tableNameWithType, null, _timeoutMs);
      triggerLocalTierMigration(tableNameWithType, tableTiers,
          _pinotHelixResourceManager.getHelixZkManager().getMessagingService());
      LOGGER.info("Migrated segments of table: {} to new tiers on hosting servers", tableNameWithType);
    } catch (Exception e) {
      LOGGER.error("Failed to migrate segments of table: {} to new tiers on hosting servers", tableNameWithType, e);
    }
  }

  @VisibleForTesting
  static void updateSegmentTargetTier(String tableNameWithType, String segmentName, List<Tier> sortedTiers,
      PinotHelixResourceManager pinotHelixResourceManager) {
    ZNRecord segmentMetadataZNRecord =
        pinotHelixResourceManager.getSegmentMetadataZnRecord(tableNameWithType, segmentName);
    if (segmentMetadataZNRecord == null) {
      LOGGER.debug("No ZK metadata for segment: {} of table: {}", segmentName, tableNameWithType);
      return;
    }
    Tier targetTier = null;
    for (Tier tier : sortedTiers) {
      TierSegmentSelector tierSegmentSelector = tier.getSegmentSelector();
      if (tierSegmentSelector.selectSegment(tableNameWithType, segmentName)) {
        targetTier = tier;
        break;
      }
    }
    SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(segmentMetadataZNRecord);
    String targetTierName = null;
    if (targetTier == null) {
      if (segmentZKMetadata.getTier() == null) {
        LOGGER.debug("Segment: {} of table: {} is already on the default tier", segmentName, tableNameWithType);
        return;
      }
      LOGGER.info("Segment: {} of table: {} is put back on default tier", segmentName, tableNameWithType);
    } else {
      targetTierName = targetTier.getName();
      if (targetTierName.equals(segmentZKMetadata.getTier())) {
        LOGGER.debug("Segment: {} of table: {} is already on the target tier: {}", segmentName, tableNameWithType,
            targetTierName);
        return;
      }
      LOGGER.info("Segment: {} of table: {} is put onto new tier: {}", segmentName, tableNameWithType, targetTierName);
    }
    // Update the tier in segment ZK metadata and write it back to ZK.
    segmentZKMetadata.setTier(targetTierName);
    pinotHelixResourceManager.updateZkMetadata(tableNameWithType, segmentZKMetadata,
        segmentMetadataZNRecord.getVersion());
  }

  @VisibleForTesting
  static void triggerLocalTierMigration(String tableNameWithType, TableTierReader.TableTierDetails tableTiers,
      ClusterMessagingService messagingService) {
    Map<String, Map<String, String>> currentTiers = tableTiers.getSegmentCurrentTiers();
    Map<String, String> targetTiers = tableTiers.getSegmentTargetTiers();
    LOGGER.debug("Got segment current tiers: {} and target tiers: {}", currentTiers, targetTiers);
    Map<String, Set<String>> serverToSegmentsToMigrate = new HashMap<>();
    for (Map.Entry<String, Map<String, String>> segmentTiers : currentTiers.entrySet()) {
      String segmentName = segmentTiers.getKey();
      Map<String, String> serverToCurrentTiers = segmentTiers.getValue();
      String targetTier = targetTiers.get(segmentName);
      for (Map.Entry<String, String> serverTier : serverToCurrentTiers.entrySet()) {
        String tier = serverTier.getValue();
        String server = serverTier.getKey();
        if ((tier == null && targetTier == null) || (tier != null && tier.equals(targetTier))) {
          LOGGER.debug("Segment: {} is already on the target tier: {} on server: {}", segmentName,
              TierConfigUtils.normalizeTierName(tier), server);
        } else {
          LOGGER.debug("Segment: {} needs to move from current tier: {} to target tier: {} on server: {}", segmentName,
              TierConfigUtils.normalizeTierName(tier), TierConfigUtils.normalizeTierName(targetTier), server);
          serverToSegmentsToMigrate.computeIfAbsent(server, (s) -> new HashSet<>()).add(segmentName);
        }
      }
    }
    if (serverToSegmentsToMigrate.size() > 0) {
      LOGGER.info("Notify servers: {} to move segments to new tiers locally", serverToSegmentsToMigrate.keySet());
      reloadSegmentsForLocalTierMigration(tableNameWithType, serverToSegmentsToMigrate, messagingService);
    } else {
      LOGGER.info("No server needs to move segments to new tiers locally");
    }
  }

  private static void reloadSegmentsForLocalTierMigration(String tableNameWithType,
      Map<String, Set<String>> serverToSegmentsToMigrate, ClusterMessagingService messagingService) {
    for (Map.Entry<String, Set<String>> entry : serverToSegmentsToMigrate.entrySet()) {
      String serverName = entry.getKey();
      Set<String> segmentNames = entry.getValue();
      // One SegmentReloadMessage per server but takes all segment names.
      Criteria recipientCriteria = new Criteria();
      recipientCriteria.setRecipientInstanceType(InstanceType.PARTICIPANT);
      recipientCriteria.setInstanceName(serverName);
      recipientCriteria.setResource(tableNameWithType);
      recipientCriteria.setSessionSpecific(true);
      SegmentReloadMessage segmentReloadMessage =
          new SegmentReloadMessage(tableNameWithType, new ArrayList<>(segmentNames), false);
      LOGGER.info("Sending SegmentReloadMessage to server: {} to reload segments: {} of table: {}", serverName,
          segmentNames, tableNameWithType);
      int numMessagesSent = messagingService.send(recipientCriteria, segmentReloadMessage, null, -1);
      if (numMessagesSent > 0) {
        LOGGER.info("Sent SegmentReloadMessage to server: {} for table: {}", serverName, tableNameWithType);
      } else {
        LOGGER.warn("No SegmentReloadMessage sent to server: {} for table: {}", serverName, tableNameWithType);
      }
    }
  }
}
