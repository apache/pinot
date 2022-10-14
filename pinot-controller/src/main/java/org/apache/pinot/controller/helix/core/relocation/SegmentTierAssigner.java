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
import java.util.Collections;
import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.tier.Tier;
import org.apache.pinot.common.tier.TierSegmentSelector;
import org.apache.pinot.common.utils.config.TierConfigUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.periodictask.ControllerPeriodicTask;
import org.apache.pinot.spi.config.table.TableConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Periodic task to calculate the target tier the segment belongs to and set it into segment ZK metadata as goal
 * state, which can be checked by servers when loading the segment to put it onto the target storage tier.
 */
public class SegmentTierAssigner extends ControllerPeriodicTask<Void> {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentTierAssigner.class);

  public SegmentTierAssigner(PinotHelixResourceManager pinotHelixResourceManager,
      LeadControllerManager leadControllerManager, ControllerConf config, ControllerMetrics controllerMetrics) {
    super(SegmentTierAssigner.class.getSimpleName(), config.getSegmentTierAssignerFrequencyInSeconds(),
        config.getSegmentTierAssignerInitialDelayInSeconds(), pinotHelixResourceManager, leadControllerManager,
        controllerMetrics);
  }

  @Override
  protected void processTable(String tableNameWithType) {
    TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(tableNameWithType);
    Preconditions.checkState(tableConfig != null, "Failed to find table config for table: {}", tableNameWithType);
    List<Tier> sortedTiers;
    if (CollectionUtils.isEmpty(tableConfig.getTierConfigsList())) {
      LOGGER.info("No tierConfigs so use default tier for segments of table: {}", tableNameWithType);
      sortedTiers = Collections.emptyList();
    } else {
      LOGGER.info("Checking and updating target tiers for segments of table: {}", tableNameWithType);
      sortedTiers = TierConfigUtils
          .getSortedTiers(tableConfig.getTierConfigsList(), _pinotHelixResourceManager.getHelixZkManager());
      LOGGER.debug("Sorted tiers: {} configured for table: {}", sortedTiers, tableNameWithType);
    }
    for (String segmentName : _pinotHelixResourceManager.getSegmentsFor(tableNameWithType, true)) {
      updateSegmentTier(tableNameWithType, segmentName, sortedTiers);
    }
  }

  @VisibleForTesting
  void updateSegmentTier(String tableNameWithType, String segmentName, List<Tier> sortedTiers) {
    ZNRecord segmentMetadataZNRecord =
        _pinotHelixResourceManager.getSegmentMetadataZnRecord(tableNameWithType, segmentName);
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
    _pinotHelixResourceManager
        .updateZkMetadata(tableNameWithType, segmentZKMetadata, segmentMetadataZNRecord.getVersion());
  }
}
