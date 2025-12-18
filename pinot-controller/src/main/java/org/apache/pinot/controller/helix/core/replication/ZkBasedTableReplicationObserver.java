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

package org.apache.pinot.controller.helix.core.replication;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.controllerjob.ControllerJobTypes;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceJobConstants;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Observes the table replication progress and updates the status in Zookeeper.
 */
public class ZkBasedTableReplicationObserver implements TableReplicationObserver {
  private static final Logger LOGGER = LoggerFactory.getLogger(ZkBasedTableReplicationObserver.class);

  private final PinotHelixResourceManager _pinotHelixResourceManager;
  private final String _jobId;
  private final String _tableNameWithType;
  private final TableReplicationProgressStats _progressStats;
  private final List<String> _segmentsToCopy;

  public ZkBasedTableReplicationObserver(String jobId, String tableNameWithType, List<String> segmentsToCopy,
      PinotHelixResourceManager pinotHelixResourceManager) {
    _jobId = jobId;
    _tableNameWithType = tableNameWithType;
    _segmentsToCopy = segmentsToCopy;
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _progressStats = new TableReplicationProgressStats(segmentsToCopy.size());
  }

  @Override
  public void onTrigger(Trigger trigger, String segmentName) {
    switch (trigger) {
      // Table
      case START_TRIGGER:
        break;
      case SEGMENT_REPLICATE_COMPLETED_TRIGGER:
        // Update progress stats and track in ZK every 100 segments
        int remaining = _progressStats.updateSegmentStatus(segmentName,
            TableReplicationProgressStats.SegmentStatus.COMPLETED);
        if (remaining % 100 == 0) {
          trackStatsInZk();
        }
        break;
      case SEGMENT_REPLICATE_ERRORED_TRIGGER:
        // Update progress stats and track in ZK immediately on error
        _progressStats.updateSegmentStatus(segmentName, TableReplicationProgressStats.SegmentStatus.ERROR);
        trackStatsInZk();
        break;
      default:
    }
  }

  private void trackStatsInZk() {
    Map<String, String> jobMetadata = new HashMap<>();
    jobMetadata.put(CommonConstants.ControllerJob.JOB_ID, _jobId);
    jobMetadata.put(CommonConstants.ControllerJob.JOB_TYPE, ControllerJobTypes.TABLE_REPLICATION.name());
    jobMetadata.put(CommonConstants.ControllerJob.SUBMISSION_TIME_MS, Long.toString(System.currentTimeMillis()));
    jobMetadata.put(CommonConstants.ControllerJob.TABLE_NAME_WITH_TYPE, _tableNameWithType);
    try {
      jobMetadata.put(CommonConstants.ControllerJob.SEGMENTS_TO_BE_COPIED,
          JsonUtils.objectToString(_segmentsToCopy));
      jobMetadata.put(RebalanceJobConstants.JOB_METADATA_KEY_REBALANCE_PROGRESS_STATS,
          JsonUtils.objectToString(_progressStats));
    } catch (JsonProcessingException e) {
      LOGGER.error("Error serialising replication stats to JSON for persisting to ZK {}", _jobId, e);
    }
    _pinotHelixResourceManager.addControllerJobToZK(_jobId, jobMetadata, ControllerJobTypes.TABLE_REPLICATION);
  }
}
