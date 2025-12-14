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
import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.WatermarkInductionResult;
import org.apache.pinot.controller.helix.core.controllerjob.ControllerJobTypes;
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
  private final WatermarkInductionResult _res;

  public ZkBasedTableReplicationObserver(String jobId, String tableNameWithType, WatermarkInductionResult res,
      PinotHelixResourceManager pinotHelixResourceManager) {
    _jobId = jobId;
    _tableNameWithType = tableNameWithType;
    _res = res;
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _progressStats = new TableReplicationProgressStats(res.getHistoricalSegments().size());
  }

  @Override
  public void onTrigger(Trigger trigger, String segmentName) {
    switch (trigger) {
      // Table
      case START_TRIGGER:
        // in case of zero segments to be copied, track stats in ZK
        trackStatsInZk();
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
    LOGGER.info("[copyTable] Tracking replication stats in ZK for job: {}", _jobId);
    try {
      Map<String, String> jobMetadata = _pinotHelixResourceManager
          .commonTableReplicationJobMetadata(_tableNameWithType, _jobId, System.currentTimeMillis(), _res);
      String progress = JsonUtils.objectToString(_progressStats);
      jobMetadata.put(CommonConstants.ControllerJob.REPLICATION_PROGRESS, progress);
      int remaining = JsonUtils.stringToObject(progress, JsonNode.class).get("remainingSegments").asInt();
      if (remaining == 0) {
        jobMetadata.put(CommonConstants.ControllerJob.REPLICATION_JOB_STATUS, "COMPLETED");
      } else {
        jobMetadata.put(CommonConstants.ControllerJob.REPLICATION_JOB_STATUS, "IN_PROGRESS");
      }
      _pinotHelixResourceManager.addControllerJobToZK(_jobId, jobMetadata, ControllerJobTypes.TABLE_REPLICATION);
    } catch (JsonProcessingException e) {
      LOGGER.error("Error serialising replication stats to JSON for persisting to ZK {}", _jobId, e);
    }
  }
}
