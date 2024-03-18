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
package org.apache.pinot.controller.helix.core.rebalance.tenant;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.metadata.controllerjob.ControllerJobType;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceJobConstants;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ZkBasedTenantRebalanceObserver implements TenantRebalanceObserver {
  private static final Logger LOGGER = LoggerFactory.getLogger(ZkBasedTenantRebalanceObserver.class);

  private final PinotHelixResourceManager _pinotHelixResourceManager;
  private final String _jobId;
  private final String _tenantName;
  private final List<String> _unprocessedTables;
  private final TenantRebalanceProgressStats _progressStats;
  // Keep track of number of updates. Useful during debugging.
  private int _numUpdatesToZk;

  public ZkBasedTenantRebalanceObserver(String jobId, String tenantName, Set<String> tables,
      PinotHelixResourceManager pinotHelixResourceManager) {
    Preconditions.checkState(tables != null && !tables.isEmpty(), "List of tables to observe is empty.");
    _jobId = jobId;
    _tenantName = tenantName;
    _unprocessedTables = new ArrayList<>(tables);
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _progressStats = new TenantRebalanceProgressStats(tables);
    _numUpdatesToZk = 0;
  }

  @Override
  public void onTrigger(Trigger trigger, String tableName, String description) {
    switch (trigger) {
      case START_TRIGGER:
        _progressStats.setStartTimeMs(System.currentTimeMillis());
        break;
      case REBALANCE_STARTED_TRIGGER:
        _progressStats.updateTableStatus(tableName, TenantRebalanceProgressStats.TableStatus.PROCESSING.name());
        _progressStats.putTableRebalanceJobId(tableName, description);
        break;
      case REBALANCE_COMPLETED_TRIGGER:
        _progressStats.updateTableStatus(tableName, TenantRebalanceProgressStats.TableStatus.PROCESSED.name());
        _unprocessedTables.remove(tableName);
        _progressStats.setRemainingTables(_unprocessedTables.size());
        break;
      case REBALANCE_ERRORED_TRIGGER:
        _progressStats.updateTableStatus(tableName, description);
        _unprocessedTables.remove(tableName);
        _progressStats.setRemainingTables(_unprocessedTables.size());
        break;
      default:
    }
    trackStatsInZk();
  }

  @Override
  public void onSuccess(String msg) {
    _progressStats.setCompletionStatusMsg(msg);
    _progressStats.setTimeToFinishInSeconds((System.currentTimeMillis() - _progressStats.getStartTimeMs()) / 1000);
    trackStatsInZk();
  }

  @Override
  public void onError(String errorMsg) {
    _progressStats.setCompletionStatusMsg(errorMsg);
    _progressStats.setTimeToFinishInSeconds(System.currentTimeMillis() - _progressStats.getStartTimeMs());
    trackStatsInZk();
  }

  private void trackStatsInZk() {
    Map<String, String> jobMetadata = new HashMap<>();
    jobMetadata.put(CommonConstants.ControllerJob.TENANT_NAME, _tenantName);
    jobMetadata.put(CommonConstants.ControllerJob.JOB_ID, _jobId);
    jobMetadata.put(CommonConstants.ControllerJob.SUBMISSION_TIME_MS, Long.toString(System.currentTimeMillis()));
    jobMetadata.put(CommonConstants.ControllerJob.JOB_TYPE, ControllerJobType.TENANT_REBALANCE);
    try {
      jobMetadata.put(RebalanceJobConstants.JOB_METADATA_KEY_REBALANCE_PROGRESS_STATS,
          JsonUtils.objectToString(_progressStats));
    } catch (JsonProcessingException e) {
      LOGGER.error("Error serialising rebalance stats to JSON for persisting to ZK {}", _jobId, e);
    }
    _pinotHelixResourceManager.addControllerJobToZK(_jobId, jobMetadata, ControllerJobType.TENANT_REBALANCE);
    _numUpdatesToZk++;
    LOGGER.debug("Number of updates to Zk: {} for rebalanceJob: {}  ", _numUpdatesToZk, _jobId);
  }
}
