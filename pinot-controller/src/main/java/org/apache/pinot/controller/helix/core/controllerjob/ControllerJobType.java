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
package org.apache.pinot.controller.helix.core.controllerjob;

import java.util.EnumMap;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceJobConstants;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceResult;
import org.apache.pinot.controller.helix.core.rebalance.TableRebalanceProgressStats;
import org.apache.pinot.controller.helix.core.rebalance.tenant.TenantRebalanceProgressStats;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * Controller jobs that store metadata in the ZK property store.
 */
public enum ControllerJobType {
  RELOAD_SEGMENT,
  FORCE_COMMIT,
  TABLE_REBALANCE,
  TENANT_REBALANCE;

  private static final EnumMap<ControllerJobType, Integer> ZK_NUM_JOBS_LIMIT = new EnumMap<>(ControllerJobType.class);

  /**
   * Gets the maximum number of job metadata entries that can be stored in ZK for this job type.
   */
  public Integer getZkNumJobsLimit() {
    return ZK_NUM_JOBS_LIMIT.getOrDefault(this, ControllerConf.DEFAULT_MAXIMUM_CONTROLLER_JOBS_IN_ZK);
  }

  public static void init(ControllerConf controllerConf) {
    ZK_NUM_JOBS_LIMIT.put(RELOAD_SEGMENT, controllerConf.getMaxReloadSegmentZkJobs());
    ZK_NUM_JOBS_LIMIT.put(FORCE_COMMIT, controllerConf.getMaxForceCommitZkJobs());
    ZK_NUM_JOBS_LIMIT.put(TABLE_REBALANCE, controllerConf.getMaxTableRebalanceZkJobs());
    ZK_NUM_JOBS_LIMIT.put(TENANT_REBALANCE, controllerConf.getMaxTenantRebalanceZkJobs());
  }

  /**
   * Checks if the job metadata entry can be safely deleted. Note that the job metadata entry will only be attempted
   * to be deleted when the number of entries in the job metadata map exceeds the configured limit for the job type.
   *
   * @param jobMetadataEntry The job metadata entry to check - a pair of job ID and job metadata map
   * @return true if the job metadata entry can be safely deleted, false otherwise
   */
  public boolean canDelete(Pair<String, Map<String, String>> jobMetadataEntry) {
    switch (this) {
      case TABLE_REBALANCE:
        try {
          TableRebalanceProgressStats stats = JsonUtils.stringToObject(
              jobMetadataEntry.getRight().get(RebalanceJobConstants.JOB_METADATA_KEY_REBALANCE_PROGRESS_STATS),
              TableRebalanceProgressStats.class);
          // If the rebalance job is in progress, the job metadata entry should not be deleted even if the number of
          // jobs exceeds the configured limit for the job type.
          // Note that even if a rebalance job gets stuck for some reason (for instance, due to a controller crash),
          // the RebalanceChecker periodic controller job will make sure that the rebalance job will be retried and the
          // old job will be marked as ABORTED.
          return stats.getStatus() != RebalanceResult.Status.IN_PROGRESS;
        } catch (Exception e) {
          // If the stats are corrupted for some reason, let's assume that the rebalance job is no longer in progress
          // and the job metadata entry can be cleaned up.
          return true;
        }
      case TENANT_REBALANCE:
        try {
          TenantRebalanceProgressStats stats = JsonUtils.stringToObject(
              jobMetadataEntry.getRight().get(RebalanceJobConstants.JOB_METADATA_KEY_REBALANCE_PROGRESS_STATS),
              TenantRebalanceProgressStats.class);
          // TODO: Add handling for stuck tenant rebalance jobs.
          return stats.getCompletionStatusMsg() != null;
        } catch (Exception e) {
          // If the stats are corrupted for some reason, let's assume that the tenant rebalance job is no longer in
          // progress and the job metadata entry can be cleaned up.
          return true;
        }
      default:
        return true;
    }
  }
}
