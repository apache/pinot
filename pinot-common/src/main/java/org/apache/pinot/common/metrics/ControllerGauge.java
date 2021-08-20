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
package org.apache.pinot.common.metrics;

import org.apache.pinot.common.Utils;


/**
 * Controller gauges.
 */
public enum ControllerGauge implements AbstractMetrics.Gauge {

  // Number of complete replicas of table in external view containing all segments online in ideal state
  NUMBER_OF_REPLICAS("replicas", false),

  // Percentage of complete online replicas in external view as compared to replicas in ideal state
  PERCENT_OF_REPLICAS("percent", false),

  SEGMENTS_IN_ERROR_STATE("segments", false),

  // Percentage of segments with at least one online replica in external view as compared to total number of segments in
  // ideal state
  PERCENT_SEGMENTS_AVAILABLE("segments", false),

  SEGMENT_COUNT("SegmentCount", false),
  IDEALSTATE_ZNODE_SIZE("idealstate", false),
  IDEALSTATE_ZNODE_BYTE_SIZE("idealstate", false),
  REALTIME_TABLE_COUNT("TableCount", true),
  OFFLINE_TABLE_COUNT("TableCount", true),
  DISABLED_TABLE_COUNT("TableCount", true),
  PERIODIC_TASK_NUM_TABLES_PROCESSED("PeriodicTaskNumTablesProcessed", true),
  NUM_MINION_TASKS_IN_PROGRESS("NumMinionTasksInProgress", true),
  NUM_MINION_SUBTASKS_WAITING("NumMinionSubtasksWaiting", true),
  NUM_MINION_SUBTASKS_RUNNING("NumMinionSubtasksRunning", true),
  NUM_MINION_SUBTASKS_ERROR("NumMinionSubtasksError", true),
  PERCENT_MINION_SUBTASKS_IN_QUEUE("PercentMinionSubtasksInQueue", true),
  PERCENT_MINION_SUBTASKS_IN_ERROR("PercentMinionSubtasksInError", true),

  // Pinot controller leader
  PINOT_CONTROLLER_LEADER("PinotControllerLeader", true),

  // Pinot controller resource enabled
  PINOT_LEAD_CONTROLLER_RESOURCE_ENABLED("PinotLeadControllerResourceEnabled", true),

  // Number of partitions for which current controller becomes the leader
  CONTROLLER_LEADER_PARTITION_COUNT("ControllerLeaderPartitionCount", true),

  // Estimated size of offline table
  OFFLINE_TABLE_ESTIMATED_SIZE("OfflineTableEstimatedSize", false),

  // Table quota based on setting in table config
  TABLE_QUOTA("TableQuotaBasedOnTableConfig", false),

  // Table storage quota utilization
  TABLE_STORAGE_QUOTA_UTILIZATION("TableStorageQuotaUtilization", false),

  // Percentage of segments we failed to get size for
  TABLE_STORAGE_EST_MISSING_SEGMENT_PERCENT("TableStorageEstMissingSegmentPercent", false),

  // Number of scheduled Cron jobs
  CRON_SCHEDULER_JOB_SCHEDULED("cronSchedulerJobScheduled", false),

  // Number of Tasks Status
  TASK_STATUS("taskStatus", false),

  // Number of dropped minion instances
  DROPPED_MINION_INSTANCES("droppedMinionInstances", true),

  // Number of online minion instances
  ONLINE_MINION_INSTANCES("onlineMinionInstances", true);

  private final String _gaugeName;
  private final String _unit;
  private final boolean _global;

  ControllerGauge(String unit, boolean global) {
    _unit = unit;
    _global = global;
    _gaugeName = Utils.toCamelCase(name().toLowerCase());
  }

  @Override
  public String getGaugeName() {
    return _gaugeName;
  }

  @Override
  public String getUnit() {
    return _unit;
  }

  /**
   * Returns true if the gauge is global (not attached to a particular resource)
   *
   * @return true if the gauge is global
   */
  @Override
  public boolean isGlobal() {
    return _global;
  }
}
