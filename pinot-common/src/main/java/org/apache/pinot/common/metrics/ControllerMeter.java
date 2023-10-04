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
 * Meters for the controller.
 */
public enum ControllerMeter implements AbstractMetrics.Meter {
  HELIX_ZOOKEEPER_RECONNECTS("reconnects", true),
  HEALTHCHECK_OK_CALLS("healthcheck", true),
  HEALTHCHECK_BAD_CALLS("healthcheck", true),
  CONTROLLER_INSTANCE_POST_ERROR("InstancePostError", true),
  CONTROLLER_INSTANCE_DELETE_ERROR("InstanceDeleteError", true),
  CONTROLLER_SEGMENT_UPLOAD_ERROR("SegmentUploadError", true),
  CONTROLLER_SCHEMA_UPLOAD_ERROR("SchemaUploadError", true),
  CONTROLLER_TABLE_ADD_ERROR("TableAddError", true),
  CONTROLLER_TABLE_UPDATE_ERROR("TableUpdateError", true),
  CONTROLLER_TABLE_SCHEMA_UPDATE_ERROR("TableSchemaUpdateError", true),
  CONTROLLER_TABLE_TENANT_UPDATE_ERROR("TableTenantUpdateError", true),
  CONTROLLER_TABLE_TENANT_CREATE_ERROR("TableTenantCreateError", true),
  CONTROLLER_TABLE_TENANT_DELETE_ERROR("TableTenantDeleteError", true),
  CONTROLLER_REALTIME_TABLE_SEGMENT_ASSIGNMENT_ERROR("errors", true),
  CONTROLLER_REALTIME_TABLE_SEGMENT_ASSIGNMENT_MISMATCH("mismatch", true),
  CONTROLLER_LEADERSHIP_CHANGE_WITHOUT_CALLBACK("leadershipChangeWithoutCallback", true),
  LLC_STATE_MACHINE_ABORTS("aborts", false),
  LLC_ZOOKEEPER_FETCH_FAILURES("failures", false),
  LLC_ZOOKEEPER_UPDATE_FAILURES("failures", false),
  LLC_STREAM_DATA_LOSS("dataLoss", false),
  CONTROLLER_PERIODIC_TASK_RUN("periodicTaskRun", false),
  CONTROLLER_PERIODIC_TASK_ERROR("periodicTaskError", false),
  PERIODIC_TASK_ERROR("periodicTaskError", false),
  NUMBER_TIMES_SCHEDULE_TASKS_CALLED("tasks", true),
  NUMBER_TASKS_SUBMITTED("tasks", false),
  NUMBER_SEGMENT_UPLOAD_TIMEOUT_EXCEEDED("SegmentUploadTimeouts", true),
  // Segment replace api failure metrics
  NUMBER_START_REPLACE_FAILURE("NumStartReplaceFailure", false),
  NUMBER_END_REPLACE_FAILURE("NumEndReplaceFailure", false),
  NUMBER_REVERT_REPLACE_FAILURE("NumRevertReplaceFailure", false),
  CRON_SCHEDULER_JOB_TRIGGERED("cronSchedulerJobTriggered", false),
  CRON_SCHEDULER_JOB_SKIPPED("cronSchedulerJobSkipped", false),
  LLC_SEGMENTS_DEEP_STORE_UPLOAD_RETRY_SUCCESS("LLCSegmentDeepStoreUploadRetrySuccess", false),
  LLC_SEGMENTS_DEEP_STORE_UPLOAD_RETRY_ERROR("LLCSegmentDeepStoreUploadRetryError", false),
  SEGMENT_MISSING_DEEP_STORE_LINK("RealtimeSegmentMissingDeepStoreLink", false),
  DELETED_TMP_SEGMENT_COUNT("DeletedTmpSegmentCount", false),
  TABLE_REBALANCE_FAILURE("TableRebalanceFailure", false),
  TABLE_REBALANCE_FAILURE_DETECTED("TableRebalanceFailureDetected", false),
  TABLE_REBALANCE_RETRY("TableRebalanceRetry", false),
  NUMBER_ADHOC_TASKS_SUBMITTED("adhocTasks", false);


  private final String _brokerMeterName;
  private final String _unit;
  private final boolean _global;

  ControllerMeter(String unit, boolean global) {
    _unit = unit;
    _global = global;
    _brokerMeterName = Utils.toCamelCase(name().toLowerCase());
  }

  @Override
  public String getMeterName() {
    return _brokerMeterName;
  }

  @Override
  public String getUnit() {
    return _unit;
  }

  /**
   * Returns true if the metric is global (not attached to a particular resource)
   *
   * @return true if the metric is global
   */
  @Override
  public boolean isGlobal() {
    return _global;
  }
}
