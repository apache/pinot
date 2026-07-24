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
  CONTROLLER_TABLE_SEGMENT_UPLOAD_ERROR("TableSegmentUploadError", false),
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
  TABLE_REBALANCE_FAILURE_DETECTED("TableRebalanceFailureDetected", false),
  TABLE_REBALANCE_RETRY("TableRebalanceRetry", false),
  TABLE_REBALANCE_RETRY_TOO_MANY_TIMES("TableRebalanceRetryTooManyTimes", false),
  NUMBER_ADHOC_TASKS_SUBMITTED("adhocTasks", false),
  IDEAL_STATE_UPDATE_FAILURE("IdealStateUpdateFailure", false),
  IDEAL_STATE_UPDATE_RETRY("IdealStateUpdateRetry", false),
  IDEAL_STATE_UPDATE_SUCCESS("IdealStateUpdateSuccess", false),
  SEGMENT_SIZE_AUTO_REDUCTION("SegmentSizeAutoReduction", false),
  // Total Bytes read from deep store
  DEEP_STORE_READ_BYTES_COMPLETED("deepStoreReadBytesCompleted", true),
  // Total Bytes written to deep store
  DEEP_STORE_WRITE_BYTES_COMPLETED("deepStoreWriteBytesCompleted", true),
  // Tracks failures encountered while fetching partition group metadata
  PARTITION_GROUP_METADATA_FETCH_ERROR("failures", true),
  OFFSET_AUTO_RESET_SKIPPED_OFFSETS("autoResetSkippedOffsets", false),
  OFFSET_AUTO_RESET_BACKFILL_OFFSETS("autoResetBackfillOffsets", false),
  // Audit logging metrics
  AUDIT_REQUEST_FAILURES("failures", true),
  AUDIT_RESPONSE_FAILURES("failures", true),
  AUDIT_REQUEST_PAYLOAD_TRUNCATED("count", true),
  // Upsert compact merge task metrics
  UPSERT_COMPACT_MERGE_SEGMENT_SKIPPED_CONSENSUS_FAILURE("UpsertCompactMergeSegmentsSkipped", false),
  // Query workload propagation metrics
  QUERY_WORKLOAD_PROPAGATION_COUNT("count", true),
  QUERY_WORKLOAD_PROPAGATION_ERROR("count", true),
  QUERY_WORKLOAD_MESSAGES_COUNT("count", true),
  QUERY_WORKLOAD_MESSAGES_ERROR("count", true),
  QUERY_WORKLOAD_COMPUTE_INSTANCE_COST_COUNT("count", true),
  QUERY_WORKLOAD_COMPUTE_INSTANCE_COST_ERROR("count", true),
  QUERY_WORKLOAD_LISTENER_CHANGES_COUNT("count", true),
  QUERY_WORKLOAD_REQUEST_DROPPED("count", true),
  QUERY_WORKLOAD_HTTP_CALLBACK_DROPPED("count", true),
  // Number of segment-delete requests rejected because the targets participate in a live segment lineage entry.
  LINEAGE_BLOCKED_DELETE_COUNT("LineageBlockedDeleteCount", false),

  // Insert statement metrics — naming convention: PascalCase string, mirroring the older meter
  // names in this enum (e.g. "PinotControllerHealthCheckStatus"). Once shipped, these strings
  // are externally consumed by dashboards/alerts and cannot be renamed without coordinated
  // operator migration; see InsertStatementState's wire-compatibility note for the same constraint.
  INSERT_STATEMENTS_SUBMITTED("InsertStatementsSubmitted", true),
  INSERT_STATEMENTS_ABORTED("InsertStatementsAborted", true),
  INSERT_STATEMENTS_GC("InsertStatementsGarbageCollected", true),
  INSERT_STATEMENTS_VISIBLE("InsertStatementsVisible", true),
  /**
   * Increments when {@code InsertExecutor.abort} throws or returns an error result. Persistent
   * non-zero values indicate plugin abort handlers leaking resources without surfacing failures.
   */
  INSERT_ABORT_HOOK_FAILED("InsertAbortHookFailed", true),
  /**
   * Increments when the cleanup sweep throws while processing a table. Persistent non-zero values
   * indicate a wedged manifest (e.g., corrupt JSON, forward-incompatible schemaVersion) — the
   * sweep retries every 5 minutes and the metric makes the wedge visible without log scraping.
   */
  INSERT_CLEANUP_SWEEP_FAILURES("InsertCleanupSweepFailures", true),
  /**
   * Increments when {@code InsertStatementStore.deleteStatement} returns false during cleanup
   * GC of a terminal manifest. Distinct from INSERT_CLEANUP_SWEEP_FAILURES (which counts thrown
   * exceptions) — this one counts silent transient ZK failures that would otherwise be invisible.
   * Persistent non-zero values indicate cleanup wedge requiring operator attention.
   */
  INSERT_GC_FAILED("InsertGcFailed", true),
  /**
   * Increments when a FILE insert reaches the rare TASK_NAME_PERSIST_ERROR branch — task was
   * scheduled with the Minion task framework but persisting its name to ZK failed after retries.
   * The manifest is best-effort aborted to prevent orphan post-failover, but the Minion task may
   * still be running and pushing segments. This is a v1 limitation; persistent non-zero values
   * mean operator cleanup of orphan segments may be required. Should be near-zero in healthy
   * clusters; alert on any non-zero rate.
   */
  INSERT_TASK_NAME_PERSIST_FAILED("InsertTaskNamePersistFailed", true);

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
