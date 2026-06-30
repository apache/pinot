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
package org.apache.pinot.spi.ingest;

import org.apache.pinot.spi.annotations.InterfaceStability;

/// Stable string codes returned in {@link InsertResult#getErrorCode()}.
///
/// These string values are surfaced to dashboards, alerts, and client-side retry logic. Keep them
/// frozen — once shipped, renaming a code requires coordinated client migration.
///
/// Codes are organized by lifecycle phase:
/// - **Pre-acceptance** (`REJECTED` state): coordinator rejected the request
///       before persisting a manifest. No statementId is queryable via `getStatus` afterward.
/// - **Post-acceptance** (`ABORTED`/`VISIBLE` state): manifest exists in
///       ZK; the code distinguishes failure modes during execution or persistence.
/// - **Query-side**: rejection of a `getStatus`/`abort`/`complete`
///       call — typically because the statement does not exist or is in an incompatible state.
///
/// This is a constants holder, not an enum, because executor plugins may emit additional codes
/// the coordinator does not know about. Treat the field as opaque on the wire.
@InterfaceStability.Evolving
public final class InsertErrorCode {
  private InsertErrorCode() {
  }

  /// ---- Pre-acceptance (state=REJECTED, no manifest) -----------------------------------------

  /// Coordinator is not started — feature flag off or controller starting/stopping.
  public static final String COORDINATOR_NOT_READY = "COORDINATOR_NOT_READY";
  /// Could not resolve the table name (does not exist, ambiguous hybrid type, type mismatch).
  public static final String TABLE_RESOLUTION_ERROR = "TABLE_RESOLUTION_ERROR";
  /// Idempotency reservation could not be created (typically a ZK-availability issue).
  public static final String IDEMPOTENCY_ERROR = "IDEMPOTENCY_ERROR";
  /// Same requestId was previously used with a different payloadHash — rejecting to avoid silent overwrite.
  public static final String IDEMPOTENCY_CONFLICT = "IDEMPOTENCY_CONFLICT";
  /// Requested {@link InsertConsistencyMode} not supported by this controller version.
  public static final String UNSUPPORTED_CONSISTENCY_MODE = "UNSUPPORTED_CONSISTENCY_MODE";
  /// No {@link InsertExecutor} registered for the requested {@link InsertType}.
  public static final String NO_EXECUTOR = "NO_EXECUTOR";
  /// Stale-rebind race lost; client should retry.
  public static final String REBIND_RACE_LOST = "REBIND_RACE_LOST";
  /// Failed to persist the manifest in ZK during initial create.
  public static final String STORE_ERROR = "STORE_ERROR";
  /// Segment names provided to /insert/complete failed Pinot's segment-name pattern.
  public static final String INVALID_SEGMENT_NAME = "INVALID_SEGMENT_NAME";
  /// ROW insert request had null/empty rows; coordinator rejects pre-acceptance.
  public static final String EMPTY_ROWS = "EMPTY_ROWS";

  /// ---- Post-acceptance (state=ABORTED, manifest exists) -------------------------------------

  /// Executor threw during execute(); manifest is now ABORTED.
  public static final String EXECUTOR_ERROR = "EXECUTOR_ERROR";
  /// Executor threw but the manifest reached a terminal state durably; data may already be queryable.
  public static final String EXECUTOR_ERROR_BUT_DURABLE = "EXECUTOR_ERROR_BUT_DURABLE";
  /// Failed to persist a state transition after CAS retries; cleanup sweep will reconcile.
  public static final String STATE_PERSIST_ERROR = "STATE_PERSIST_ERROR";
  /// Concurrent writer changed the manifest's state during this operation.
  public static final String CONCURRENT_STATE_CHANGE = "CONCURRENT_STATE_CHANGE";
  /// Tried a state transition that is not legal from the current state (e.g., abort a VISIBLE).
  public static final String INVALID_STATE = "INVALID_STATE";
  /// /insert/complete invoked with a wrong-typed FILE executor wrapper.
  public static final String WRONG_EXECUTOR = "WRONG_EXECUTOR";
  /// Failed to persist Minion task name to the manifest after task creation.
  public static final String TASK_NAME_PERSIST_ERROR = "TASK_NAME_PERSIST_ERROR";
  /// Execution succeeded and data is durably visible, but post-success bookkeeping (e.g., releasing
  /// the idempotency reservation) failed. The statement state is authoritative; the bookkeeping
  /// artifact will be reconciled by the cleanup sweep.
  public static final String POST_SUCCESS_BOOKKEEPING_ERROR = "POST_SUCCESS_BOOKKEEPING_ERROR";

  /// ---- Executor-tier (raised by InsertExecutor implementations) -----------------------------

  /// Target table does not exist when the executor looked it up.
  public static final String TABLE_NOT_FOUND = "TABLE_NOT_FOUND";
  /// Table mode (e.g., dedup/upsert configuration) rejects this insert per safety rules.
  public static final String TABLE_MODE_REJECTED = "TABLE_MODE_REJECTED";
  /// Schema is missing for the target table.
  public static final String SCHEMA_NOT_FOUND = "SCHEMA_NOT_FOUND";
  /// Row had a null primary-key value for an upsert table.
  public static final String PRIMARY_KEY_REJECTED = "PRIMARY_KEY_REJECTED";
  /// Row had an invalid value for a partition column (e.g., null or wrong type).
  public static final String PARTITION_VALUE_REJECTED = "PARTITION_VALUE_REJECTED";
  /// Segment generation failed during the row-insert path.
  public static final String SEGMENT_BUILD_FAILED = "SEGMENT_BUILD_FAILED";
  /// Segment upload failed and destructive rollback was enabled, so already-registered segments
  /// were deleted. The pending segment may have left an orphan tar in deep store. The
  /// registered-as-orphan path uses the distinct {@link #SEGMENT_UPLOAD_FAILED_PARTIAL} code.
  public static final String SEGMENT_UPLOAD_FAILED = "SEGMENT_UPLOAD_FAILED";
  /// Multi-partition upload partially succeeded; rollback was disabled by config so already-uploaded
  /// segments remain registered. Uploaded segment names are returned in `result.segmentNames`.
  public static final String SEGMENT_UPLOAD_FAILED_PARTIAL = "SEGMENT_UPLOAD_FAILED_PARTIAL";
  /// /insert FROM FILE was given a URI that could not be parsed or resolved.
  public static final String INVALID_FILE_URI = "INVALID_FILE_URI";
  /// Minion task could not be scheduled or produced no subtasks.
  public static final String TASK_SCHEDULE_FAILED = "TASK_SCHEDULE_FAILED";
  /// /insert/complete was called for a statement that the executor cannot find.
  public static final String STATEMENT_NOT_FOUND = "STATEMENT_NOT_FOUND";
  /// /insert/complete was called for a statement already in ABORTED state.
  public static final String STATEMENT_ABORTED = "STATEMENT_ABORTED";
  /// /insert/complete was called from a state that cannot transition to VISIBLE.
  public static final String INVALID_STATE_FOR_COMPLETION = "INVALID_STATE_FOR_COMPLETION";

  /// ---- Query-side ---------------------------------------------------------------------------

  /// No manifest exists for the requested statementId.
  public static final String NOT_FOUND = "NOT_FOUND";
}
