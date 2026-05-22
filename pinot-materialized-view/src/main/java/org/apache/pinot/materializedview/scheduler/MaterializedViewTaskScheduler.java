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
package org.apache.pinot.materializedview.scheduler;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.materializedview.analysis.MaterializedViewAnalyzer;
import org.apache.pinot.materializedview.context.MaterializedViewTaskGeneratorContext;
import org.apache.pinot.materializedview.metadata.MaterializedViewDefinitionMetadata;
import org.apache.pinot.materializedview.metadata.MaterializedViewDefinitionMetadata.MaterializedViewSplitSpec;
import org.apache.pinot.materializedview.metadata.MaterializedViewDefinitionMetadataUtils;
import org.apache.pinot.materializedview.metadata.MaterializedViewRuntimeMetadata;
import org.apache.pinot.materializedview.metadata.MaterializedViewRuntimeMetadataUtils;
import org.apache.pinot.materializedview.metadata.PartitionFingerprint;
import org.apache.pinot.materializedview.metadata.PartitionInfo;
import org.apache.pinot.materializedview.metadata.PartitionState;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants.MaterializedViewTask;
import org.apache.pinot.spi.utils.TimeUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Task generator for [MaterializedViewTask].
///
/// Unlike segment-conversion tasks, this generator does not scan source segments. It only
/// computes a time window and appends it to the user-defined SQL, producing a
/// [PinotTaskConfig] for the executor.
///
/// Two-step decision logic (evaluated per table, per schedule cycle):
///
///   - **Overwrite STALE** – If any partition is marked [PartitionState#STALE]
///       (by the event-driven `MaterializedViewConsistencyManager`), the generator
///       performs a precise fingerprint verification. If the data truly changed, it generates
///       an `OVERWRITE` task for the earliest stale partition. If the fingerprint
///       matches (false positive), the partition is reverted to [PartitionState#VALID].
///       This step has the highest priority to maintain consistency.
///   - **Append** – If no STALE partitions exist and the watermark can advance (next
///       window is outside the buffer period), generate a normal `APPEND` task.
///
///
/// Dirty marking (STALE detection) is handled externally by
/// `MaterializedViewConsistencyManager`, which reacts to base table segment changes
/// (add, replace, delete) and proactively marks affected partitions in
/// [MaterializedViewRuntimeMetadata].
///
/// <h3>Partition model (TIME-WINDOWED ONLY in PR 1)</h3>
///
/// All selection logic assumes time-windowed partitions of fixed width `bucketTimePeriod`:
/// the watermark is `long ms`, APPEND windows are `[watermarkMs, watermarkMs+bucketMs)`, and
/// the per-window source-time predicate is spliced into the SQL via text manipulation. A
/// future fixed-partition MV will need a different selection strategy (FIFO over STALE
/// partitions keyed by categorical id, no time arithmetic, no watermark advance) — see
/// `pinot-materialized-view/DESIGN.md`. The minion executor itself is partition-shape
/// neutral: it runs whichever SQL the scheduler emits.
public class MaterializedViewTaskScheduler {
  private static final Logger LOGGER = LoggerFactory.getLogger(MaterializedViewTaskScheduler.class);

  /// Identifier pattern used to validate a column name before it is interpolated into the
  /// time-range filter SQL fragment.  Compiled once to avoid recompiling on every call.
  private static final Pattern IDENTIFIER_PATTERN = Pattern.compile("[A-Za-z_][A-Za-z0-9_.]*");

  /// Compile-time default for the APPEND batch-scheduling-loop iteration cap. Overridable
  /// per cluster (no restart) via `MaterializedViewTask.CLUSTER_CONFIG_KEY_MAX_BATCH_LOOP_ITERATIONS`.
  /// Sized to comfortably exceed any realistic `availableSlots + partitionInfos.size()`
  /// for production MVs (a 10-year hourly MV has ~88k partitions, a 30-year daily MV ~11k);
  /// pathological maps beyond that stop the loop early so the scheduler can recover on the
  /// next cycle. Distinct from [MaterializedViewTask#MAX_TASKS_PER_BATCH_USER_CAP],
  /// which is the user-facing upper bound on the `maxTasksPerBatch` table-config value.
  static final int DEFAULT_MAX_BATCH_LOOP_ITERATIONS = 100_000;

  private final MaterializedViewTaskGeneratorContext _context;

  public MaterializedViewTaskScheduler(MaterializedViewTaskGeneratorContext context) {
    _context = context;
  }

  public String getTaskType() {
    return MaterializedViewTask.TASK_TYPE;
  }

  public List<PinotTaskConfig> generateTasks(List<TableConfig> tableConfigs) {
    String taskType = MaterializedViewTask.TASK_TYPE;
    List<PinotTaskConfig> pinotTaskConfigs = new ArrayList<>();

    for (TableConfig tableConfig : tableConfigs) {
      String offlineTableName = tableConfig.getTableName();

      if (!tableConfig.isMaterializedView()) {
        LOGGER.warn("Skip generating task: {} for table: {} because isMaterializedView is not set",
            taskType, offlineTableName);
        continue;
      }

      if (tableConfig.getTableType() != TableType.OFFLINE) {
        LOGGER.warn("Skip generating task: {} for non-OFFLINE table: {}", taskType, offlineTableName);
        continue;
      }
      LOGGER.info("Start generating task configs for table: {} for task: {}", offlineTableName, taskType);

      TableTaskConfig tableTaskConfig = tableConfig.getTaskConfig();
      Preconditions.checkState(tableTaskConfig != null);
      Map<String, String> taskConfigs = tableTaskConfig.getConfigsForTaskType(taskType);
      Preconditions.checkState(taskConfigs != null, "Task config shouldn't be null for table: %s", offlineTableName);

      String definedSQL = taskConfigs.get(MaterializedViewTask.DEFINED_SQL_KEY);
      Preconditions.checkState(definedSQL != null && !definedSQL.isEmpty(),
          "definedSQL must be specified for table: %s", offlineTableName);

      String sourceTableName = MaterializedViewAnalyzer.extractSourceTableName(definedSQL);
      String sourceTableWithType = resolveSourceTableNameWithType(sourceTableName);

      // Bucket and buffer.  bucketTimePeriod is required (validated by MaterializedViewAnalyzer
      // at create time; re-checked here so a hand-edited table config does not silently fall
      // back to an implicit default).
      String bucketTimePeriod = taskConfigs.get(MaterializedViewTask.BUCKET_TIME_PERIOD_KEY);
      Preconditions.checkState(bucketTimePeriod != null && !bucketTimePeriod.isEmpty(),
          "MaterializedViewTask requires '%s' to be set on table: %s",
          MaterializedViewTask.BUCKET_TIME_PERIOD_KEY, offlineTableName);
      long bucketMs = TimeUtils.convertPeriodToMillis(bucketTimePeriod);
      String bufferTimePeriod =
          taskConfigs.getOrDefault(MaterializedViewTask.BUFFER_TIME_PERIOD_KEY, "0d");
      long bufferMs = TimeUtils.convertPeriodToMillis(bufferTimePeriod);
      Preconditions.checkState(bufferMs >= 0,
          "bufferTimePeriod must be non-negative for table: %s, got: %s",
          offlineTableName, bufferTimePeriod);

      String maxTasksPerBatchStr = taskConfigs.getOrDefault(
          MaterializedViewTask.MAX_TASKS_PER_BATCH_KEY,
          String.valueOf(MaterializedViewTask.DEFAULT_MAX_TASKS_PER_BATCH));
      int maxTasksPerBatch;
      try {
        maxTasksPerBatch = Integer.parseInt(maxTasksPerBatchStr);
      } catch (NumberFormatException e) {
        throw new IllegalStateException(
            "Invalid maxTasksPerBatch '" + maxTasksPerBatchStr + "' for table: " + offlineTableName, e);
      }
      Preconditions.checkState(maxTasksPerBatch >= 1,
          "maxTasksPerBatch must be >= 1 for table: %s, got: %s", offlineTableName, maxTasksPerBatch);

      // Resolve the effective LIMIT once via the AST. The user-declared value is used when
      // present; otherwise DEFAULT_MATERIALIZED_VIEW_QUERY_LIMIT (a bounded cap, not Integer.MAX_VALUE).
      // Both values flow downstream: (a) appended to the broker SQL so the broker's silent
      // default-LIMIT-of-10 cannot truncate, and (b) stored in EFFECTIVE_LIMIT_KEY so the
      // executor's saturation gate fires if a window's result set saturates the cap.
      Optional<Integer> declaredLimit = MaterializedViewAnalyzer.tryExtractDeclaredLimit(definedSQL);
      boolean userDeclaredLimit = declaredLimit.isPresent();
      // The analyzer's create-time validation guarantees any present LIMIT is positive
      // (and at most MAX_MATERIALIZED_VIEW_QUERY_LIMIT, currently 100_000_000); the orElse default
      // is also positive. effectiveLimit > 0 holds.
      int defaultLimit = MaterializedViewTaskUtils.readPositiveIntClusterConfigOrDefault(
          _context::getClusterConfig,
          MaterializedViewTask.CLUSTER_CONFIG_KEY_DEFAULT_QUERY_LIMIT,
          MaterializedViewTask.DEFAULT_MATERIALIZED_VIEW_QUERY_LIMIT);
      int effectiveLimit = declaredLimit.orElse(defaultLimit);

      // Load watermarkMs and partitionInfos from MaterializedViewRuntimeMetadata
      String viewTableWithType = TableNameBuilder.OFFLINE.tableNameWithType(offlineTableName);
      HelixPropertyStore<ZNRecord> propertyStore = _context.getPropertyStore();
      long watermarkMs = getWatermarkMs(offlineTableName, sourceTableName, bucketMs, definedSQL, taskConfigs);

      Stat rtStat = new Stat();
      MaterializedViewRuntimeMetadata runtime = MaterializedViewRuntimeMetadataUtils.fetchWithVersion(
          propertyStore, viewTableWithType, rtStat);
      Map<Long, PartitionInfo> partitionInfos = new HashMap<>();
      int runtimeVersion = -1;
      if (runtime != null) {
        partitionInfos = new HashMap<>(runtime.getPartitions());
        runtimeVersion = rtStat.getVersion();
      }

      // Walk the in-flight task configs ONCE and bucket per-mode. APPEND tasks must not
      // starve DELETE/OVERWRITE (and vice versa) — they have independent gates. We also
      // track the highest in-flight APPEND windowEnd so we know where to start new ones.
      InFlightTaskCounts counts = countInFlightTasks(offlineTableName, taskType, watermarkMs);

      // ── Step 1: Handle STALE partitions ──
      // Under Design C there is no separate EXPIRED state.  When the scheduler finds a STALE
      // partition it re-computes the source fingerprint and dispatches one of:
      //   - DELETE task: source data is gone (segmentCount == 0) — drop MV segments + remove the
      //     partition entry from the runtime metadata
      //   - revert to VALID: fingerprint matches the stored value (false positive STALE marking)
      //   - OVERWRITE task: source changed — re-materialize the partition
      if (counts.exclusiveModeCount() > 0) {
        LOGGER.debug("Found {} in-flight DELETE/{} OVERWRITE tasks for table: {}; skipping",
            counts._inFlightDeleteCount.get(), counts._inFlightOverwriteCount.get(), offlineTableName);
      } else {
        PinotTaskConfig staleTask = tryHandleStalePartition(offlineTableName, sourceTableName,
            sourceTableWithType, definedSQL, taskConfigs, partitionInfos, bucketMs,
            effectiveLimit, userDeclaredLimit);
        if (staleTask != null) {
          pinotTaskConfigs.add(staleTask);
          LOGGER.info("Generated {} task for table: {}",
              staleTask.getConfigs().get(MaterializedViewTask.TASK_MODE_KEY), offlineTableName);
          continue;
        }
      }

      // ── Step 2: Append new data — schedule up to maxTasksPerBatch windows ──
      int inFlightAppend = counts._inFlightAppendCount.get();
      int availableSlots = maxTasksPerBatch - inFlightAppend;
      if (availableSlots <= 0) {
        LOGGER.debug("MV table {} already has {} in-flight APPEND tasks (max={}); skipping",
            offlineTableName, inFlightAppend, maxTasksPerBatch);
        continue;
      }
      // Start scheduling from the max of (a) highest in-flight APPEND windowEnd and
      // (b) highest contiguous VALID upper from partitionInfos. (b) prevents replay of
      // already-VALID windows when a previous batch had a mid-batch failure that left
      // some windows VALID and others FAILED — the in-flight set is then empty but
      // some windows past `watermarkMs` are already done.
      long contiguousValidUpper = MaterializedViewTaskUtils.computeContiguousUpperMs(
          watermarkMs, partitionInfos, bucketMs);
      long maxInFlightAppendWindowEndMs =
          Math.max(counts._maxInFlightAppendWindowEndMs.get(), contiguousValidUpper);

      // Start scheduling from the end of the highest in-flight window to avoid duplicates.
      // Floor-align to bucketMs in case an in-flight task was scheduled under a different
      // bucketTimePeriod (operator changed config mid-flight); the partition map is keyed by
      // aligned starts, so a misaligned nextWindowStartMs would miss VALID-skip lookups.
      long nextWindowStartMs = Math.floorDiv(maxInFlightAppendWindowEndMs, bucketMs) * bucketMs;
      long cutoffMs = System.currentTimeMillis() - bufferMs;
      int scheduled = 0;

      // Hard cap on iterations to defend against pathological partition maps. The loop
      // advances nextWindowStartMs by bucketMs each iteration, so a finite cutoff bounds it,
      // but skipping VALID slots could still in principle iterate forever if the cutoff is
      // far in the future. Cap at availableSlots + partitionInfos.size() — any more is a bug.
      // Also clamp at DEFAULT_MAX_BATCH_LOOP_ITERATIONS (or its cluster-config override) so an MV
      // table accumulating years of VALID partitions doesn't burn unbounded CPU per cycle.
      int maxBatchLoopIterations = MaterializedViewTaskUtils.readPositiveIntClusterConfigOrDefault(
          _context::getClusterConfig,
          MaterializedViewTask.CLUSTER_CONFIG_KEY_MAX_BATCH_LOOP_ITERATIONS,
          DEFAULT_MAX_BATCH_LOOP_ITERATIONS);
      int maxIterations = Math.min(availableSlots + partitionInfos.size(), maxBatchLoopIterations);
      int iterations = 0;
      while (scheduled < availableSlots && iterations < maxIterations) {
        iterations++;
        long windowEndMs = nextWindowStartMs + bucketMs;
        if (windowEndMs > cutoffMs) {
          break;
        }
        // Skip already-VALID slots from a prior partial batch (mid-batch failure recovery).
        // Re-running an APPEND for a VALID partition would produce duplicate segments.
        PartitionInfo existing = partitionInfos.get(nextWindowStartMs);
        if (existing != null && existing.getState() == PartitionState.VALID) {
          nextWindowStartMs = windowEndMs;
          continue;
        }
        PinotTaskConfig appendTask = buildTaskConfig(offlineTableName, sourceTableName,
            sourceTableWithType, definedSQL, taskConfigs, nextWindowStartMs, windowEndMs,
            MaterializedViewTask.TASK_MODE_APPEND, effectiveLimit, userDeclaredLimit);
        pinotTaskConfigs.add(appendTask);
        LOGGER.info("Generated APPEND task for table: {} window [{}, {})", offlineTableName,
            nextWindowStartMs, windowEndMs);
        nextWindowStartMs = windowEndMs;
        scheduled++;
      }
      // Surface the cap-hit case (scheduler ran out of iterations before filling availableSlots)
      // so a corrupt partition map doesn't silently masquerade as "caught up".
      if (iterations >= maxIterations && scheduled < availableSlots) {
        LOGGER.error("MV table {} APPEND scheduler hit maxIterations cap ({}); partition map "
            + "may be corrupted (size={}, scheduled={}). Investigate stale VALID partitions.",
            offlineTableName, maxIterations, partitionInfos.size(), scheduled);
      }

      if (scheduled == 0) {
        LOGGER.debug("MV table {} is caught up (watermark={}), no dirty partitions.", offlineTableName, watermarkMs);
      }
    }
    return pinotTaskConfigs;
  }

  /// Step 1: Finds the earliest STALE partition and dispatches the appropriate task.
  ///
  /// Re-computes the source fingerprint and picks one of:
  ///   - DELETE task: source data is gone (segmentCount == 0) — task will drop MV segments
  ///     and remove the partition entry from runtime metadata
  ///   - revert to VALID in place: fingerprint matches stored value (false positive)
  ///   - OVERWRITE task: fingerprint differs — re-materialize the partition
  ///
  /// @return a [PinotTaskConfig] for DELETE or OVERWRITE, or `null` if no actionable
  ///         STALE partition exists (either none STALE, or the only STALE one was reverted
  ///         to VALID in place).
  private PinotTaskConfig tryHandleStalePartition(String viewTableName, String sourceTableName,
      String sourceTableWithType, String definedSQL, Map<String, String> taskConfigs,
      Map<Long, PartitionInfo> partitionInfos, long bucketMs,
      int effectiveLimit, boolean userDeclaredLimit) {
    long earliestStaleMs = Long.MAX_VALUE;
    for (Map.Entry<Long, PartitionInfo> entry : partitionInfos.entrySet()) {
      if (entry.getValue().getState() == PartitionState.STALE && entry.getKey() < earliestStaleMs) {
        earliestStaleMs = entry.getKey();
      }
    }
    if (earliestStaleMs == Long.MAX_VALUE) {
      return null;
    }

    long windowStartMs = earliestStaleMs;
    long windowEndMs = windowStartMs + bucketMs;
    PartitionInfo staleInfo = partitionInfos.get(earliestStaleMs);

    PartitionFingerprint currentFp = computeWindowFingerprint(sourceTableWithType, windowStartMs, windowEndMs);

    if (currentFp.getSegmentCount() == 0) {
      LOGGER.info("STALE partition [{}, {}) base data deleted for table: {}. Generating DELETE task.",
          windowStartMs, windowEndMs, viewTableName);
      Map<String, String> configs = new HashMap<>();
      configs.put(MinionConstants.TABLE_NAME_KEY, viewTableName);
      configs.put(MaterializedViewTask.WINDOW_START_MS_KEY, String.valueOf(windowStartMs));
      configs.put(MaterializedViewTask.WINDOW_END_MS_KEY, String.valueOf(windowEndMs));
      configs.put(MaterializedViewTask.TASK_MODE_KEY, MaterializedViewTask.TASK_MODE_DELETE);
      configs.put(MinionConstants.UPLOAD_URL_KEY, _context.getVipUrl() + "/segments");
      return new PinotTaskConfig(MaterializedViewTask.TASK_TYPE, configs);
    }

    if (currentFp.equals(staleInfo.getFingerprint())) {
      LOGGER.info("STALE partition [{}, {}) fingerprint matches for table: {}. "
          + "Reverting to VALID (false positive).", windowStartMs, windowEndMs, viewTableName);
      persistPartitionStateChangeWithRetry(viewTableName, earliestStaleMs, PartitionState.VALID);
      return null;
    }

    LOGGER.info("Confirmed STALE partition at {} for table: {}. Generating OVERWRITE task for window [{}, {})",
        windowStartMs, viewTableName, windowStartMs, windowEndMs);
    return buildTaskConfig(viewTableName, sourceTableName, sourceTableWithType, definedSQL,
        taskConfigs, windowStartMs, windowEndMs, MaterializedViewTask.TASK_MODE_OVERWRITE,
        effectiveLimit, userDeclaredLimit);
  }

  /// CAS-retry budget for STALE -> VALID transitions written by the scheduler.
  /// The runtime znode is concurrently mutated by the executor (after each task completion) and by
  /// the consistency manager (on base table changes). A bounded retry loop converges in practice.
  private static final int MAX_PARTITION_STATE_PERSIST_RETRIES = 8;

  /// Persists a STALE -> VALID transition (false-positive recovery) under a CAS retry loop.
  /// On each attempt the latest runtime znode is re-fetched, the target partition's state is
  /// re-evaluated, and the change is rewritten on top of the current version. This preserves
  /// concurrent updates from the executor (watermark advance) and the consistency manager
  /// (other partitions' STALE markings).
  ///
  /// If the partition is no longer STALE on a retry (executor or consistency manager
  /// already changed it), the method exits successfully — the desired transition is either
  /// already done or no longer applicable to a stale view of the world.
  ///
  /// If the budget is exhausted, logs ERROR. The next scheduling cycle will retry.
  private void persistPartitionStateChangeWithRetry(String viewTableName, long partitionStartMs,
      PartitionState newState) {
    String viewTableWithType = TableNameBuilder.OFFLINE.tableNameWithType(viewTableName);
    Exception lastException = null;
    for (int attempt = 0; attempt < MAX_PARTITION_STATE_PERSIST_RETRIES; attempt++) {
      Stat stat = new Stat();
      MaterializedViewRuntimeMetadata current = MaterializedViewRuntimeMetadataUtils.fetchWithVersion(
          _context.getPropertyStore(), viewTableWithType, stat);
      if (current == null) {
        LOGGER.warn("Runtime metadata missing for MV table: {} during partition state persist; aborting",
            viewTableName);
        return;
      }
      Map<Long, PartitionInfo> currentInfos = current.getPartitions();
      PartitionInfo info = currentInfos.get(partitionStartMs);
      if (info == null || info.getState() != PartitionState.STALE) {
        LOGGER.info("Partition {} for MV table: {} is no longer STALE on attempt {}; skipping persist",
            partitionStartMs, viewTableName, attempt + 1);
        return;
      }
      Map<Long, PartitionInfo> updatedInfos = new HashMap<>(currentInfos);
      updatedInfos.put(partitionStartMs, info.withState(newState));
      MaterializedViewRuntimeMetadata updated = new MaterializedViewRuntimeMetadata(
          current.getMaterializedViewTableNameWithType(),
          current.getWatermarkMs(),
          updatedInfos);
      try {
        MaterializedViewRuntimeMetadataUtils.persist(_context.getPropertyStore(), updated, stat.getVersion());
        LOGGER.info("Persisted partition {} state {} -> {} for MV table: {} on attempt {}",
            partitionStartMs, PartitionState.STALE, newState, viewTableName, attempt + 1);
        return;
      } catch (IllegalStateException e) {
        // Writer-side invariant violation surfaced by `validateForPersist` on the freshly-fetched
        // runtime. Retrying will not change the underlying state — fail fast so the caller can
        // surface the bug instead of burning the retry budget.
        LOGGER.error("Aborting CAS retry for MV table {}: writer-side invariant violation "
                + "({}). Generator will not retry until the underlying runtime znode is fixed.",
            viewTableName, e.getMessage());
        return;
      } catch (Exception e) {
        lastException = e;
        LOGGER.debug("CAS conflict on attempt {} persisting partition {} state for MV table: {}",
            attempt + 1, partitionStartMs, viewTableName, e);
      }
      // Small jittered backoff so a tight CAS race doesn't burn the budget in microseconds
      // and starve the competing writers — also gives transient ZK errors a chance to resolve.
      try {
        Thread.sleep(5L + ThreadLocalRandom.current().nextInt(20));
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        LOGGER.warn("Interrupted while persisting partition {} state for MV table: {}",
            partitionStartMs, viewTableName);
        return;
      }
    }
    LOGGER.error("Failed to persist partition {} state {} for MV table: {} after {} retries. "
            + "Generator will retry on next scheduling cycle. Last exception:",
        partitionStartMs, newState, viewTableName, MAX_PARTITION_STATE_PERSIST_RETRIES, lastException);
  }

  /// Builds a complete [PinotTaskConfig] for either APPEND or OVERWRITE mode.
  ///
  /// @param effectiveLimit pre-resolved LIMIT (user-declared, or
  ///     [MaterializedViewTask#DEFAULT_MATERIALIZED_VIEW_QUERY_LIMIT] when absent in `definedSQL`).
  ///     Same value flows to the broker SQL and to `EFFECTIVE_LIMIT_KEY` for the gate.
  /// @param userDeclaredLimit `true` if `definedSQL` already contains a LIMIT clause
  ///     (AST-detected by caller). When `false`, `effectiveLimit` is appended to the
  ///     broker SQL here.
  private PinotTaskConfig buildTaskConfig(String viewTableName, String sourceTableName,
      String sourceTableWithType, String definedSQL, Map<String, String> taskConfigs,
      long windowStartMs, long windowEndMs, String taskMode, int effectiveLimit,
      boolean userDeclaredLimit) {
    String taskType = MaterializedViewTask.TASK_TYPE;

    PartitionFingerprint windowFingerprint =
        computeWindowFingerprint(sourceTableWithType, windowStartMs, windowEndMs);

    // The source time column may use any DateTimeFieldSpec format (TIMESTAMP, INT-days, etc.).
    // Convert the window boundaries to the source's native unit so the appended WHERE filter
    // compares apples to apples.
    DateTimeFieldSpec sourceTimeFieldSpec = resolveSourceTimeFieldSpec(sourceTableName);
    String sourceTimeColumn = sourceTimeFieldSpec.getName();
    DateTimeFormatSpec sourceTimeFormat = sourceTimeFieldSpec.getFormatSpec();
    String windowStart = sourceTimeFormat.fromMillisToFormat(windowStartMs);
    String windowEnd = sourceTimeFormat.fromMillisToFormat(windowEndMs);
    String sqlWithTimeRange = appendTimeRange(definedSQL, sourceTimeColumn, windowStart, windowEnd);
    // If the user did not declare LIMIT, append the bounded default so the broker doesn't
    // apply its own (small) cluster default. AST-based detection by the caller is reliable —
    // a text scan would mis-fire on string literals or comments containing "LIMIT".
    if (!userDeclaredLimit) {
      sqlWithTimeRange = sqlWithTimeRange.trim();
      if (sqlWithTimeRange.endsWith(";")) {
        sqlWithTimeRange = sqlWithTimeRange.substring(0, sqlWithTimeRange.length() - 1).trim();
      }
      sqlWithTimeRange = sqlWithTimeRange + " LIMIT " + effectiveLimit;
    }
    // Defense: re-parse the final broker-bound SQL and verify the LIMIT is syntactically
    // active. Catches text-manipulation hazards in either branch — auto-injected LIMIT
    // swallowed by a trailing comment, or appendTimeRange's clause-keyword scan corrupting
    // SQL that contained those keywords inside string literals.
    Optional<Integer> verifyLimit;
    try {
      verifyLimit = MaterializedViewAnalyzer.tryExtractDeclaredLimit(sqlWithTimeRange);
    } catch (IllegalStateException e) {
      // Calcite parse failure (validateSqlSyntax wraps SqlCompilationException as
      // IllegalStateException) — surface with MV table context for operator triage.
      throw new IllegalStateException("Broker-bound SQL is unparseable for MV table: "
          + viewTableName + ". Check definedSQL for syntax issues (trailing comments, unbalanced "
          + "quotes, etc). SQL: " + sqlWithTimeRange, e);
    }
    String observedLimit = verifyLimit.map(String::valueOf).orElse("<absent>");
    Preconditions.checkState(
        verifyLimit.isPresent() && verifyLimit.get().intValue() == effectiveLimit,
        "LIMIT verification failed for MV table: %s. Re-parsed SQL has LIMIT=%s, expected %s. "
            + "definedSQL likely contains text (comments, literals) that interfered with SQL "
            + "manipulation. SQL: %s",
        viewTableName, observedLimit, effectiveLimit, sqlWithTimeRange);

    Map<String, String> configs = new HashMap<>();
    configs.put(MinionConstants.TABLE_NAME_KEY, viewTableName);
    configs.put(MaterializedViewTask.DEFINED_SQL_KEY, sqlWithTimeRange);
    configs.put(MaterializedViewTask.WINDOW_START_MS_KEY, String.valueOf(windowStartMs));
    configs.put(MaterializedViewTask.WINDOW_END_MS_KEY, String.valueOf(windowEndMs));
    configs.put(MaterializedViewTask.SOURCE_TABLE_NAME_KEY, sourceTableName);
    configs.put(MaterializedViewTask.TASK_MODE_KEY, taskMode);
    configs.put(MaterializedViewTask.EFFECTIVE_LIMIT_KEY, String.valueOf(effectiveLimit));
    configs.put(MinionConstants.UPLOAD_URL_KEY, _context.getVipUrl() + "/segments");

    String maxNumRecords = taskConfigs.get(MaterializedViewTask.MAX_NUM_RECORDS_PER_SEGMENT_KEY);
    if (maxNumRecords != null) {
      configs.put(MaterializedViewTask.MAX_NUM_RECORDS_PER_SEGMENT_KEY, maxNumRecords);
    }

    Map<Long, PartitionFingerprint> fingerprintMap = new HashMap<>();
    fingerprintMap.put(windowStartMs, windowFingerprint);
    configs.put(MaterializedViewTask.PARTITION_FINGERPRINTS_KEY,
        PartitionFingerprint.encodeMap(fingerprintMap));

    return new PinotTaskConfig(taskType, configs);
  }

  public void validateTaskConfigs(TableConfig tableConfig, Schema schema, Map<String, String> taskConfigs) {
    MaterializedViewAnalyzer.analyze(
        taskConfigs.get(MaterializedViewTask.DEFINED_SQL_KEY), tableConfig, schema, taskConfigs, _context);
  }

  /// Resolves the [DateTimeFieldSpec] for the source table's time column. The returned
  /// spec provides both the column name and its format for converting ms watermarks to the
  /// column's native format (e.g. days since epoch for `1:DAYS:EPOCH`).
  private DateTimeFieldSpec resolveSourceTimeFieldSpec(String rawSourceTableName) {
    String sourceTableWithType = resolveSourceTableNameWithType(rawSourceTableName);
    TableConfig sourceTableConfig = _context.getTableConfig(sourceTableWithType);
    Preconditions.checkState(sourceTableConfig != null,
        "Source table config not found for: %s", rawSourceTableName);

    String timeColumn = sourceTableConfig.getValidationConfig().getTimeColumnName();
    Preconditions.checkState(timeColumn != null && !timeColumn.isEmpty(),
        "Time column not configured for source table: %s", rawSourceTableName);

    Schema sourceSchema = _context.getTableSchema(sourceTableWithType);
    Preconditions.checkState(sourceSchema != null,
        "Schema not found for source table: %s", rawSourceTableName);

    DateTimeFieldSpec fieldSpec = sourceSchema.getSpecForTimeColumn(timeColumn);
    Preconditions.checkState(fieldSpec != null,
        "No DateTimeFieldSpec found for time column '%s' in source table: %s", timeColumn, rawSourceTableName);
    return fieldSpec;
  }

  /// Resolves the MV table's designated time column from its [TableConfig]. The MV
  /// side of a split query filters on this column (e.g. `materializedViewTime < watermarkMs`),
  /// which may differ from the source time column when the defined SQL renames or buckets
  /// the time via a `dateTimeConvert`/`DATETRUNC` expression.
  private String resolveMaterializedViewTimeColumn(String viewTableWithType) {
    TableConfig viewTableConfig = _context.getTableConfig(viewTableWithType);
    Preconditions.checkState(viewTableConfig != null,
        "MV table config not found for: %s", viewTableWithType);

    String timeColumn = viewTableConfig.getValidationConfig().getTimeColumnName();
    Preconditions.checkState(timeColumn != null && !timeColumn.isEmpty(),
        "Time column not configured for MV table: %s (required for split queries)", viewTableWithType);
    return timeColumn;
  }

  /// Appends a time-range WHERE clause to the SQL.  Window values are raw epoch millis since
  /// both base and MV time columns are TIMESTAMP (enforced by [MaterializedViewAnalyzer]).
  /// If a WHERE clause already exists, appends with AND; otherwise inserts before GROUP BY /
  /// ORDER BY / the trailing semicolon.
  ///
  /// Keyword scans (`WHERE`, `FROM`, `GROUP BY`, ...) skip text inside single-quoted
  /// string literals so a column value like `'WHERE me'` cannot fool the splitter
  /// into corrupting the SQL. Identifiers and column names are required to be simple
  /// (validated by the analyzer); double-quoted identifiers are treated like string
  /// literals (skipped).
  static String appendTimeRange(String sql, String timeColumn, String windowStart, String windowEnd) {
    Preconditions.checkArgument(sql != null, "SQL must not be null");
    // Validate column name to prevent SQL injection (column names must be simple identifiers).
    Preconditions.checkArgument(IDENTIFIER_PATTERN.matcher(timeColumn).matches(),
        "Time column name contains invalid characters: %s", timeColumn);
    // Reject SQL containing nested SELECTs / subqueries: the keyword splitter below finds the
    // FIRST `WHERE` and inserts the time predicate after it, which would attach the predicate to
    // the inner subquery's WHERE rather than the outer query's — producing a semantically-wrong
    // task SQL. MV definitions are intentionally restricted to flat queries; reject up front
    // rather than silently corrupt. Counts standalone SELECT tokens outside string literals
    // and comments via the same mask used downstream.
    Preconditions.checkArgument(countStandaloneSelectKeywords(sql) <= 1,
        "MV definedSQL must not contain a nested SELECT / subquery; got: %s", sql);

    // windowStart/windowEnd come from DateTimeFormatSpec.fromMillisToFormat — numeric for EPOCH
    // formats (TIMESTAMP, epoch-days, etc.), or quoted strings for SIMPLE_DATE_FORMAT.  Quote
    // non-numeric values so the SQL parser treats them as string literals.
    String quotedStart = isNumeric(windowStart) ? windowStart : "'" + windowStart + "'";
    String quotedEnd = isNumeric(windowEnd) ? windowEnd : "'" + windowEnd + "'";
    String timeFilter = timeColumn + " >= " + quotedStart + " AND " + timeColumn + " < " + quotedEnd;

    // Remove trailing semicolon for easier manipulation
    String trimmed = sql.trim();
    if (trimmed.endsWith(";")) {
      trimmed = trimmed.substring(0, trimmed.length() - 1).trim();
    }

    // Use Locale.ROOT — the default locale can change a string's length on
    // toUpperCase (e.g. de_DE turns 'ß' into "SS", growing length by one), which
    // would misalign the quoteMask and let user-supplied keywords inside string
    // literals fool the splitter.
    String upperSql = trimmed.toUpperCase(Locale.ROOT);
    // Build a parallel mask that marks positions inside single-quoted string literals or
    // double-quoted identifiers, so keyword scans skip over user-controlled text.
    boolean[] quoteMask = buildQuoteMask(trimmed);
    // indexOfKeywordWithBoundary returns the start index of the keyword.
    int whereIdx = indexOfKeywordWithBoundary(upperSql, quoteMask, "WHERE", 0);
    if (whereIdx >= 0) {
      int afterWhere = whereIdx + "WHERE".length();
      int insertPos = findClauseEnd(upperSql, quoteMask, afterWhere);
      return trimmed.substring(0, insertPos) + " AND " + timeFilter + trimmed.substring(insertPos);
    }

    // No WHERE — insert before GROUP BY / ORDER BY / LIMIT / HAVING / end
    int fromIdx = indexOfKeywordWithBoundary(upperSql, quoteMask, "FROM", 0);
    Preconditions.checkState(fromIdx >= 0, "definedSQL is missing a FROM clause: %s", sql);
    int afterFrom = fromIdx + "FROM".length();
    int insertPos = findClauseEnd(upperSql, quoteMask, afterFrom);
    return trimmed.substring(0, insertPos) + " WHERE " + timeFilter + trimmed.substring(insertPos);
  }

  private static boolean isNumeric(String value) {
    if (value == null || value.isEmpty()) {
      return false;
    }
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      if (!Character.isDigit(c) && c != '-' && c != '.') {
        return false;
      }
    }
    return true;
  }

  /// Counts standalone occurrences of the `SELECT` keyword in `sql` outside string literals,
  /// double-quoted identifiers, and SQL comments.  A flat MV `definedSQL` has exactly one;
  /// a value of two or more indicates a nested SELECT / subquery, which is unsupported because
  /// the text-based [#appendTimeRange] inserts its time predicate after the FIRST `WHERE` and
  /// would attach to the inner query's WHERE rather than the outer query.
  static int countStandaloneSelectKeywords(String sql) {
    String upper = sql.toUpperCase(Locale.ROOT);
    boolean[] mask = buildQuoteMask(sql);
    int count = 0;
    int from = 0;
    while (true) {
      int idx = indexOfKeywordWithBoundary(upper, mask, "SELECT", from);
      if (idx < 0) {
        break;
      }
      count++;
      from = idx + "SELECT".length();
    }
    return count;
  }

  private static long parseLong(String value, long defaultValue) {
    if (value == null || value.isEmpty()) {
      return defaultValue;
    }
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  /// Builds a parallel mask of the SQL where `mask[i] == true` means position `i`
  /// is inside text the keyword scanner must skip:
  ///
  ///   - single-quoted string literals (with ANSI doubled-quote escape `''`)
  ///   - double-quoted identifiers (with ANSI doubled-quote escape `""`)
  ///   - `--` line comments through end-of-line
  ///   - `/* ... *``/` block comments
  ///
  /// Without comment masking a `definedSQL` fragment such as `-- WHERE x` would
  /// fool the splitter into treating the comment text as a real WHERE clause.
  private static boolean[] buildQuoteMask(String sql) {
    boolean[] mask = new boolean[sql.length()];
    char active = 0; // 0 means outside any quoted region; otherwise the quote char.
    int i = 0;
    int len = sql.length();
    while (i < len) {
      char c = sql.charAt(i);
      if (active == 0) {
        if (c == '-' && i + 1 < len && sql.charAt(i + 1) == '-') {
          // -- line comment: mask through end-of-line (or end-of-string).
          int eol = sql.indexOf('\n', i + 2);
          int end = eol < 0 ? len : eol;
          for (int j = i; j < end; j++) {
            mask[j] = true;
          }
          i = end;
        } else if (c == '/' && i + 1 < len && sql.charAt(i + 1) == '*') {
          // /* ... */ block comment: mask through closing */ (or end-of-string).
          int close = sql.indexOf("*/", i + 2);
          int end = close < 0 ? len : close + 2;
          for (int j = i; j < end; j++) {
            mask[j] = true;
          }
          i = end;
        } else if (c == '\'' || c == '"') {
          active = c;
          mask[i] = true;
          i++;
        } else {
          i++;
        }
      } else {
        mask[i] = true;
        if (c == active) {
          // ANSI doubled-quote escape: '' or "" stays inside the literal.
          if (i + 1 < len && sql.charAt(i + 1) == active) {
            mask[i + 1] = true;
            i += 2;
          } else {
            active = 0;
            i++;
          }
        } else {
          i++;
        }
      }
    }
    return mask;
  }

  /// Finds the next standalone occurrence of `keyword` in `upperSql` starting
  /// at `fromIdx`, requiring whitespace boundaries on both sides (or string edge) so
  /// a column or alias containing the keyword as a substring cannot match. Skips any
  /// positions covered by `quoteMask`. Returns the start index of the keyword
  /// itself, or -1 if not found. Callers that want to insert *before* the keyword (and
  /// preserve the leading whitespace) should use the returned index minus 1 — except
  /// when the keyword is at index 0, in which case there is no preceding boundary char.
  private static int indexOfKeywordWithBoundary(String upperSql, boolean[] quoteMask, String keyword,
      int fromIdx) {
    int idx = fromIdx;
    int len = upperSql.length();
    while (idx <= len - keyword.length()) {
      int found = upperSql.indexOf(keyword, idx);
      if (found < 0) {
        return -1;
      }
      int end = found + keyword.length();
      boolean leftBoundary = found == 0 || isSqlBoundary(upperSql.charAt(found - 1));
      boolean rightBoundary = end == len || isSqlBoundary(upperSql.charAt(end));
      if (leftBoundary && rightBoundary) {
        // Reject if any character of the keyword overlaps a quoted/comment region.
        boolean inMasked = false;
        for (int j = 0; j < keyword.length(); j++) {
          if (quoteMask[found + j]) {
            inMasked = true;
            break;
          }
        }
        if (!inMasked) {
          return found;
        }
      }
      idx = found + 1;
    }
    return -1;
  }

  private static boolean isSqlBoundary(char c) {
    return Character.isWhitespace(c) || c == '(' || c == ')' || c == ',' || c == ';';
  }

  /// Finds the position immediately before the next major SQL clause keyword (GROUP, ORDER,
  /// HAVING, LIMIT) starting from `fromIdx`, skipping over quoted regions and
  /// comments. Returns the index of the boundary char preceding the keyword (so a
  /// substring-split at this position preserves the whitespace), or the end of the string
  /// if no keyword is found. Match is by whitespace boundary on both sides so a keyword
  /// preceded by a newline (e.g. after a line comment) is still detected.
  private static int findClauseEnd(String upperSql, boolean[] quoteMask, int fromIdx) {
    String[] keywords = {"GROUP", "ORDER", "HAVING", "LIMIT"};
    int minIdx = upperSql.length();
    for (String keyword : keywords) {
      int kw = indexOfKeywordWithBoundary(upperSql, quoteMask, keyword, fromIdx);
      // Adjust to the boundary char preceding the keyword. The keyword cannot legitimately
      // appear at index 0 of a trimmed SQL (which starts with SELECT), so kw == 0 is
      // unreachable here; defend against it just in case.
      int idx = (kw < 0) ? -1 : (kw == 0 ? 0 : kw - 1);
      if (idx >= 0 && idx < minIdx) {
        minIdx = idx;
      }
    }
    return minIdx;
  }

  /// Per-mode in-flight task counts for one MV table, derived from the live task configs.
  /// APPEND uses the count to enforce `maxTasksPerBatch`. DELETE and OVERWRITE each
  /// gate themselves at "single concurrent task" — and they are also mutually exclusive with
  /// each other (both touch existing MV segments via segment-replace), so each step checks
  /// both counts.
  ///
  /// Atomic fields are used defensively: context implementations usually iterate synchronously,
  /// but a future change to async iteration would otherwise silently miscount and let
  /// DELETE/OVERWRITE schedule simultaneously, double-replacing segments.
  private static final class InFlightTaskCounts {
    final AtomicInteger _inFlightAppendCount = new AtomicInteger();
    final AtomicInteger _inFlightDeleteCount = new AtomicInteger();
    final AtomicInteger _inFlightOverwriteCount = new AtomicInteger();
    final AtomicLong _maxInFlightAppendWindowEndMs;

    InFlightTaskCounts(long initialMaxAppendWindowEndMs) {
      _maxInFlightAppendWindowEndMs = new AtomicLong(initialMaxAppendWindowEndMs);
    }

    int exclusiveModeCount() {
      return _inFlightDeleteCount.get() + _inFlightOverwriteCount.get();
    }
  }

  /// Walks the live task configs for `offlineTableName` once and buckets per-mode.
  /// Required because OVERWRITE/DELETE and APPEND must have independent gates — sharing
  /// a single "incompleteTasks" check made an in-flight APPEND silently block any DELETE
  /// of a stale partition.
  private InFlightTaskCounts countInFlightTasks(String offlineTableName, String taskType,
      long watermarkMs) {
    InFlightTaskCounts counts = new InFlightTaskCounts(watermarkMs);
    _context.forRunningTasks(offlineTableName, taskType, cfg -> {
      String mode = cfg.get(MaterializedViewTask.TASK_MODE_KEY);
      Preconditions.checkState(mode != null && !mode.isEmpty(),
          "In-flight task missing %s for table: %s — buildTaskConfig must always set the mode",
          MaterializedViewTask.TASK_MODE_KEY, offlineTableName);
      if (MaterializedViewTask.TASK_MODE_APPEND.equals(mode)) {
        String windowEndStr = cfg.get(MaterializedViewTask.WINDOW_END_MS_KEY);
        Preconditions.checkState(windowEndStr != null,
            "In-flight APPEND task missing %s for table: %s — buildTaskConfig must always set it",
            MaterializedViewTask.WINDOW_END_MS_KEY, offlineTableName);
        long end = Long.parseLong(windowEndStr);
        counts._maxInFlightAppendWindowEndMs.accumulateAndGet(end, Math::max);
        counts._inFlightAppendCount.incrementAndGet();
      } else if (MaterializedViewTask.TASK_MODE_DELETE.equals(mode)) {
        counts._inFlightDeleteCount.incrementAndGet();
      } else if (MaterializedViewTask.TASK_MODE_OVERWRITE.equals(mode)) {
        counts._inFlightOverwriteCount.incrementAndGet();
      } else {
        // Forward-compat: unknown mode (e.g. a future REBUILD/BACKFILL added by a newer
        // controller). Don't classify it as APPEND — that would silently miscount. Warn so
        // operators see the version mismatch.
        LOGGER.warn("Unknown task mode '{}' for in-flight task in table: {}; ignoring for "
            + "scheduling decisions", mode, offlineTableName);
      }
    });
    return counts;
  }

  /// Reads the scheduling watermark from MaterializedViewRuntimeMetadata or initialises it
  /// on cold-start by finding the minimum segment start time from the source table
  /// and aligning it to the bucket boundary.
  ///
  /// On cold-start, `watermarkMs` is 0 and the partitions map is empty, so the broker
  /// will not attempt split queries against the empty MV table. The first successful APPEND
  /// (via the executor) will advance `watermarkMs` and add the partition entry.
  @VisibleForTesting
  long getWatermarkMs(String viewTableName, String sourceTableName, long bucketMs, String definedSQL,
      Map<String, String> taskConfigs) {
    String viewTableWithType = TableNameBuilder.OFFLINE.tableNameWithType(viewTableName);
    HelixPropertyStore<ZNRecord> propertyStore = _context.getPropertyStore();
    MaterializedViewRuntimeMetadata runtime =
        MaterializedViewRuntimeMetadataUtils.fetch(propertyStore, viewTableWithType);

    if (runtime != null) {
      return runtime.getWatermarkMs();
    }

    // Cold-start: find the earliest segment start time from the source table
    String sourceTableWithType = resolveSourceTableNameWithType(sourceTableName);
    List<SegmentZKMetadata> segmentsZKMetadata = _context.getSegmentsZKMetadata(sourceTableWithType);

    long minStartTimeMs = Long.MAX_VALUE;
    for (SegmentZKMetadata segmentZKMetadata : segmentsZKMetadata) {
      long startTimeMs = segmentZKMetadata.getStartTimeMs();
      if (startTimeMs >= 0) {
        minStartTimeMs = Math.min(minStartTimeMs, startTimeMs);
      }
    }
    Preconditions.checkState(minStartTimeMs != Long.MAX_VALUE,
        "No valid segments found in source table: %s for cold-start watermark", sourceTableName);

    long watermarkMs = Math.floorDiv(minStartTimeMs, bucketMs) * bucketMs;

    // Empty partitions map on cold-start: the broker treats every bucket as "not covered"
    // until the first APPEND populates the map.  Freshness is now derived on read from
    // (now - watermarkMs) against the per-table staleness SLO.
    MaterializedViewRuntimeMetadata newRuntime = new MaterializedViewRuntimeMetadata(
        viewTableWithType, watermarkMs, new HashMap<>());
    // Create-if-absent: two scheduler runs racing on cold-start would otherwise blind-clobber
    // each other.  If a concurrent writer already created the znode, fall back to fetching
    // their value rather than overwriting — their persisted state may already include
    // updates from the consistency manager / executor.
    if (MaterializedViewRuntimeMetadataUtils.createIfAbsent(propertyStore, newRuntime)) {
      LOGGER.info("Cold-start: initialized MaterializedViewRuntimeMetadata with watermark {} for MV table: {}",
          watermarkMs, viewTableWithType);
    } else {
      MaterializedViewRuntimeMetadata existing =
          MaterializedViewRuntimeMetadataUtils.fetch(propertyStore, viewTableWithType);
      if (existing != null) {
        LOGGER.info("Cold-start: another writer already initialized MV runtime metadata for {} "
            + "(watermark={}); using existing values.", viewTableWithType, existing.getWatermarkMs());
        watermarkMs = existing.getWatermarkMs();
      }
    }

    // Initialize MaterializedViewDefinitionMetadata with base table info and partition expression maps
    Schema viewSchema = _context.getTableSchema(viewTableWithType);
    Map<String, String> partitionExprMaps = (viewSchema != null)
        ? MaterializedViewAnalyzer.extractPartitionExprMaps(definedSQL, viewSchema)
        : new HashMap<>();

    // Resolve split spec from both the source and MV tables' time columns.  The MV column
    // is enforced TIMESTAMP by MaterializedViewAnalyzer (no format needed there); the base
    // column may use any format, so we persist its `DateTimeFieldSpec.getFormat()` for the
    // broker's base-side filter conversion.
    DateTimeFieldSpec sourceTimeFieldSpec = resolveSourceTimeFieldSpec(sourceTableName);
    String sourceTimeColumn = sourceTimeFieldSpec.getName();
    String sourceTimeFormat = sourceTimeFieldSpec.getFormat();
    String viewTimeColumn = resolveMaterializedViewTimeColumn(viewTableWithType);
    MaterializedViewSplitSpec splitSpec =
        new MaterializedViewSplitSpec(sourceTimeColumn, sourceTimeFormat, viewTimeColumn, bucketMs);

    long stalenessThresholdMs = parseLong(
        taskConfigs.get(MaterializedViewTask.STALENESS_THRESHOLD_MS_KEY),
        MaterializedViewTask.DEFAULT_STALENESS_THRESHOLD_MS);
    MaterializedViewDefinitionMetadata definition = new MaterializedViewDefinitionMetadata(
        viewTableWithType,
        Collections.singletonList(sourceTableName),
        definedSQL,
        partitionExprMaps,
        splitSpec,
        stalenessThresholdMs,
        /*rewriteEnabled=*/ true);
    // Create-if-absent so a racing scheduler run cannot clobber definition metadata that may
    // have diverged due to a concurrent schema update (different `partitionExprMaps` / split
    // spec).  If the znode already exists, the controller-side definition writer (or another
    // scheduler) already wrote it; we leave that authoritative copy in place.
    if (MaterializedViewDefinitionMetadataUtils.createIfAbsent(propertyStore, definition)) {
      LOGGER.info("Cold-start: initialized MaterializedViewDefinitionMetadata for MV table: {} with source table: {}",
          viewTableWithType, sourceTableName);
    } else {
      LOGGER.info("Cold-start: MaterializedViewDefinitionMetadata for MV table: {} already exists; "
          + "leaving authoritative copy in place.", viewTableWithType);
    }

    return watermarkMs;
  }

  /// Resolves the source table name with type suffix. Tries OFFLINE first, then REALTIME.
  private String resolveSourceTableNameWithType(String rawSourceTableName) {
    String sourceTableWithType = TableNameBuilder.OFFLINE.tableNameWithType(rawSourceTableName);
    TableConfig sourceTableConfig = _context.getTableConfig(sourceTableWithType);
    if (sourceTableConfig != null) {
      return sourceTableWithType;
    }
    sourceTableWithType = TableNameBuilder.REALTIME.tableNameWithType(rawSourceTableName);
    sourceTableConfig = _context.getTableConfig(sourceTableWithType);
    Preconditions.checkState(sourceTableConfig != null,
        "Source table config not found for: %s", rawSourceTableName);
    return sourceTableWithType;
  }

  /// Computes a [PartitionFingerprint] by fetching segments from ZK.
  private PartitionFingerprint computeWindowFingerprint(String sourceTableWithType,
      long windowStartMs, long windowEndMs) {
    return computeWindowFingerprint(_context.getSegmentsZKMetadata(sourceTableWithType),
        windowStartMs, windowEndMs);
  }

  /// Computes a [PartitionFingerprint] for the given time window from pre-fetched
  /// segment metadata.
  ///
  /// The fingerprint is `Hashing.farmHashFingerprint64` over the sorted concatenation of
  /// `<segmentName>\0<crc>\n` lines. Sorting makes the hash insensitive to listing order;
  /// FarmHash64 is non-cryptographic but collision-resistant for non-adversarial inputs.
  /// Replaces a previous XOR-CRC scheme that exhibited cancellation collisions (swap two
  /// segments with the same combined contribution → identical fingerprint).
  private PartitionFingerprint computeWindowFingerprint(List<SegmentZKMetadata> allSegments,
      long windowStartMs, long windowEndMs) {
    List<SegmentZKMetadata> overlapping = new ArrayList<>();
    for (SegmentZKMetadata seg : allSegments) {
      long segStartMs = seg.getStartTimeMs();
      long segEndMs = seg.getEndTimeMs();
      if (segStartMs < windowEndMs && segEndMs >= windowStartMs) {
        overlapping.add(seg);
      }
    }
    overlapping.sort(Comparator.comparing(SegmentZKMetadata::getSegmentName));

    Hasher hasher = Hashing.farmHashFingerprint64().newHasher();
    for (SegmentZKMetadata seg : overlapping) {
      hasher.putString(seg.getSegmentName(), StandardCharsets.UTF_8);
      hasher.putByte((byte) 0);
      hasher.putLong(seg.getCrc());
      hasher.putByte((byte) '\n');
    }
    long crcFingerprint = hasher.hash().asLong();
    LOGGER.info("Computed partition fingerprint for window [{}, {}): segmentCount={}, crcFingerprint={}",
        windowStartMs, windowEndMs, overlapping.size(), crcFingerprint);
    return new PartitionFingerprint(overlapping.size(), crcFingerprint);
  }
}
