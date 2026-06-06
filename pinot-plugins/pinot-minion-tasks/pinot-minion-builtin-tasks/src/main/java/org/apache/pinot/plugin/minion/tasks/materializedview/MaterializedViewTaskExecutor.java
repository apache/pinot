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
package org.apache.pinot.plugin.minion.tasks.materializedview;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.helix.HelixManager;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.auth.AuthProviderUtils;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadataCustomMapModifier;
import org.apache.pinot.common.restlet.resources.StartReplaceSegmentsRequest;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.materializedview.executor.MaterializedViewQueryExecutor;
import org.apache.pinot.materializedview.metadata.MaterializedViewPartitionManager;
import org.apache.pinot.materializedview.metadata.MaterializedViewRuntimeMetadata;
import org.apache.pinot.materializedview.metadata.MaterializedViewRuntimeMetadataUtils;
import org.apache.pinot.materializedview.metadata.PartitionFingerprint;
import org.apache.pinot.materializedview.metadata.PartitionInfo;
import org.apache.pinot.materializedview.metadata.PartitionState;
import org.apache.pinot.materializedview.scheduler.MaterializedViewTaskUtils;
import org.apache.pinot.minion.MinionConf;
import org.apache.pinot.minion.MinionContext;
import org.apache.pinot.minion.event.MinionEventObserver;
import org.apache.pinot.minion.event.MinionEventObservers;
import org.apache.pinot.minion.executor.MinionTaskZkMetadataManager;
import org.apache.pinot.plugin.minion.tasks.BaseTaskExecutor;
import org.apache.pinot.plugin.minion.tasks.SegmentConversionResult;
import org.apache.pinot.plugin.minion.tasks.SegmentConversionUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.CommonConstants.MaterializedViewTask;
import org.apache.pinot.spi.utils.Obfuscator;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Executor for [MaterializedViewTask].
///
/// This task receives a SQL query with an appended time range (from the generator),
/// executes it via a pluggable [MaterializedViewQueryExecutor] (e.g. gRPC, Arrow Flight),
/// and builds a segment from the query results for the MV table.
///
/// Lifecycle:
///
///   - `preProcess` – validates watermark against windowStartMs
///   - `executeTask` – queries broker, builds segment, uploads
///   - `postProcess` – advances watermark to windowEndMs
///
public class MaterializedViewTaskExecutor extends BaseTaskExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(MaterializedViewTaskExecutor.class);

  private final MinionTaskZkMetadataManager _minionTaskZkMetadataManager;
  private final MinionConf _minionConf;
  private final MaterializedViewQueryExecutor _queryExecutor;

  public MaterializedViewTaskExecutor(MinionTaskZkMetadataManager minionTaskZkMetadataManager,
      MinionConf minionConf, MaterializedViewQueryExecutor queryExecutor) {
    _minionTaskZkMetadataManager = minionTaskZkMetadataManager;
    _minionConf = minionConf;
    _queryExecutor = queryExecutor;
  }

  public void preProcess(PinotTaskConfig pinotTaskConfig) {
    Map<String, String> configs = pinotTaskConfig.getConfigs();
    String tableName = configs.get(MinionConstants.TABLE_NAME_KEY);
    String taskMode = configs.getOrDefault(MaterializedViewTask.TASK_MODE_KEY,
        MaterializedViewTask.TASK_MODE_APPEND);
    long windowStartMs = Long.parseLong(configs.get(MaterializedViewTask.WINDOW_START_MS_KEY));

    // Fetch MaterializedViewRuntimeMetadata for watermark validation and optimistic locking
    HelixPropertyStore<ZNRecord> propertyStore = MINION_CONTEXT.getHelixPropertyStore();
    Stat stat = new Stat();
    MaterializedViewRuntimeMetadata runtime = MaterializedViewRuntimeMetadataUtils.fetchWithVersion(
        propertyStore, tableName, stat);

    if (runtime != null) {
      if (MaterializedViewTask.TASK_MODE_APPEND.equals(taskMode)) {
        Preconditions.checkState(runtime.getWatermarkMs() <= windowStartMs,
            "watermarkMs %d should not be larger than windowStartMs %d for table %s",
            runtime.getWatermarkMs(), windowStartMs, tableName);
      } else if (MaterializedViewTask.TASK_MODE_OVERWRITE.equals(taskMode)) {
        PartitionInfo partitionInfo = runtime.getPartitions().get(windowStartMs);
        Preconditions.checkState(partitionInfo != null && partitionInfo.getState() == PartitionState.STALE,
            "Overwrite target partition %d should exist and be STALE for table %s",
            windowStartMs, tableName);
      } else if (MaterializedViewTask.TASK_MODE_DELETE.equals(taskMode)) {
        // DELETE is an executor-internal cleanup triggered when the scheduler finds the source
        // data for a STALE partition has been retention-deleted (zero overlapping segments).
        // The partition must exist and be STALE; the executor drops the corresponding MV
        // segments and rewrites the entry to `VALID + PartitionFingerprint.EMPTY` so the
        // partition stays "tracked but empty" instead of disappearing from the map.  Keeping
        // the entry around removes `absent` as a runtime state, so a later backfill into the
        // same window flips VALID → STALE → OVERWRITE through the normal path; if we removed
        // the entry, the consistency manager (which only flips existing VALID entries) would
        // not re-mark it and the backfill would not propagate.  The VALID-empty write is itself
        // guarded by a commit-time emptiness re-check (see executeDeleteTask / clearValid): if a
        // backfill landed between dispatch and commit, the bucket is left STALE for OVERWRITE.
        PartitionInfo partitionInfo = runtime.getPartitions().get(windowStartMs);
        Preconditions.checkState(partitionInfo != null && partitionInfo.getState() == PartitionState.STALE,
            "Delete target partition %d should exist and be STALE for table %s",
            windowStartMs, tableName);
      }
    } else {
      // No runtime znode: the scheduler's cold-start path (which creates the runtime metadata
      // before dispatching any task) has not run, or the znode was deleted (e.g. DROP MV).  All
      // commit-time ops (appendValid / refreshValid / clearValid) REQUIRE an existing runtime
      // znode and would throw at postProcess — but only AFTER executeTask has run the query and
      // committed segment lineage, leaving MV segments active yet untracked by the partition-state
      // engine.  Fail fast here, before any side effects, so Helix retries cleanly once the runtime
      // exists (or surfaces a real missing-runtime fault to the operator).
      throw new IllegalStateException("MaterializedViewRuntimeMetadata for table: " + tableName
          + " not found; the scheduler must initialize the runtime znode before tasks run. "
          + "Failing fast before query execution and segment-lineage mutation.");
    }
  }

  @Override
  public SegmentConversionResult executeTask(PinotTaskConfig pinotTaskConfig)
      throws Exception {
    preProcess(pinotTaskConfig);

    MinionEventObserver eventObserver =
        MinionEventObservers.getInstance().getMinionEventObserver(pinotTaskConfig.getTaskId());

    Map<String, String> configs = pinotTaskConfig.getConfigs();
    String taskType = pinotTaskConfig.getTaskType();
    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("Starting task: {} with configs: {}", taskType, Obfuscator.DEFAULT.toJsonString(configs));
    }

    String tableName = configs.get(MinionConstants.TABLE_NAME_KEY);
    long windowStartMs = Long.parseLong(configs.get(MaterializedViewTask.WINDOW_START_MS_KEY));
    long windowEndMs = Long.parseLong(configs.get(MaterializedViewTask.WINDOW_END_MS_KEY));
    String taskMode = configs.getOrDefault(MaterializedViewTask.TASK_MODE_KEY,
        MaterializedViewTask.TASK_MODE_APPEND);

    // DELETE mode: skip query execution; remove existing MV segments and rewrite the runtime
    // PartitionInfo to VALID-empty (see executeDeleteTask / postProcess), unless a commit-time
    // re-check finds the source window was backfilled — in which case the bucket is left STALE.
    if (MaterializedViewTask.TASK_MODE_DELETE.equals(taskMode)) {
      return executeDeleteTask(pinotTaskConfig, eventObserver, tableName, windowStartMs, windowEndMs);
    }
    PartitionFingerprint taskFingerprint = getTaskFingerprint(configs, tableName, windowStartMs);
    validateSourceFingerprintAtCommit(configs, tableName, windowStartMs, windowEndMs, taskFingerprint);

    String definedSQL = configs.get(MaterializedViewTask.DEFINED_SQL_KEY);
    LOGGER.info("MaterializedViewTask for table: {}, window: [{}, {}), SQL: {}",
        tableName, windowStartMs, windowEndMs, definedSQL);

    TableConfig tableConfig = getTableConfig(tableName);
    Schema schema = getSchema(tableName);

    eventObserver.notifyProgress(pinotTaskConfig, "Executing query for MV table: " + tableName);
    AuthProvider authProvider = resolveAuthProvider(configs);
    String uploadURL = configs.get(MinionConstants.UPLOAD_URL_KEY);
    Map<String, String> authHeaders = AuthProviderUtils.makeAuthHeadersMap(authProvider);

    String maxRecordsStr = configs.get(MaterializedViewTask.MAX_NUM_RECORDS_PER_SEGMENT_KEY);
    int maxNumRecordsPerSegment = maxRecordsStr != null
        ? Integer.parseInt(maxRecordsStr)
        : MaterializedViewTask.DEFAULT_MAX_NUM_RECORDS_PER_SEGMENT;
    int effectiveLimit = MaterializedViewTaskUtils.parseEffectiveLimit(configs, tableName);

    // Generate a per-attempt UUID so segment names are unique across retries of the same window.
    // Helix reuses the same subtask id (PinotTaskConfig#getTaskId) on every retry, so we cannot
    // rely on taskId for uniqueness — a retry after a partial upload would reproduce identical
    // names and the controller would reject the new lineage entry.
    String attemptId = UUID.randomUUID().toString();

    File tempDir = new File(FileUtils.getTempDirectory(),
        "materialized_view_task_" + tableName + "_" + attemptId);
    FileUtils.forceMkdir(tempDir);

    try {
      // Phase 1: Stream the gRPC response one frame at a time and build segments chunk-by-chunk.
      // Heap residency is bounded by `maxNumRecordsPerSegment` rows (one in-flight segment's
      // worth of buffered GenericRows) regardless of total window size.  Saturation against
      // `effectiveLimit` is checked as rows arrive, so an over-limit window fails BEFORE
      // committing any segment.
      List<SegmentConversionResult> conversionResults = new ArrayList<>();
      List<File> tarFiles = new ArrayList<>();
      long totalRows = 0L;
      int segIdx = 0;

      try (MaterializedViewQueryExecutor.QueryHandle queryHandle =
          _queryExecutor.executeQuery(definedSQL, authHeaders)) {
        DataSchema dataSchema = queryHandle.getDataSchema();
        FieldSpec[] fieldSpecs = resolveOutputFieldSpecs(dataSchema, schema);
        Iterator<Object[]> rowIterator = queryHandle.rows();

        List<GenericRow> chunkBuffer = new ArrayList<>(Math.min(maxNumRecordsPerSegment, 4096));
        while (rowIterator.hasNext()) {
          chunkBuffer.add(toGenericRow(dataSchema.getColumnNames(), fieldSpecs, rowIterator.next()));
          totalRows++;
          if (chunkBuffer.size() >= maxNumRecordsPerSegment) {
            buildSegmentForChunk(tableName, windowStartMs, windowEndMs, attemptId, segIdx,
                chunkBuffer, tableConfig, schema, tempDir, conversionResults, tarFiles,
                eventObserver, pinotTaskConfig);
            chunkBuffer = new ArrayList<>(maxNumRecordsPerSegment);
            segIdx++;
          }
        }
        if (!chunkBuffer.isEmpty()) {
          buildSegmentForChunk(tableName, windowStartMs, windowEndMs, attemptId, segIdx,
              chunkBuffer, tableConfig, schema, tempDir, conversionResults, tarFiles,
              eventObserver, pinotTaskConfig);
          segIdx++;
        }
      }

      // Completeness gate: the broker enforces LIMIT by truncating at exactly N rows, so a query
      // that genuinely has more than N rows returns exactly N. Treat `totalRows == effectiveLimit`
      // as a saturation: we cannot distinguish "exactly N" from "≥ N truncated". Fail BEFORE
      // any segment is committed via lineage so the partition is not marked VALID against
      // truncated data; the chunk-build path above only stages files to disk, no ZK / lineage
      // mutation has happened yet.
      if (totalRows >= effectiveLimit) {
        MaterializedViewTaskUtils.failOnSaturation(tableName, windowStartMs, windowEndMs,
            totalRows, effectiveLimit);
      }

      LOGGER.info("Query streamed {} rows for table: {} into {} segment(s)",
          totalRows, tableName, conversionResults.size());

      if (totalRows == 0L) {
        LOGGER.info("No data returned for window [{}, {}) of table: {}.", windowStartMs, windowEndMs, tableName);
        if (MaterializedViewTask.TASK_MODE_OVERWRITE.equals(taskMode)) {
          validateSourceFingerprintAtCommit(configs, tableName, windowStartMs, windowEndMs, taskFingerprint);
          replaceWindowSegments(tableName, windowStartMs, windowEndMs, Collections.emptyList(),
              uploadURL, authProvider);
        }
        postProcess(pinotTaskConfig);
        return new SegmentConversionResult.Builder()
            .setTableNameWithType(tableName)
            .build();
      }

      int numSegments = conversionResults.size();

      // Phase 2: Segment lineage — find old segments and start replace
      List<String> segmentsTo = new ArrayList<>();
      for (SegmentConversionResult r : conversionResults) {
        segmentsTo.add(r.getSegmentName());
      }

      validateSourceFingerprintAtCommit(configs, tableName, windowStartMs, windowEndMs, taskFingerprint);
      String lineageEntryId =
          startWindowSegmentReplace(tableName, windowStartMs, windowEndMs, segmentsTo, uploadURL, authProvider);

      try {
        // Phase 3: Upload all segments
        for (int i = 0; i < conversionResults.size(); i++) {
          SegmentConversionResult result = conversionResults.get(i);
          File tarFile = tarFiles.get(i);
          String segmentName = result.getSegmentName();

          eventObserver.notifyProgress(pinotTaskConfig,
              String.format("Uploading segment %d/%d: %s", i + 1, numSegments, segmentName));

          List<Header> httpHeaders = getSegmentPushMetadataHeaders(pinotTaskConfig, authProvider, result);
          List<NameValuePair> parameters = getSegmentPushCommonParams(tableName);
          SegmentConversionUtils.uploadSegment(configs, httpHeaders, parameters, tableName, segmentName,
              uploadURL, tarFile);

          reportSegmentUploadMetrics(result.getFile(), tableName, taskType);

          LOGGER.info("Successfully uploaded segment {}/{}: {} for table: {}",
              i + 1, numSegments, segmentName, tableName);
        }

        // Phase 4: End segment replace to atomically swap lineage
        if (lineageEntryId != null) {
          validateSourceFingerprintAtCommit(configs, tableName, windowStartMs, windowEndMs, taskFingerprint);
          SegmentConversionUtils.endSegmentReplace(
              tableName, uploadURL, lineageEntryId,
              _minionConf.getEndReplaceSegmentsTimeoutMs(), authProvider);
          LOGGER.info("Ended segment replace for table: {}, lineageEntryId: {}", tableName, lineageEntryId);
        }
      } catch (Exception e) {
        // Best-effort revert of the IN_PROGRESS lineage entry so the next attempt is not blocked.
        // If revert itself fails, the next startSegmentReplace will mark the previous entry as
        // REVERTED and clean up leftover segments — same recovery contract as ConsistentDataPushUtils.
        if (lineageEntryId != null) {
          revertWindowSegmentReplace(tableName, lineageEntryId, uploadURL, authProvider);
        }
        throw e;
      }

      postProcess(pinotTaskConfig);

      return conversionResults.get(conversionResults.size() - 1);
    } finally {
      FileUtils.deleteQuietly(tempDir);
    }
  }

  private void replaceWindowSegments(String tableName, long windowStartMs, long windowEndMs, List<String> segmentsTo,
      String uploadURL, AuthProvider authProvider)
      throws Exception {
    String lineageEntryId =
        startWindowSegmentReplace(tableName, windowStartMs, windowEndMs, segmentsTo, uploadURL, authProvider);
    if (lineageEntryId != null) {
      SegmentConversionUtils.endSegmentReplace(
          tableName, uploadURL, lineageEntryId, _minionConf.getEndReplaceSegmentsTimeoutMs(), authProvider);
      LOGGER.info("Ended segment replace for table: {}, lineageEntryId: {}", tableName, lineageEntryId);
    }
  }

  private String startWindowSegmentReplace(String tableName, long windowStartMs, long windowEndMs,
      List<String> segmentsTo, String uploadURL, AuthProvider authProvider)
      throws Exception {
    String segmentPrefix = tableName + "_" + windowStartMs + "_" + windowEndMs;
    Set<String> allExistingSegments = SegmentConversionUtils.getSegmentNamesForTable(
        tableName, new URI(uploadURL).resolve("/"), authProvider);
    List<String> segmentsFrom = new ArrayList<>();
    for (String name : allExistingSegments) {
      if (name.equals(segmentPrefix) || name.startsWith(segmentPrefix + "_")) {
        segmentsFrom.add(name);
      }
    }

    if (segmentsFrom.isEmpty() && segmentsTo.isEmpty()) {
      return null;
    }
    String lineageEntryId = SegmentConversionUtils.startSegmentReplace(
        tableName, uploadURL, new StartReplaceSegmentsRequest(segmentsFrom, segmentsTo), authProvider);
    LOGGER.info("Started segment replace for table: {}, lineageEntryId: {}, segmentsFrom: {}, segmentsTo: {}",
        tableName, lineageEntryId, segmentsFrom, segmentsTo);
    return lineageEntryId;
  }

  /// Best-effort revert of a started segment replace lineage entry.
  ///
  /// Used when the upload phase or `endSegmentReplace` throws after a successful
  /// `startSegmentReplace`, to avoid an orphaned IN_PROGRESS lineage entry that would
  /// block subsequent task retries. Always treats the table as OFFLINE — MV tables are
  /// always offline by construction.
  ///
  /// Failures are swallowed (logged only): if the controller is unreachable now, the
  /// next `startSegmentReplace` call will mark the previous entry as REVERTED and clean
  /// up any leftover segments.
  private void revertWindowSegmentReplace(String tableNameWithType, String lineageEntryId, String uploadURL,
      AuthProvider authProvider) {
    String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);
    try (FileUploadDownloadClient client = new FileUploadDownloadClient()) {
      URI revertUri = FileUploadDownloadClient.getRevertReplaceSegmentsURI(
          new URI(uploadURL), rawTableName, TableType.OFFLINE.name(), lineageEntryId, true);
      client.revertReplaceSegments(revertUri, authProvider);
      LOGGER.info("Reverted segment replace for table: {}, lineageEntryId: {}", tableNameWithType, lineageEntryId);
    } catch (Exception revertException) {
      LOGGER.error("Failed to revert segment replace for table: {}, lineageEntryId: {}. Next "
              + "startSegmentReplace will clean up the orphaned entry.",
          tableNameWithType, lineageEntryId, revertException);
    }
  }

  /// Handles DELETE mode: removes all existing MV segments for the given time window
  /// via segment lineage replace (segmentsFrom=[old segments], segmentsTo=[]).  No query
  /// is executed and no new MV segments are created; the runtime PartitionInfo is rewritten
  /// to `VALID + PartitionFingerprint.EMPTY` by [#postProcess], so the partition stays
  /// tracked-but-empty rather than disappearing.
  ///
  /// A commit-time emptiness re-check guards both the segment delete (here) and the VALID-empty
  /// write ([#postProcess] / [MaterializedViewPartitionManager#clearValid]): if the source window
  /// was backfilled after the scheduler dispatched DELETE, this method aborts without mutating MV
  /// segments or partition state, leaving the bucket STALE so the next scheduling cycle
  /// re-materializes it via OVERWRITE.
  private SegmentConversionResult executeDeleteTask(PinotTaskConfig pinotTaskConfig,
      MinionEventObserver eventObserver, String tableName, long windowStartMs, long windowEndMs)
      throws Exception {
    Map<String, String> configs = pinotTaskConfig.getConfigs();
    String uploadURL = configs.get(MinionConstants.UPLOAD_URL_KEY);
    AuthProvider authProvider = resolveAuthProvider(configs);

    // Commit-time backfill guard: the scheduler dispatched DELETE because the source window had
    // zero overlapping segments, but a backfill may have landed since.  If the source is no
    // longer empty, abort before touching MV segments or partition state — leaving the bucket
    // STALE so the next scheduling cycle re-materializes it via OVERWRITE.  Skipping the
    // segment-replace delete here also avoids a transient empty-results window: the existing
    // (stale) MV segments keep serving until OVERWRITE replaces them — the normal STALE contract —
    // instead of the bucket briefly going empty.  postProcess() re-checks the same condition right
    // before the VALID-empty write (see clearValid) to close the narrow window where a backfill
    // lands during the delete below.
    PartitionFingerprint sourceFingerprint =
        computeSourceWindowFingerprint(configs, tableName, windowStartMs, windowEndMs);
    if (shouldAbortDeleteForBackfill(sourceFingerprint)) {
      LOGGER.warn("DELETE aborted for MV table: {}, window: [{}, {}): source backfilled with {} "
              + "overlapping segment(s) after dispatch. Leaving partition STALE for OVERWRITE.",
          tableName, windowStartMs, windowEndMs, sourceFingerprint.getSegmentCount());
      return new SegmentConversionResult.Builder()
          .setTableNameWithType(tableName)
          .build();
    }

    LOGGER.info("DELETE task for table: {}, window: [{}, {}). Removing MV segments.",
        tableName, windowStartMs, windowEndMs);
    eventObserver.notifyProgress(pinotTaskConfig,
        "Deleting MV segments for window [" + windowStartMs + ", " + windowEndMs + ")");

    String segmentPrefix = tableName + "_" + windowStartMs + "_" + windowEndMs;
    Set<String> allExistingSegments = SegmentConversionUtils.getSegmentNamesForTable(
        tableName, new URI(uploadURL).resolve("/"), authProvider);
    List<String> segmentsFrom = new ArrayList<>();
    for (String name : allExistingSegments) {
      if (name.equals(segmentPrefix) || name.startsWith(segmentPrefix + "_")) {
        segmentsFrom.add(name);
      }
    }

    if (!segmentsFrom.isEmpty()) {
      List<String> segmentsTo = Collections.emptyList();
      String lineageEntryId = SegmentConversionUtils.startSegmentReplace(
          tableName, uploadURL,
          new StartReplaceSegmentsRequest(segmentsFrom, segmentsTo),
          authProvider);
      LOGGER.info("Started segment delete-replace for table: {}, lineageEntryId: {}, segmentsFrom: {}",
          tableName, lineageEntryId, segmentsFrom);

      SegmentConversionUtils.endSegmentReplace(
          tableName, uploadURL, lineageEntryId,
          _minionConf.getEndReplaceSegmentsTimeoutMs(), authProvider);
      LOGGER.info("Ended segment delete-replace for table: {}, lineageEntryId: {}", tableName, lineageEntryId);
    } else {
      LOGGER.info("No existing segments found for prefix: {} in table: {}. Nothing to delete.",
          segmentPrefix, tableName);
    }

    postProcess(pinotTaskConfig);

    return new SegmentConversionResult.Builder()
        .setTableNameWithType(tableName)
        .build();
  }

  public void postProcess(PinotTaskConfig pinotTaskConfig) {
    Map<String, String> configs = pinotTaskConfig.getConfigs();
    String tableName = configs.get(MinionConstants.TABLE_NAME_KEY);
    String taskMode = configs.getOrDefault(MaterializedViewTask.TASK_MODE_KEY,
        MaterializedViewTask.TASK_MODE_APPEND);
    long windowStartMs = Long.parseLong(configs.get(MaterializedViewTask.WINDOW_START_MS_KEY));
    long windowEndMs = Long.parseLong(configs.get(MaterializedViewTask.WINDOW_END_MS_KEY));

    MaterializedViewPartitionManager partitionManager = new MaterializedViewPartitionManager(
        MINION_CONTEXT.getHelixPropertyStore(),
        MaterializedViewTaskExecutor::readMinionClusterConfig);

    if (MaterializedViewTask.TASK_MODE_DELETE.equals(taskMode)) {
      // DELETE writes `VALID + PartitionFingerprint.EMPTY` rather than removing the entry, but
      // only after a commit-time re-check that the source window is still empty.  The scheduler
      // dispatched DELETE because the source had zero overlapping segments; a backfill may have
      // landed since.  The supplier recomputes the source fingerprint from live ZK metadata and
      // is invoked inside clearValid's CAS loop, so the source is re-read on every attempt
      // immediately before the write; clearValid leaves the bucket STALE (so the next scheduling
      // cycle re-materializes it via OVERWRITE) when the source is no longer empty — see
      // `MaterializedViewPartitionManager#clearValid`.
      partitionManager.clearValid(tableName, windowStartMs,
          () -> computeSourceWindowFingerprint(configs, tableName, windowStartMs, windowEndMs));
      return;
    }

    // Compute and validate the source fingerprint ONCE before mutating the runtime znode.
    // Both operations are deterministic given the source-side ZK state: if validation fails
    // on this attempt (real source drift), the manager's CAS retry cannot succeed and would
    // mask the actionable error.  The CAS retry that lives inside the manager exists only
    // to absorb concurrent ConsistencyManager STALE markings on the runtime znode itself.
    PartitionFingerprint newFingerprint = getTaskFingerprint(configs, tableName, windowStartMs);
    validateSourceFingerprintAtCommit(configs, tableName, windowStartMs, windowEndMs, newFingerprint);

    if (MaterializedViewTask.TASK_MODE_APPEND.equals(taskMode)) {
      // APPEND advances watermarkMs through the manager's contiguous-VALID walk over the
      // resulting map.  Watermark advancement is bundled with the bucket transition because
      // both flow from the same map snapshot; splitting them would create a transient window
      // where the bucket is VALID but the watermark hasn't caught up.
      partitionManager.appendValid(tableName, windowStartMs, windowEndMs, newFingerprint);
    } else if (MaterializedViewTask.TASK_MODE_OVERWRITE.equals(taskMode)) {
      partitionManager.refreshValid(tableName, windowStartMs, newFingerprint);
    } else {
      throw new IllegalStateException(
          "Unknown MV task mode '" + taskMode + "' for table: " + tableName);
    }
  }

  /// Reads a single Helix CLUSTER-scope config value via the minion's `HelixManager`. Returns
  /// `null` when the key is unset or the Helix manager has not yet been initialized. Used by
  /// the executor (and other minion-side MV consumers) to pick up live cluster-config overrides
  /// without a restart.
  static String readMinionClusterConfig(String configName) {
    try {
      HelixManager helixManager = MinionContext.getInstance().getHelixManager();
      if (helixManager == null) {
        return null;
      }
      HelixConfigScope scope = new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER)
          .forCluster(helixManager.getClusterName())
          .build();
      Map<String, String> values =
          helixManager.getClusterManagmentTool().getConfig(scope, Collections.singletonList(configName));
      return values == null ? null : values.get(configName);
    } catch (Exception e) {
      LOGGER.debug("Failed to read minion cluster config '{}': {}", configName, e.getMessage());
      return null;
    }
  }

  private PartitionFingerprint getTaskFingerprint(Map<String, String> configs, String tableName, long windowStartMs) {
    String fingerprintStr = configs.get(MaterializedViewTask.PARTITION_FINGERPRINTS_KEY);
    Preconditions.checkState(fingerprintStr != null && !fingerprintStr.isEmpty(),
        "Missing source partition fingerprint for MV task table %s windowStartMs %s", tableName, windowStartMs);
    Map<Long, PartitionFingerprint> taskFingerprints = PartitionFingerprint.decodeMap(fingerprintStr);
    PartitionFingerprint fingerprint = taskFingerprints.get(windowStartMs);
    Preconditions.checkState(fingerprint != null,
        "Missing source partition fingerprint for MV task table %s windowStartMs %s", tableName, windowStartMs);
    return fingerprint;
  }

  /// Computes the current source-table [PartitionFingerprint] for `[windowStartMs, windowEndMs)`
  /// from live ZK segment metadata.  Shared by the APPEND / OVERWRITE commit-time fingerprint
  /// validation and the DELETE commit-time emptiness re-check.  Requires
  /// [MaterializedViewTask#SOURCE_TABLE_NAME_KEY] in the task config (the scheduler sets it for
  /// every task mode); fails loud if absent so a malformed / pre-upgrade task cannot silently
  /// skip the validation.
  private PartitionFingerprint computeSourceWindowFingerprint(Map<String, String> configs, String tableName,
      long windowStartMs, long windowEndMs) {
    String sourceTableName = configs.get(MaterializedViewTask.SOURCE_TABLE_NAME_KEY);
    Preconditions.checkState(sourceTableName != null && !sourceTableName.isEmpty(),
        "Missing source table name for MV task table %s window [%s, %s)", tableName, windowStartMs, windowEndMs);
    String sourceTableWithType = resolveSourceTableNameWithType(sourceTableName);
    return MaterializedViewTaskUtils.computeWindowFingerprint(
        ZKMetadataProvider.getSegmentsZKMetadata(MINION_CONTEXT.getHelixPropertyStore(), sourceTableWithType),
        windowStartMs, windowEndMs);
  }

  /// Returns `true` when a DELETE task must abort because the source window is no longer empty
  /// (a backfill landed after the scheduler dispatched DELETE on a then-empty source).  A
  /// non-zero `segmentCount` is the canonical "non-empty source window" criterion — identical to
  /// the scheduler's DELETE dispatch test (MaterializedViewTaskScheduler computes the same
  /// fingerprint and dispatches DELETE only when `segmentCount == 0`) and equivalent to comparing
  /// against [PartitionFingerprint#EMPTY], since `computeWindowFingerprint` maps an empty overlap
  /// to `EMPTY`.  Aborting leaves the bucket STALE for OVERWRITE rather than deleting MV segments
  /// or writing VALID-empty over live source data.
  @VisibleForTesting
  static boolean shouldAbortDeleteForBackfill(PartitionFingerprint sourceWindowFingerprint) {
    return sourceWindowFingerprint.getSegmentCount() != 0;
  }

  private void validateSourceFingerprintAtCommit(Map<String, String> configs, String tableName, long windowStartMs,
      long windowEndMs, PartitionFingerprint taskFingerprint) {
    PartitionFingerprint currentFingerprint =
        computeSourceWindowFingerprint(configs, tableName, windowStartMs, windowEndMs);
    Preconditions.checkState(taskFingerprint.equals(currentFingerprint),
        "Source table changed while refreshing MV table %s window [%s, %s): taskFingerprint=%s, "
            + "currentFingerprint=%s. Leaving MV partition stale for retry.",
        tableName, windowStartMs, windowEndMs, taskFingerprint, currentFingerprint);
  }

  private String resolveSourceTableNameWithType(String sourceTableName) {
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(sourceTableName);
    if (tableType != null) {
      return sourceTableName;
    }
    String rawSourceTableName = TableNameBuilder.extractRawTableName(sourceTableName);
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(rawSourceTableName);
    if (ZKMetadataProvider.getTableConfig(MINION_CONTEXT.getHelixPropertyStore(), offlineTableName) != null) {
      return offlineTableName;
    }
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(rawSourceTableName);
    Preconditions.checkState(
        ZKMetadataProvider.getTableConfig(MINION_CONTEXT.getHelixPropertyStore(), realtimeTableName) != null,
        "Source table config not found for: %s", sourceTableName);
    return realtimeTableName;
  }

  @Override
  protected SegmentZKMetadataCustomMapModifier getSegmentZKMetadataCustomMapModifier(
      PinotTaskConfig pinotTaskConfig, SegmentConversionResult segmentConversionResult) {
    return new SegmentZKMetadataCustomMapModifier(
        SegmentZKMetadataCustomMapModifier.ModifyMode.UPDATE, Collections.emptyMap());
  }

  /// Fails the task if the query result set saturated the declared `LIMIT`, since that
  /// strongly suggests the window was truncated and the resulting MV would be incomplete.
  ///
  /// Throwing here (before any segment build or `postProcess`) ensures:
  ///
  ///   - the partition is NOT marked [PartitionState#VALID];
  ///   - the runtime `watermarkMs` / partitions map are NOT advanced, so the broker will not
  ///       rewrite subsequent queries against the incomplete MV;
  ///   - Helix retries the task, letting transient causes self-heal.
  ///
  ///
  /// If the config is missing (older tasks in flight during rolling upgrade) or non-positive,
  /// the task fails loud — silently disabling the saturation gate is exactly the silent-truncation
  /// regression this guard protects against. Helix retries with the same task config, so a
  /// pre-upgrade task without `EFFECTIVE_LIMIT_KEY` will exhaust its retry budget and
  /// surface as a failed task; the operator must regenerate the task once the controller is
  /// upgraded. Documented upgrade order: upgrade controller before minion executors.
  @VisibleForTesting
  static void verifyResultNotTruncated(Map<String, String> configs, String tableName,
      long windowStartMs, long windowEndMs, int actualRows) {
    MaterializedViewTaskUtils.verifyResultNotTruncated(configs, tableName, windowStartMs, windowEndMs, actualRows);
  }

  /// Converts raw query result rows into [GenericRow] objects using column names
  /// from the [DataSchema].
  ///
  /// Each column name returned by the query must exist in the MV [Schema]. A
  /// mismatch indicates the `definedSQL` produced a column the MV table cannot store
  /// (e.g. an alias was renamed, the schema is out of date, or the analyzer mapping is
  /// stale); proceeding would silently drop the column from the persisted segment, so we
  /// fail loud instead. The analyzer enforces this invariant at table-create time, so
  /// hitting it at runtime points at a real correctness drift.
  /// Resolves a per-column `FieldSpec[]` once for the streaming convert loop.  Fails loud if the
  /// gRPC response includes a column not declared in the MV schema — the analyzer enforces this
  /// invariant at table-create time, so hitting it at runtime points at a real correctness drift.
  private static FieldSpec[] resolveOutputFieldSpecs(DataSchema dataSchema, Schema schema) {
    String[] columnNames = dataSchema.getColumnNames();
    FieldSpec[] fieldSpecs = new FieldSpec[columnNames.length];
    for (int i = 0; i < columnNames.length; i++) {
      String columnName = columnNames[i];
      FieldSpec fieldSpec = schema.getFieldSpecFor(columnName);
      Preconditions.checkState(fieldSpec != null,
          "MV query returned column '%s' which is not declared in the MV schema. "
              + "Update the MV schema to include this column or fix the definedSQL projection.",
          columnName);
      fieldSpecs[i] = fieldSpec;
    }
    return fieldSpecs;
  }

  /// Converts a single gRPC-returned row into a `GenericRow` using the pre-resolved field-spec
  /// array.  Allocation cost: one `GenericRow` + one `HashMap` per row; pre-allocated structures
  /// (`columnNames`, `fieldSpecs`) are reused across rows.
  private static GenericRow toGenericRow(String[] columnNames, FieldSpec[] fieldSpecs, Object[] row) {
    GenericRow genericRow = new GenericRow();
    for (int i = 0; i < columnNames.length; i++) {
      String columnName = columnNames[i];
      Object value = row[i];
      FieldSpec fieldSpec = fieldSpecs[i];
      if (fieldSpec.getDataType().getStoredType() == FieldSpec.DataType.BYTES) {
        value = decodeBytesValue(columnName, value);
      }
      genericRow.putValue(columnName, value);
    }
    return genericRow;
  }

  /// Builds, tars, and registers a single segment from one chunk of buffered rows.  Mutates
  /// `conversionResults` and `tarFiles` in place so the upload phase can drive them as a
  /// flat list — same shape the previous list-based implementation used.
  private void buildSegmentForChunk(String tableName, long windowStartMs, long windowEndMs,
      String attemptId, int segIdx, List<GenericRow> chunk, TableConfig tableConfig, Schema schema,
      File tempDir, List<SegmentConversionResult> conversionResults, List<File> tarFiles,
      MinionEventObserver eventObserver, PinotTaskConfig pinotTaskConfig)
      throws Exception {
    String segmentName = MaterializedViewTaskUtils.buildSegmentName(
        tableName, windowStartMs, windowEndMs, attemptId, segIdx);

    File segmentOutputDir = new File(tempDir, "segmentOutput_" + segIdx);
    FileUtils.forceMkdir(segmentOutputDir);

    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
    segmentGeneratorConfig.setTableName(tableName);
    segmentGeneratorConfig.setOutDir(segmentOutputDir.getAbsolutePath());
    segmentGeneratorConfig.setSegmentName(segmentName);

    eventObserver.notifyProgress(pinotTaskConfig,
        String.format("Building segment %d: %s (%d rows)", segIdx, segmentName, chunk.size()));

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(chunk));
    driver.build();

    File segmentDir = new File(segmentOutputDir, segmentName);
    Preconditions.checkState(segmentDir.exists(), "Segment generation failed for: %s", segmentName);

    File segmentTarFile = new File(tempDir, segmentName + TarCompressionUtils.TAR_GZ_FILE_EXTENSION);
    TarCompressionUtils.createCompressedTarFile(segmentDir, segmentTarFile);

    conversionResults.add(new SegmentConversionResult.Builder()
        .setFile(segmentDir)
        .setSegmentName(segmentName)
        .setTableNameWithType(tableName)
        .build());
    tarFiles.add(segmentTarFile);
  }

  @VisibleForTesting
  static Object decodeBytesValue(String columnName, Object value) {
    if (value == null || value instanceof byte[]) {
      return value;
    }
    if (!(value instanceof String)) {
      return value;
    }
    String stringValue = (String) value;
    try {
      return BytesUtils.toBytes(stringValue);
    } catch (IllegalArgumentException hexException) {
      try {
        return Base64.getDecoder().decode(stringValue);
      } catch (IllegalArgumentException base64Exception) {
        base64Exception.addSuppressed(hexException);
        throw new IllegalArgumentException(
            "Cannot decode BYTES value for column: " + columnName + " as hex or base64", base64Exception);
      }
    }
  }

  /// Builds a segment name that is stable within a single attempt but unique across retries of the
  /// same window. The `attemptId` must be a per-invocation value (e.g., a fresh UUID) and
  /// must NOT be the Helix subtask id, which is reused across retries. Using the Helix subtask id
  /// would reproduce identical names on retry, causing the controller to reject the new lineage
  /// entry when segments from a previous partial attempt already exist.
  @VisibleForTesting
  static String buildSegmentName(String tableName, long windowStartMs, long windowEndMs,
      String attemptId, int segIdx) {
    return MaterializedViewTaskUtils.buildSegmentName(tableName, windowStartMs, windowEndMs, attemptId, segIdx);
  }
}
