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
package org.apache.pinot.controller.helix.core.ingest;

import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.math.BigDecimal;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.common.utils.URIUtils;
import org.apache.pinot.controller.api.resources.ControllerFilePathProvider;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.PinotResourceManagerResponse;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.segment.spi.partition.PartitionFunctionFactory;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.ingest.InsertErrorCode;
import org.apache.pinot.spi.ingest.InsertExecutor;
import org.apache.pinot.spi.ingest.InsertRequest;
import org.apache.pinot.spi.ingest.InsertResult;
import org.apache.pinot.spi.ingest.InsertStatementState;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Controller-side INSERT INTO ... VALUES executor that builds a Pinot segment from in-memory
/// rows and uploads it directly to the segment store.
///
/// For OFFLINE tables the segment is generated and uploaded immediately. For REALTIME tables
/// the same approach is used (Pinot supports offline-style segment upload for backfill).
///
/// This executor is designed for interactive / quickstart use cases where low row counts are
/// inserted via SQL. It is not intended for high-throughput production ingestion.
///
/// Instances are thread-safe; each {@link #execute} call uses its own temporary directory.
///
/// ## Known v1 limitation: single-phase visibility
///
/// {@link #execute} uploads segments to the deep store and registers them in Helix IdealState
/// within a single call, returning {@link InsertStatementState#VISIBLE} before the coordinator has
/// persisted that state to ZK. If the subsequent manifest persist fails after {@code
/// persistWithCasRetry}'s three CAS attempts, the data is queryable but the manifest is stuck in
/// `ACCEPTED`. The cleanup sweep deliberately does NOT abort such stuck-ACCEPTED ROW
/// manifests — flipping them to ABORTED would falsely report failure for live data. Instead, the
/// sweep GCs them after a long `ACCEPTED_ROW_RETENTION_MS` window (default 7 days), giving
/// operators time to investigate. Until GC, `/insert/status` will report state=ACCEPTED.
///
/// A proper two-phase protocol (stage-without-register, then finalize on coordinator signal)
/// is a follow-up. Documented so operators can reason about the tradeoff: duplicate data is
/// prevented (we never release the requestId on persist failure), but metric counters and UI
/// status may lag the actual visible state in rare ZK-down-for-minutes scenarios.
public class ControllerRowInsertExecutor implements InsertExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(ControllerRowInsertExecutor.class);

  private static final String SEGMENT_NAME_PREFIX = "insert_";
  private static final String WORKING_DIR_PREFIX = "row_insert_";
  private static final String OUTPUT_SEGMENT_DIR = "output_segment";
  private static final String SEGMENT_TAR_DIR = "segment_tar";

  private final PinotHelixResourceManager _resourceManager;
  private final boolean _allowDestructiveRollback;

  /// Creates a new executor with destructive rollback DISABLED. Partial-batch upload failures
  /// leave already-registered segments in IdealState; operators reconcile manually.
  public ControllerRowInsertExecutor(PinotHelixResourceManager resourceManager) {
    this(resourceManager, false);
  }

  /// Creates a new executor.
  ///
  /// @param resourceManager           the Helix resource manager for table config lookup and segment upload
  /// @param allowDestructiveRollback  if `true`, partial-batch upload failure deletes
  ///     already-registered segments via `_resourceManager.deleteSegment`. This can yank
  ///     segments out from under live queries and is OFF by default. Set via
  ///     `controller.insert.row.allow.destructive.rollback` on interactive/quickstart-tier
  ///     controllers only.
  public ControllerRowInsertExecutor(PinotHelixResourceManager resourceManager, boolean allowDestructiveRollback) {
    _resourceManager = resourceManager;
    _allowDestructiveRollback = allowDestructiveRollback;
  }

  @Override
  public InsertResult execute(InsertRequest request) {
    String statementId = request.getStatementId();
    String tableName = request.getTableName();
    TableType tableType = request.getTableType();
    List<GenericRow> rows = request.getRows();

    LOGGER.info("Executing row insert statement {} for table {} with {} rows", statementId, tableName,
        rows == null ? 0 : rows.size());

    /// 1. Validate rows. The coordinator's submit path pre-rejects empty rows for ROW with
    /// EMPTY_ROWS and state=REJECTED (no manifest persisted) — that's the path real callers hit.
    /// This branch is defense-in-depth for direct executor.execute callers (tests, future
    /// programmatic SPI consumers) that bypass the coordinator; it returns state=ABORTED via
    /// buildErrorResult because by convention REJECTED is reserved for coordinator-tier pre-
    /// acceptance rejections (no-manifest semantics). In v1 an executor only reports terminal
    /// states for which a manifest exists, i.e., ABORTED/VISIBLE (future async paths may emit
    /// ACCEPTED, but the coordinator owns the lifecycle past that point).
    if (rows == null || rows.isEmpty()) {
      return buildErrorResult(statementId, "No rows provided in the insert request.", InsertErrorCode.EMPTY_ROWS);
    }

    /// 2. Resolve the fully-qualified table name
    String tableNameWithType = resolveTableNameWithType(tableName, tableType);

    /// 3. Look up table config and schema
    TableConfig tableConfig = _resourceManager.getTableConfig(tableNameWithType);
    if (tableConfig == null) {
      return buildErrorResult(statementId, "Table not found: " + tableNameWithType, InsertErrorCode.TABLE_NOT_FOUND);
    }

    /// 3a. Validate table mode safety (same rules as FileInsertExecutor)
    String safetyError = validateTableModeSafety(tableConfig);
    if (safetyError != null) {
      return buildErrorResult(statementId, safetyError, InsertErrorCode.TABLE_MODE_REJECTED);
    }

    Schema schema = _resourceManager.getTableSchema(tableNameWithType);
    if (schema == null) {
      return buildErrorResult(statementId, "Schema not found for table: " + tableNameWithType,
          InsertErrorCode.SCHEMA_NOT_FOUND);
    }

    /// 3b. Reject null primary-key values for full upsert tables. PK columns are tracked on the
    /// schema, NOT on the partition config — a previous version of this code conflated the two.
    String pkValidationError = validatePrimaryKeyValues(rows, tableConfig, schema);
    if (pkValidationError != null) {
      return buildErrorResult(statementId, pkValidationError, InsertErrorCode.PRIMARY_KEY_REJECTED);
    }

    /// 4. Split rows by partition to produce correctly partitioned segments
    boolean hasPartitionConfig = tableConfig.getIndexingConfig() != null
        && tableConfig.getIndexingConfig().getSegmentPartitionConfig() != null
        && !tableConfig.getIndexingConfig().getSegmentPartitionConfig().getColumnPartitionMap().isEmpty();
    Map<Integer, List<GenericRow>> partitionedRows;
    try {
      partitionedRows = partitionRows(rows, tableConfig, schema);
    } catch (IllegalArgumentException e) {
      return buildErrorResult(statementId, e.getMessage(), InsertErrorCode.PARTITION_VALUE_REJECTED);
    }

    /// 5. Build all segments first (staging phase), then upload all atomically.
    /// If any build fails, no segments have been uploaded yet so nothing to roll back.
    /// If upload fails partway through, we roll back already-uploaded segments.
    File workingDir = null;
    try {
      workingDir = createWorkingDir(tableNameWithType);
      FileUtils.forceMkdir(workingDir);

      /// Phase 1: Build all segments locally without uploading
      List<StagedSegment> stagedSegments = new ArrayList<>();

      for (Map.Entry<Integer, List<GenericRow>> entry : partitionedRows.entrySet()) {
        int partitionId = entry.getKey();
        List<GenericRow> partitionRows = entry.getValue();

        /// Always include the _p<partitionId> suffix when the table has a partition config, even if
        /// all rows in this batch happen to land on a single partition. Otherwise downstream tooling
        /// that parses the suffix (partition-aware purge tasks, segment lineage by partition) sees
        /// an unsuffixed name and falsely concludes the segment is unpartitioned.
        String segmentName = hasPartitionConfig
            ? SEGMENT_NAME_PREFIX + statementId + "_p" + partitionId
            : SEGMENT_NAME_PREFIX + statementId;

        File partitionDir = new File(workingDir, "partition_" + partitionId);
        File outputDir = new File(partitionDir, OUTPUT_SEGMENT_DIR);
        File segmentTarDir = new File(partitionDir, SEGMENT_TAR_DIR);
        FileUtils.forceMkdir(outputDir);
        FileUtils.forceMkdir(segmentTarDir);

        SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
        segmentGeneratorConfig.setOutDir(outputDir.getAbsolutePath());
        segmentGeneratorConfig.setSegmentName(segmentName);
        segmentGeneratorConfig.setTableName(tableNameWithType);

        GenericRowRecordReader recordReader = new GenericRowRecordReader(partitionRows);
        SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
        driver.init(segmentGeneratorConfig, recordReader);
        driver.build();

        File segmentOutputDir = driver.getOutputDirectory();
        File segmentTarFile = new File(segmentTarDir, segmentName + ".tar.gz");
        TarCompressionUtils.createCompressedTarFile(segmentOutputDir, segmentTarFile);

        stagedSegments.add(new StagedSegment(segmentName, segmentOutputDir, segmentTarFile, partitionRows.size(),
            partitionId));

        LOGGER.info("Staged segment {} ({} rows, partition {})", segmentName, partitionRows.size(), partitionId);
      }

      /// Phase 2: Upload all staged segments. Track what was fully uploaded (deep-store copy AND
      /// Helix register both succeeded) so rollback can call deleteSegment on registered segments.
      /// The failing segment is tracked separately (`pendingSegmentName`) so the manifest doesn't
      /// claim phantom uploads that never landed in IdealState.
      List<String> uploadedSegmentNames = new ArrayList<>();
      String pendingSegmentName = null;
      try {
        for (StagedSegment staged : stagedSegments) {
          pendingSegmentName = staged._segmentName;
          uploadSegment(tableNameWithType, staged._segmentDir, staged._tarFile, staged._segmentName);
          /// Add to the success list ONLY after uploadSegment returns: at that point both the
          /// deep-store copy and the Helix register succeeded. A pre-add-then-throw would put a
          /// phantom name in the manifest's segmentNames if rollback failed.
          uploadedSegmentNames.add(staged._segmentName);
          pendingSegmentName = null;
          LOGGER.info("Uploaded segment {} ({} rows, partition {})",
              staged._segmentName, staged._rowCount, staged._partitionId);
        }
      } catch (Exception uploadEx) {
        if (_allowDestructiveRollback) {
          LOGGER.error("Failed to upload segment during row insert for statement {} on table {}. "
              + "Rolling back {} already-uploaded segments (destructive rollback enabled). Pending "
              + "segment {} may have left orphan tar in deep store (operator cleanup may be required).",
              statementId, tableNameWithType, uploadedSegmentNames.size(), pendingSegmentName, uploadEx);
          rollbackUploadedSegments(tableNameWithType, uploadedSegmentNames);
          return buildErrorResult(statementId,
              "Failed to upload segment (rolled back " + uploadedSegmentNames.size() + " segments): "
                  + uploadEx.getMessage(),
              InsertErrorCode.SEGMENT_UPLOAD_FAILED);
        }
        /// Default: do NOT call deleteSegment on already-registered segments — that would yank
        /// segments out from under live queries on the table. Surface the partial set in the
        /// result so operators can reconcile manually. The pending segment (if any) may have
        /// left an orphan tar in deep store and an unregistered segment in IdealState.
        LOGGER.error("Failed to upload segment during row insert for statement {} on table {}. "
            + "Leaving {} already-uploaded segments registered (destructive rollback disabled). "
            + "Operator must reconcile. Pending segment {} may have left orphan tar in deep store.",
            statementId, tableNameWithType, uploadedSegmentNames.size(), pendingSegmentName, uploadEx);
        return new InsertResult.Builder()
            .setStatementId(statementId)
            .setState(InsertStatementState.ABORTED)
            .setMessage("Failed to upload segment after registering " + uploadedSegmentNames.size()
                + " segments; left in IdealState for operator reconciliation: " + uploadEx.getMessage())
            .setErrorCode(InsertErrorCode.SEGMENT_UPLOAD_FAILED_PARTIAL)
            .setSegmentNames(uploadedSegmentNames)
            .build();
      }

      return new InsertResult.Builder()
          .setStatementId(statementId)
          .setState(InsertStatementState.VISIBLE)
          .setMessage("Successfully inserted " + rows.size() + " rows as " + uploadedSegmentNames.size()
              + " segment(s)")
          .setSegmentNames(uploadedSegmentNames)
          .build();
    } catch (Exception e) {
      LOGGER.error("Failed to execute row insert for statement {} on table {}", statementId, tableNameWithType, e);
      return buildErrorResult(statementId, "Failed to build segment: " + e.getMessage(),
          InsertErrorCode.SEGMENT_BUILD_FAILED);
    } finally {
      FileUtils.deleteQuietly(workingDir);
    }
  }

  @Override
  public void abort(String statementId) {
    /// Row inserts are synchronous and complete within execute(), so abort is a no-op. A trace log
    /// is included so a future maintainer who wires asynchronous row inserts can spot stray abort
    /// calls reaching here without needing to add the log themselves. If trace logging shows up,
    /// re-evaluate the no-op contract before adding rollback logic — the manifest state machine
    /// already handles abort at the coordinator layer.
    LOGGER.trace("ControllerRowInsertExecutor.abort({}): no-op for synchronous ROW path", statementId);
  }

  /// Partitions rows by the table's segment partition config. If no partition config exists,
  /// all rows are placed in partition 0 (single segment).
  ///
  /// Partition values are coerced to the column's declared data type before hashing so that
  /// INSERT-time partitioning matches query-time partitioning (which coerces predicate values to
  /// the declared type). Without coercion, logically-equivalent values (e.g., `BigDecimal(3.0)`
  /// from a SQL literal versus `Long(3)` from a query predicate) would hash to different
  /// partitions, causing query predicate pruning to skip segments that actually contain matching rows.
  ///
  /// For full upsert tables, null primary keys are rejected up-front since they violate the
  /// upsert invariant.
  private Map<Integer, List<GenericRow>> partitionRows(List<GenericRow> rows, TableConfig tableConfig,
      Schema schema) {
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    SegmentPartitionConfig partitionConfig = indexingConfig != null
        ? indexingConfig.getSegmentPartitionConfig() : null;

    if (partitionConfig == null || partitionConfig.getColumnPartitionMap() == null
        || partitionConfig.getColumnPartitionMap().isEmpty()) {
      /// No partition config — single segment
      return Collections.singletonMap(0, rows);
    }

    /// Multi-column configs are rejected by validateTableModeSafety before this point.
    Map<String, ColumnPartitionConfig> columnPartitionMap = partitionConfig.getColumnPartitionMap();
    Map.Entry<String, ColumnPartitionConfig> entry = columnPartitionMap.entrySet().iterator().next();
    String partitionColumn = entry.getKey();
    ColumnPartitionConfig colConfig = entry.getValue();
    int numPartitions = colConfig.getNumPartitions();
    PartitionFunction partitionFunction = PartitionFunctionFactory.getPartitionFunction(
        colConfig.getFunctionName(), numPartitions, colConfig.getFunctionConfig());

    FieldSpec fieldSpec = schema.getFieldSpecFor(partitionColumn);
    FieldSpec.DataType storedType = fieldSpec != null ? fieldSpec.getDataType().getStoredType() : null;

    Map<Integer, List<GenericRow>> partitioned = new HashMap<>();
    for (GenericRow row : rows) {
      Object value = row.getValue(partitionColumn);
      if (value == null) {
        /// Match server-side stream behavior: hash "" through the partition function rather than
        /// defaulting to partition 0. Primary-key null rejection is handled separately in
        /// validatePrimaryKeyValues, before this method is called, so we don't repeat it here.
        int partition = partitionFunction.getPartition("");
        partitioned.computeIfAbsent(partition, k -> new ArrayList<>()).add(row);
        continue;
      }
      String partitionValue = normalizeForPartition(value, storedType);
      int partition = partitionFunction.getPartition(partitionValue);
      partitioned.computeIfAbsent(partition, k -> new ArrayList<>()).add(row);
    }
    return partitioned;
  }

  /// Validates that no row has a null value in any primary-key column when the table is in full
  /// upsert mode. Primary-key columns are configured on the schema, not on the partition config.
  /// Returns an error message if any row violates the invariant, or `null` if all rows are OK
  /// (or the table is not a full-upsert table).
  @Nullable
  @VisibleForTesting
  static String validatePrimaryKeyValues(List<GenericRow> rows, TableConfig tableConfig, Schema schema) {
    UpsertConfig upsertConfig = tableConfig.getUpsertConfig();
    if (upsertConfig == null || upsertConfig.getMode() != UpsertConfig.Mode.FULL) {
      return null;
    }
    List<String> pkColumns = schema.getPrimaryKeyColumns();
    if (pkColumns == null || pkColumns.isEmpty()) {
      /// Schema validation should have rejected this earlier, but fail loudly rather than silently
      /// accepting a full-upsert table with no PK columns.
      return "Full upsert table is missing primary-key columns in schema";
    }
    for (int rowIdx = 0; rowIdx < rows.size(); rowIdx++) {
      GenericRow row = rows.get(rowIdx);
      for (String pkColumn : pkColumns) {
        if (row.getValue(pkColumn) == null) {
          return "Null value in primary-key column '" + pkColumn + "' is not allowed for full upsert tables "
              + "(row index " + rowIdx + ")";
        }
      }
    }
    return null;
  }

  /// Coerces a value to the column's declared stored type then stringifies it, so that
  /// logically-equivalent values hash to the same partition regardless of the source literal's
  /// Java type. Falls back to raw stringification if the column's type is unknown or the coercion
  /// fails (bad data is caught later during segment build).
  @VisibleForTesting
  static String normalizeForPartition(Object value, @Nullable FieldSpec.DataType storedType) {
    if (storedType == null) {
      /// The schema lookup at the call site should have already resolved the column's type. A null
      /// storedType here means the partition column is missing from the schema — coercing through
      /// String.valueOf would silently mis-route the row vs the query path's typed partition lookup,
      /// ending in segment-pruning at query time. Fail loudly so the operator sees the schema bug.
      throw new IllegalArgumentException(
          "Cannot normalize partition value: column has no declared type in schema (value=" + value + ")");
    }
    /// Canonicalize numeric inputs before String conversion. The SQL parser produces BigDecimal for
    /// every numeric literal, so a literal like `3.0` arrives as BigDecimal("3.0"). Without
    /// stripTrailingZeros, String.valueOf yields "3.0" which LONG.convert rejects with
    /// NumberFormatException — even though `INSERT INTO t (id) VALUES (3.0)` is intended as the
    /// value 3 in a LONG-partitioned table. Strip trailing zeros so integral-typed coercion sees
    /// the canonical integer form. Genuinely-fractional values (3.5) still fail at convert time.
    String stringForm;
    if (value instanceof BigDecimal) {
      stringForm = ((BigDecimal) value).stripTrailingZeros().toPlainString();
    } else {
      stringForm = String.valueOf(value);
    }
    Object coerced;
    try {
      coerced = storedType.convert(stringForm);
    } catch (Exception e) {
      /// Don't silently degrade to raw String.valueOf — that risks hashing a malformed value into a
      /// partition where logically-equivalent well-typed values would land elsewhere, and the row
      /// gets mis-routed before segment-build catches the type error. Fail loudly so the caller
      /// surfaces PARTITION_VALUE_REJECTED.
      throw new IllegalArgumentException(
          "Could not coerce value=" + value + " to declared partition-column type " + storedType
              + ": " + e.getMessage(), e);
    }
    if (coerced instanceof byte[]) {
      return BytesUtils.toHexString((byte[]) coerced);
    }
    return String.valueOf(coerced);
  }

  /// Creates the temporary working directory for segment build. Package-private for test overriding.
  File createWorkingDir(String tableNameWithType) {
    ControllerFilePathProvider pathProvider = ControllerFilePathProvider.getInstance();
    String uniqueSuffix = UUID.randomUUID().toString().replace("-", "").substring(0, 12);
    return new File(pathProvider.getFileUploadTempDir(),
        WORKING_DIR_PREFIX + tableNameWithType + "_" + uniqueSuffix);
  }

  /// Uploads the segment tar file to the controller deep store and registers it in ZooKeeper
  /// via the resource manager. Package-private for test overriding.
  void uploadSegment(String tableNameWithType, File segmentDir, File segmentTarFile,
      String segmentName)
      throws Exception {
    ControllerFilePathProvider pathProvider = ControllerFilePathProvider.getInstance();
    URI dataDirURI = pathProvider.getDataDirURI();
    String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);

    /// Determine the final segment location in deep store
    String encodedSegmentName = URIUtils.encode(segmentName);
    String finalSegmentLocationPath = URIUtils.getPath(dataDirURI.toString(), rawTableName, encodedSegmentName);
    URI finalSegmentLocationURI = URIUtils.getUri(finalSegmentLocationPath);

    /// Determine the download URI
    String segmentDownloadURI;
    if (dataDirURI.getScheme().equalsIgnoreCase("file")) {
      segmentDownloadURI = URIUtils.getPath(pathProvider.getVip(), "segments", rawTableName, encodedSegmentName);
    } else {
      segmentDownloadURI = finalSegmentLocationPath;
    }

    /// Copy segment tar to deep store
    PinotFS pinotFS = PinotFSFactory.create(finalSegmentLocationURI.getScheme());
    pinotFS.copyFromLocalFile(segmentTarFile, finalSegmentLocationURI);
    LOGGER.info("Copied segment tar {} to deep store at {}", segmentTarFile.getName(), finalSegmentLocationURI);

    /// Read segment metadata from the built segment directory
    SegmentMetadata segmentMetadata = new SegmentMetadataImpl(segmentDir);

    /// Register segment in ZooKeeper and assign to instances
    _resourceManager.addNewSegment(tableNameWithType, segmentMetadata, segmentDownloadURI);
    LOGGER.info("Registered segment {} in ZooKeeper for table {}", segmentName, tableNameWithType);
  }

  /// Resolves the table name with a type suffix. If the table name already contains a type suffix,
  /// returns it as-is. Otherwise, defaults to OFFLINE.
  private String resolveTableNameWithType(String tableName, TableType tableType) {
    if (TableNameBuilder.isTableResource(tableName)) {
      return tableName;
    }
    if (tableType != null) {
      return TableNameBuilder.forType(tableType).tableNameWithType(tableName);
    }
    return TableNameBuilder.OFFLINE.tableNameWithType(tableName);
  }

  /// Best-effort rollback of segments that were already uploaded to deep store and registered
  /// in ZooKeeper. Removes segments from ZK and attempts to delete from deep store.
  ///
  /// **v1 LIMITATION — destructive on live data.** `deleteSegment` removes
  /// the segment from IdealState, deletes from deep store, and fires lineage events. By the time
  /// we run this rollback the segment is already registered with servers and may be answering
  /// queries; a query in flight against this partition will see the segment disappear mid-query.
  /// There is no fence between `addNewSegment` and the rollback. The hazard is bounded
  /// because INSERT INTO VALUES is documented as interactive / quickstart-only, but operators
  /// running it against live tables should be aware. v2 should stage segments via
  /// `startReplaceSegments` / `endReplaceSegments` so the failed batch never reaches
  /// IdealState — see `FileInsertExecutor` for the lineage pattern.
  ///
  /// Branches on the response status (not just thrown exceptions): `deleteSegment` can
  /// return `!isSuccessful()` silently when the segment participates in a live lineage entry
  /// (Pinot's lineage-aware delete protocol refuses to remove segments under active replacement).
  /// Logging a generic success message in that case would mislead an operator into thinking the
  /// segment had been rolled back when it is still live in IdealState.
  private void rollbackUploadedSegments(String tableNameWithType, List<String> segmentNames) {
    for (String segmentName : segmentNames) {
      try {
        PinotResourceManagerResponse response = _resourceManager.deleteSegment(tableNameWithType, segmentName);
        /// deleteSegment is contractually non-null per PinotHelixResourceManager.deleteSegmentsInternal.
        if (!response.isSuccessful()) {
          LOGGER.warn("Rollback refused for segment {} from table {}: {}. Manual cleanup may be required "
                  + "(segment may participate in a live lineage entry that the lineage-aware delete protocol "
                  + "is refusing to break).",
              segmentName, tableNameWithType, response.getMessage());
        } else {
          LOGGER.info("Rolled back segment {} from table {}", segmentName, tableNameWithType);
        }
      } catch (Exception e) {
        LOGGER.error("Failed to rollback segment {} from table {}. Manual cleanup may be required.",
            segmentName, tableNameWithType, e);
      }
    }
  }

  /// Holds a locally-built segment that is ready for upload but has not been published yet.
  private static class StagedSegment {
    final String _segmentName;
    final File _segmentDir;
    final File _tarFile;
    final int _rowCount;
    final int _partitionId;

    StagedSegment(String segmentName, File segmentDir, File tarFile, int rowCount, int partitionId) {
      _segmentName = segmentName;
      _segmentDir = segmentDir;
      _tarFile = tarFile;
      _rowCount = rowCount;
      _partitionId = partitionId;
    }
  }

  /// Validates table mode safety rules for row insert.
  ///
  /// - Append tables (OFFLINE or REALTIME without upsert/dedup): allowed
  /// - REALTIME full-upsert: allowed only if partition config is present
  /// - REALTIME partial-upsert: rejected
  /// - Dedup tables: rejected
  ///
  /// @param tableConfig the table configuration to validate
  /// @return error message if rejected, or `null` if allowed
  @Nullable
  String validateTableModeSafety(TableConfig tableConfig) {
    return InsertTableModeValidator.validate(tableConfig, "row");
  }

  /// Builds an error {@link InsertResult} in the ABORTED state.
  private InsertResult buildErrorResult(String statementId, String message, String errorCode) {
    return new InsertResult.Builder()
        .setStatementId(statementId)
        .setState(InsertStatementState.ABORTED)
        .setMessage(message)
        .setErrorCode(errorCode)
        .build();
  }
}
