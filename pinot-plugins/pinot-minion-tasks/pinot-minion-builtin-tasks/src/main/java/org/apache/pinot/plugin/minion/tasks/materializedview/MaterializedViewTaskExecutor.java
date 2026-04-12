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

import com.google.common.base.Preconditions;
import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.helix.AccessOption;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.auth.AuthProviderUtils;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadataCustomMapModifier;
import org.apache.pinot.common.minion.MaterializedViewMetadata;
import org.apache.pinot.common.minion.MaterializedViewMetadataUtils;
import org.apache.pinot.common.minion.MaterializedViewTaskMetadata;
import org.apache.pinot.common.restlet.resources.StartReplaceSegmentsRequest;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.common.MinionConstants.MaterializedViewTask;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.minion.MinionConf;
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
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.Obfuscator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Executor for {@link MaterializedViewTask}.
 *
 * <p>This task receives a SQL query with an appended time range (from the generator),
 * executes it via a pluggable {@link MvQueryExecutor} (e.g. gRPC, Arrow Flight),
 * and builds a segment from the query results for the MV table.
 *
 * <p>Lifecycle:
 * <ol>
 *   <li>{@code preProcess} – validates watermark against windowStartMs</li>
 *   <li>{@code executeTask} – queries broker, builds segment, uploads</li>
 *   <li>{@code postProcess} – advances watermark to windowEndMs</li>
 * </ol>
 */
public class MaterializedViewTaskExecutor extends BaseTaskExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(MaterializedViewTaskExecutor.class);

  private final MinionTaskZkMetadataManager _minionTaskZkMetadataManager;
  private final MinionConf _minionConf;
  private final MvQueryExecutor _queryExecutor;
  private int _expectedVersion = Integer.MIN_VALUE;
  private int _mvMetadataExpectedVersion = Integer.MIN_VALUE;

  public MaterializedViewTaskExecutor(MinionTaskZkMetadataManager minionTaskZkMetadataManager,
      MinionConf minionConf, MvQueryExecutor queryExecutor) {
    _minionTaskZkMetadataManager = minionTaskZkMetadataManager;
    _minionConf = minionConf;
    _queryExecutor = queryExecutor;
  }

  public void preProcess(PinotTaskConfig pinotTaskConfig) {
    Map<String, String> configs = pinotTaskConfig.getConfigs();
    String tableName = configs.get(MinionConstants.TABLE_NAME_KEY);

    ZNRecord znRecord = _minionTaskZkMetadataManager.getTaskMetadataZNRecord(
        tableName, MaterializedViewTask.TASK_TYPE);
    Preconditions.checkState(znRecord != null,
        "MaterializedViewTaskMetadata ZNRecord for table: %s should not be null", tableName);

    MaterializedViewTaskMetadata metadata = MaterializedViewTaskMetadata.fromZNRecord(znRecord);
    long windowStartMs = Long.parseLong(configs.get(MaterializedViewTask.WINDOW_START_MS_KEY));
    Preconditions.checkState(metadata.getWatermarkMs() <= windowStartMs,
        "watermarkMs %d should not be larger than windowStartMs %d for table %s",
        metadata.getWatermarkMs(), windowStartMs, tableName);

    _expectedVersion = znRecord.getVersion();

    // Fetch MaterializedViewMetadata version for optimistic locking in postProcess
    HelixPropertyStore<ZNRecord> propertyStore = MINION_CONTEXT.getHelixPropertyStore();
    String mvMetadataPath = ZKMetadataProvider.constructPropertyStorePathForMaterializedViewMetadata(tableName);
    org.apache.zookeeper.data.Stat stat = new org.apache.zookeeper.data.Stat();
    ZNRecord mvZnRecord = propertyStore.get(mvMetadataPath, stat, AccessOption.PERSISTENT);
    if (mvZnRecord != null) {
      _mvMetadataExpectedVersion = stat.getVersion();
    } else {
      LOGGER.warn("MaterializedViewMetadata ZNRecord for table: {} not found; "
          + "partition mapping will be initialized in postProcess", tableName);
      _mvMetadataExpectedVersion = -1;
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
    String definedSQL = configs.get(MaterializedViewTask.DEFINED_SQL_KEY);
    long windowStartMs = Long.parseLong(configs.get(MaterializedViewTask.WINDOW_START_MS_KEY));
    long windowEndMs = Long.parseLong(configs.get(MaterializedViewTask.WINDOW_END_MS_KEY));

    LOGGER.info("MaterializedViewTask for table: {}, window: [{}, {}), SQL: {}",
        tableName, windowStartMs, windowEndMs, definedSQL);

    TableConfig tableConfig = getTableConfig(tableName);
    Schema schema = getSchema(tableName);

    // Execute the query via the pluggable query executor (gRPC, Arrow Flight, etc.)
    eventObserver.notifyProgress(pinotTaskConfig, "Executing query for MV table: " + tableName);
    AuthProvider authProvider = resolveAuthProvider(configs);
    Map<String, String> authHeaders = AuthProviderUtils.makeAuthHeadersMap(authProvider);
    MvQueryExecutor.QueryResult queryResult = _queryExecutor.executeQuery(definedSQL, authHeaders);
    List<GenericRow> rows = convertToGenericRows(queryResult.getDataSchema(), queryResult.getRows());
    LOGGER.info("Query returned {} rows for table: {}", rows.size(), tableName);

    // Skip segment creation when query returns no data; still advance watermark
    if (rows.isEmpty()) {
      LOGGER.info("No data returned for window [{}, {}) of table: {}. "
          + "Skipping segment creation and advancing watermark.", windowStartMs, windowEndMs, tableName);
      postProcess(pinotTaskConfig);
      return new SegmentConversionResult.Builder()
          .setTableNameWithType(tableName)
          .build();
    }

    String maxRecordsStr = configs.get(MaterializedViewTask.MAX_NUM_RECORDS_PER_SEGMENT_KEY);
    int maxNumRecordsPerSegment = maxRecordsStr != null
        ? Integer.parseInt(maxRecordsStr)
        : MaterializedViewTask.DEFAULT_MAX_NUM_RECORDS_PER_SEGMENT;

    // Split rows into chunks to honour maxNumRecordsPerSegment
    int totalRows = rows.size();
    int numSegments = (totalRows + maxNumRecordsPerSegment - 1) / maxNumRecordsPerSegment;
    LOGGER.info("Splitting {} rows into {} segment(s) (maxNumRecordsPerSegment={})",
        totalRows, numSegments, maxNumRecordsPerSegment);

    String uploadURL = configs.get(MinionConstants.UPLOAD_URL_KEY);

    File tempDir = new File(FileUtils.getTempDirectory(),
        "mv_task_" + tableName + "_" + UUID.randomUUID());
    FileUtils.forceMkdir(tempDir);

    try {
      // Phase 1: Build all segments and collect results
      List<SegmentConversionResult> conversionResults = new ArrayList<>();
      List<File> tarFiles = new ArrayList<>();

      for (int segIdx = 0; segIdx < numSegments; segIdx++) {
        int fromIndex = segIdx * maxNumRecordsPerSegment;
        int toIndex = Math.min(fromIndex + maxNumRecordsPerSegment, totalRows);
        List<GenericRow> chunk = rows.subList(fromIndex, toIndex);

        String segmentName = numSegments == 1
            ? tableName + "_" + windowStartMs + "_" + windowEndMs
            : tableName + "_" + windowStartMs + "_" + windowEndMs + "_" + segIdx;

        File segmentOutputDir = new File(tempDir, "segmentOutput_" + segIdx);
        FileUtils.forceMkdir(segmentOutputDir);

        SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
        segmentGeneratorConfig.setTableName(tableName);
        segmentGeneratorConfig.setOutDir(segmentOutputDir.getAbsolutePath());
        segmentGeneratorConfig.setSegmentName(segmentName);

        eventObserver.notifyProgress(pinotTaskConfig,
            String.format("Building segment %d/%d: %s (%d rows)", segIdx + 1, numSegments, segmentName, chunk.size()));

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

      // Phase 2: Segment lineage — find old segments and start replace
      List<String> segmentsTo = new ArrayList<>();
      for (SegmentConversionResult r : conversionResults) {
        segmentsTo.add(r.getSegmentName());
      }

      String segmentPrefix = tableName + "_" + windowStartMs + "_" + windowEndMs;
      Set<String> allExistingSegments = SegmentConversionUtils.getSegmentNamesForTable(
          tableName, new URI(uploadURL).resolve("/"), authProvider);
      List<String> segmentsFrom = new ArrayList<>();
      for (String name : allExistingSegments) {
        if (name.equals(segmentPrefix) || name.startsWith(segmentPrefix + "_")) {
          segmentsFrom.add(name);
        }
      }

      String lineageEntryId = null;
      if (!segmentsFrom.isEmpty() || !segmentsTo.isEmpty()) {
        lineageEntryId = SegmentConversionUtils.startSegmentReplace(
            tableName, uploadURL,
            new StartReplaceSegmentsRequest(segmentsFrom, segmentsTo),
            authProvider);
        LOGGER.info("Started segment replace for table: {}, lineageEntryId: {}, "
            + "segmentsFrom: {}, segmentsTo: {}", tableName, lineageEntryId, segmentsFrom, segmentsTo);
      }

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
        SegmentConversionUtils.endSegmentReplace(
            tableName, uploadURL, lineageEntryId,
            _minionConf.getEndReplaceSegmentsTimeoutMs(), authProvider);
        LOGGER.info("Ended segment replace for table: {}, lineageEntryId: {}", tableName, lineageEntryId);
      }

      postProcess(pinotTaskConfig);

      return conversionResults.get(conversionResults.size() - 1);
    } finally {
      FileUtils.deleteQuietly(tempDir);
    }
  }

  public void postProcess(PinotTaskConfig pinotTaskConfig) {
    Map<String, String> configs = pinotTaskConfig.getConfigs();
    String tableName = configs.get(MinionConstants.TABLE_NAME_KEY);
    long watermarkMs = Long.parseLong(configs.get(MaterializedViewTask.WINDOW_END_MS_KEY));

    MaterializedViewTaskMetadata newMetadata = new MaterializedViewTaskMetadata(tableName, watermarkMs);
    _minionTaskZkMetadataManager.setTaskMetadataZNRecord(newMetadata,
        MaterializedViewTask.TASK_TYPE, _expectedVersion);

    // Update partition mapping in MaterializedViewMetadata
    updatePartitionMapping(configs, tableName);
  }

  /**
   * Updates the {@link MaterializedViewMetadata} in ZK with the partition mapping for the
   * completed time window. Uses the time-bucket index as partition ID for both base and MV
   * partitions (1:1 mapping for time-window-based MVs).
   */
  private void updatePartitionMapping(Map<String, String> configs, String tableName) {
    String sourceTableName = configs.get(MaterializedViewTask.SOURCE_TABLE_NAME_KEY);
    if (sourceTableName == null) {
      LOGGER.warn("SOURCE_TABLE_NAME_KEY not found in task config for table: {}; "
          + "skipping partition mapping update", tableName);
      return;
    }

    long windowStartMs = Long.parseLong(configs.get(MaterializedViewTask.WINDOW_START_MS_KEY));
    long windowEndMs = Long.parseLong(configs.get(MaterializedViewTask.WINDOW_END_MS_KEY));
    long bucketMs = windowEndMs - windowStartMs;
    int partitionId = (int) (windowStartMs / bucketMs);

    HelixPropertyStore<ZNRecord> propertyStore = MINION_CONTEXT.getHelixPropertyStore();

    // Fetch existing metadata or build a shell with empty maps
    MaterializedViewMetadata existing = MaterializedViewMetadataUtils
        .fetchMaterializedViewMetadata(propertyStore, tableName);

    Map<Integer, Set<Integer>> baseToMv;
    Map<Integer, Set<Integer>> mvToBase;
    List<String> baseTables;
    String timeRangeRefTable;
    String definedSql;

    if (existing != null) {
      baseToMv = new HashMap<>(existing.getBaseToMvPartitionMap());
      mvToBase = new HashMap<>(existing.getMvToBasePartitionMap());
      baseTables = existing.getBaseTables();
      timeRangeRefTable = existing.getTimeRangeRefTable();
      definedSql = existing.getDefinedSql();
      if (definedSql == null) {
        definedSql = configs.get(MaterializedViewTask.ORIGINAL_DEFINED_SQL_KEY);
      }
    } else {
      baseToMv = new HashMap<>();
      mvToBase = new HashMap<>();
      baseTables = Collections.singletonList(sourceTableName);
      timeRangeRefTable = sourceTableName;
      definedSql = configs.get(MaterializedViewTask.ORIGINAL_DEFINED_SQL_KEY);
    }

    // Merge partition mapping for this window (1:1 mapping for time-window-based MVs)
    baseToMv.computeIfAbsent(partitionId, k -> new HashSet<>()).add(partitionId);
    mvToBase.computeIfAbsent(partitionId, k -> new HashSet<>()).add(partitionId);

    MaterializedViewMetadata updated = new MaterializedViewMetadata(
        tableName, baseTables, timeRangeRefTable, definedSql, baseToMv, mvToBase);
    MaterializedViewMetadataUtils.persistMaterializedViewMetadata(
        propertyStore, updated, _mvMetadataExpectedVersion);

    LOGGER.info("Updated partition mapping for table: {}, partitionId: {}, sourceTable: {}",
        tableName, partitionId, sourceTableName);
  }

  @Override
  protected SegmentZKMetadataCustomMapModifier getSegmentZKMetadataCustomMapModifier(
      PinotTaskConfig pinotTaskConfig, SegmentConversionResult segmentConversionResult) {
    return new SegmentZKMetadataCustomMapModifier(
        SegmentZKMetadataCustomMapModifier.ModifyMode.UPDATE, Collections.emptyMap());
  }

  /**
   * Converts raw query result rows into {@link GenericRow} objects using column names
   * from the {@link DataSchema}.
   */
  private List<GenericRow> convertToGenericRows(DataSchema dataSchema, List<Object[]> rows) {
    String[] columnNames = dataSchema.getColumnNames();
    List<GenericRow> genericRows = new ArrayList<>(rows.size());
    for (Object[] row : rows) {
      GenericRow genericRow = new GenericRow();
      for (int i = 0; i < columnNames.length; i++) {
        genericRow.putValue(columnNames[i], row[i]);
      }
      genericRows.add(genericRow);
    }
    return genericRows;
  }
}
