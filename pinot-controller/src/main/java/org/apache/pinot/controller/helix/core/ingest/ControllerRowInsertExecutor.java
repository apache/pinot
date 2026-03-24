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

import java.io.File;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.common.utils.URIUtils;
import org.apache.pinot.controller.api.resources.ControllerFilePathProvider;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.ingest.InsertExecutor;
import org.apache.pinot.spi.ingest.InsertRequest;
import org.apache.pinot.spi.ingest.InsertResult;
import org.apache.pinot.spi.ingest.InsertStatementState;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Controller-side INSERT INTO ... VALUES executor that builds a Pinot segment from in-memory
 * rows and uploads it directly to the segment store.
 *
 * <p>For OFFLINE tables the segment is generated and uploaded immediately. For REALTIME tables
 * the same approach is used (Pinot supports offline-style segment upload for backfill).
 *
 * <p>This executor is designed for interactive / quickstart use cases where low row counts are
 * inserted via SQL. It is not intended for high-throughput production ingestion.
 *
 * <p>Instances are thread-safe; each {@link #execute} call uses its own temporary directory.
 */
public class ControllerRowInsertExecutor implements InsertExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(ControllerRowInsertExecutor.class);

  private static final String SEGMENT_NAME_PREFIX = "insert_";
  private static final String WORKING_DIR_PREFIX = "row_insert_";
  private static final String OUTPUT_SEGMENT_DIR = "output_segment";
  private static final String SEGMENT_TAR_DIR = "segment_tar";

  private final PinotHelixResourceManager _resourceManager;

  /**
   * Creates a new executor backed by the given resource manager.
   *
   * @param resourceManager the Helix resource manager for table config lookup and segment upload
   */
  public ControllerRowInsertExecutor(PinotHelixResourceManager resourceManager) {
    _resourceManager = resourceManager;
  }

  @Override
  public InsertResult execute(InsertRequest request) {
    String statementId = request.getStatementId();
    String tableName = request.getTableName();
    TableType tableType = request.getTableType();
    List<GenericRow> rows = request.getRows();

    LOGGER.info("Executing row insert statement {} for table {} with {} rows", statementId, tableName,
        rows == null ? 0 : rows.size());

    // 1. Validate rows
    if (rows == null || rows.isEmpty()) {
      return buildErrorResult(statementId, "No rows provided in the insert request.", "EMPTY_ROWS");
    }

    // 2. Resolve the fully-qualified table name
    String tableNameWithType = resolveTableNameWithType(tableName, tableType);

    // 3. Look up table config and schema
    TableConfig tableConfig = _resourceManager.getTableConfig(tableNameWithType);
    if (tableConfig == null) {
      return buildErrorResult(statementId, "Table not found: " + tableNameWithType, "TABLE_NOT_FOUND");
    }

    Schema schema = _resourceManager.getTableSchema(tableNameWithType);
    if (schema == null) {
      return buildErrorResult(statementId, "Schema not found for table: " + tableNameWithType, "SCHEMA_NOT_FOUND");
    }

    // 4. Build segment and upload
    File workingDir = null;
    try {
      ControllerFilePathProvider pathProvider = ControllerFilePathProvider.getInstance();
      String uniqueSuffix = UUID.randomUUID().toString().replace("-", "").substring(0, 12);
      workingDir = new File(pathProvider.getFileUploadTempDir(),
          WORKING_DIR_PREFIX + tableNameWithType + "_" + uniqueSuffix);
      File outputDir = new File(workingDir, OUTPUT_SEGMENT_DIR);
      File segmentTarDir = new File(workingDir, SEGMENT_TAR_DIR);
      FileUtils.forceMkdir(outputDir);
      FileUtils.forceMkdir(segmentTarDir);

      // Generate a unique segment name
      String segmentName = SEGMENT_NAME_PREFIX + statementId;

      // Configure segment generation
      SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
      segmentGeneratorConfig.setOutDir(outputDir.getAbsolutePath());
      segmentGeneratorConfig.setSegmentName(segmentName);
      segmentGeneratorConfig.setTableName(tableNameWithType);

      // Build the segment using GenericRowRecordReader
      GenericRowRecordReader recordReader = new GenericRowRecordReader(rows);
      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
      driver.init(segmentGeneratorConfig, recordReader);
      driver.build();

      File segmentOutputDir = driver.getOutputDirectory();
      LOGGER.info("Built segment {} at {}", segmentName, segmentOutputDir.getAbsolutePath());

      // Create tar.gz of the segment
      File segmentTarFile = new File(segmentTarDir, segmentName + ".tar.gz");
      TarCompressionUtils.createCompressedTarFile(segmentOutputDir, segmentTarFile);

      // Upload the segment to deep store and register in ZK
      uploadSegment(tableNameWithType, tableConfig, segmentOutputDir, segmentTarFile, segmentName);

      LOGGER.info("Successfully uploaded segment {} for table {}", segmentName, tableNameWithType);

      return new InsertResult.Builder()
          .setStatementId(statementId)
          .setState(InsertStatementState.VISIBLE)
          .setMessage("Successfully inserted " + rows.size() + " rows as segment: " + segmentName)
          .setSegmentNames(Collections.singletonList(segmentName))
          .build();
    } catch (Exception e) {
      LOGGER.error("Failed to execute row insert for statement {} on table {}", statementId, tableNameWithType, e);
      return buildErrorResult(statementId, "Failed to build and upload segment: " + e.getMessage(),
          "SEGMENT_BUILD_FAILED");
    } finally {
      FileUtils.deleteQuietly(workingDir);
    }
  }

  @Override
  public InsertResult getStatus(String statementId) {
    // This executor does not track statement state, so getStatus() cannot reliably report visibility.
    // To avoid misrepresenting the status for unknown or unsupported statement IDs, we report ABORTED.
    return new InsertResult.Builder()
        .setStatementId(statementId)
        .setState(InsertStatementState.ABORTED)
        .setMessage("ControllerRowInsertExecutor does not track statement status; getStatus is unsupported for "
            + "statementId=" + statementId)
        .build();
  }

  @Override
  public InsertResult abort(String statementId) {
    // Row inserts are synchronous and complete within execute(), so abort is a no-op.
    return new InsertResult.Builder()
        .setStatementId(statementId)
        .setState(InsertStatementState.ABORTED)
        .setMessage("Row insert statements are synchronous and cannot be aborted after completion.")
        .build();
  }

  /**
   * Uploads the segment tar file to the controller deep store and registers it in ZooKeeper
   * via the resource manager.
   */
  private void uploadSegment(String tableNameWithType, TableConfig tableConfig, File segmentDir, File segmentTarFile,
      String segmentName)
      throws Exception {
    ControllerFilePathProvider pathProvider = ControllerFilePathProvider.getInstance();
    URI dataDirURI = pathProvider.getDataDirURI();
    String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);

    // Determine the final segment location in deep store
    String encodedSegmentName = URIUtils.encode(segmentName);
    String finalSegmentLocationPath = URIUtils.getPath(dataDirURI.toString(), rawTableName, encodedSegmentName);
    URI finalSegmentLocationURI = URIUtils.getUri(finalSegmentLocationPath);

    // Determine the download URI
    String segmentDownloadURI;
    if (dataDirURI.getScheme().equalsIgnoreCase("file")) {
      segmentDownloadURI = URIUtils.getPath(pathProvider.getVip(), "segments", rawTableName, encodedSegmentName);
    } else {
      segmentDownloadURI = finalSegmentLocationPath;
    }

    // Copy segment tar to deep store
    PinotFS pinotFS = PinotFSFactory.create(finalSegmentLocationURI.getScheme());
    pinotFS.copyFromLocalFile(segmentTarFile, finalSegmentLocationURI);
    LOGGER.info("Copied segment tar {} to deep store at {}", segmentTarFile.getName(), finalSegmentLocationURI);

    // Read segment metadata from the built segment directory
    SegmentMetadata segmentMetadata = new SegmentMetadataImpl(segmentDir);

    // Register segment in ZooKeeper and assign to instances
    _resourceManager.addNewSegment(tableNameWithType, segmentMetadata, segmentDownloadURI);
    LOGGER.info("Registered segment {} in ZooKeeper for table {}", segmentName, tableNameWithType);
  }

  /**
   * Resolves the table name with a type suffix. If the table name already contains a type suffix,
   * returns it as-is. Otherwise, defaults to OFFLINE.
   */
  private String resolveTableNameWithType(String tableName, TableType tableType) {
    if (TableNameBuilder.isTableResource(tableName)) {
      return tableName;
    }
    if (tableType != null) {
      return TableNameBuilder.forType(tableType).tableNameWithType(tableName);
    }
    return TableNameBuilder.OFFLINE.tableNameWithType(tableName);
  }

  /**
   * Builds an error {@link InsertResult} in the ABORTED state.
   */
  private InsertResult buildErrorResult(String statementId, String message, String errorCode) {
    return new InsertResult.Builder()
        .setStatementId(statementId)
        .setState(InsertStatementState.ABORTED)
        .setMessage(message)
        .setErrorCode(errorCode)
        .build();
  }
}
