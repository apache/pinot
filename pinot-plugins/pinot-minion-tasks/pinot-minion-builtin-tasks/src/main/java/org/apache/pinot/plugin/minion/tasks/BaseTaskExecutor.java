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
package org.apache.pinot.plugin.minion.tasks;

import com.google.common.base.Preconditions;
import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.hc.core5.http.message.BasicNameValuePair;
import org.apache.pinot.common.auth.AuthProviderUtils;
import org.apache.pinot.common.auth.NullAuthProvider;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadataCustomMapModifier;
import org.apache.pinot.common.metrics.MinionMeter;
import org.apache.pinot.common.metrics.MinionMetrics;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.common.utils.fetcher.SegmentFetcherFactory;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.core.util.PeerServerSegmentFinder;
import org.apache.pinot.minion.MinionContext;
import org.apache.pinot.minion.executor.PinotTaskExecutor;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.apache.pinot.spi.ingestion.batch.spec.PinotClusterSpec;
import org.apache.pinot.spi.ingestion.batch.spec.PushJobSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationJobSpec;
import org.apache.pinot.spi.ingestion.batch.spec.TableSpec;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class BaseTaskExecutor implements PinotTaskExecutor {
  protected static final Logger LOGGER = LoggerFactory.getLogger(BaseTaskExecutor.class);
  protected static final MinionContext MINION_CONTEXT = MinionContext.getInstance();
  protected static final int SEGMENT_PUSH_DEFAULT_ATTEMPTS = 5;
  protected static final int SEGMENT_PUSH_DEFAULT_PARALLELISM = 1;
  protected static final long SEGMENT_PUSH_DEFAULT_RETRY_INTERVAL_MILLIS = 1000L;

  protected boolean _cancelled = false;
  protected final MinionMetrics _minionMetrics = MinionMetrics.get();

  @Override
  public void cancel() {
    _cancelled = true;
  }

  /**
   * Returns the segment ZK metadata custom map modifier.
   */
  protected abstract SegmentZKMetadataCustomMapModifier getSegmentZKMetadataCustomMapModifier(
      PinotTaskConfig pinotTaskConfig, SegmentConversionResult segmentConversionResult);

  protected TableConfig getTableConfig(String tableNameWithType) {
    TableConfig tableConfig =
        ZKMetadataProvider.getTableConfig(MINION_CONTEXT.getHelixPropertyStore(), tableNameWithType);
    Preconditions.checkState(tableConfig != null, "Failed to find table config for table: %s", tableNameWithType);
    return tableConfig;
  }

  protected Schema getSchema(String tableName) {
    Schema schema = ZKMetadataProvider.getTableSchema(MINION_CONTEXT.getHelixPropertyStore(), tableName);
    Preconditions.checkState(schema != null, "Failed to find schema for table: %s", tableName);
    return schema;
  }

  protected long getSegmentCrc(String tableNameWithType, String segmentName) {
    SegmentZKMetadata segmentZKMetadata =
        ZKMetadataProvider.getSegmentZKMetadata(MINION_CONTEXT.getHelixPropertyStore(), tableNameWithType, segmentName);
    /*
     * If the segmentZKMetadata is null, it is likely that the segment has been deleted, return -1 as CRC in this case,
     * so that task can terminate early when verify CRC. If we throw exception, helix will keep retrying this forever
     * and task status would be left unchanged without proper cleanup.
     */
    return segmentZKMetadata == null ? -1 : segmentZKMetadata.getCrc();
  }

  protected void reportSegmentDownloadMetrics(File indexDir, String tableNameWithType, String taskType) {
    long downloadSegmentSize = FileUtils.sizeOfDirectory(indexDir);
    addTaskMeterMetrics(MinionMeter.SEGMENT_BYTES_DOWNLOADED, downloadSegmentSize, tableNameWithType, taskType);
    addTaskMeterMetrics(MinionMeter.SEGMENT_DOWNLOAD_COUNT, 1L, tableNameWithType, taskType);
  }

  protected void reportSegmentUploadMetrics(File indexDir, String tableNameWithType, String taskType) {
    long uploadSegmentSize = FileUtils.sizeOfDirectory(indexDir);
    addTaskMeterMetrics(MinionMeter.SEGMENT_BYTES_UPLOADED, uploadSegmentSize, tableNameWithType, taskType);
    addTaskMeterMetrics(MinionMeter.SEGMENT_UPLOAD_COUNT, 1L, tableNameWithType, taskType);
  }

  protected void reportTaskProcessingMetrics(String tableNameWithType, String taskType, int numRecordsProcessed,
      int numRecordsPurged) {
    reportTaskProcessingMetrics(tableNameWithType, taskType, numRecordsProcessed);
    addTaskMeterMetrics(MinionMeter.RECORDS_PURGED_COUNT, numRecordsPurged, tableNameWithType, taskType);
  }

  protected void reportTaskProcessingMetrics(String tableNameWithType, String taskType, int numRecordsProcessed) {
    addTaskMeterMetrics(MinionMeter.RECORDS_PROCESSED_COUNT, numRecordsProcessed, tableNameWithType, taskType);
  }

  private void addTaskMeterMetrics(MinionMeter meter, long unitCount, String tableName, String taskType) {
    _minionMetrics.addMeteredGlobalValue(meter, unitCount);
    _minionMetrics.addMeteredTableValue(tableName, meter, unitCount);
    _minionMetrics.addMeteredTableValue(tableName, taskType, meter, unitCount);
  }

  /**
   * Resolves the AuthProvider to use for Minion tasks.
   * Priority order:
   * 1. If AUTH_TOKEN is explicitly provided in task configs (by Controller), use it for this specific task
   * 2. Otherwise, fall back to the runtime AuthProvider from MinionContext (enables per-request token rotation)
   *
   * This approach allows:
   * - Controller to override credentials per-task (e.g., for multi-tenancy or privileged operations)
   * - Dynamic token rotation when no explicit override is provided
   * - Clean separation between task-specific and global authentication
   */
  protected static AuthProvider resolveAuthProvider(Map<String, String> taskConfigs) {
    String explicitToken = taskConfigs.get(MinionConstants.AUTH_TOKEN);
    if (StringUtils.isNotBlank(explicitToken)) {
      return AuthProviderUtils.makeAuthProvider(explicitToken);
    }

    AuthProvider runtimeProvider = MINION_CONTEXT.getTaskAuthProvider();
    if (runtimeProvider == null || runtimeProvider instanceof NullAuthProvider) {
      return new NullAuthProvider();
    }

    return runtimeProvider;
  }

  protected File downloadSegmentToLocalAndUntar(String tableNameWithType, String segmentName, String deepstoreURL,
      String taskType, File tempDataDir, String suffix)
      throws Exception {
    File tarredSegmentFile = new File(tempDataDir, "tarredSegmentFile" + suffix);
    File segmentDir = new File(tempDataDir, "segmentDir" + suffix);
    File indexDir;
    TableConfig tableConfig = getTableConfig(tableNameWithType);
    String crypterName = tableConfig.getValidationConfig().getCrypterClassName();
    LOGGER.info("Downloading segment {} from {} to {}", segmentName, deepstoreURL, tarredSegmentFile.getAbsolutePath());

    try {
      // download from deepstore first
      SegmentFetcherFactory.fetchAndDecryptSegmentToLocal(deepstoreURL, tarredSegmentFile, crypterName);
      // untar the segment file
      indexDir = TarCompressionUtils.untar(tarredSegmentFile, segmentDir).get(0);
    } catch (Exception e) {
      LOGGER.error("Segment download failed from deepstore for {}, crypter:{}", deepstoreURL, crypterName, e);
      String peerDownloadScheme = tableConfig.getValidationConfig().getPeerSegmentDownloadScheme();
      if (MinionTaskUtils.extractMinionAllowDownloadFromServer(tableConfig, taskType,
          MINION_CONTEXT.isAllowDownloadFromServer()) && peerDownloadScheme != null) {
        // if allowDownloadFromServer is enabled, download the segment from a peer server as deepstore download failed
        LOGGER.info("Trying to download from servers for segment {} post deepstore download failed", segmentName);
        SegmentFetcherFactory.fetchAndDecryptSegmentToLocal(segmentName, peerDownloadScheme, () -> {
          List<URI> uris =
              PeerServerSegmentFinder.getPeerServerURIs(MINION_CONTEXT.getHelixManager(), tableNameWithType,
                  segmentName, peerDownloadScheme);
          Collections.shuffle(uris);
          return uris;
        }, tarredSegmentFile, crypterName);
        // untar the segment file
        indexDir = TarCompressionUtils.untar(tarredSegmentFile, segmentDir).get(0);
      } else {
        throw e;
      }
    } finally {
      if (!FileUtils.deleteQuietly(tarredSegmentFile)) {
        LOGGER.warn("Failed to delete tarred input segment: {}", tarredSegmentFile.getAbsolutePath());
      }
    }
    return indexDir;
  }

  /**
   * Builds a {@link PushJobSpec} from task configs. Used for both TAR and METADATA push modes.
   */
  protected PushJobSpec getPushJobSpec(Map<String, String> configs) {
    PushJobSpec pushJobSpec = new PushJobSpec();
    pushJobSpec.setPushAttempts(SEGMENT_PUSH_DEFAULT_ATTEMPTS);
    pushJobSpec.setPushParallelism(SEGMENT_PUSH_DEFAULT_PARALLELISM);
    pushJobSpec.setPushRetryIntervalMillis(SEGMENT_PUSH_DEFAULT_RETRY_INTERVAL_MILLIS);
    pushJobSpec.setSegmentUriPrefix(configs.get(BatchConfigProperties.PUSH_SEGMENT_URI_PREFIX));
    pushJobSpec.setSegmentUriSuffix(configs.get(BatchConfigProperties.PUSH_SEGMENT_URI_SUFFIX));
    boolean batchSegmentUpload = Boolean.parseBoolean(configs.getOrDefault(
        BatchConfigProperties.BATCH_SEGMENT_UPLOAD, "false"));
    if (batchSegmentUpload) {
      pushJobSpec.setBatchSegmentUpload(true);
    }
    return pushJobSpec;
  }

  /**
   * Builds a {@link SegmentGenerationJobSpec} for segment push (TAR or METADATA). Requires
   * {@link BatchConfigProperties#PUSH_CONTROLLER_URI} in configs for METADATA push.
   */
  protected SegmentGenerationJobSpec generateSegmentGenerationJobSpec(String tableName, Map<String, String> configs,
      PushJobSpec pushJobSpec) {
    TableSpec tableSpec = new TableSpec();
    tableSpec.setTableName(tableName);

    PinotClusterSpec pinotClusterSpec = new PinotClusterSpec();
    pinotClusterSpec.setControllerURI(configs.get(BatchConfigProperties.PUSH_CONTROLLER_URI));
    SegmentGenerationJobSpec spec = new SegmentGenerationJobSpec();
    spec.setPushJobSpec(pushJobSpec);
    spec.setTableSpec(tableSpec);
    spec.setPinotClusterSpecs(new PinotClusterSpec[]{pinotClusterSpec});
    spec.setAuthToken(configs.get(BatchConfigProperties.AUTH_TOKEN));
    return spec;
  }

  /**
   * Copies the local segment tar file to the output PinotFS. Requires
   * {@link BatchConfigProperties#OUTPUT_SEGMENT_DIR_URI} in configs.
   *
   * @return the URI of the segment tar on the output filesystem
   */
  protected URI moveSegmentToOutputPinotFS(Map<String, String> configs, File localSegmentTarFile)
      throws Exception {
    URI outputSegmentDirURI = URI.create(configs.get(BatchConfigProperties.OUTPUT_SEGMENT_DIR_URI));
    try (PinotFS outputFileFS = MinionTaskUtils.getOutputPinotFS(configs, outputSegmentDirURI)) {
      URI outputSegmentTarURI =
          URI.create(MinionTaskUtils.normalizeDirectoryURI(outputSegmentDirURI) + localSegmentTarFile.getName());
      if (!Boolean.parseBoolean(configs.get(BatchConfigProperties.OVERWRITE_OUTPUT))
          && outputFileFS.exists(outputSegmentTarURI)) {
        throw new RuntimeException("Output file: " + outputSegmentTarURI + " already exists. Set 'overwriteOutput' to "
            + "true to ignore this error");
      }
      outputFileFS.copyFromLocalFile(localSegmentTarFile, outputSegmentTarURI);
      return outputSegmentTarURI;
    }
  }

  /**
   * Returns HTTP parameters common to segment upload and metadata push (parallel push protection, table name, type).
   */
  protected List<NameValuePair> getSegmentPushCommonParams(String tableNameWithType) {
    List<NameValuePair> params = new ArrayList<>();
    params.add(new BasicNameValuePair(FileUploadDownloadClient.QueryParameters.ENABLE_PARALLEL_PUSH_PROTECTION,
        "true"));
    params.add(new BasicNameValuePair(FileUploadDownloadClient.QueryParameters.TABLE_NAME,
        TableNameBuilder.extractRawTableName(tableNameWithType)));
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableNameWithType);
    if (tableType != null) {
      params.add(new BasicNameValuePair(FileUploadDownloadClient.QueryParameters.TABLE_TYPE, tableType.toString()));
    } else {
      throw new RuntimeException("Failed to determine the tableType from name: " + tableNameWithType);
    }
    return params;
  }

  /**
   * Returns HTTP headers for segment metadata push (ZK metadata custom map modifier + auth). Used when pushing
   * segment URI and metadata to the controller instead of uploading the tar via HTTP.
   *
   * @param segmentConversionResult the conversion result for the segment; may be null when building headers for
   *                                 multiple segments where a single modifier does not apply
   */
  protected List<Header> getSegmentPushMetadataHeaders(PinotTaskConfig pinotTaskConfig, AuthProvider authProvider,
      SegmentConversionResult segmentConversionResult) {
    SegmentZKMetadataCustomMapModifier modifier =
        getSegmentZKMetadataCustomMapModifier(pinotTaskConfig, segmentConversionResult);
    Header modifierHeader =
        new BasicHeader(FileUploadDownloadClient.CustomHeaders.SEGMENT_ZK_METADATA_CUSTOM_MAP_MODIFIER,
            modifier.toJsonString());
    List<Header> headers = new ArrayList<>();
    headers.add(modifierHeader);
    headers.addAll(AuthProviderUtils.toRequestHeaders(authProvider));
    return headers;
  }

  /**
   * Returns the segment push mode for upload. Default is TAR (HTTP upload). Subclasses may override to use METADATA
   * mode (move segment to output PinotFS and send metadata to controller) when needed.
   *
   * @param configs task configs; may contain {@link BatchConfigProperties#PUSH_MODE}
   * @return push type (TAR or METADATA)
   */
  protected BatchConfigProperties.SegmentPushType getSegmentPushType(Map<String, String> configs) {
    String pushMode = configs.getOrDefault(BatchConfigProperties.PUSH_MODE,
        BatchConfigProperties.SegmentPushType.TAR.name());
    return BatchConfigProperties.SegmentPushType.valueOf(pushMode.toUpperCase());
  }
}
