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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicNameValuePair;
import org.apache.pinot.common.auth.AuthProviderUtils;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadataCustomMapModifier;
import org.apache.pinot.common.restlet.resources.StartReplaceSegmentsRequest;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.common.utils.fetcher.SegmentFetcherFactory;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.minion.MinionConf;
import org.apache.pinot.minion.event.MinionEventObserver;
import org.apache.pinot.minion.event.MinionEventObservers;
import org.apache.pinot.minion.exception.TaskCancelledException;
import org.apache.pinot.segment.local.utils.SegmentPushUtils;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.apache.pinot.spi.ingestion.batch.spec.PinotClusterSpec;
import org.apache.pinot.spi.ingestion.batch.spec.PushJobSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationJobSpec;
import org.apache.pinot.spi.ingestion.batch.spec.TableSpec;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Base class which provides a framework for N -> M segment conversion tasks.
 * <p> This class handles segment download and upload
 *
 * {@link BaseMultipleSegmentsConversionExecutor} assumes that output segments are new segments derived from input
 * segments. So, we do not check crc or modify zk metadata when uploading segments. In case of modifying the existing
 * segments, {@link BaseSingleSegmentConversionExecutor} has to be used.
 *
 * TODO: add test for SegmentZKMetadataCustomMapModifier
 */
public abstract class BaseMultipleSegmentsConversionExecutor extends BaseTaskExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseMultipleSegmentsConversionExecutor.class);
  private static final String CUSTOM_SEGMENT_UPLOAD_CONTEXT_LINEAGE_ENTRY_ID = "lineageEntryId";

  private static final int DEFUALT_PUSH_ATTEMPTS = 5;
  private static final int DEFAULT_PUSH_PARALLELISM = 1;
  private static final long DEFAULT_PUSH_RETRY_INTERVAL_MILLIS = 1000L;

  protected MinionConf _minionConf;

  // Tracking finer grained progress status.
  protected PinotTaskConfig _pinotTaskConfig;
  protected MinionEventObserver _eventObserver;

  public BaseMultipleSegmentsConversionExecutor(MinionConf minionConf) {
    _minionConf = minionConf;
  }

  /**
   * Converts the segment based on the given {@link PinotTaskConfig}.
   *
   * @param pinotTaskConfig Task config
   * @param segmentDirs Index directories for the original segments
   * @param workingDir Working directory for the converted segment
   * @return a list of segment conversion result
   * @throws Exception
   */
  protected abstract List<SegmentConversionResult> convert(PinotTaskConfig pinotTaskConfig, List<File> segmentDirs,
      File workingDir)
      throws Exception;

  /**
   * Pre processing operations to be done at the beginning of task execution
   *
   * The default implementation checks whether all segments to process exist in the table, if not, terminate early to
   * avoid wasting computing resources.
   */
  protected void preProcess(PinotTaskConfig pinotTaskConfig) throws Exception {
    Map<String, String> configs = pinotTaskConfig.getConfigs();
    String tableNameWithType = configs.get(MinionConstants.TABLE_NAME_KEY);
    String inputSegmentNames = configs.get(MinionConstants.SEGMENT_NAME_KEY);
    String uploadURL = configs.get(MinionConstants.UPLOAD_URL_KEY);
    AuthProvider authProvider = AuthProviderUtils.makeAuthProvider(configs.get(MinionConstants.AUTH_TOKEN));
    Set<String> segmentNamesForTable = SegmentConversionUtils.getSegmentNamesForTable(tableNameWithType,
        FileUploadDownloadClient.extractBaseURI(new URI(uploadURL)), authProvider);
    Set<String> nonExistingSegmentNames =
        new HashSet<>(Arrays.asList(inputSegmentNames.split(MinionConstants.SEGMENT_NAME_SEPARATOR)));
    nonExistingSegmentNames.removeAll(segmentNamesForTable);
    if (!CollectionUtils.isEmpty(nonExistingSegmentNames)) {
      throw new RuntimeException(String.format("table: %s does have the following segments to process: %s",
          tableNameWithType, nonExistingSegmentNames));
    }
  }

  /**
   * Post processing operations to be done before exiting a successful task execution
   */
  protected void postProcess(PinotTaskConfig pinotTaskConfig) throws Exception {
  }

  protected void preUploadSegments(SegmentUploadContext context)
      throws Exception {
    // Update the segment lineage to indicate that the segment replacement is in progress.
    _eventObserver.notifyProgress(_pinotTaskConfig,
        "Prepare to upload segments: " + context.getSegmentConversionResults().size());
    if (context.isReplaceSegmentsEnabled()) {
      List<String> segmentsFrom =
          Arrays.stream(StringUtils.split(context.getInputSegmentNames(), MinionConstants.SEGMENT_NAME_SEPARATOR))
              .map(String::trim).collect(Collectors.toList());
      List<String> segmentsTo =
          context.getSegmentConversionResults().stream().map(SegmentConversionResult::getSegmentName)
              .collect(Collectors.toList());
      String lineageEntryId =
          SegmentConversionUtils.startSegmentReplace(context.getTableNameWithType(), context.getUploadURL(),
              new StartReplaceSegmentsRequest(segmentsFrom, segmentsTo), context.getAuthProvider());
      context.setCustomContext(CUSTOM_SEGMENT_UPLOAD_CONTEXT_LINEAGE_ENTRY_ID, lineageEntryId);
    }
  }

  protected void postUploadSegments(SegmentUploadContext context)
      throws Exception {
    // Update the segment lineage to indicate that the segment replacement is done.
    _eventObserver.notifyProgress(_pinotTaskConfig,
        "Finishing uploading segments: " + context.getSegmentConversionResults().size());
    if (context.isReplaceSegmentsEnabled()) {
      String lineageEntryId = (String) context.getCustomContext(CUSTOM_SEGMENT_UPLOAD_CONTEXT_LINEAGE_ENTRY_ID);
      SegmentConversionUtils.endSegmentReplace(context.getTableNameWithType(), context.getUploadURL(),
          lineageEntryId, _minionConf.getEndReplaceSegmentsTimeoutMs(), context.getAuthProvider());
    }
  }

  // For tests only.
  @VisibleForTesting
  public void setMinionEventObserver(MinionEventObserver observer) {
    _eventObserver = observer;
  }

  @Override
  public List<SegmentConversionResult> executeTask(PinotTaskConfig pinotTaskConfig)
      throws Exception {
    preProcess(pinotTaskConfig);
    _pinotTaskConfig = pinotTaskConfig;
    _eventObserver = MinionEventObservers.getInstance().getMinionEventObserver(pinotTaskConfig.getTaskId());
    String taskType = pinotTaskConfig.getTaskType();
    Map<String, String> configs = pinotTaskConfig.getConfigs();
    String tableNameWithType = configs.get(MinionConstants.TABLE_NAME_KEY);
    String inputSegmentNames = configs.get(MinionConstants.SEGMENT_NAME_KEY);
    String uploadURL = configs.get(MinionConstants.UPLOAD_URL_KEY);
    String downloadURLString = configs.get(MinionConstants.DOWNLOAD_URL_KEY);
    String[] downloadURLs = downloadURLString.split(MinionConstants.URL_SEPARATOR);
    AuthProvider authProvider = AuthProviderUtils.makeAuthProvider(configs.get(MinionConstants.AUTH_TOKEN));
    LOGGER.info("Start executing {} on table: {}, input segments: {} with downloadURLs: {}, uploadURL: {}", taskType,
        tableNameWithType, inputSegmentNames, downloadURLString, uploadURL);
    File tempDataDir = new File(new File(MINION_CONTEXT.getDataDir(), taskType), "tmp-" + UUID.randomUUID());
    Preconditions.checkState(tempDataDir.mkdirs());
    String crypterName = getTableConfig(tableNameWithType).getValidationConfig().getCrypterClassName();
    try {
      List<File> inputSegmentDirs = new ArrayList<>();
      for (int i = 0; i < downloadURLs.length; i++) {
        // Download the segment file
        _eventObserver.notifyProgress(_pinotTaskConfig, String
            .format("Downloading segment from: %s (%d out of %d)", downloadURLs[i], (i + 1), downloadURLs.length));
        File tarredSegmentFile = new File(tempDataDir, "tarredSegmentFile_" + i);
        LOGGER.info("Downloading segment from {} to {}", downloadURLs[i], tarredSegmentFile.getAbsolutePath());
        SegmentFetcherFactory.fetchAndDecryptSegmentToLocal(downloadURLs[i], tarredSegmentFile, crypterName);

        // Un-tar the segment file
        _eventObserver.notifyProgress(_pinotTaskConfig, String
            .format("Decompressing segment from: %s (%d out of %d)", downloadURLs[i], (i + 1), downloadURLs.length));
        File segmentDir = new File(tempDataDir, "segmentDir_" + i);
        File indexDir = TarGzCompressionUtils.untar(tarredSegmentFile, segmentDir).get(0);
        inputSegmentDirs.add(indexDir);
        if (!FileUtils.deleteQuietly(tarredSegmentFile)) {
          LOGGER.warn("Failed to delete tarred input segment: {}", tarredSegmentFile.getAbsolutePath());
        }
      }

      // Convert the segments
      File workingDir = new File(tempDataDir, "workingDir");
      Preconditions.checkState(workingDir.mkdir());
      List<SegmentConversionResult> segmentConversionResults = convert(pinotTaskConfig, inputSegmentDirs, workingDir);

      // Create a directory for converted tarred segment files
      File convertedTarredSegmentDir = new File(tempDataDir, "convertedTarredSegmentDir");
      Preconditions.checkState(convertedTarredSegmentDir.mkdir());

      int numOutputSegments = segmentConversionResults.size();
      List<File> tarredSegmentFiles = new ArrayList<>(numOutputSegments);
      int count = 1;
      for (SegmentConversionResult segmentConversionResult : segmentConversionResults) {
        // Tar the converted segment
        _eventObserver.notifyProgress(_pinotTaskConfig, String
            .format("Compressing segment: %s (%d out of %d)", segmentConversionResult.getSegmentName(), count++,
                numOutputSegments));
        File convertedSegmentDir = segmentConversionResult.getFile();
        File convertedSegmentTarFile = new File(convertedTarredSegmentDir,
            segmentConversionResult.getSegmentName() + TarGzCompressionUtils.TAR_GZ_FILE_EXTENSION);
        TarGzCompressionUtils.createTarGzFile(convertedSegmentDir, convertedSegmentTarFile);
        tarredSegmentFiles.add(convertedSegmentTarFile);
        if (!FileUtils.deleteQuietly(convertedSegmentDir)) {
          LOGGER.warn("Failed to delete converted segment: {}", convertedSegmentDir.getAbsolutePath());
        }
      }

      // Delete the input segment after tarring the converted segment to avoid deleting the converted segment when the
      // conversion happens in-place (converted segment dir is the same as input segment dir). It could also happen when
      // the conversion is not required, and the input segment dir is returned as the result.
      for (File inputSegmentDir : inputSegmentDirs) {
        if (inputSegmentDir.exists() && !FileUtils.deleteQuietly(inputSegmentDir)) {
          LOGGER.warn("Failed to delete input segment: {}", inputSegmentDir.getAbsolutePath());
        }
      }

      // Check whether the task get cancelled before uploading the segment
      if (_cancelled) {
        LOGGER.info("{} on table: {}, segments: {} got cancelled", taskType, tableNameWithType, inputSegmentNames);
        throw new TaskCancelledException(
            taskType + " on table: " + tableNameWithType + ", segments: " + inputSegmentNames + " got cancelled");
      }

      SegmentUploadContext segmentUploadContext = new SegmentUploadContext(pinotTaskConfig, segmentConversionResults);
      preUploadSegments(segmentUploadContext);

      // Upload the tarred segments
      for (int i = 0; i < numOutputSegments; i++) {
        File convertedTarredSegmentFile = tarredSegmentFiles.get(i);
        SegmentConversionResult segmentConversionResult = segmentConversionResults.get(i);
        String resultSegmentName = segmentConversionResult.getSegmentName();
        _eventObserver.notifyProgress(_pinotTaskConfig,
            String.format("Uploading segment: %s (%d out of %d)", resultSegmentName, (i + 1), numOutputSegments));

        // Set segment ZK metadata custom map modifier into HTTP header to modify the segment ZK metadata
        SegmentZKMetadataCustomMapModifier segmentZKMetadataCustomMapModifier =
            getSegmentZKMetadataCustomMapModifier(pinotTaskConfig, segmentConversionResult);
        Header segmentZKMetadataCustomMapModifierHeader =
            new BasicHeader(FileUploadDownloadClient.CustomHeaders.SEGMENT_ZK_METADATA_CUSTOM_MAP_MODIFIER,
                segmentZKMetadataCustomMapModifier.toJsonString());

        String pushMode =
            configs.getOrDefault(BatchConfigProperties.PUSH_MODE, BatchConfigProperties.SegmentPushType.TAR.name());
        URI outputSegmentTarURI;
        if (BatchConfigProperties.SegmentPushType.valueOf(pushMode.toUpperCase())
            != BatchConfigProperties.SegmentPushType.TAR) {
          outputSegmentTarURI = moveSegmentToOutputPinotFS(configs, convertedTarredSegmentFile);
          LOGGER.info("Moved generated segment from [{}] to location: [{}]", convertedTarredSegmentFile,
              outputSegmentTarURI);
        } else {
          outputSegmentTarURI = convertedTarredSegmentFile.toURI();
        }

        List<Header> httpHeaders = new ArrayList<>();
        httpHeaders.add(segmentZKMetadataCustomMapModifierHeader);
        httpHeaders.addAll(AuthProviderUtils.toRequestHeaders(authProvider));

        // Set parameters for upload request
        NameValuePair enableParallelPushProtectionParameter =
            new BasicNameValuePair(FileUploadDownloadClient.QueryParameters.ENABLE_PARALLEL_PUSH_PROTECTION, "true");
        NameValuePair tableNameParameter = new BasicNameValuePair(FileUploadDownloadClient.QueryParameters.TABLE_NAME,
            TableNameBuilder.extractRawTableName(tableNameWithType));
        NameValuePair tableTypeParameter = new BasicNameValuePair(FileUploadDownloadClient.QueryParameters.TABLE_TYPE,
            TableNameBuilder.getTableTypeFromTableName(tableNameWithType).toString());
        // RealtimeToOfflineSegmentsTask pushed segments to the corresponding offline table
        // TODO: This is not clean to put the override here, but let's think about it harder to see what is the proper
        //  way to override it.
        if (MinionConstants.RealtimeToOfflineSegmentsTask.TASK_TYPE.equals(taskType)) {
          tableTypeParameter = new BasicNameValuePair(FileUploadDownloadClient.QueryParameters.TABLE_TYPE,
              TableType.OFFLINE.toString());
        }
        List<NameValuePair> parameters = Arrays.asList(enableParallelPushProtectionParameter, tableNameParameter,
            tableTypeParameter);

        pushSegment(tableNameParameter.getValue(), configs, outputSegmentTarURI, httpHeaders, parameters,
            segmentConversionResult);
        if (!FileUtils.deleteQuietly(convertedTarredSegmentFile)) {
          LOGGER.warn("Failed to delete tarred converted segment: {}", convertedTarredSegmentFile.getAbsolutePath());
        }
      }

      postUploadSegments(segmentUploadContext);

      String outputSegmentNames = segmentConversionResults.stream().map(SegmentConversionResult::getSegmentName)
          .collect(Collectors.joining(","));
      postProcess(pinotTaskConfig);
      LOGGER
          .info("Done executing {} on table: {}, input segments: {}, output segments: {}", taskType, tableNameWithType,
              inputSegmentNames, outputSegmentNames);

      return segmentConversionResults;
    } finally {
      FileUtils.deleteQuietly(tempDataDir);
    }
  }

  private void pushSegment(String tableName, Map<String, String> taskConfigs, URI outputSegmentTarURI,
      List<Header> headers, List<NameValuePair> parameters, SegmentConversionResult segmentConversionResult)
      throws Exception {
    String pushMode =
        taskConfigs.getOrDefault(BatchConfigProperties.PUSH_MODE, BatchConfigProperties.SegmentPushType.TAR.name());
    LOGGER.info("Trying to push Pinot segment with push mode {} from {}", pushMode, outputSegmentTarURI);

    PushJobSpec pushJobSpec = new PushJobSpec();
    pushJobSpec.setPushAttempts(DEFUALT_PUSH_ATTEMPTS);
    pushJobSpec.setPushParallelism(DEFAULT_PUSH_PARALLELISM);
    pushJobSpec.setPushRetryIntervalMillis(DEFAULT_PUSH_RETRY_INTERVAL_MILLIS);
    pushJobSpec.setSegmentUriPrefix(taskConfigs.get(BatchConfigProperties.PUSH_SEGMENT_URI_PREFIX));
    pushJobSpec.setSegmentUriSuffix(taskConfigs.get(BatchConfigProperties.PUSH_SEGMENT_URI_SUFFIX));

    SegmentGenerationJobSpec spec = generatePushJobSpec(tableName, taskConfigs, pushJobSpec);

    switch (BatchConfigProperties.SegmentPushType.valueOf(pushMode.toUpperCase())) {
      case TAR:
          File tarFile = new File(outputSegmentTarURI);
          String segmentName = segmentConversionResult.getSegmentName();
          String tableNameWithType = segmentConversionResult.getTableNameWithType();
          String uploadURL = taskConfigs.get(MinionConstants.UPLOAD_URL_KEY);
          SegmentConversionUtils.uploadSegment(taskConfigs, headers, parameters, tableNameWithType, segmentName,
              uploadURL, tarFile);
        break;
      case METADATA:
        if (taskConfigs.containsKey(BatchConfigProperties.OUTPUT_SEGMENT_DIR_URI)) {
          URI outputSegmentDirURI = URI.create(taskConfigs.get(BatchConfigProperties.OUTPUT_SEGMENT_DIR_URI));
          try (PinotFS outputFileFS = MinionTaskUtils.getOutputPinotFS(taskConfigs, outputSegmentDirURI)) {
            Map<String, String> segmentUriToTarPathMap =
                SegmentPushUtils.getSegmentUriToTarPathMap(outputSegmentDirURI, pushJobSpec,
                    new String[]{outputSegmentTarURI.toString()});
            SegmentPushUtils.sendSegmentUriAndMetadata(spec, outputFileFS, segmentUriToTarPathMap, headers, parameters);
          }
        } else {
          throw new RuntimeException("Output dir URI missing for metadata push");
        }
        break;
      default:
        throw new UnsupportedOperationException("Unrecognized push mode - " + pushMode);
    }
  }

  private SegmentGenerationJobSpec generatePushJobSpec(String tableName, Map<String, String> taskConfigs,
      PushJobSpec pushJobSpec) {

    TableSpec tableSpec = new TableSpec();
    tableSpec.setTableName(tableName);

    PinotClusterSpec pinotClusterSpec = new PinotClusterSpec();
    pinotClusterSpec.setControllerURI(taskConfigs.get(BatchConfigProperties.PUSH_CONTROLLER_URI));
    PinotClusterSpec[] pinotClusterSpecs = new PinotClusterSpec[]{pinotClusterSpec};

    SegmentGenerationJobSpec spec = new SegmentGenerationJobSpec();
    spec.setPushJobSpec(pushJobSpec);
    spec.setTableSpec(tableSpec);
    spec.setPinotClusterSpecs(pinotClusterSpecs);
    spec.setAuthToken(taskConfigs.get(BatchConfigProperties.AUTH_TOKEN));

    return spec;
  }

  private URI moveSegmentToOutputPinotFS(Map<String, String> taskConfigs, File localSegmentTarFile)
      throws Exception {
    URI outputSegmentDirURI = URI.create(taskConfigs.get(BatchConfigProperties.OUTPUT_SEGMENT_DIR_URI));
    try (PinotFS outputFileFS = MinionTaskUtils.getOutputPinotFS(taskConfigs, outputSegmentDirURI)) {
      URI outputSegmentTarURI =
          URI.create(MinionTaskUtils.normalizeDirectoryURI(outputSegmentDirURI) + localSegmentTarFile.getName());
      if (!Boolean.parseBoolean(taskConfigs.get(BatchConfigProperties.OVERWRITE_OUTPUT)) && outputFileFS.exists(
          outputSegmentTarURI)) {
        throw new RuntimeException(String.format("Output file: %s already exists. "
                + "Set 'overwriteOutput' to true to ignore this error", outputSegmentTarURI));
      } else {
        outputFileFS.copyFromLocalFile(localSegmentTarFile, outputSegmentTarURI);
      }
      return outputSegmentTarURI;
    }
  }

  // SegmentUploadContext holds the info to conduct certain actions
  // before and after uploading multiple segments.
  protected static class SegmentUploadContext {
    private final PinotTaskConfig _pinotTaskConfig;
    private final List<SegmentConversionResult> _segmentConversionResults;

    private final String _tableNameWithType;
    private final String _uploadURL;
    private final AuthProvider _authProvider;
    private final String _inputSegmentNames;
    private final boolean _replaceSegmentsEnabled;
    private final Map<String, Object> _customMap;

    public SegmentUploadContext(PinotTaskConfig pinotTaskConfig,
        List<SegmentConversionResult> segmentConversionResults) {
      _pinotTaskConfig = pinotTaskConfig;
      _segmentConversionResults = segmentConversionResults;

      Map<String, String> configs = pinotTaskConfig.getConfigs();
      _tableNameWithType = configs.get(MinionConstants.TABLE_NAME_KEY);
      _uploadURL = configs.get(MinionConstants.UPLOAD_URL_KEY);
      _authProvider = AuthProviderUtils.makeAuthProvider(configs.get(MinionConstants.AUTH_TOKEN));
      _inputSegmentNames = configs.get(MinionConstants.SEGMENT_NAME_KEY);
      String replaceSegmentsString = configs.get(MinionConstants.ENABLE_REPLACE_SEGMENTS_KEY);
      _replaceSegmentsEnabled = Boolean.parseBoolean(replaceSegmentsString);
      _customMap = new HashMap<>();
    }

    public PinotTaskConfig getPinotTaskConfig() {
      return _pinotTaskConfig;
    }

    public List<SegmentConversionResult> getSegmentConversionResults() {
      return _segmentConversionResults;
    }

    public String getTableNameWithType() {
      return _tableNameWithType;
    }

    public String getUploadURL() {
      return _uploadURL;
    }

    public AuthProvider getAuthProvider() {
      return _authProvider;
    }

    public String getInputSegmentNames() {
      return _inputSegmentNames;
    }

    public boolean isReplaceSegmentsEnabled() {
      return _replaceSegmentsEnabled;
    }

    public Object getCustomContext(String key) {
      return _customMap.get(key);
    }

    public void setCustomContext(String key, Object value) {
      _customMap.put(key, value);
    }
  }
}
