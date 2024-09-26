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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.hc.core5.http.message.BasicNameValuePair;
import org.apache.pinot.common.auth.AuthProviderUtils;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadataCustomMapModifier;
import org.apache.pinot.common.metrics.MinionMeter;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.minion.event.MinionEventObserver;
import org.apache.pinot.minion.event.MinionEventObservers;
import org.apache.pinot.minion.exception.TaskCancelledException;
import org.apache.pinot.plugin.minion.tasks.purge.PurgeTaskExecutor;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Base class which provides a framework for a single segment conversion task that refreshes the existing segment.
 * <p>This class handles segment download and upload.
 * <p>To extends this base class, override the {@link #convert(PinotTaskConfig, File, File)} method to plug in the logic
 * to convert the segment.
 */
public abstract class BaseSingleSegmentConversionExecutor extends BaseTaskExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseSingleSegmentConversionExecutor.class);

  // Tracking finer grained progress status.
  protected PinotTaskConfig _pinotTaskConfig;
  protected MinionEventObserver _eventObserver;

  /**
   * Converts the segment based on the given task config and returns the conversion result.
   */
  protected abstract SegmentConversionResult convert(PinotTaskConfig pinotTaskConfig, File indexDir, File workingDir)
      throws Exception;

  @Override
  public SegmentConversionResult executeTask(PinotTaskConfig pinotTaskConfig)
      throws Exception {
    _pinotTaskConfig = pinotTaskConfig;
    _eventObserver = MinionEventObservers.getInstance().getMinionEventObserver(pinotTaskConfig.getTaskId());
    String taskType = pinotTaskConfig.getTaskType();
    Map<String, String> configs = pinotTaskConfig.getConfigs();
    String tableNameWithType = configs.get(MinionConstants.TABLE_NAME_KEY);
    String segmentName = configs.get(MinionConstants.SEGMENT_NAME_KEY);
    String downloadURL = configs.get(MinionConstants.DOWNLOAD_URL_KEY);
    String uploadURL = configs.get(MinionConstants.UPLOAD_URL_KEY);
    String originalSegmentCrc = configs.get(MinionConstants.ORIGINAL_SEGMENT_CRC_KEY);
    AuthProvider authProvider = AuthProviderUtils.makeAuthProvider(configs.get(MinionConstants.AUTH_TOKEN));

    long currentSegmentCrc = getSegmentCrc(tableNameWithType, segmentName);
    if (Long.parseLong(originalSegmentCrc) != currentSegmentCrc) {
      LOGGER.info("Segment CRC does not match, skip the task. Original CRC: {}, current CRC: {}", originalSegmentCrc,
          currentSegmentCrc);
      return new SegmentConversionResult.Builder().setTableNameWithType(tableNameWithType).setSegmentName(segmentName)
          .build();
    }

    LOGGER.info("Start executing {} on table: {}, segment: {} with downloadURL: {}, uploadURL: {}", taskType,
        tableNameWithType, segmentName, downloadURL, uploadURL);

    File tempDataDir = new File(new File(MINION_CONTEXT.getDataDir(), taskType), "tmp-" + UUID.randomUUID());
    Preconditions.checkState(tempDataDir.mkdirs(), "Failed to create temporary directory: %s", tempDataDir);
    try {
      // Download and decompress the segment file
      _eventObserver.notifyProgress(_pinotTaskConfig, "Downloading and decompressing segment from: "
          + downloadURL);
      File indexDir;
      try {
        indexDir = downloadSegmentToLocalAndUntar(tableNameWithType, segmentName, downloadURL, taskType,
            tempDataDir, "");
      } catch (Exception e) {
        LOGGER.error("Failed to download segment from download url: {}", downloadURL, e);
        _minionMetrics.addMeteredTableValue(tableNameWithType, MinionMeter.SEGMENT_DOWNLOAD_FAIL_COUNT, 1L);
        _eventObserver.notifyTaskError(_pinotTaskConfig, e);
        throw e;
      }

      // Publish metrics related to segment download
      reportSegmentDownloadMetrics(indexDir, tableNameWithType, taskType);

      // Convert the segment
      File workingDir = new File(tempDataDir, "workingDir");
      Preconditions.checkState(workingDir.mkdir());
      SegmentConversionResult segmentConversionResult = convert(pinotTaskConfig, indexDir, workingDir);
      Preconditions.checkState(segmentConversionResult.getSegmentName().equals(segmentName),
          "Converted segment name: %s does not match original segment name: %s",
          segmentConversionResult.getSegmentName(), segmentName);

      File convertedSegmentDir = segmentConversionResult.getFile();
      if (convertedSegmentDir == null) {
        return segmentConversionResult;
      }

      // Publish metrics related to segment upload
      reportSegmentUploadMetrics(workingDir, tableNameWithType, taskType);

      // Collect the task processing metrics from various single segment executors and publish them here.
      SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(indexDir);
      Object numRecordsPurged = segmentConversionResult.getCustomProperty(PurgeTaskExecutor.NUM_RECORDS_PURGED_KEY);
      if (numRecordsPurged != null) {
        reportTaskProcessingMetrics(tableNameWithType, taskType, segmentMetadata.getTotalDocs(),
            (int) numRecordsPurged);
      } else {
        reportTaskProcessingMetrics(tableNameWithType, taskType, segmentMetadata.getTotalDocs());
      }

      // Tar the converted segment
      _eventObserver.notifyProgress(_pinotTaskConfig, "Compressing segment: " + segmentName);
      File convertedTarredSegmentFile = new File(tempDataDir, segmentName + TarCompressionUtils.TAR_GZ_FILE_EXTENSION);
      TarCompressionUtils.createCompressedTarFile(convertedSegmentDir, convertedTarredSegmentFile);
      if (!FileUtils.deleteQuietly(convertedSegmentDir)) {
        LOGGER.warn("Failed to delete converted segment: {}", convertedSegmentDir.getAbsolutePath());
      }

      // Delete the input segment after tarring the converted segment to avoid deleting the converted segment when the
      // conversion happens in-place (converted segment dir is the same as input segment dir). It could also happen when
      // the conversion is not required, and the input segment dir is returned as the result.
      if (indexDir.exists() && !FileUtils.deleteQuietly(indexDir)) {
        LOGGER.warn("Failed to delete input segment: {}", indexDir.getAbsolutePath());
      }

      // Check whether the task get cancelled before uploading the segment
      if (_cancelled) {
        LOGGER.info("{} on table: {}, segment: {} got cancelled", taskType, tableNameWithType, segmentName);
        throw new TaskCancelledException(
            taskType + " on table: " + tableNameWithType + ", segment: " + segmentName + " got cancelled");
      }

      // Set original segment CRC into HTTP IF-MATCH header to check whether the original segment get refreshed, so that
      // the newer segment won't get override
      Header ifMatchHeader = new BasicHeader(HttpHeaders.IF_MATCH, originalSegmentCrc);

      // Only upload segment if it exists
      Header refreshOnlyHeader = new BasicHeader(FileUploadDownloadClient.CustomHeaders.REFRESH_ONLY, "true");

      // Set segment ZK metadata custom map modifier into HTTP header to modify the segment ZK metadata
      // NOTE: even segment is not changed, still need to upload the segment to update the segment ZK metadata so that
      // segment will not be submitted again
      SegmentZKMetadataCustomMapModifier segmentZKMetadataCustomMapModifier =
          getSegmentZKMetadataCustomMapModifier(pinotTaskConfig, segmentConversionResult);
      Header segmentZKMetadataCustomMapModifierHeader =
          new BasicHeader(FileUploadDownloadClient.CustomHeaders.SEGMENT_ZK_METADATA_CUSTOM_MAP_MODIFIER,
              segmentZKMetadataCustomMapModifier.toJsonString());

      List<Header> httpHeaders = new ArrayList<>();
      httpHeaders.add(ifMatchHeader);
      httpHeaders.add(refreshOnlyHeader);
      httpHeaders.add(segmentZKMetadataCustomMapModifierHeader);
      httpHeaders.addAll(AuthProviderUtils.toRequestHeaders(authProvider));

      // Set parameters for upload request.
      NameValuePair enableParallelPushProtectionParameter =
          new BasicNameValuePair(FileUploadDownloadClient.QueryParameters.ENABLE_PARALLEL_PUSH_PROTECTION, "true");
      NameValuePair tableNameParameter = new BasicNameValuePair(FileUploadDownloadClient.QueryParameters.TABLE_NAME,
          TableNameBuilder.extractRawTableName(tableNameWithType));
      NameValuePair tableTypeParameter = new BasicNameValuePair(FileUploadDownloadClient.QueryParameters.TABLE_TYPE,
          TableNameBuilder.getTableTypeFromTableName(tableNameWithType).toString());
      List<NameValuePair> parameters =
          Arrays.asList(enableParallelPushProtectionParameter, tableNameParameter, tableTypeParameter);

      // Upload the tarred segment
      _eventObserver.notifyProgress(_pinotTaskConfig, "Uploading segment: " + segmentName);
      boolean uploadSuccessful = true;
      try {
        SegmentConversionUtils.uploadSegment(configs, httpHeaders, parameters, tableNameWithType, segmentName,
            uploadURL, convertedTarredSegmentFile);
      } catch (Exception e) {
        uploadSuccessful = false;
        _minionMetrics.addMeteredTableValue(tableNameWithType, MinionMeter.SEGMENT_UPLOAD_FAIL_COUNT, 1L);
        LOGGER.error("Segment upload failed for segment {}, table {}", segmentName, tableNameWithType, e);
        _eventObserver.notifyTaskError(_pinotTaskConfig, e);
      }
      if (!FileUtils.deleteQuietly(convertedTarredSegmentFile)) {
        LOGGER.warn("Failed to delete tarred converted segment: {}", convertedTarredSegmentFile.getAbsolutePath());
      }

      if (uploadSuccessful) {
        LOGGER.info("Done executing {} on table: {}, segment: {}", taskType, tableNameWithType, segmentName);
      }

      return segmentConversionResult;
    } finally {
      FileUtils.deleteQuietly(tempDataDir);
    }
  }

  // For tests only.
  @VisibleForTesting
  public void setMinionEventObserver(MinionEventObserver observer) {
    _eventObserver = observer;
  }
}
