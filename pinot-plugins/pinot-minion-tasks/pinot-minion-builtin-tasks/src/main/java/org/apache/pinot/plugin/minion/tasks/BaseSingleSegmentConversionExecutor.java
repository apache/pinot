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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicNameValuePair;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadataCustomMapModifier;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.common.utils.fetcher.SegmentFetcherFactory;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.minion.exception.TaskCancelledException;
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

  /**
   * Converts the segment based on the given task config and returns the conversion result.
   */
  protected abstract SegmentConversionResult convert(PinotTaskConfig pinotTaskConfig, File indexDir, File workingDir)
      throws Exception;

  @Override
  public SegmentConversionResult executeTask(PinotTaskConfig pinotTaskConfig)
      throws Exception {
    String taskType = pinotTaskConfig.getTaskType();
    Map<String, String> configs = pinotTaskConfig.getConfigs();
    String tableNameWithType = configs.get(MinionConstants.TABLE_NAME_KEY);
    String segmentName = configs.get(MinionConstants.SEGMENT_NAME_KEY);
    String downloadURL = configs.get(MinionConstants.DOWNLOAD_URL_KEY);
    String uploadURL = configs.get(MinionConstants.UPLOAD_URL_KEY);
    String originalSegmentCrc = configs.get(MinionConstants.ORIGINAL_SEGMENT_CRC_KEY);
    String authToken = configs.get(MinionConstants.AUTH_TOKEN);

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
    String crypterName = getTableConfig(tableNameWithType).getValidationConfig().getCrypterClassName();

    try {
      // Download the tarred segment file
      File tarredSegmentFile = new File(tempDataDir, "tarredSegment");
      LOGGER.info("Downloading segment from {} to {}", downloadURL, tarredSegmentFile.getAbsolutePath());
      SegmentFetcherFactory.fetchAndDecryptSegmentToLocal(downloadURL, tarredSegmentFile, crypterName);

      // Un-tar the segment file
      File segmentDir = new File(tempDataDir, "segmentDir");
      File indexDir = TarGzCompressionUtils.untar(tarredSegmentFile, segmentDir).get(0);
      if (!FileUtils.deleteQuietly(tarredSegmentFile)) {
        LOGGER.warn("Failed to delete tarred input segment: {}", tarredSegmentFile.getAbsolutePath());
      }

      // Convert the segment
      File workingDir = new File(tempDataDir, "workingDir");
      Preconditions.checkState(workingDir.mkdir());
      SegmentConversionResult segmentConversionResult = convert(pinotTaskConfig, indexDir, workingDir);
      Preconditions.checkState(segmentConversionResult.getSegmentName().equals(segmentName),
          "Converted segment name: %s does not match original segment name: %s",
          segmentConversionResult.getSegmentName(), segmentName);

      // Delete the input segment
      if (!FileUtils.deleteQuietly(indexDir)) {
        LOGGER.warn("Failed to delete input segment: {}", indexDir.getAbsolutePath());
      }

      // Tar the converted segment
      File convertedSegmentDir = segmentConversionResult.getFile();
      File convertedTarredSegmentFile =
          new File(tempDataDir, segmentName + TarGzCompressionUtils.TAR_GZ_FILE_EXTENSION);
      TarGzCompressionUtils.createTarGzFile(convertedSegmentDir, convertedTarredSegmentFile);
      if (!FileUtils.deleteQuietly(convertedSegmentDir)) {
        LOGGER.warn("Failed to delete converted segment: {}", convertedSegmentDir.getAbsolutePath());
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
      httpHeaders.add(segmentZKMetadataCustomMapModifierHeader);
      httpHeaders.addAll(FileUploadDownloadClient.makeAuthHeader(authToken));

      // Set parameters for upload request.
      NameValuePair enableParallelPushProtectionParameter =
          new BasicNameValuePair(FileUploadDownloadClient.QueryParameters.ENABLE_PARALLEL_PUSH_PROTECTION, "true");
      NameValuePair tableNameParameter = new BasicNameValuePair(FileUploadDownloadClient.QueryParameters.TABLE_NAME,
          TableNameBuilder.extractRawTableName(tableNameWithType));
      List<NameValuePair> parameters = Arrays.asList(enableParallelPushProtectionParameter, tableNameParameter);

      // Upload the tarred segment
      SegmentConversionUtils.uploadSegment(configs, httpHeaders, parameters, tableNameWithType, segmentName, uploadURL,
          convertedTarredSegmentFile);
      if (!FileUtils.deleteQuietly(convertedTarredSegmentFile)) {
        LOGGER.warn("Failed to delete tarred converted segment: {}", convertedTarredSegmentFile.getAbsolutePath());
      }

      LOGGER.info("Done executing {} on table: {}, segment: {}", taskType, tableNameWithType, segmentName);
      return segmentConversionResult;
    } finally {
      FileUtils.deleteQuietly(tempDataDir);
    }
  }
}
