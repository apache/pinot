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
package org.apache.pinot.minion.executor;

import com.google.common.base.Preconditions;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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

  /**
   * Returns the segment ZK metadata custom map modifier.
   */
  protected abstract SegmentZKMetadataCustomMapModifier getSegmentZKMetadataCustomMapModifier();

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

    LOGGER.info("Start executing {} on table: {}, segment: {} with downloadURL: {}, uploadURL: {}", taskType,
        tableNameWithType, segmentName, downloadURL, uploadURL);

    File tempDataDir = new File(new File(MINION_CONTEXT.getDataDir(), taskType), "tmp-" + System.nanoTime());
    Preconditions.checkState(tempDataDir.mkdirs(), "Failed to create temporary directory: %s", tempDataDir);
    try {
      // Download the tarred segment file
      File tarredSegmentFile = new File(tempDataDir, "tarredSegment");
      LOGGER.info("Downloading segment from {} to {}", downloadURL, tarredSegmentFile.getAbsolutePath());
      SegmentFetcherFactory.fetchSegmentToLocal(downloadURL, tarredSegmentFile);

      // Un-tar the segment file
      File segmentDir = new File(tempDataDir, "segmentDir");
      TarGzCompressionUtils.unTar(tarredSegmentFile, segmentDir);
      File[] files = segmentDir.listFiles();
      Preconditions.checkState(files != null && files.length == 1);
      File indexDir = files[0];

      // Convert the segment
      File workingDir = new File(tempDataDir, "workingDir");
      Preconditions.checkState(workingDir.mkdir());
      SegmentConversionResult segmentConversionResult = convert(pinotTaskConfig, indexDir, workingDir);
      Preconditions.checkState(segmentConversionResult.getSegmentName().equals(segmentName),
          "Converted segment name: %s does not match original segment name: %s",
          segmentConversionResult.getSegmentName(), segmentName);

      // Tar the converted segment
      File convertedTarredSegment = new File(TarGzCompressionUtils
          .createTarGzOfDirectory(segmentConversionResult.getFile().getPath(),
              new File(tempDataDir, "convertedTarredSegment").getPath()));

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
      SegmentZKMetadataCustomMapModifier segmentZKMetadataCustomMapModifier = getSegmentZKMetadataCustomMapModifier();
      Header segmentZKMetadataCustomMapModifierHeader =
          new BasicHeader(FileUploadDownloadClient.CustomHeaders.SEGMENT_ZK_METADATA_CUSTOM_MAP_MODIFIER,
              segmentZKMetadataCustomMapModifier.toJsonString());

      List<Header> httpHeaders = Arrays.asList(ifMatchHeader, segmentZKMetadataCustomMapModifierHeader);

      // Set parameters for upload request.
      NameValuePair enableParallelPushProtectionParameter =
          new BasicNameValuePair(FileUploadDownloadClient.QueryParameters.ENABLE_PARALLEL_PUSH_PROTECTION, "true");
      NameValuePair tableNameParameter = new BasicNameValuePair(FileUploadDownloadClient.QueryParameters.TABLE_NAME,
          TableNameBuilder.extractRawTableName(tableNameWithType));
      List<NameValuePair> parameters = Arrays.asList(enableParallelPushProtectionParameter, tableNameParameter);

      // Upload the tarred segment
      SegmentConversionUtils.uploadSegment(configs, httpHeaders, parameters, tableNameWithType, segmentName, uploadURL,
          convertedTarredSegment);

      LOGGER.info("Done executing {} on table: {}, segment: {}", taskType, tableNameWithType, segmentName);
      return segmentConversionResult;
    } finally {
      FileUtils.deleteQuietly(tempDataDir);
    }
  }
}
