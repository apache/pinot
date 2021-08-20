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
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.http.Header;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicNameValuePair;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadataCustomMapModifier;
import org.apache.pinot.common.restlet.resources.StartReplaceSegmentsRequest;
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

  /**
   * Converts the segment based on the given {@link PinotTaskConfig}.
   *
   * @param pinotTaskConfig Task config
   * @param segmentDirs Index directories for the original segments
   * @param workingDir Working directory for the converted segment
   * @return a list of segment conversion result
   * @throws Exception
   */
  protected abstract List<SegmentConversionResult> convert(PinotTaskConfig pinotTaskConfig, List<File> segmentDirs, File workingDir)
      throws Exception;

  /**
   * Pre processing operations to be done at the beginning of task execution
   */
  protected void preProcess(PinotTaskConfig pinotTaskConfig) {
  }

  /**
   * Post processing operations to be done before exiting a successful task execution
   */
  protected void postProcess(PinotTaskConfig pinotTaskConfig) {
  }

  @Override
  public List<SegmentConversionResult> executeTask(PinotTaskConfig pinotTaskConfig)
      throws Exception {
    preProcess(pinotTaskConfig);

    String taskType = pinotTaskConfig.getTaskType();
    Map<String, String> configs = pinotTaskConfig.getConfigs();
    String tableNameWithType = configs.get(MinionConstants.TABLE_NAME_KEY);
    String inputSegmentNames = configs.get(MinionConstants.SEGMENT_NAME_KEY);
    String downloadURLString = configs.get(MinionConstants.DOWNLOAD_URL_KEY);
    String[] downloadURLs = downloadURLString.split(MinionConstants.URL_SEPARATOR);
    String uploadURL = configs.get(MinionConstants.UPLOAD_URL_KEY);
    String authToken = configs.get(MinionConstants.AUTH_TOKEN);
    String replaceSegmentsString = configs.get(MinionConstants.ENABLE_REPLACE_SEGMENTS_KEY);
    boolean replaceSegmentsEnabled = Boolean.parseBoolean(replaceSegmentsString);

    LOGGER.info("Start executing {} on table: {}, input segments: {} with downloadURLs: {}, uploadURL: {}", taskType, tableNameWithType, inputSegmentNames,
        downloadURLString, uploadURL);

    File tempDataDir = new File(new File(MINION_CONTEXT.getDataDir(), taskType), "tmp-" + UUID.randomUUID());
    Preconditions.checkState(tempDataDir.mkdirs());
    String crypterName = getTableConfig(tableNameWithType).getValidationConfig().getCrypterClassName();

    try {
      List<File> inputSegmentDirs = new ArrayList<>();
      for (int i = 0; i < downloadURLs.length; i++) {
        // Download the segment file
        File tarredSegmentFile = new File(tempDataDir, "tarredSegmentFile_" + i);
        LOGGER.info("Downloading segment from {} to {}", downloadURLs[i], tarredSegmentFile.getAbsolutePath());
        SegmentFetcherFactory.fetchAndDecryptSegmentToLocal(downloadURLs[i], tarredSegmentFile, crypterName);

        // Un-tar the segment file
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
      for (SegmentConversionResult segmentConversionResult : segmentConversionResults) {
        // Tar the converted segment
        File convertedSegmentDir = segmentConversionResult.getFile();
        File convertedSegmentTarFile =
            new File(convertedTarredSegmentDir, segmentConversionResult.getSegmentName() + TarGzCompressionUtils.TAR_GZ_FILE_EXTENSION);
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
        throw new TaskCancelledException(taskType + " on table: " + tableNameWithType + ", segments: " + inputSegmentNames + " got cancelled");
      }

      // Update the segment lineage to indicate that the segment replacement is in progress.
      String lineageEntryId = null;
      if (replaceSegmentsEnabled) {
        List<String> segmentsFrom = Arrays.stream(inputSegmentNames.split(",")).map(String::trim).collect(Collectors.toList());
        List<String> segmentsTo = segmentConversionResults.stream().map(SegmentConversionResult::getSegmentName).collect(Collectors.toList());
        lineageEntryId = SegmentConversionUtils.startSegmentReplace(tableNameWithType, uploadURL, new StartReplaceSegmentsRequest(segmentsFrom, segmentsTo));
      }

      // Upload the tarred segments
      for (int i = 0; i < numOutputSegments; i++) {
        File convertedTarredSegmentFile = tarredSegmentFiles.get(i);
        SegmentConversionResult segmentConversionResult = segmentConversionResults.get(i);
        String resultSegmentName = segmentConversionResult.getSegmentName();

        // Set segment ZK metadata custom map modifier into HTTP header to modify the segment ZK metadata
        SegmentZKMetadataCustomMapModifier segmentZKMetadataCustomMapModifier = getSegmentZKMetadataCustomMapModifier(pinotTaskConfig, segmentConversionResult);
        Header segmentZKMetadataCustomMapModifierHeader =
            new BasicHeader(FileUploadDownloadClient.CustomHeaders.SEGMENT_ZK_METADATA_CUSTOM_MAP_MODIFIER, segmentZKMetadataCustomMapModifier.toJsonString());

        List<Header> httpHeaders = new ArrayList<>();
        httpHeaders.add(segmentZKMetadataCustomMapModifierHeader);
        httpHeaders.addAll(FileUploadDownloadClient.makeAuthHeader(authToken));

        // Set parameters for upload request
        NameValuePair enableParallelPushProtectionParameter =
            new BasicNameValuePair(FileUploadDownloadClient.QueryParameters.ENABLE_PARALLEL_PUSH_PROTECTION, "true");
        NameValuePair tableNameParameter =
            new BasicNameValuePair(FileUploadDownloadClient.QueryParameters.TABLE_NAME, TableNameBuilder.extractRawTableName(tableNameWithType));
        List<NameValuePair> parameters = Arrays.asList(enableParallelPushProtectionParameter, tableNameParameter);

        SegmentConversionUtils.uploadSegment(configs, httpHeaders, parameters, tableNameWithType, resultSegmentName, uploadURL, convertedTarredSegmentFile);
        if (!FileUtils.deleteQuietly(convertedTarredSegmentFile)) {
          LOGGER.warn("Failed to delete tarred converted segment: {}", convertedTarredSegmentFile.getAbsolutePath());
        }
      }

      // Update the segment lineage to indicate that the segment replacement is done.
      if (replaceSegmentsEnabled) {
        SegmentConversionUtils.endSegmentReplace(tableNameWithType, uploadURL, lineageEntryId);
      }

      String outputSegmentNames = segmentConversionResults.stream().map(SegmentConversionResult::getSegmentName).collect(Collectors.joining(","));
      postProcess(pinotTaskConfig);
      LOGGER
          .info("Done executing {} on table: {}, input segments: {}, output segments: {}", taskType, tableNameWithType, inputSegmentNames, outputSegmentNames);

      return segmentConversionResults;
    } finally {
      FileUtils.deleteQuietly(tempDataDir);
    }
  }
}
