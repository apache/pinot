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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.commons.io.FileUtils;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;
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
 */
public abstract class BaseMultipleSegmentsConversionExecutor extends BaseTaskExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseMultipleSegmentsConversionExecutor.class);

  /**
   * Converts the segment based on the given {@link PinotTaskConfig}.
   *
   * @param pinotTaskConfig Task config
   * @param originalIndexDir Index directory for the original segment
   * @param workingDir Working directory for the converted segment
   * @return a list of segment conversion result
   * @throws Exception
   */
  protected abstract List<SegmentConversionResult> convert(@Nonnull PinotTaskConfig pinotTaskConfig,
      @Nonnull List<File> originalIndexDir, @Nonnull File workingDir)
      throws Exception;

  @Override
  public List<SegmentConversionResult> executeTask(@Nonnull PinotTaskConfig pinotTaskConfig)
      throws Exception {
    String taskType = pinotTaskConfig.getTaskType();
    Map<String, String> configs = pinotTaskConfig.getConfigs();
    String tableNameWithType = configs.get(MinionConstants.TABLE_NAME_KEY);
    String inputSegmentNames = configs.get(MinionConstants.SEGMENT_NAME_KEY);
    String downloadURLString = configs.get(MinionConstants.DOWNLOAD_URL_KEY);
    String[] downloadURLs = downloadURLString.split(MinionConstants.URL_SEPARATOR);
    String uploadURL = configs.get(MinionConstants.UPLOAD_URL_KEY);

    LOGGER.info("Start executing {} on table: {}, input segments: {} with downloadURLs: {}, uploadURL: {}", taskType,
        tableNameWithType, inputSegmentNames, downloadURLString, uploadURL);

    File tempDataDir = new File(new File(MINION_CONTEXT.getDataDir(), taskType), "tmp-" + System.nanoTime());
    Preconditions.checkState(tempDataDir.mkdirs());

    try {
      List<File> inputSegmentFiles = new ArrayList<>();
      for (int i = 0; i < downloadURLs.length; i++) {
        // Download the segment file
        File tarredSegmentFile = new File(tempDataDir, "tarredSegmentFile_" + i);
        LOGGER.info("Downloading segment from {} to {}", downloadURLs[i], tarredSegmentFile.getAbsolutePath());
        SegmentFetcherFactory.fetchSegmentToLocal(downloadURLs[i], tarredSegmentFile);

        // Un-tar the segment file
        File segmentDir = new File(tempDataDir, "segmentDir_" + i);
        TarGzCompressionUtils.unTar(tarredSegmentFile, segmentDir);
        File[] files = segmentDir.listFiles();
        Preconditions.checkState(files != null && files.length == 1);
        File indexDir = files[0];
        inputSegmentFiles.add(indexDir);
      }

      // Convert the segments
      File workingDir = new File(tempDataDir, "workingDir");
      Preconditions.checkState(workingDir.mkdir());
      List<SegmentConversionResult> segmentConversionResults = convert(pinotTaskConfig, inputSegmentFiles, workingDir);

      // Create a directory for converted tarred segment files
      File convertedTarredSegmentDir = new File(tempDataDir, "convertedTarredSegmentDir");
      Preconditions.checkState(convertedTarredSegmentDir.mkdir());

      int numOutputSegments = segmentConversionResults.size();
      List<File> tarredSegmentFiles = new ArrayList<>(numOutputSegments);
      for (int i = 0; i < numOutputSegments; i++) {
        // Tar the converted segment
        SegmentConversionResult segmentConversionResult = segmentConversionResults.get(i);
        File convertedIndexDir = segmentConversionResult.getFile();
        File convertedTarredSegmentFile = new File(TarGzCompressionUtils
            .createTarGzOfDirectory(convertedIndexDir.getPath(),
                new File(convertedTarredSegmentDir, segmentConversionResult.getSegmentName()).getPath()));
        tarredSegmentFiles.add(convertedTarredSegmentFile);
      }

      // Check whether the task get cancelled before uploading the segment
      if (_cancelled) {
        LOGGER.info("{} on table: {}, segments: {} got cancelled", taskType, tableNameWithType, inputSegmentNames);
        throw new TaskCancelledException(
            taskType + " on table: " + tableNameWithType + ", segments: " + inputSegmentNames + " got cancelled");
      }

      // Upload the tarred segments
      for (int i = 0; i < numOutputSegments; i++) {
        File convertedTarredSegmentFile = tarredSegmentFiles.get(i);
        String resultSegmentName = segmentConversionResults.get(i).getSegmentName();

        // Set parameters for upload request
        NameValuePair enableParallelPushProtectionParameter =
            new BasicNameValuePair(FileUploadDownloadClient.QueryParameters.ENABLE_PARALLEL_PUSH_PROTECTION, "true");
        NameValuePair tableNameParameter = new BasicNameValuePair(FileUploadDownloadClient.QueryParameters.TABLE_NAME,
            TableNameBuilder.extractRawTableName(tableNameWithType));
        List<NameValuePair> parameters = Arrays.asList(enableParallelPushProtectionParameter, tableNameParameter);

        SegmentConversionUtils.uploadSegment(configs, null, parameters, tableNameWithType, resultSegmentName, uploadURL,
            convertedTarredSegmentFile);
      }

      String outputSegmentNames = segmentConversionResults.stream().map(SegmentConversionResult::getSegmentName)
          .collect(Collectors.joining(","));
      LOGGER
          .info("Done executing {} on table: {}, input segments: {}, output segments: {}", taskType, tableNameWithType,
              inputSegmentNames, outputSegmentNames);

      return segmentConversionResults;
    } finally {
      FileUtils.deleteQuietly(tempDataDir);
    }
  }
}
