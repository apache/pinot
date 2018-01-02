/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.minion.executor;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.config.PinotTaskConfig;
import com.linkedin.pinot.common.segment.fetcher.SegmentFetcherFactory;
import com.linkedin.pinot.common.utils.FileUploadUtils;
import com.linkedin.pinot.common.utils.PinotMinionUserAgentHeader;
import com.linkedin.pinot.common.utils.TarGzCompressionUtils;
import com.linkedin.pinot.core.common.MinionConstants;
import com.linkedin.pinot.minion.exception.TaskCancelledException;
import java.io.File;
import java.io.FileInputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import org.apache.commons.httpclient.Header;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Base class which provides a framework for segment conversion tasks.
 * <p>This class handles segment download and upload.
 * <p>To extends this base class, override the {@link #convert(PinotTaskConfig, File, File)} method to plug in the logic
 * to convert the segment.
 */
public abstract class BaseSegmentConversionExecutor extends BaseTaskExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseSegmentConversionExecutor.class);

  /**
   * Convert the segment based on the given {@link PinotTaskConfig}.
   *
   * @param pinotTaskConfig Task config
   * @param originalIndexDir Index directory for the original segment
   * @param workingDir Working directory for the converted segment
   * @return Index directory for the converted segment
   * @throws Exception
   */
  protected abstract File convert(@Nonnull PinotTaskConfig pinotTaskConfig, @Nonnull File originalIndexDir,
      @Nonnull File workingDir) throws Exception;

  @Override
  public void executeTask(@Nonnull PinotTaskConfig pinotTaskConfig) throws Exception {
    String taskType = pinotTaskConfig.getTaskType();
    Map<String, String> configs = pinotTaskConfig.getConfigs();
    String tableName = configs.get(MinionConstants.TABLE_NAME_KEY);
    String segmentName = configs.get(MinionConstants.SEGMENT_NAME_KEY);
    String downloadURL = configs.get(MinionConstants.DOWNLOAD_URL_KEY);
    String uploadURL = configs.get(MinionConstants.UPLOAD_URL_KEY);
    String originalSegmentCrc = configs.get(MinionConstants.ORIGINAL_SEGMENT_CRC_KEY);

    LOGGER.info("Start executing {} on table: {}, segment: {} with downloadURL: {}, uploadURL: {}", taskType, tableName,
        segmentName, downloadURL, uploadURL);

    File tempDataDir = new File(new File(MINION_CONTEXT.getDataDir(), taskType), "tmp-" + System.nanoTime());
    Preconditions.checkState(tempDataDir.mkdirs());
    try {
      // Download the tarred segment file
      File tarredSegmentFile = new File(tempDataDir, "tarredSegmentFile");
      SegmentFetcherFactory.getSegmentFetcherBasedOnURI(downloadURL)
          .fetchSegmentToLocal(downloadURL, tarredSegmentFile);

      // Un-tar the segment file
      File segmentDir = new File(tempDataDir, "segmentDir");
      TarGzCompressionUtils.unTar(tarredSegmentFile, segmentDir);
      File[] files = segmentDir.listFiles();
      Preconditions.checkState(files != null && files.length == 1);
      File indexDir = files[0];

      // Convert the segment
      File workingDir = new File(tempDataDir, "workingDir");
      Preconditions.checkState(workingDir.mkdir());
      File convertedIndexDir = convert(pinotTaskConfig, indexDir, workingDir);

      // Tar the converted segment
      File convertedTarredSegmentDir = new File(tempDataDir, "convertedTarredSegmentDir");
      Preconditions.checkState(convertedTarredSegmentDir.mkdir());
      File convertedTarredSegmentFile = new File(
          TarGzCompressionUtils.createTarGzOfDirectory(convertedIndexDir.getPath(),
              new File(convertedTarredSegmentDir, segmentName).getPath()));

      // Check whether the task get cancelled before uploading the segment
      if (_cancelled) {
        LOGGER.info("{} on table: {}, segment: {} got cancelled", taskType, tableName, segmentName);
        throw new TaskCancelledException(
            taskType + " on table: " + tableName + ", segment: " + segmentName + " got cancelled");
      }

      // Upload the converted tarred segment file with original segment crc and task type encoded in the http headers.
      // Original segment crc is used to check whether the original segment get refreshed, so that the newer segment
      // won't get override. We can decide how to handle the segment based on the task type on controller side.
      // NOTE: even segment is not changed, still need to upload the segment to update the segment ZK metadata so that
      // segment will not be submitted again
      Header ifMatchHeader = new Header(HttpHeaders.IF_MATCH, originalSegmentCrc);
      String userAgentParameter = PinotMinionUserAgentHeader.constructUserAgentHeader(taskType, MINION_CONTEXT.getMinionVersion());
      Header userAgentHeader = new Header(HttpHeaders.USER_AGENT, userAgentParameter);
      List<Header> httpHeaders = Arrays.asList(ifMatchHeader, userAgentHeader);
      FileUploadUtils.sendFile(uploadURL, segmentName,
          new FileInputStream(convertedTarredSegmentFile), convertedTarredSegmentFile.length(),
          FileUploadUtils.SendFileMethod.POST, httpHeaders);

      LOGGER.info("Done executing {} on table: {}, segment: {}", taskType, tableName, segmentName);
    } finally {
      FileUtils.deleteQuietly(tempDataDir);
    }
  }
}
