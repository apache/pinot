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
import com.linkedin.pinot.common.exception.HttpErrorStatusException;
import com.linkedin.pinot.common.metadata.segment.SegmentZKMetadataCustomMapModifier;
import com.linkedin.pinot.common.segment.fetcher.SegmentFetcherFactory;
import com.linkedin.pinot.common.utils.FileUploadDownloadClient;
import com.linkedin.pinot.common.utils.TarGzCompressionUtils;
import com.linkedin.pinot.common.utils.retry.RetryPolicies;
import com.linkedin.pinot.common.utils.retry.RetryPolicy;
import com.linkedin.pinot.core.common.MinionConstants;
import com.linkedin.pinot.minion.exception.TaskCancelledException;
import java.io.File;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import javax.annotation.Nonnull;
import org.apache.commons.io.FileUtils;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicNameValuePair;
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

  private static final int DEFAULT_MAX_NUM_ATTEMPTS = 5;
  private static final long DEFAULT_INITIAL_RETRY_DELAY_MS = 1000L; // 1 second
  private static final float DEFAULT_RETRY_SCALE_FACTOR = 2f;

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

  protected abstract SegmentZKMetadataCustomMapModifier getSegmentZKMetadataCustomMapModifier() throws Exception;

  @Override
  public void executeTask(@Nonnull PinotTaskConfig pinotTaskConfig) throws Exception {
    String taskType = pinotTaskConfig.getTaskType();
    Map<String, String> configs = pinotTaskConfig.getConfigs();
    String tableName = configs.get(MinionConstants.TABLE_NAME_KEY);
    final String segmentName = configs.get(MinionConstants.SEGMENT_NAME_KEY);
    String downloadURL = configs.get(MinionConstants.DOWNLOAD_URL_KEY);
    final String uploadURL = configs.get(MinionConstants.UPLOAD_URL_KEY);
    String originalSegmentCrc = configs.get(MinionConstants.ORIGINAL_SEGMENT_CRC_KEY);

    LOGGER.info("Start executing {} on table: {}, segment: {} with downloadURL: {}, uploadURL: {}", taskType, tableName,
        segmentName, downloadURL, uploadURL);

    File tempDataDir = new File(new File(MINION_CONTEXT.getDataDir(), taskType), "tmp-" + System.nanoTime());
    Preconditions.checkState(tempDataDir.mkdirs());
    try {
      // Download the tarred segment file
      File tarredSegmentFile = new File(tempDataDir, "tarredSegmentFile");
      SegmentFetcherFactory.getInstance()
          .getSegmentFetcherBasedOnURI(downloadURL)
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
      final File convertedTarredSegmentFile = new File(
          TarGzCompressionUtils.createTarGzOfDirectory(convertedIndexDir.getPath(),
              new File(convertedTarredSegmentDir, segmentName).getPath()));

      // Check whether the task get cancelled before uploading the segment
      if (_cancelled) {
        LOGGER.info("{} on table: {}, segment: {} got cancelled", taskType, tableName, segmentName);
        throw new TaskCancelledException(
            taskType + " on table: " + tableName + ", segment: " + segmentName + " got cancelled");
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
      final List<Header> httpHeaders = Arrays.asList(ifMatchHeader, segmentZKMetadataCustomMapModifierHeader);

      // Set query parameters
      final List<NameValuePair> parameters = Collections.<NameValuePair>singletonList(
          new BasicNameValuePair(FileUploadDownloadClient.QueryParameters.ENABLE_PARALLEL_PUSH_PROTECTION, "true"));

      String maxNumAttemptsConfig = configs.get(MinionConstants.MAX_NUM_ATTEMPTS_KEY);
      int maxNumAttempts =
          maxNumAttemptsConfig != null ? Integer.parseInt(maxNumAttemptsConfig) : DEFAULT_MAX_NUM_ATTEMPTS;
      String initialRetryDelayMsConfig = configs.get(MinionConstants.INITIAL_RETRY_DELAY_MS_KEY);
      long initialRetryDelayMs = initialRetryDelayMsConfig != null ? Long.parseLong(initialRetryDelayMsConfig)
          : DEFAULT_INITIAL_RETRY_DELAY_MS;
      String retryScaleFactorConfig = configs.get(MinionConstants.RETRY_SCALE_FACTOR_KEY);
      float retryScaleFactor =
          retryScaleFactorConfig != null ? Float.parseFloat(retryScaleFactorConfig) : DEFAULT_RETRY_SCALE_FACTOR;
      RetryPolicy retryPolicy =
          RetryPolicies.exponentialBackoffRetryPolicy(maxNumAttempts, initialRetryDelayMs, retryScaleFactor);

      try (FileUploadDownloadClient fileUploadDownloadClient = new FileUploadDownloadClient()) {
        retryPolicy.attempt(new Callable<Boolean>() {
          @Override
          public Boolean call() throws Exception {
            try {
              fileUploadDownloadClient.uploadSegment(new URI(uploadURL), segmentName, convertedTarredSegmentFile,
                  httpHeaders, parameters, FileUploadDownloadClient.DEFAULT_SOCKET_TIMEOUT_MS);
              return true;
            } catch (HttpErrorStatusException e) {
              int statusCode = e.getStatusCode();
              if (statusCode == HttpStatus.SC_CONFLICT || statusCode >= 500) {
                // Temporary exception
                LOGGER.warn("Caught temporary exception while uploading segment: {}, will retry", segmentName, e);
                return false;
              } else {
                // Permanent exception
                LOGGER.error("Caught permanent exception while uploading segment: {}, won't retry", segmentName, e);
                throw e;
              }
            } catch (Exception e) {
              LOGGER.warn("Caught temporary exception while uploading segment: {}, will retry", segmentName, e);
              return false;
            }
          }
        });
      }

      LOGGER.info("Done executing {} on table: {}, segment: {}", taskType, tableName, segmentName);
    } finally {
      FileUtils.deleteQuietly(tempDataDir);
    }
  }
}
