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
package org.apache.pinot.server.predownload;

import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.common.utils.fetcher.SegmentFetcherFactory;
import org.apache.pinot.server.conf.ServerConf;
import org.apache.pinot.server.starter.helix.HelixInstanceDataManagerConfig;
import org.apache.pinot.spi.config.instance.InstanceDataManagerConfig;
import org.apache.pinot.spi.crypt.PinotCrypterFactory;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.retry.AttemptsExceededException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PredownloadScheduler {

  private static final Logger LOGGER = LoggerFactory.getLogger(PredownloadScheduler.class);
  private static final String TMP_DIR_NAME = "tmp";
  // Segment download dir in format of "tmp-" + segmentName + "-" + UUID.randomUUID()
  private static final String TMP_DIR_FORMAT = "tmp-%s-%s";
  // TODO: make download timeout configurable
  private static final long DOWNLOAD_SEGMENTS_TIMEOUT_MIN = 60;
  private static final long LOAD_SEGMENTS_TIMEOUT_MIN = 5;
  private final PropertiesConfiguration _properties;
  private final PinotConfiguration _pinotConfig;
  private final InstanceDataManagerConfig _instanceDataManagerConfig;
  private final String _clusterName;
  private final String _instanceId;
  private final String _zkAddress;
  @VisibleForTesting
  Executor _executor;
  @VisibleForTesting
  Set<String> _failedSegments;
  private PredownloadMetrics _predownloadMetrics;
  private int _numOfSkippedSegments;
  private int _numOfUnableToDownloadSegments;
  private int _numOfDownloadSegments;
  private long _totalDownloadedSizeBytes;
  private PredownloadZKClient _predownloadZkClient;
  private List<PredownloadSegmentInfo> _predownloadSegmentInfoList;
  private Map<String, PredownloadTableInfo> _tableInfoMap;

  public PredownloadScheduler(PropertiesConfiguration properties)
      throws Exception {
    _properties = properties;
    _clusterName = properties.getString(CommonConstants.Helix.CONFIG_OF_CLUSTER_NAME);
    _zkAddress = properties.getString(CommonConstants.Helix.CONFIG_OF_ZOOKEEPR_SERVER);
    _instanceId = properties.getString(CommonConstants.Server.CONFIG_OF_INSTANCE_ID);
    _pinotConfig = new PinotConfiguration(properties);
    _instanceDataManagerConfig =
        new HelixInstanceDataManagerConfig(new ServerConf(_pinotConfig).getInstanceDataManagerConfig());
    // Get the number of available processors (vCPUs)
    int numProcessors = Runtime.getRuntime().availableProcessors();
    _failedSegments = ConcurrentHashMap.newKeySet();
    // TODO: tune the value
    _executor = Executors.newFixedThreadPool(numProcessors * 3);
    LOGGER.info("Created thread pool with num of threads: {}", numProcessors * 3);
    _numOfSkippedSegments = 0;
    _numOfDownloadSegments = 0;
  }

  public void start() {

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          LOGGER.info("Trying to stop predownload process!");
          stop();
        } catch (Exception e) {
          LOGGER.error("error shutting down predownload process : ", e);
        }
      }
    });

    long startTime = System.currentTimeMillis();
    initializeZK();
    initializeMetricsReporter();
    initializeSegmentFetcher();
    getSegmentsInfo();
    loadSegmentsFromLocal();
    PredownloadCompletionReason reason = downloadSegments();
    long timeTaken = System.currentTimeMillis() - startTime;
    LOGGER.info(
        "Predownload process took {} sec, tried to download {} segments, skipped {} segments "
            + "and unable to download {} segments. Download size: {} MB. Download speed: {} MB/s",
        timeTaken / 1000, _numOfDownloadSegments, _numOfSkippedSegments, _numOfUnableToDownloadSegments,
        _totalDownloadedSizeBytes / (1024 * 1024),
        (_totalDownloadedSizeBytes / (1024 * 1024)) / (timeTaken / 1000 + 1));
    if (reason.isSucceed()) {
      _predownloadMetrics.preDownloadSucceed(_totalDownloadedSizeBytes, timeTaken);
    }
    PredownloadStatusRecorder.predownloadComplete(reason, _clusterName, _instanceId, String.join(",", _failedSegments));
  }

  public void stop() {
    if (_predownloadZkClient != null) {
      _predownloadZkClient.close();
    }
    if (_executor != null) {
      ((ThreadPoolExecutor) _executor).shutdownNow();
    }
  }

  void initializeZK() {
    LOGGER.info("Initializing ZK client with address: {} and instanceId: {}", _zkAddress, _instanceId);
    _predownloadZkClient = new PredownloadZKClient(_zkAddress, _clusterName, _instanceId);
    _predownloadZkClient.start();
  }

  void initializeMetricsReporter() {
    LOGGER.info("Initializing metrics reporter");

    _predownloadMetrics = new PredownloadMetrics();
    PredownloadStatusRecorder.registerMetrics(_predownloadMetrics);
  }

  @VisibleForTesting
  void getSegmentsInfo() {
    LOGGER.info("Getting segments info from ZK");
    _predownloadSegmentInfoList = _predownloadZkClient.getSegmentsOfInstance(_predownloadZkClient.getDataAccessor());
    if (_predownloadSegmentInfoList.isEmpty()) {
      PredownloadCompletionReason reason = PredownloadCompletionReason.NO_SEGMENT_TO_PREDOWNLOAD;
      PredownloadStatusRecorder.predownloadComplete(reason, _clusterName, _instanceId, "");
    }
    _tableInfoMap = new HashMap<>();
    _predownloadZkClient.updateSegmentMetadata(_predownloadSegmentInfoList, _tableInfoMap,
        _instanceDataManagerConfig);
  }

  @VisibleForTesting
  void loadSegmentsFromLocal() {
    LOGGER.info("Loading segments from local to reduce number of segments to download");
    long startTime = System.currentTimeMillis();
    List<CompletableFuture<Void>> futures = new ArrayList<>();

    // Submit tasks to the executor
    for (PredownloadSegmentInfo predownloadSegmentInfo : _predownloadSegmentInfoList) {
      CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
        boolean loadSegmentSuccess = false;
        try {
          PredownloadTableInfo predownloadTableInfo = _tableInfoMap.get(predownloadSegmentInfo.getTableNameWithType());
          if (predownloadTableInfo != null) {
            loadSegmentSuccess =
                predownloadTableInfo.loadSegmentFromLocal(predownloadSegmentInfo);
          }
        } catch (Exception e) {
          LOGGER.error("Failed to load from local for segment: {} of table: {} with issue ",
              predownloadSegmentInfo.getSegmentName(), predownloadSegmentInfo.getTableNameWithType(), e);
        }
        if (!loadSegmentSuccess && predownloadSegmentInfo.canBeDownloaded()) {
          _failedSegments.add(predownloadSegmentInfo.getSegmentName());
        }
      }, _executor);

      futures.add(future);
    }

    // Wait for all CompletableFuture tasks to complete or timeout
    CompletableFuture<Void> allOf = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));

    try {
      // Wait indefinitely for all tasks to complete
      allOf.get(LOAD_SEGMENTS_TIMEOUT_MIN, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      LOGGER.error("Task interrupted", e);
      Thread.currentThread().interrupt(); // Preserve interrupted status
    } catch (ExecutionException e) {
      LOGGER.error("Task encountered an exception", e.getCause());
    } catch (TimeoutException e) {
      LOGGER.error("Task timed out", e);
    }
    long timeTaken = System.currentTimeMillis() - startTime;
    LOGGER.info("Load segments from local took {} sec", timeTaken / 1000);
  }

  @VisibleForTesting
  void initializeSegmentFetcher() {
    LOGGER.info("Initializing segment fetchers");
    // Initialize the components to download segments from deep store
    PinotConfiguration segmentFetcherFactoryConfig =
        _pinotConfig.subset(CommonConstants.Server.PREFIX_OF_CONFIG_OF_SEGMENT_FETCHER_FACTORY);
    PinotConfiguration pinotFSConfig = _pinotConfig.subset(CommonConstants.Server.PREFIX_OF_CONFIG_OF_PINOT_FS_FACTORY);
    PinotConfiguration pinotCrypterConfig =
        _pinotConfig.subset(CommonConstants.Server.PREFIX_OF_CONFIG_OF_PINOT_CRYPTER);
    try {
      SegmentFetcherFactory.init(segmentFetcherFactoryConfig);
      PinotFSFactory.init(pinotFSConfig);
      PinotCrypterFactory.init(pinotCrypterConfig);
    } catch (Exception e) {
      LOGGER.error("Failed to initialize segment fetcher factory: {}", e);
      PredownloadStatusRecorder.predownloadComplete(PredownloadCompletionReason.CANNOT_CONNECT_TO_DEEPSTORE,
          _clusterName,
          _instanceId, "");
    }
  }

  public PredownloadCompletionReason downloadSegments() {
    LOGGER.info("Downloading segments from deep store");
    long startTime = System.currentTimeMillis();
    List<CompletableFuture<Void>> futures = new ArrayList<>();

    // Submit tasks to the executor
    for (PredownloadSegmentInfo predownloadSegmentInfo : _predownloadSegmentInfoList) {
      if (predownloadSegmentInfo.isDownloaded()) {
        _numOfSkippedSegments++;
        continue;
      } else if (!predownloadSegmentInfo.canBeDownloaded()) {
        _numOfUnableToDownloadSegments++;
        continue;
      } else {
        _numOfDownloadSegments++;
      }
      CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
        try {
          downloadSegment(predownloadSegmentInfo);
        } catch (Exception e) {
          LOGGER.error("Failed to download segment: {} of table: {} with issue ",
              predownloadSegmentInfo.getSegmentName(),
              predownloadSegmentInfo.getTableNameWithType(), e);
        }
      }, _executor);

      // TODO: add future.orTimeout() to handle per segment downloading timeout
      futures.add(future);
    }

    // Wait for all CompletableFuture tasks to complete or timeout
    CompletableFuture<Void> allOf = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));

    try {
      // Wait indefinitely for all tasks to complete
      allOf.get(DOWNLOAD_SEGMENTS_TIMEOUT_MIN, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      LOGGER.error("Task interrupted", e);
      Thread.currentThread().interrupt(); // Preserve interrupted status
    } catch (ExecutionException e) {
      LOGGER.error("Task encountered an exception", e.getCause());
    } catch (TimeoutException e) {
      LOGGER.error("Task timed out", e);
    }
    long timeTaken = System.currentTimeMillis() - startTime;
    LOGGER.info("Download segments from deep store took {} sec", timeTaken / 1000);
    return _failedSegments.isEmpty() ? PredownloadCompletionReason.ALL_SEGMENTS_DOWNLOADED
        : PredownloadCompletionReason.SOME_SEGMENTS_DOWNLOAD_FAILED;
  }

  void downloadSegment(PredownloadSegmentInfo predownloadSegmentInfo)
      throws Exception {
    try {
      long startTime = System.currentTimeMillis();
      File tempRootDir = getTmpSegmentDataDir(predownloadSegmentInfo);
      if (_instanceDataManagerConfig.isStreamSegmentDownloadUntar()
          && predownloadSegmentInfo.getCrypterName() == null) {
        try {
          // TODO: increase rate limit here
          File untaredSegDir = downloadAndStreamUntarWithRateLimit(predownloadSegmentInfo, tempRootDir,
              _instanceDataManagerConfig.getStreamSegmentDownloadUntarRateLimit());
          moveSegment(predownloadSegmentInfo, untaredSegDir);
        } finally {
          FileUtils.deleteQuietly(tempRootDir);
        }
      } else {
        try {
          File tarFile = downloadAndDecrypt(predownloadSegmentInfo, tempRootDir);
          untarAndMoveSegment(predownloadSegmentInfo, tarFile, tempRootDir);
        } finally {
          FileUtils.deleteQuietly(tempRootDir);
        }
      }
      _failedSegments.remove(predownloadSegmentInfo.getSegmentName());
      PredownloadTableInfo predownloadTableInfo = _tableInfoMap.get(predownloadSegmentInfo.getTableNameWithType());
      if (predownloadTableInfo != null) {
        predownloadTableInfo.loadSegmentFromLocal(predownloadSegmentInfo);
      }
      _totalDownloadedSizeBytes += predownloadSegmentInfo.getLocalSizeBytes();
      _predownloadMetrics.segmentDownloaded(true, predownloadSegmentInfo.getSegmentName(),
          predownloadSegmentInfo.getLocalSizeBytes(),
          System.currentTimeMillis() - startTime);
    } catch (Exception e) {
      _failedSegments.add(predownloadSegmentInfo.getSegmentName());
      _predownloadMetrics.segmentDownloaded(false, predownloadSegmentInfo.getSegmentName(), 0, 0);
      throw e;
    }
  }

  private File getTmpSegmentDataDir(PredownloadSegmentInfo predownloadSegmentInfo)
      throws Exception {
    PredownloadTableInfo predownloadTableInfo = _tableInfoMap.get(predownloadSegmentInfo.getTableNameWithType());
    if (predownloadTableInfo == null) {
      throw new PredownloadException("Table info not found for segment: " + predownloadSegmentInfo.getSegmentName());
    }
    String tableDataDir =
        predownloadTableInfo.getInstanceDataManagerConfig().getInstanceDataDir() + File.separator
            + predownloadTableInfo.getTableConfig()
            .getTableName();
    File resourceTmpDir = new File(tableDataDir, TMP_DIR_NAME);
    File tmpDir =
        new File(resourceTmpDir,
            String.format(TMP_DIR_FORMAT, predownloadSegmentInfo.getSegmentName(), UUID.randomUUID()));
    if (tmpDir.exists()) {
      FileUtils.deleteQuietly(tmpDir);
    }
    FileUtils.forceMkdir(tmpDir);
    return tmpDir;
  }

  // Reference: {#link
  // org.apache.pinot.core.data.manager.BaseTableDataManager#downloadAndStreamUntarWithRateLimit}
  private File downloadAndStreamUntarWithRateLimit(PredownloadSegmentInfo predownloadSegmentInfo, File tempRootDir,
      long maxStreamRateInByte)
      throws Exception {
    String segmentName = predownloadSegmentInfo.getSegmentName();
    String tableNameWithType = predownloadSegmentInfo.getTableNameWithType();
    LOGGER.info("Trying to download segment {} using streamed download-untar with maxStreamRateInByte {}", segmentName,
        maxStreamRateInByte);
    String uri = predownloadSegmentInfo.getDownloadUrl();
    try {
      File ret =
          SegmentFetcherFactory.fetchAndStreamUntarToLocal(uri, tempRootDir, maxStreamRateInByte, new AtomicInteger(0));
      LOGGER.info("Download and untarred segment: {} for table: {} from: {}", segmentName, tableNameWithType, uri);
      return ret;
    } catch (AttemptsExceededException e) {
      LOGGER.error("Attempts exceeded when stream download-untarring segment: {} for table: {} from: {} to: {}",
          segmentName, tableNameWithType, uri, tempRootDir);
      throw e;
    }
  }

  // Reference: {#link org.apache.pinot.core.data.manager.BaseTableDataManager#downloadAndDecrypt}
  File downloadAndDecrypt(PredownloadSegmentInfo predownloadSegmentInfo, File tempRootDir)
      throws Exception {
    String segmentName = predownloadSegmentInfo.getSegmentName();
    String tableNameWithType = predownloadSegmentInfo.getTableNameWithType();
    File tarFile = new File(tempRootDir, segmentName + TarCompressionUtils.TAR_GZ_FILE_EXTENSION);
    String uri = predownloadSegmentInfo.getDownloadUrl();
    try {
      LOGGER.info("Trying to download segment {}", segmentName);
      SegmentFetcherFactory.fetchAndDecryptSegmentToLocal(uri, tarFile, predownloadSegmentInfo.getCrypterName());
      LOGGER.info("Downloaded tarred segment: {} for table: {} from: {} to: {}, file length: {}", segmentName,
          tableNameWithType, uri, tarFile, tarFile.length());
      return tarFile;
    } catch (AttemptsExceededException e) {
      LOGGER.error("Attempts exceeded when downloading segment: {} for table: {} from: {} to: {}", segmentName,
          tableNameWithType, uri, tarFile);
      // TODO: add download from peer logic
      throw e;
    }
  }

  private File moveSegment(PredownloadSegmentInfo predownloadSegmentInfo, File untaredSegDir)
      throws IOException {
    try {
      File indexDir =
          predownloadSegmentInfo.getSegmentDataDir(_tableInfoMap.get(predownloadSegmentInfo.getTableNameWithType()));
      FileUtils.deleteDirectory(indexDir);
      FileUtils.moveDirectory(untaredSegDir, indexDir);
      return indexDir;
    } catch (Exception e) {
      LOGGER.error("Failed to move segment: {} of table: {}", predownloadSegmentInfo.getSegmentName(),
          predownloadSegmentInfo.getTableNameWithType());
      throw e;
    }
  }

  File untarAndMoveSegment(PredownloadSegmentInfo predownloadSegmentInfo, File tarFile, File tempRootDir)
      throws IOException {
    String segmentName = predownloadSegmentInfo.getSegmentName();
    String tableNameWithType = predownloadSegmentInfo.getTableNameWithType();
    File untarDir = new File(tempRootDir, segmentName);
    try {
      // If an exception is thrown when untarring, it means the tar file is broken
      // or not found after the retry. Thus, there's no need to retry again.
      File untaredSegDir = TarCompressionUtils.untar(tarFile, untarDir).get(0);
      LOGGER.info("Uncompressed tar file: {} into target dir: {}", tarFile, untarDir);
      // Replace the existing index directory.
      File indexDir =
          predownloadSegmentInfo.getSegmentDataDir(_tableInfoMap.get(predownloadSegmentInfo.getTableNameWithType()));
      FileUtils.deleteDirectory(indexDir);
      FileUtils.moveDirectory(untaredSegDir, indexDir);
      LOGGER.info("Successfully downloaded segment: {} of table: {} to index dir: {}", segmentName, tableNameWithType,
          indexDir);
      return indexDir;
    } catch (Exception e) {
      LOGGER.error("Failed to untar segment: {} of table: {} from: {} to: {}", segmentName, tableNameWithType, tarFile,
          untarDir);
      throw e;
    }
  }
}
