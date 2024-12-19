package org.apache.pinot.tools.predownload;

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
  @SuppressWarnings("NullAway.Init")
  private PredownloadMetrics _predownloadMetrics;
  private int _numOfSkippedSegments;
  private int _numOfUnableToDownloadSegments;
  private int _numOfDownloadSegments;
  private long _totalDownloadedSizeBytes;
  @SuppressWarnings("NullAway.Init")
  private ZKClient _zkClient;
  @SuppressWarnings("NullAway.Init")
  private List<SegmentInfo> _segmentInfoList;
  @SuppressWarnings("NullAway.Init")
  private Map<String, TableInfo> _tableInfoMap;

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
          e.printStackTrace();
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
    PredownloadCompleteReason reason = downloadSegments();
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
    StatusRecorder.predownloadComplete(reason, _clusterName, _instanceId, String.join(",", _failedSegments));
  }

  public void stop() {
    if (_zkClient != null) {
      _zkClient.close();
    }
    if (_executor != null) {
      ((ThreadPoolExecutor) _executor).shutdownNow();
    }
  }

  void initializeZK() {
    LOGGER.info("Initializing ZK client with address: {} and instanceId: {}", _zkAddress, _instanceId);
    _zkClient = new ZKClient(_zkAddress, _clusterName, _instanceId);
    _zkClient.start();
  }

  void initializeMetricsReporter() {
    LOGGER.info("Initializing metrics reporter");

    _predownloadMetrics = new PredownloadMetrics();
    StatusRecorder.registerMetrics(_predownloadMetrics);
  }

  @VisibleForTesting
  void getSegmentsInfo() {
    LOGGER.info("Getting segments info from ZK");
    _segmentInfoList = _zkClient.getSegmentsOfInstance(_zkClient.getDataAccessor());
    if (_segmentInfoList.isEmpty()) {
      PredownloadCompleteReason reason = PredownloadCompleteReason.NO_SEGMENT_TO_PREDOWNLOAD;
      StatusRecorder.predownloadComplete(reason, _clusterName, _instanceId, "");
    }
    _tableInfoMap = new HashMap<>();
    _zkClient.updateSegmentMetadata(_segmentInfoList, _tableInfoMap, _instanceDataManagerConfig);
  }

  @VisibleForTesting
  void loadSegmentsFromLocal() {
    LOGGER.info("Loading segments from local to reduce number of segments to download");
    long startTime = System.currentTimeMillis();
    List<CompletableFuture<Void>> futures = new ArrayList<>();

    // Submit tasks to the executor
    for (SegmentInfo segmentInfo : _segmentInfoList) {
      CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
        boolean loadSegmentSuccess = false;
        try {
          TableInfo tableInfo = _tableInfoMap.get(segmentInfo.getTableNameWithType());
          if (tableInfo != null) {
            loadSegmentSuccess = tableInfo.loadSegmentFromLocal(segmentInfo, _instanceDataManagerConfig);
          }
        } catch (Exception e) {
          LOGGER.error("Failed to load from local for segment: {} of table: {} with issue ",
              segmentInfo.getSegmentName(), segmentInfo.getTableNameWithType(), e);
        }
        if (!loadSegmentSuccess && segmentInfo.canBeDownloaded()) {
          _failedSegments.add(segmentInfo.getSegmentName());
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
      StatusRecorder.predownloadComplete(PredownloadCompleteReason.CANNOT_CONNECT_TO_DEEPSTORE, _clusterName,
          _instanceId, "");
    }
  }

  public PredownloadCompleteReason downloadSegments() {
    LOGGER.info("Downloading segments from deep store");
    long startTime = System.currentTimeMillis();
    List<CompletableFuture<Void>> futures = new ArrayList<>();

    // Submit tasks to the executor
    for (SegmentInfo segmentInfo : _segmentInfoList) {
      if (segmentInfo.isDownloaded()) {
        _numOfSkippedSegments++;
        continue;
      } else if (!segmentInfo.canBeDownloaded()) {
        _numOfUnableToDownloadSegments++;
        continue;
      } else {
        _numOfDownloadSegments++;
      }
      CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
        try {
          downloadSegment(segmentInfo);
        } catch (Exception e) {
          LOGGER.error("Failed to download segment: {} of table: {} with issue ", segmentInfo.getSegmentName(),
              segmentInfo.getTableNameWithType(), e);
        }
      }, _executor);

      // TODO: add future.orTimeout() to handle per segment downloading timeout
      // Right now not able to use due to monorepo incapability with JAVA9+ syntax
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
    return _failedSegments.isEmpty() ? PredownloadCompleteReason.ALL_SEGMENTS_DOWNLOADED
        : PredownloadCompleteReason.SOME_SEGMENTS_DOWNLOAD_FAILED;
  }

  void downloadSegment(SegmentInfo segmentInfo)
      throws Exception {
    try {
      long startTime = System.currentTimeMillis();
      File tempRootDir = getTmpSegmentDataDir(segmentInfo);
      if (_instanceDataManagerConfig.isStreamSegmentDownloadUntar() && segmentInfo.getCrypterName() == null) {
        try {
          // TODO: increase rate limit here
          File untaredSegDir = downloadAndStreamUntarWithRateLimit(segmentInfo, tempRootDir,
              _instanceDataManagerConfig.getStreamSegmentDownloadUntarRateLimit());
          moveSegment(segmentInfo, untaredSegDir);
        } finally {
          FileUtils.deleteQuietly(tempRootDir);
        }
      } else {
        try {
          File tarFile = downloadAndDecrypt(segmentInfo, tempRootDir);
          untarAndMoveSegment(segmentInfo, tarFile, tempRootDir);
        } finally {
          FileUtils.deleteQuietly(tempRootDir);
        }
      }
      _failedSegments.remove(segmentInfo.getSegmentName());
      TableInfo tableInfo = _tableInfoMap.get(segmentInfo.getTableNameWithType());
      if (tableInfo != null) {
        tableInfo.loadSegmentFromLocal(segmentInfo, _instanceDataManagerConfig);
      }
      _totalDownloadedSizeBytes += segmentInfo.getLocalSizeBytes();
      _predownloadMetrics.segmentDownloaded(true, segmentInfo.getSegmentName(), segmentInfo.getLocalSizeBytes(),
          System.currentTimeMillis() - startTime);
    } catch (Exception e) {
      _failedSegments.add(segmentInfo.getSegmentName());
      _predownloadMetrics.segmentDownloaded(false, segmentInfo.getSegmentName(), 0, 0);
      throw e;
    }
  }

  private File getTmpSegmentDataDir(SegmentInfo segmentInfo)
      throws Exception {
    TableInfo tableInfo = _tableInfoMap.get(segmentInfo.getTableNameWithType());
    if (tableInfo == null) {
      throw new PredownloadException("Table info not found for segment: " + segmentInfo.getSegmentName());
    }
    // TableDataManagerConfig is removed in 1.1: https://github.com/apache/pinot/pull/12189
    String tableDataDir =
        tableInfo.getInstanceDataManagerConfig().getInstanceDataDir() + File.separator + tableInfo.getTableConfig()
            .getTableName();
    File resourceTmpDir = new File(tableDataDir, TMP_DIR_NAME);
    File tmpDir =
        new File(resourceTmpDir, String.format(TMP_DIR_FORMAT, segmentInfo.getSegmentName(), UUID.randomUUID()));
    if (tmpDir.exists()) {
      FileUtils.deleteQuietly(tmpDir);
    }
    FileUtils.forceMkdir(tmpDir);
    return tmpDir;
  }

  // Reference: {#link
  // org.apache.pinot.core.data.manager.BaseTableDataManager#downloadAndStreamUntarWithRateLimit}
  private File downloadAndStreamUntarWithRateLimit(SegmentInfo segmentInfo, File tempRootDir, long maxStreamRateInByte)
      throws Exception {
    String segmentName = segmentInfo.getSegmentName();
    String tableNameWithType = segmentInfo.getTableNameWithType();
    LOGGER.info("Trying to download segment {} using streamed download-untar with maxStreamRateInByte {}", segmentName,
        maxStreamRateInByte);
    String uri = segmentInfo.getDownloadUrl();
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
  File downloadAndDecrypt(SegmentInfo segmentInfo, File tempRootDir)
      throws Exception {
    String segmentName = segmentInfo.getSegmentName();
    String tableNameWithType = segmentInfo.getTableNameWithType();
    File tarFile = new File(tempRootDir, segmentName + TarCompressionUtils.TAR_GZ_FILE_EXTENSION);
    String uri = segmentInfo.getDownloadUrl();
    try {
      LOGGER.info("Trying to download segment {}", segmentName);
      SegmentFetcherFactory.fetchAndDecryptSegmentToLocal(uri, tarFile, segmentInfo.getCrypterName());
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

  private File moveSegment(SegmentInfo segmentInfo, File untaredSegDir)
      throws IOException {
    try {
      File indexDir = segmentInfo.getSegmentDataDir(_tableInfoMap.get(segmentInfo.getTableNameWithType()));
      FileUtils.deleteDirectory(indexDir);
      FileUtils.moveDirectory(untaredSegDir, indexDir);
      return indexDir;
    } catch (Exception e) {
      LOGGER.error("Failed to move segment: {} of table: {}", segmentInfo.getSegmentName(),
          segmentInfo.getTableNameWithType());
      throw e;
    }
  }

  File untarAndMoveSegment(SegmentInfo segmentInfo, File tarFile, File tempRootDir)
      throws IOException {
    String segmentName = segmentInfo.getSegmentName();
    String tableNameWithType = segmentInfo.getTableNameWithType();
    File untarDir = new File(tempRootDir, segmentName);
    try {
      // If an exception is thrown when untarring, it means the tar file is broken
      // or not found after the retry. Thus, there's no need to retry again.
      File untaredSegDir = TarCompressionUtils.untar(tarFile, untarDir).get(0);
      LOGGER.info("Uncompressed tar file: {} into target dir: {}", tarFile, untarDir);
      // Replace the existing index directory.
      File indexDir = segmentInfo.getSegmentDataDir(_tableInfoMap.get(segmentInfo.getTableNameWithType()));
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
