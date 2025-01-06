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
package org.apache.pinot.server.api.resources.reingestion.utils;

import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.indexsegment.mutable.MutableSegmentImpl;
import org.apache.pinot.segment.local.io.writer.impl.MmapMemoryManager;
import org.apache.pinot.segment.local.realtime.converter.RealtimeSegmentConverter;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentConfig;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentStatsHistory;
import org.apache.pinot.segment.local.segment.creator.TransformPipeline;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.utils.IngestionUtils;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.partition.PartitionFunctionFactory;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentZKPropsConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.stream.MessageBatch;
import org.apache.pinot.spi.stream.PartitionGroupConsumer;
import org.apache.pinot.spi.stream.PartitionGroupConsumptionStatus;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConsumerFactory;
import org.apache.pinot.spi.stream.StreamConsumerFactoryProvider;
import org.apache.pinot.spi.stream.StreamDataDecoder;
import org.apache.pinot.spi.stream.StreamDataDecoderImpl;
import org.apache.pinot.spi.stream.StreamDataDecoderResult;
import org.apache.pinot.spi.stream.StreamMessage;
import org.apache.pinot.spi.stream.StreamMessageDecoder;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffsetFactory;
import org.apache.pinot.spi.utils.retry.RetryPolicies;
import org.apache.pinot.spi.utils.retry.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Simplified Segment Data Manager for ingesting data from a start offset to an end offset.
 */
public class SimpleRealtimeSegmentDataManager extends SegmentDataManager {

  private static final int DEFAULT_CAPACITY = 100_000;
  private static final int DEFAULT_FETCH_TIMEOUT_MS = 5000;
  public static final FileUploadDownloadClient FILE_UPLOAD_DOWNLOAD_CLIENT = new FileUploadDownloadClient();

  private final String _segmentName;
  private final String _tableNameWithType;
  private final int _partitionGroupId;
  private final String _segmentNameStr;
  private final SegmentZKMetadata _segmentZKMetadata;
  private final TableConfig _tableConfig;
  private final Schema _schema;
  private final StreamConfig _streamConfig;
  private final StreamPartitionMsgOffsetFactory _offsetFactory;
  private final StreamConsumerFactory _consumerFactory;
  private StreamMetadataProvider _partitionMetadataProvider;
  private final PartitionGroupConsumer _consumer;
  private final StreamDataDecoder _decoder;
  private final MutableSegmentImpl _realtimeSegment;
  private final File _resourceTmpDir;
  private final File _resourceDataDir;
  private final Logger _logger;
  private Thread _consumerThread;
  private final AtomicBoolean _shouldStop = new AtomicBoolean(false);
  private final AtomicBoolean _isDoneConsuming = new AtomicBoolean(false);
  private final StreamPartitionMsgOffset _startOffset;
  private final StreamPartitionMsgOffset _endOffset;
  private volatile StreamPartitionMsgOffset _currentOffset;
  private volatile int _numRowsIndexed = 0;
  private final String _segmentStoreUriStr;
  private final int _fetchTimeoutMs;
  private final TransformPipeline _transformPipeline;
  private volatile boolean _isSuccess = false;
  private volatile Throwable _consumptionException;
  private final ServerMetrics _serverMetrics;

  public SimpleRealtimeSegmentDataManager(String segmentName, String tableNameWithType, int partitionGroupId,
      SegmentZKMetadata segmentZKMetadata, TableConfig tableConfig, Schema schema,
      IndexLoadingConfig indexLoadingConfig, StreamConfig streamConfig, String startOffsetStr, String endOffsetStr,
      File resourceTmpDir, File resourceDataDir, ServerMetrics serverMetrics)
      throws Exception {

    _segmentName = segmentName;
    _tableNameWithType = tableNameWithType;
    _partitionGroupId = partitionGroupId;
    _segmentZKMetadata = segmentZKMetadata;
    _tableConfig = tableConfig;
    _schema = schema;
    _segmentStoreUriStr = indexLoadingConfig.getSegmentStoreURI();
    _streamConfig = streamConfig;
    _resourceTmpDir = resourceTmpDir;
    _resourceDataDir = resourceDataDir;
    _serverMetrics = serverMetrics;
    _logger = LoggerFactory.getLogger(SimpleRealtimeSegmentDataManager.class.getName() + "_" + _segmentName);

    _offsetFactory = StreamConsumerFactoryProvider.create(_streamConfig).createStreamMsgOffsetFactory();
    _startOffset = _offsetFactory.create(startOffsetStr);
    _endOffset = _offsetFactory.create(endOffsetStr);

    String clientId = getClientId();

    _consumerFactory = StreamConsumerFactoryProvider.create(_streamConfig);
    _partitionMetadataProvider = _consumerFactory.createPartitionMetadataProvider(clientId, _partitionGroupId);
    _segmentNameStr = _segmentZKMetadata.getSegmentName();

    // Create a simple PartitionGroupConsumptionStatus
    PartitionGroupConsumptionStatus partitionGroupConsumptionStatus =
        new PartitionGroupConsumptionStatus(_partitionGroupId, 0, _startOffset, null, null);

    _consumer = _consumerFactory.createPartitionGroupConsumer(clientId, partitionGroupConsumptionStatus);

    // Initialize decoder
    Set<String> fieldsToRead = IngestionUtils.getFieldsForRecordExtractor(_tableConfig, _schema);
    _decoder = createDecoder(fieldsToRead);

    // Fetch capacity from indexLoadingConfig or use default
    int capacity = streamConfig.getFlushThresholdRows();
    if (capacity <= 0) {
      capacity = DEFAULT_CAPACITY;
    }

    // Fetch average number of multi-values from indexLoadingConfig
    int avgNumMultiValues = indexLoadingConfig.getRealtimeAvgMultiValueCount();

    // Load stats history, here we are using the same stats while as the RealtimeSegmentDataManager so that we are
    // much more efficient in allocating buffers. It also works with empty file
    String tableDataDir = indexLoadingConfig.getInstanceDataManagerConfig() != null
        ? indexLoadingConfig.getInstanceDataManagerConfig().getInstanceDataDir() + File.separator + _tableNameWithType
        : resourceTmpDir.getAbsolutePath();
    File statsHistoryFile = new File(tableDataDir, "segment-stats.ser");
    RealtimeSegmentStatsHistory statsHistory = RealtimeSegmentStatsHistory.deserialzeFrom(statsHistoryFile);

    // Initialize mutable segment with configurations
    RealtimeSegmentConfig.Builder realtimeSegmentConfigBuilder =
        new RealtimeSegmentConfig.Builder(indexLoadingConfig).setTableNameWithType(_tableNameWithType).setSegmentName(_segmentName)
            .setStreamName(_streamConfig.getTopicName()).setSegmentZKMetadata(_segmentZKMetadata)
            .setStatsHistory(statsHistory).setSchema(_schema).setCapacity(capacity)
            .setAvgNumMultiValues(avgNumMultiValues).setOffHeap(indexLoadingConfig.isRealtimeOffHeapAllocation())
            .setFieldConfigList(tableConfig.getFieldConfigList())
            .setConsumerDir(_resourceDataDir.getAbsolutePath()).setMemoryManager(
                new MmapMemoryManager(FileUtils.getTempDirectory().getAbsolutePath(), _segmentNameStr, _serverMetrics));

    setPartitionParameters(realtimeSegmentConfigBuilder, _tableConfig.getIndexingConfig().getSegmentPartitionConfig());

    _realtimeSegment = new MutableSegmentImpl(realtimeSegmentConfigBuilder.build(), _serverMetrics);

    _transformPipeline = new TransformPipeline(tableConfig, schema);

    // Initialize fetch timeout
    _fetchTimeoutMs =
        _streamConfig.getFetchTimeoutMillis() > 0 ? _streamConfig.getFetchTimeoutMillis() : DEFAULT_FETCH_TIMEOUT_MS;
  }

  private String getClientId() {
    return _tableNameWithType + "-" + _partitionGroupId;
  }

  public void startConsumption() {
    // Start the consumer thread
    _consumerThread = new Thread(new PartitionConsumer(), _segmentName);
    _consumerThread.start();
  }

  private StreamDataDecoder createDecoder(Set<String> fieldsToRead)
      throws Exception {
    AtomicReference<StreamDataDecoder> localStreamDataDecoder = new AtomicReference<>();
    RetryPolicy retryPolicy = RetryPolicies.exponentialBackoffRetryPolicy(5, 1000L, 1.2f);
    retryPolicy.attempt(() -> {
      try {
        StreamMessageDecoder streamMessageDecoder = createMessageDecoder(fieldsToRead);
        localStreamDataDecoder.set(new StreamDataDecoderImpl(streamMessageDecoder));
        return true;
      } catch (Exception e) {
        _logger.warn("Failed to create StreamMessageDecoder. Retrying...", e);
        return false;
      }
    });
    return localStreamDataDecoder.get();
  }

  /**
   * Creates a {@link StreamMessageDecoder} using properties in {@link StreamConfig}.
   *
   * @param fieldsToRead The fields to read from the source stream
   * @return The initialized StreamMessageDecoder
   */
  private StreamMessageDecoder createMessageDecoder(Set<String> fieldsToRead) {
    String decoderClass = _streamConfig.getDecoderClass();
    try {
      StreamMessageDecoder decoder = PluginManager.get().createInstance(decoderClass);
      decoder.init(fieldsToRead, _streamConfig, _tableConfig, _schema);
      return decoder;
    } catch (Exception e) {
      throw new RuntimeException(
          "Caught exception while creating StreamMessageDecoder from stream config: " + _streamConfig, e);
    }
  }

  private class PartitionConsumer implements Runnable {
    @Override
    public void run() {
      try {
        _consumer.start(_startOffset);
        _currentOffset = _startOffset;
        TransformPipeline.Result reusedResult = new TransformPipeline.Result();
        while (!_shouldStop.get() && _currentOffset.compareTo(_endOffset) < 0) {
          // Fetch messages
          MessageBatch messageBatch = _consumer.fetchMessages(_currentOffset, _fetchTimeoutMs);

          int messageCount = messageBatch.getMessageCount();

          for (int i = 0; i < messageCount; i++) {
            if (_shouldStop.get()) {
              break;
            }
            StreamMessage streamMessage = messageBatch.getStreamMessage(i);
            if (streamMessage.getMetadata() != null && streamMessage.getMetadata().getOffset() != null
                && streamMessage.getMetadata().getOffset().compareTo(_endOffset) >= 0) {
              _shouldStop.set(true);
              _logger.info("Reached end offset: {} for partition group: {}", _endOffset, _partitionGroupId);
              break;
            }

            // Decode message
            StreamDataDecoderResult decodedResult = _decoder.decode(streamMessage);
            if (decodedResult.getException() == null) {
              // Index message
              GenericRow row = decodedResult.getResult();

              _transformPipeline.processRow(row, reusedResult);

              List<GenericRow> transformedRows = reusedResult.getTransformedRows();

              // TODO: Do enrichment and transforms before indexing
              for (GenericRow transformedRow : transformedRows) {
                _realtimeSegment.index(transformedRow, streamMessage.getMetadata());
                _numRowsIndexed++;
              }
            } else {
              _logger.warn("Failed to decode message at offset {}: {}", _currentOffset, decodedResult.getException());
            }
          }

          _currentOffset = messageBatch.getOffsetOfNextBatch();
        }
        _isSuccess = true;
      } catch (Exception e) {
        _logger.error("Exception in consumer thread", e);
        _consumptionException = e;
        throw new RuntimeException(e);
      } finally {
        try {
          _consumer.close();
        } catch (Exception e) {
          _logger.warn("Failed to close consumer", e);
        }
        _isDoneConsuming.set(true);
      }
    }
  }

  public void stopConsumption() {
    _shouldStop.set(true);
    if (_consumerThread.isAlive()) {
      _consumerThread.interrupt();
      try {
        _consumerThread.join();
      } catch (InterruptedException e) {
        _logger.warn("Interrupted while waiting for consumer thread to finish");
      }
    }
  }

  @Override
  public MutableSegment getSegment() {
    return _realtimeSegment;
  }

  @Override
  public String getSegmentName() {
    return _segmentName;
  }

  @Override
  protected void doDestroy() {
    _realtimeSegment.destroy();
  }

  @Override
  public void doOffload() {
    stopConsumption();
  }

  public boolean isDoneConsuming() {
    return _isDoneConsuming.get();
  }

  public boolean isSuccess() {
    return _isSuccess;
  }

  public Throwable getConsumptionException() {
    return _consumptionException;
  }

  @VisibleForTesting
  public SegmentBuildDescriptor buildSegmentInternal() throws Exception {
    _logger.info("Building segment from {} to {}", _startOffset, _currentOffset);
    final long lockAcquireTimeMillis = now();
    // Build a segment from in-memory rows.
    // Use a temporary directory
    Path tempSegmentFolder = null;
    try {
      tempSegmentFolder =
          java.nio.file.Files.createTempDirectory(_resourceTmpDir.toPath(), "tmp-" + _segmentNameStr + "-");
    } catch (IOException e) {
      _logger.error("Failed to create temporary directory for segment build", e);
      return null;
    }

    SegmentZKPropsConfig segmentZKPropsConfig = new SegmentZKPropsConfig();
    segmentZKPropsConfig.setStartOffset(_startOffset.toString());
    segmentZKPropsConfig.setEndOffset(_endOffset.toString());

    // Build the segment
    RealtimeSegmentConverter converter =
        new RealtimeSegmentConverter(_realtimeSegment, segmentZKPropsConfig, tempSegmentFolder.toString(),
            _schema, _tableNameWithType, _tableConfig, _segmentZKMetadata.getSegmentName(),
            _tableConfig.getIndexingConfig().isNullHandlingEnabled());
    try {
      converter.build(null, _serverMetrics);
    } catch (Exception e) {
      _logger.error("Failed to build segment", e);
      FileUtils.deleteQuietly(tempSegmentFolder.toFile());
      return null;
    }
    final long buildTimeMillis = now() - lockAcquireTimeMillis;

    File dataDir = _resourceDataDir;
    File indexDir = new File(dataDir, _segmentNameStr);
    FileUtils.deleteQuietly(indexDir);

    File tempIndexDir = new File(tempSegmentFolder.toFile(), _segmentNameStr);
    if (!tempIndexDir.exists()) {
      _logger.error("Temp index directory {} does not exist", tempIndexDir);
      FileUtils.deleteQuietly(tempSegmentFolder.toFile());
      return null;
    }
    try {
      FileUtils.moveDirectory(tempIndexDir, indexDir);
    } catch (IOException e) {
      _logger.error("Caught exception while moving index directory from: {} to: {}", tempIndexDir, indexDir, e);
      return null;
    } finally {
      FileUtils.deleteQuietly(tempSegmentFolder.toFile());
    }

    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(indexDir);

    long segmentSizeBytes = FileUtils.sizeOfDirectory(indexDir);
    File segmentTarFile = new File(dataDir, _segmentNameStr + TarCompressionUtils.TAR_GZ_FILE_EXTENSION);
    try {
      TarCompressionUtils.createCompressedTarFile(indexDir, segmentTarFile);
    } catch (IOException e) {
      _logger.error("Caught exception while tarring index directory from: {} to: {}", indexDir, segmentTarFile, e);
      return null;
    }

    File metadataFile = SegmentDirectoryPaths.findMetadataFile(indexDir);
    if (metadataFile == null) {
      _logger.error("Failed to find metadata file under index directory: {}", indexDir);
      return null;
    }
    File creationMetaFile = SegmentDirectoryPaths.findCreationMetaFile(indexDir);
    if (creationMetaFile == null) {
      _logger.error("Failed to find creation meta file under index directory: {}", indexDir);
      return null;
    }
    Map<String, File> metadataFiles = new HashMap<>();
    metadataFiles.put(V1Constants.MetadataKeys.METADATA_FILE_NAME, metadataFile);
    metadataFiles.put(V1Constants.SEGMENT_CREATION_META, creationMetaFile);
    return new SegmentBuildDescriptor(segmentTarFile, metadataFiles, _currentOffset, buildTimeMillis, buildTimeMillis,
        segmentSizeBytes, segmentMetadata);
  }

  protected long now() {
    return System.currentTimeMillis();
  }

  public void removeSegmentFile(SegmentBuildDescriptor segmentBuildDescriptor) {
    if (segmentBuildDescriptor != null) {
      segmentBuildDescriptor.deleteSegmentFile();
    }
  }

  /*
   * set the following partition parameters in RT segment config builder:
   *  - partition column
   *  - partition function
   *  - partition group id
   */
  private void setPartitionParameters(RealtimeSegmentConfig.Builder realtimeSegmentConfigBuilder,
      SegmentPartitionConfig segmentPartitionConfig) {
    if (segmentPartitionConfig != null) {
      Map<String, ColumnPartitionConfig> columnPartitionMap = segmentPartitionConfig.getColumnPartitionMap();
      if (columnPartitionMap.size() == 1) {
        Map.Entry<String, ColumnPartitionConfig> entry = columnPartitionMap.entrySet().iterator().next();
        String partitionColumn = entry.getKey();
        ColumnPartitionConfig columnPartitionConfig = entry.getValue();
        String partitionFunctionName = columnPartitionConfig.getFunctionName();

        // NOTE: Here we compare the number of partitions from the config and the stream, and log a warning and emit a
        //       metric when they don't match, but use the one from the stream. The mismatch could happen when the
        //       stream partitions are changed, but the table config has not been updated to reflect the change.
        //       In such case, picking the number of partitions from the stream can keep the segment properly
        //       partitioned as long as the partition function is not changed.
        int numPartitions = columnPartitionConfig.getNumPartitions();
        try {
          // TODO: currentPartitionGroupConsumptionStatus should be fetched from idealState + segmentZkMetadata,
          //  so that we get back accurate partitionGroups info
          //  However this is not an issue for Kafka, since partitionGroups never expire and every partitionGroup has
          //  a single partition
          //  Fix this before opening support for partitioning in Kinesis
          int numPartitionGroups =
              _partitionMetadataProvider.computePartitionGroupMetadata(getClientId(), _streamConfig,
                  Collections.emptyList(), /*maxWaitTimeMs=*/5000).size();

          if (numPartitionGroups != numPartitions) {
            _logger.info(
                "Number of stream partitions: {} does not match number of partitions in the partition config: {}, "
                    + "using number of stream " + "partitions", numPartitionGroups, numPartitions);
            numPartitions = numPartitionGroups;
          }
        } catch (Exception e) {
          _logger.warn("Failed to get number of stream partitions in 5s, "
              + "using number of partitions in the partition config: {}", numPartitions, e);
          createPartitionMetadataProvider("Timeout getting number of stream partitions");
        }

        realtimeSegmentConfigBuilder.setPartitionColumn(partitionColumn);
        realtimeSegmentConfigBuilder.setPartitionFunction(
            PartitionFunctionFactory.getPartitionFunction(partitionFunctionName, numPartitions, null));
        realtimeSegmentConfigBuilder.setPartitionId(_partitionGroupId);
      } else {
        _logger.warn("Cannot partition on multiple columns: {}", columnPartitionMap.keySet());
      }
    }
  }

  /**
   * Creates a new stream metadata provider
   */
  private void createPartitionMetadataProvider(String reason) {
    closePartitionMetadataProvider();
    _logger.info("Creating new partition metadata provider, reason: {}", reason);
    _partitionMetadataProvider = _consumerFactory.createPartitionMetadataProvider(getClientId(), _partitionGroupId);
  }

  private void closePartitionMetadataProvider() {
    if (_partitionMetadataProvider != null) {
      try {
        _partitionMetadataProvider.close();
      } catch (Exception e) {
        _logger.warn("Could not close stream metadata provider", e);
      }
    }
  }

  public class SegmentBuildDescriptor {
    final File _segmentTarFile;
    final Map<String, File> _metadataFileMap;
    final StreamPartitionMsgOffset _offset;
    final long _waitTimeMillis;
    final long _buildTimeMillis;
    final long _segmentSizeBytes;
    final SegmentMetadataImpl _segmentMetadata;

    public SegmentBuildDescriptor(@Nullable File segmentTarFile, @Nullable Map<String, File> metadataFileMap,
        StreamPartitionMsgOffset offset, long buildTimeMillis, long waitTimeMillis, long segmentSizeBytes,
        SegmentMetadataImpl segmentMetadata) {
      _segmentTarFile = segmentTarFile;
      _metadataFileMap = metadataFileMap;
      _offset = _offsetFactory.create(offset);
      _buildTimeMillis = buildTimeMillis;
      _waitTimeMillis = waitTimeMillis;
      _segmentSizeBytes = segmentSizeBytes;
      _segmentMetadata = segmentMetadata;
    }

    public StreamPartitionMsgOffset getOffset() {
      return _offset;
    }

    public long getBuildTimeMillis() {
      return _buildTimeMillis;
    }

    public long getWaitTimeMillis() {
      return _waitTimeMillis;
    }

    @Nullable
    public File getSegmentTarFile() {
      return _segmentTarFile;
    }

    @Nullable
    public Map<String, File> getMetadataFiles() {
      return _metadataFileMap;
    }

    public long getSegmentSizeBytes() {
      return _segmentSizeBytes;
    }

    public void deleteSegmentFile() {
      if (_segmentTarFile != null) {
        FileUtils.deleteQuietly(_segmentTarFile);
      }
    }

    public SegmentMetadataImpl getSegmentMetadata() {
      return _segmentMetadata;
    }
  }
}
