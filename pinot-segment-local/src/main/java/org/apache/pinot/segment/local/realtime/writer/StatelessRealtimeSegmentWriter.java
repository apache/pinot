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
package org.apache.pinot.segment.local.realtime.writer;

import com.google.common.base.Preconditions;
import java.io.Closeable;
import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.segment.local.indexsegment.mutable.MutableSegmentImpl;
import org.apache.pinot.segment.local.io.writer.impl.MmapMemoryManager;
import org.apache.pinot.segment.local.realtime.converter.RealtimeSegmentConverter;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentConfig;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentStatsHistory;
import org.apache.pinot.segment.local.segment.creator.TransformPipeline;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.utils.IngestionUtils;
import org.apache.pinot.segment.spi.partition.PartitionFunctionFactory;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentZKPropsConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
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
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.spi.utils.retry.RetryPolicies;
import org.apache.pinot.spi.utils.retry.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Segment writer to ingest streaming data from a start offset to an end offset.
 *
 * TODO:
 *   1. Clean up this class and only keep the necessary parts.
 *   2. Use a different segment impl for better performance because it doesn't need to serve queries.
 */
public class StatelessRealtimeSegmentWriter implements Closeable {

  private static final int DEFAULT_CAPACITY = 100_000;
  private static final int DEFAULT_FETCH_TIMEOUT_MS = 5000;
  public static final String SEGMENT_STATS_FILE_NAME = "segment-stats.ser";
  public static final String RESOURCE_TMP_DIR_PREFIX = "resourceTmpDir_";
  public static final String RESOURCE_DATA_DIR_PREFIX = "resourceDataDir_";

  private final Semaphore _segBuildSemaphore;
  private final String _segmentName;
  private final String _tableNameWithType;
  private final int _partitionGroupId;
  private final SegmentZKMetadata _segmentZKMetadata;
  private final TableConfig _tableConfig;
  private final Schema _schema;
  private final StreamConfig _streamConfig;
  private final StreamConsumerFactory _consumerFactory;
  private StreamMetadataProvider _partitionMetadataProvider;
  private final PartitionGroupConsumer _consumer;
  private final StreamDataDecoder _decoder;
  private final MutableSegmentImpl _realtimeSegment;
  private final File _resourceTmpDir;
  private final File _resourceDataDir;
  private final Logger _logger;
  private Thread _consumerThread;
  private final AtomicBoolean _isDoneConsuming = new AtomicBoolean(false);
  private final StreamPartitionMsgOffset _startOffset;
  private final StreamPartitionMsgOffset _endOffset;
  private volatile StreamPartitionMsgOffset _currentOffset;
  private final int _fetchTimeoutMs;
  private final TransformPipeline _transformPipeline;
  private volatile boolean _isSuccess = false;
  private volatile Throwable _consumptionException;

  public StatelessRealtimeSegmentWriter(SegmentZKMetadata segmentZKMetadata, IndexLoadingConfig indexLoadingConfig,
      @Nullable Semaphore segBuildSemaphore)
      throws Exception {
    Preconditions.checkNotNull(indexLoadingConfig.getTableConfig(), "Table config must be set in index loading config");
    Preconditions.checkNotNull(indexLoadingConfig.getSchema(), "Schema must be set in index loading config");
    LLCSegmentName llcSegmentName = new LLCSegmentName(segmentZKMetadata.getSegmentName());

    _segmentName = segmentZKMetadata.getSegmentName();
    _partitionGroupId = llcSegmentName.getPartitionGroupId();
    _segBuildSemaphore = segBuildSemaphore;
    _tableNameWithType = TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(llcSegmentName.getTableName());
    _segmentZKMetadata = segmentZKMetadata;
    _tableConfig = indexLoadingConfig.getTableConfig();
    _schema = indexLoadingConfig.getSchema();
    String tableDataDir = indexLoadingConfig.getTableDataDir() == null ? FileUtils.getTempDirectory().getAbsolutePath()
        : indexLoadingConfig.getTableDataDir();
    File reingestionDir = new File(tableDataDir, "reingestion");
    _resourceTmpDir =
        new File(reingestionDir, RESOURCE_TMP_DIR_PREFIX + _segmentName + "_" + System.currentTimeMillis());
    _resourceDataDir =
        new File(reingestionDir, RESOURCE_DATA_DIR_PREFIX + _segmentName + "_" + System.currentTimeMillis());
    FileUtils.deleteQuietly(_resourceTmpDir);
    FileUtils.deleteQuietly(_resourceDataDir);
    FileUtils.forceMkdir(_resourceTmpDir);
    FileUtils.forceMkdir(_resourceDataDir);

    _logger = LoggerFactory.getLogger(StatelessRealtimeSegmentWriter.class.getName() + "_" + _segmentName);

    Map<String, String> streamConfigMap = IngestionConfigUtils.getStreamConfigMaps(_tableConfig).get(0);
    _streamConfig = new StreamConfig(_tableNameWithType, streamConfigMap);

    StreamPartitionMsgOffsetFactory offsetFactory =
        StreamConsumerFactoryProvider.create(_streamConfig).createStreamMsgOffsetFactory();
    _startOffset = offsetFactory.create(segmentZKMetadata.getStartOffset());
    _endOffset = offsetFactory.create(segmentZKMetadata.getEndOffset());

    String clientId = getClientId();
    _consumerFactory = StreamConsumerFactoryProvider.create(_streamConfig);
    _partitionMetadataProvider = _consumerFactory.createPartitionMetadataProvider(clientId, _partitionGroupId);

    // Create a simple PartitionGroupConsumptionStatus
    PartitionGroupConsumptionStatus partitionGroupConsumptionStatus =
        new PartitionGroupConsumptionStatus(_partitionGroupId, 0, _startOffset, null, null);

    _consumer = _consumerFactory.createPartitionGroupConsumer(clientId, partitionGroupConsumptionStatus);

    // Initialize decoder
    Set<String> fieldsToRead = IngestionUtils.getFieldsForRecordExtractor(_tableConfig, _schema);
    _decoder = createDecoder(fieldsToRead);

    // Fetch capacity from indexLoadingConfig or use default
    int capacity = _streamConfig.getFlushThresholdRows();
    if (capacity <= 0) {
      capacity = DEFAULT_CAPACITY;
    }

    // Fetch average number of multi-values from indexLoadingConfig
    int avgNumMultiValues = indexLoadingConfig.getRealtimeAvgMultiValueCount();

    // Load stats history, here we are using the same stats while as the RealtimeSegmentDataManager so that we are
    // much more efficient in allocating buffers. It also works with empty file
    File statsHistoryFile = new File(tableDataDir, SEGMENT_STATS_FILE_NAME);
    RealtimeSegmentStatsHistory statsHistory = RealtimeSegmentStatsHistory.deserialzeFrom(statsHistoryFile);

    // Initialize mutable segment with configurations
    RealtimeSegmentConfig.Builder realtimeSegmentConfigBuilder =
        new RealtimeSegmentConfig.Builder(indexLoadingConfig).setTableNameWithType(_tableNameWithType)
            .setSegmentName(_segmentName).setStreamName(_streamConfig.getTopicName())
            .setSegmentZKMetadata(_segmentZKMetadata).setStatsHistory(statsHistory).setSchema(_schema)
            .setCapacity(capacity).setAvgNumMultiValues(avgNumMultiValues)
            .setOffHeap(indexLoadingConfig.isRealtimeOffHeapAllocation())
            .setFieldConfigList(_tableConfig.getFieldConfigList()).setConsumerDir(_resourceDataDir.getAbsolutePath())
            .setMemoryManager(
                new MmapMemoryManager(FileUtils.getTempDirectory().getAbsolutePath(), _segmentName, null));

    setPartitionParameters(realtimeSegmentConfigBuilder, _tableConfig.getIndexingConfig().getSegmentPartitionConfig());

    _realtimeSegment = new MutableSegmentImpl(realtimeSegmentConfigBuilder.build(), null);

    _transformPipeline = new TransformPipeline(_tableConfig, _schema);

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
        _logger.info("Created new consumer thread {} for {}", _consumerThread, this);
        _currentOffset = _startOffset;
        TransformPipeline.Result reusedResult = new TransformPipeline.Result();
        while (_currentOffset.compareTo(_endOffset) < 0) {
          // Fetch messages
          MessageBatch messageBatch = _consumer.fetchMessages(_currentOffset, _fetchTimeoutMs);

          int messageCount = messageBatch.getMessageCount();

          for (int i = 0; i < messageCount; i++) {
            StreamMessage streamMessage = messageBatch.getStreamMessage(i);
            if (streamMessage.getMetadata() != null && streamMessage.getMetadata().getOffset() != null
                && streamMessage.getMetadata().getOffset().compareTo(_endOffset) >= 0) {
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

              for (GenericRow transformedRow : transformedRows) {
                _realtimeSegment.index(transformedRow, streamMessage.getMetadata());
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
    if (_consumerThread.isAlive()) {
      _consumerThread.interrupt();
      try {
        _consumerThread.join();
      } catch (InterruptedException e) {
        _logger.warn("Interrupted while waiting for consumer thread to finish");
      }
    }
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

  public File buildSegment() {
    _logger.info("Building segment from {} to {}", _startOffset, _endOffset);
    long startTimeMs = now();
    try {
      if (_segBuildSemaphore != null) {
        _logger.info("Trying to acquire semaphore for building segment");
        Instant acquireStart = Instant.now();
        int timeoutSeconds = 5;
        while (!_segBuildSemaphore.tryAcquire(timeoutSeconds, TimeUnit.SECONDS)) {
          _logger.warn("Could not acquire semaphore for building segment in {}",
              Duration.between(acquireStart, Instant.now()));
          timeoutSeconds = Math.min(timeoutSeconds * 2, 300);
        }
      }
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted while waiting for segment build semaphore", e);
    }
    long lockAcquireTimeMs = now();
    _logger.info("Acquired lock for building segment in {} ms", lockAcquireTimeMs - startTimeMs);

    // Build a segment from in-memory rows.
    SegmentZKPropsConfig segmentZKPropsConfig = new SegmentZKPropsConfig();
    segmentZKPropsConfig.setStartOffset(_startOffset.toString());
    segmentZKPropsConfig.setEndOffset(_endOffset.toString());

    // Build the segment
    RealtimeSegmentConverter converter =
        new RealtimeSegmentConverter(_realtimeSegment, segmentZKPropsConfig, _resourceTmpDir.getAbsolutePath(), _schema,
            _tableNameWithType, _tableConfig, _segmentZKMetadata.getSegmentName(),
            _tableConfig.getIndexingConfig().isNullHandlingEnabled());
    try {
      converter.build(null, null);
    } catch (Exception e) {
      throw new RuntimeException("Failed to build segment", e);
    }
    _logger.info("Successfully built segment (Column Mode: {}) in {} ms", converter.isColumnMajorEnabled(),
        now() - lockAcquireTimeMs);

    File indexDir = new File(_resourceTmpDir, _segmentName);
    File segmentTarFile = new File(_resourceTmpDir, _segmentName + TarCompressionUtils.TAR_GZ_FILE_EXTENSION);
    try {
      TarCompressionUtils.createCompressedTarFile(new File(_resourceTmpDir, _segmentName), segmentTarFile);
    } catch (Exception e) {
      throw new RuntimeException(
          "Caught exception while tarring index directory from: " + indexDir + " to: " + segmentTarFile, e);
    }
    return segmentTarFile;
  }

  protected long now() {
    return System.currentTimeMillis();
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

  @Override
  public void close() {
    stopConsumption();
    _realtimeSegment.destroy();
    FileUtils.deleteQuietly(_resourceTmpDir);
    FileUtils.deleteQuietly(_resourceDataDir);
  }
}
