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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Uninterruptibles;
import java.io.Closeable;
import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.segment.local.indexsegment.mutable.MutableSegmentImpl;
import org.apache.pinot.segment.local.io.writer.impl.MmapMemoryManager;
import org.apache.pinot.segment.local.realtime.DecodeRetryTracker;
import org.apache.pinot.segment.local.realtime.RealtimeIngestionRecordProcessor;
import org.apache.pinot.segment.local.realtime.converter.RealtimeSegmentConverter;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentConfig;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentStatsHistory;
import org.apache.pinot.segment.local.segment.creator.TransformPipeline;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.utils.IngestionUtils;
import org.apache.pinot.segment.local.utils.IngestionUtils.PartitioningListener;
import org.apache.pinot.segment.local.utils.SchemaUtils;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentZKPropsConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.MessageBatch;
import org.apache.pinot.spi.stream.PartitionGroupConsumer;
import org.apache.pinot.spi.stream.PartitionGroupConsumptionStatus;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConsumerFactory;
import org.apache.pinot.spi.stream.StreamConsumerFactoryProvider;
import org.apache.pinot.spi.stream.StreamDataDecoder;
import org.apache.pinot.spi.stream.StreamDataDecoderResult;
import org.apache.pinot.spi.stream.StreamMessage;
import org.apache.pinot.spi.stream.StreamMessageMetadata;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffsetFactory;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Segment writer to ingest streaming data from a start offset to an end offset.
///
/// Record transformation, indexing, NULL handling and aggregation follow normal realtime consumption. Transform
/// and indexing errors skip affected rows; terminal decode errors stop only when stopOnDecodeError is enabled.
/// Processing outcomes are counted per reconstruction and logged when consumption finishes. The consumer thread
/// owns record processing; callers must stop it before building or closing the writer.
///
/// A batch that reports data loss (records between the requested offset and the batch were deleted from the stream
/// before they could be replayed) fails the replay before anything from it is indexed: the replayed segment would
/// otherwise silently miss part of its offset range. The segment then stays in ERROR until the operator restores the
/// range from another source or deletes the segment to accept the loss.
///
/// A batch without unfiltered messages never moves the writer, whatever next offset it reports: the writer polls again
/// from its own offset. A Kafka consumer reports its client position there, which can lie past transaction markers,
/// but also past deleted records after an out-of-range reset it has not reported as loss yet. The loss check of the
/// next batch with records then covers the whole gap. Reaching the end offset never needs that position, since the
/// data manager only moves its own offset on fetched records.
///
/// TODO:
///   1. Clean up this class and only keep the necessary parts.
///   2. Use a different segment impl for better performance because it doesn't need to serve queries.
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
  private final int _streamPartitionId;
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
  private final RealtimeIngestionRecordProcessor _recordProcessor;
  private final IngestionRecordListener _ingestionRecordListener = new IngestionRecordListener();
  private volatile boolean _isSuccess = false;
  private volatile Throwable _consumptionException;
  // Set by stopConsumption() to end the consumption at the next batch, even if the stream ignores the interrupt
  private volatile boolean _shouldStop;

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
    _schema = SchemaUtils.prepareRealtimeSegmentSchema(_tableConfig, indexLoadingConfig.getSchema(), segmentZKMetadata);
    String tableDataDir = indexLoadingConfig.getTableDataDir() == null ? FileUtils.getTempDirectory().getAbsolutePath()
        : indexLoadingConfig.getTableDataDir();
    File reingestionDir = new File(tableDataDir, "reingestion");
    String attemptId = UUID.randomUUID().toString();
    _resourceTmpDir =
        new File(reingestionDir, RESOURCE_TMP_DIR_PREFIX + _segmentName + "_" + attemptId);
    _resourceDataDir =
        new File(reingestionDir, RESOURCE_DATA_DIR_PREFIX + _segmentName + "_" + attemptId);
    FileUtils.deleteQuietly(_resourceTmpDir);
    FileUtils.deleteQuietly(_resourceDataDir);
    FileUtils.forceMkdir(_resourceTmpDir);
    FileUtils.forceMkdir(_resourceDataDir);

    _logger = LoggerFactory.getLogger(StatelessRealtimeSegmentWriter.class.getName() + "_" + _segmentName);

    _streamPartitionId = IngestionConfigUtils.getStreamPartitionIdFromPinotPartitionId(_tableConfig, _partitionGroupId);
    _streamConfig = new StreamConfig(_tableNameWithType,
        IngestionConfigUtils.getStreamConfigMap(_tableConfig, _partitionGroupId));

    StreamPartitionMsgOffsetFactory offsetFactory =
        StreamConsumerFactoryProvider.create(_streamConfig).createStreamMsgOffsetFactory();
    _startOffset = offsetFactory.create(segmentZKMetadata.getStartOffset());
    _endOffset = offsetFactory.create(segmentZKMetadata.getEndOffset());
    Preconditions.checkArgument(_startOffset.compareTo(_endOffset) <= 0,
        "Invalid replay interval: [%s, %s)", _startOffset, _endOffset);

    String clientId = getClientId();
    _consumerFactory = StreamConsumerFactoryProvider.create(_streamConfig);
    _partitionMetadataProvider = _consumerFactory.createPartitionMetadataProvider(clientId, _streamPartitionId);

    // Create a simple PartitionGroupConsumptionStatus
    PartitionGroupConsumptionStatus partitionGroupConsumptionStatus =
        new PartitionGroupConsumptionStatus(_partitionGroupId, _streamPartitionId, llcSegmentName.getSequenceNumber(),
            _startOffset, _endOffset,
            segmentZKMetadata.getStatus() != null ? segmentZKMetadata.getStatus().toString() : null);

    _consumer = _consumerFactory.createPartitionGroupConsumer(clientId, partitionGroupConsumptionStatus);

    // Initialize decoder
    _decoder = IngestionUtils.createStreamDecoder(_streamConfig, _tableConfig, _schema, _logger).decoder();

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
    RealtimeSegmentStatsHistory statsHistory = RealtimeSegmentStatsHistory.deserializeFrom(statsHistoryFile);

    // Initialize mutable segment with configurations
    IngestionConfig ingestionConfig = _tableConfig.getIngestionConfig();
    RealtimeSegmentConfig.Builder realtimeSegmentConfigBuilder = new RealtimeSegmentConfig.Builder(indexLoadingConfig)
        .setTableNameWithType(_tableNameWithType)
        .setSegmentName(_segmentName)
        .setStreamName(_streamConfig.getTopicName())
        .setSchema(_schema)
        .setCapacity(capacity)
        .setAvgNumMultiValues(avgNumMultiValues)
        .setSegmentZKMetadata(_segmentZKMetadata)
        .setOffHeap(indexLoadingConfig.isRealtimeOffHeapAllocation())
        .setMemoryManager(new MmapMemoryManager(_resourceDataDir.getAbsolutePath(), _segmentName, null))
        .setStatsHistory(statsHistory)
        .setIngestionSemantics(_tableConfig)
        .setConsumerDir(_resourceDataDir.getAbsolutePath())
        .setDropRecordOnPartitionMismatch(ingestionConfig != null
            && ingestionConfig.getStreamIngestionConfig() != null
            && ingestionConfig.getStreamIngestionConfig().isDropRecordOnPartitionMismatch());

    setPartitionParameters(realtimeSegmentConfigBuilder, _tableConfig.getIndexingConfig().getSegmentPartitionConfig());

    _realtimeSegment = new MutableSegmentImpl(realtimeSegmentConfigBuilder.build(), null);

    _recordProcessor = new RealtimeIngestionRecordProcessor(new TransformPipeline(_tableConfig, _schema),
        _realtimeSegment, RealtimeIngestionRecordProcessor.parseStopOnDecodeErrorConfig(_streamConfig, _logger),
        _ingestionRecordListener);

    // Initialize fetch timeout
    _fetchTimeoutMs =
        _streamConfig.getFetchTimeoutMillis() > 0 ? _streamConfig.getFetchTimeoutMillis() : DEFAULT_FETCH_TIMEOUT_MS;
  }

  /// Tracks this reconstruction separately from live ingestion metrics, which must not count replayed rows again.
  private final class IngestionRecordListener implements RealtimeIngestionRecordProcessor.Listener {
    private long _numRowsIndexed;
    private long _numDecodeErrors;
    private long _numTransformErrors;
    private long _numIndexErrors;
    private long _numRowsFiltered;
    private long _numIncompleteRows;
    private long _numRowsSanitized;

    @Override
    public void onDecodeError(Exception exception, StreamMessageMetadata metadata, boolean fatal) {
      _numDecodeErrors++;
      _logger.warn("Failed to decode message at offset {} (fatal: {})", metadata.getOffset(), fatal, exception);
    }

    @Override
    public void onTransformError(GenericRow row, StreamMessageMetadata metadata, Exception exception) {
      _numTransformErrors++;
      _logger.warn("Failed to transform record at offset {}", metadata.getOffset(), exception);
    }

    @Override
    public void onIndexError(GenericRow row, StreamMessageMetadata metadata, Exception exception) {
      _numIndexErrors++;
      _logger.warn("Failed to index record at offset {}", metadata.getOffset(), exception);
    }

    @Override
    public void onTransformResult(TransformPipeline.Result result, StreamMessageMetadata metadata) {
      _numRowsFiltered += result.getSkippedRowCount();
      _numIncompleteRows += result.getIncompleteRowCount();
      _numRowsSanitized += result.getSanitizedRowCount();
    }

    @Override
    public void onRowIndexed(GenericRow row, StreamMessageMetadata metadata) {
      _numRowsIndexed++;
    }
  }

  /// An immutable snapshot of reconstruction outcomes. Read after consumption is done.
  @VisibleForTesting
  record IngestionStats(long indexedRows, long decodeErrors, long transformErrors, long indexErrors,
                        long filteredRows, long incompleteRows, long sanitizedRows) {
  }

  @VisibleForTesting
  IngestionStats getIngestionStats() {
    IngestionRecordListener listener = _ingestionRecordListener;
    return new IngestionStats(listener._numRowsIndexed, listener._numDecodeErrors, listener._numTransformErrors,
        listener._numIndexErrors, listener._numRowsFiltered, listener._numIncompleteRows, listener._numRowsSanitized);
  }

  private String getClientId() {
    return _tableNameWithType + "-" + _partitionGroupId;
  }

  public void startConsumption() {
    // Start the consumer thread
    _consumerThread = new Thread(new PartitionConsumer(), _segmentName);
    _consumerThread.start();
  }

  /// Returns whether to fetch and decode the message at `offset` again after a retriable decode failure, reporting it
  /// as the realtime consumer does, minus segment errors.
  private boolean shouldRetryDecode(DecodeRetryTracker decodeRetryTracker, StreamPartitionMsgOffset offset,
      Exception cause) {
    long nowMs = now();
    DecodeRetryTracker.Outcome outcome = decodeRetryTracker.onRetriableFailure(nowMs);
    if (outcome == DecodeRetryTracker.Outcome.GIVE_UP_AND_REPORT) {
      _logger.warn("Retriable decode errors have lasted {}ms, giving up retrying the message at offset: {}. Until "
              + "none happens for {}ms, retriable decode errors are handled like any other decode error",
          decodeRetryTracker.getOutageDurationMs(nowMs), offset, decodeRetryTracker.getMaxDurationMs());
    } else if (outcome == DecodeRetryTracker.Outcome.RETRY_AND_REPORT) {
      _logger.warn("Retriable decode error at offset: {}. Fetching and decoding it again", offset, cause);
    } else if (outcome == DecodeRetryTracker.Outcome.RETRY_AND_REMIND) {
      _logger.warn("Still retrying: retriable decode error at offset: {}, {}ms into the outage", offset,
          decodeRetryTracker.getOutageDurationMs(nowMs), cause);
    }
    return outcome.isRetry();
  }

  private class PartitionConsumer implements Runnable {
    @Override
    public void run() {
      try {
        _consumer.start(_startOffset);
        _logger.info("Created new consumer thread {} for {}", _consumerThread, this);
        _currentOffset = _startOffset;
        DecodeRetryTracker decodeRetryTracker = new DecodeRetryTracker(_streamConfig);
        while (_currentOffset.compareTo(_endOffset) < 0) {
          if (_shouldStop) {
            _consumptionException = new IllegalStateException(
                "Consumption stopped at offset: " + _currentOffset + " before reaching end offset: " + _endOffset);
            _logger.info("Stopped consumption at offset: {} before reaching end offset: {}", _currentOffset,
                _endOffset);
            return;
          }

          // Fetch messages
          MessageBatch messageBatch = _consumer.fetchMessages(_currentOffset, _fetchTimeoutMs);
          Preconditions.checkState(!messageBatch.hasDataLoss(), "Stream data loss at offset %s while replaying to %s: "
              + "the stream no longer holds this range (e.g. it aged out of retention), so the segment cannot be "
              + "rebuilt from it. Restore the range from another source, or delete the segment to accept the loss",
              _currentOffset, _endOffset);
          if (messageBatch.getUnfilteredMessageCount() == 0) {
            // Poll again from the same offset (see the class Javadoc)
            continue;
          }

          int messageCount = messageBatch.getMessageCount();
          // Refetch from the previous message's next offset, as some streams fetch from after the given offset
          StreamPartitionMsgOffset currentMessageFetchOffset = _currentOffset;
          boolean decodeRetryNeeded = false;

          for (int i = 0; i < messageCount; i++) {
            StreamMessage streamMessage = messageBatch.getStreamMessage(i);
            StreamMessageMetadata metadata = streamMessage.getMetadata();
            if (metadata.getOffset().compareTo(_endOffset) >= 0) {
              _logger.info("Reached end offset: {} for partition group: {}", _endOffset, _partitionGroupId);
              break;
            }

            // Decode message
            StreamDataDecoderResult decodedResult = _decoder.decode(streamMessage);
            if (decodedResult.isRetriable() && shouldRetryDecode(decodeRetryTracker, metadata.getOffset(),
                decodedResult.getException())) {
              // Refetch after a backoff rather than leave the message out; the re-ingestion timeout bounds retries
              decodeRetryNeeded = true;
              break;
            }
            decodeRetryTracker.onMessageDone();
            _recordProcessor.process(decodedResult, metadata);
            currentMessageFetchOffset = metadata.getNextOffset();
          }

          if (decodeRetryNeeded) {
            _currentOffset = currentMessageFetchOffset;
            if (!_shouldStop) {
              try {
                sleepBeforeDecodeRetry(decodeRetryTracker.nextBackoffMs());
              } catch (InterruptedException e) {
                // stopConsumption() interrupts the backoff, and the _shouldStop check above ends consumption
                Thread.currentThread().interrupt();
              }
            }
            continue;
          }
          _currentOffset = messageBatch.getOffsetOfNextBatch();
        }
        _isSuccess = true;
      } catch (Exception e) {
        _logger.error("Exception in consumer thread", e);
        _consumptionException = e;
      } finally {
        try {
          _consumer.close();
        } catch (Exception e) {
          _logger.warn("Failed to close consumer", e);
        }
        _logger.info("Re-ingestion finished (success: {}, offset: {}, end offset: {}): {}",
            _isSuccess, _currentOffset, _endOffset, getIngestionStats());
        _isDoneConsuming.set(true);
      }
    }
  }

  /// Sleeps the decode retry backoff; [#stopConsumption()] interrupts it.
  @VisibleForTesting
  void sleepBeforeDecodeRetry(long backoffMs)
      throws InterruptedException {
    Thread.sleep(backoffMs);
  }

  public void stopConsumption() {
    _shouldStop = true;
    if (_consumerThread != null && _consumerThread.isAlive()) {
      _consumerThread.interrupt();
      // Wait even if interrupted, so that the segment is not destroyed while the consumer thread is still indexing
      Uninterruptibles.joinUninterruptibly(_consumerThread);
    }
  }

  public boolean isDoneConsuming() {
    return _isDoneConsuming.get();
  }

  public boolean isSuccess() {
    return _isSuccess;
  }

  @Nullable
  public Throwable getConsumptionException() {
    return _consumptionException;
  }

  public File buildSegment() {
    _logger.info("Building segment from {} to {}", _startOffset, _endOffset);
    long startTimeMs = now();
    try {
      if (_segBuildSemaphore != null) {
        _logger.info("Trying to acquire semaphore for building segment: {}", _segmentName);
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
    try {
      long lockAcquireTimeMs = now();
      _logger.info("Acquired lock for building segment in {} ms", lockAcquireTimeMs - startTimeMs);

      // Build a segment from in-memory rows.
      SegmentZKPropsConfig segmentZKPropsConfig = new SegmentZKPropsConfig();
      segmentZKPropsConfig.setStartOffset(_startOffset.toString());
      segmentZKPropsConfig.setEndOffset(_endOffset.toString());

      // Build the segment
      RealtimeSegmentConverter converter =
          new RealtimeSegmentConverter(_realtimeSegment, segmentZKPropsConfig, _resourceTmpDir.getAbsolutePath(),
              _schema,
              _tableNameWithType, _tableConfig, _segmentZKMetadata.getSegmentName(),
              _tableConfig.getIndexingConfig().isNullHandlingEnabled());
      try {
        converter.build(null);
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
    } finally {
      if (_segBuildSemaphore != null) {
        _logger.info("Releasing semaphore for building segment");
        _segBuildSemaphore.release();
      }
    }
  }

  protected long now() {
    return System.currentTimeMillis();
  }

  private void setPartitionParameters(RealtimeSegmentConfig.Builder realtimeSegmentConfigBuilder,
      @Nullable SegmentPartitionConfig segmentPartitionConfig) {
    IngestionUtils.configureRealtimePartitioning(realtimeSegmentConfigBuilder, segmentPartitionConfig,
        _partitionGroupId, _partitionMetadataProvider, 5000, _logger, new PartitioningListener() {
          @Override
          public void onPartitionCountMismatch(int configuredCount, int streamCount) {
            _logger.info("Stream partition count {} differs from configured count {}; using stream count",
                streamCount, configuredCount);
          }

          @Override
          public void onPartitionCountFetchFailure(int configuredCount, Exception exception) {
            _logger.warn("Failed to get number of stream partitions in 5s; using configured count: {}",
                configuredCount, exception);
            createPartitionMetadataProvider("Timeout getting number of stream partitions");
          }
        });
  }

  /// Creates a new stream metadata provider
  private void createPartitionMetadataProvider(String reason) {
    closePartitionMetadataProvider();
    _logger.info("Creating new partition metadata provider, reason: {}", reason);
    _partitionMetadataProvider = _consumerFactory.createPartitionMetadataProvider(getClientId(), _streamPartitionId);
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
    if (_consumerThread == null) {
      try {
        _consumer.close();
      } catch (Exception e) {
        _logger.warn("Could not close stream consumer", e);
      }
    }
    closePartitionMetadataProvider();
    _realtimeSegment.destroy();
    FileUtils.deleteQuietly(_resourceTmpDir);
    FileUtils.deleteQuietly(_resourceDataDir);
  }
}
