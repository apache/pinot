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
package org.apache.pinot.core.data.manager.realtime;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Uninterruptibles;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.Utils;
import org.apache.pinot.common.metadata.segment.LLCRealtimeSegmentZKMetadata;
import org.apache.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.protocols.SegmentCompletionProtocol;
import org.apache.pinot.common.restlet.resources.SegmentErrorInfo;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.segment.local.indexsegment.mutable.MutableSegmentImpl;
import org.apache.pinot.segment.local.io.readerwriter.PinotDataBufferMemoryManager;
import org.apache.pinot.segment.local.realtime.converter.RealtimeSegmentConverter;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentConfig;
import org.apache.pinot.segment.local.recordtransformer.ComplexTypeTransformer;
import org.apache.pinot.segment.local.recordtransformer.CompositeTransformer;
import org.apache.pinot.segment.local.recordtransformer.RecordTransformer;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.upsert.PartitionUpsertMetadataManager;
import org.apache.pinot.segment.local.utils.IngestionUtils;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.partition.PartitionFunctionFactory;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.server.realtime.ServerSegmentCompletionProtocolHandler;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.CompletionConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.metrics.PinotMeter;
import org.apache.pinot.spi.stream.MessageBatch;
import org.apache.pinot.spi.stream.PartitionGroupConsumer;
import org.apache.pinot.spi.stream.PartitionGroupConsumptionStatus;
import org.apache.pinot.spi.stream.PartitionLevelStreamConfig;
import org.apache.pinot.spi.stream.PermanentConsumerException;
import org.apache.pinot.spi.stream.RowMetadata;
import org.apache.pinot.spi.stream.StreamConsumerFactory;
import org.apache.pinot.spi.stream.StreamConsumerFactoryProvider;
import org.apache.pinot.spi.stream.StreamDecoderProvider;
import org.apache.pinot.spi.stream.StreamMessageDecoder;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffsetFactory;
import org.apache.pinot.spi.stream.TransientConsumerException;
import org.apache.pinot.spi.utils.CommonConstants.ConsumerState;
import org.apache.pinot.spi.utils.CommonConstants.Segment.Realtime.CompletionMode;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Segment data manager for low level consumer realtime segments, which manages consumption and segment completion.
 */
public class LLRealtimeSegmentDataManager extends RealtimeSegmentDataManager {
  protected enum State {
    // The state machine starts off with this state. While in this state we consume stream events
    // and index them in memory. We continue to be in this state until the end criteria is satisfied
    // (time or number of rows)
    INITIAL_CONSUMING,

    // In this state, we consume from stream until we reach the _finalOffset (exclusive)
    CATCHING_UP,

    // In this state, we sleep for MAX_HOLDING_TIME_MS, and the make a segmentConsumed() call to the
    // controller.
    HOLDING,

    // We have been asked to go Online from Consuming state, and are trying a last attempt to catch up to the
    // target offset. In this state, we have a time constraint as well as a final offset constraint to look into
    // before we stop consuming.
    CONSUMING_TO_ONLINE,

    // We have been asked by the controller to retain the segment we have in memory at the current offset.
    // We should build the segment, and replace it with the in-memory segment.
    RETAINING,

    // We have been asked by the controller to commit the segment at the current offset. Build the segment
    // and make a segmentCommit() call to the controller.
    COMMITTING,

    // We have been asked to discard the in-memory segment we have. We will be serving queries, but not consuming
    // anymore rows from stream. We wait for a helix transition to go ONLINE, at which point, we can download the
    // segment from the controller and replace it with the in-memory segment.
    DISCARDED,

    // We have replaced our in-memory segment with the segment that has been built locally.
    RETAINED,

    // We have committed our segment to the controller, and also replaced it locally with the constructed segment.
    COMMITTED,

    // Something went wrong, we need to download the segment when we get the ONLINE transition.
    ERROR;

    public boolean shouldConsume() {
      return this.equals(INITIAL_CONSUMING) || this.equals(CATCHING_UP) || this.equals(CONSUMING_TO_ONLINE);
    }

    public boolean isFinal() {
      return this.equals(ERROR) || this.equals(COMMITTED) || this.equals(RETAINED) || this.equals(DISCARDED);
    }
  }

  private static final int MINIMUM_CONSUME_TIME_MINUTES = 10;

  @VisibleForTesting
  public class SegmentBuildDescriptor {
    final File _segmentTarFile;
    final Map<String, File> _metadataFileMap;
    final StreamPartitionMsgOffset _offset;
    final long _waitTimeMillis;
    final long _buildTimeMillis;
    final long _segmentSizeBytes;

    public SegmentBuildDescriptor(@Nullable File segmentTarFile, @Nullable Map<String, File> metadataFileMap,
        StreamPartitionMsgOffset offset, long buildTimeMillis, long waitTimeMillis, long segmentSizeBytes) {
      _segmentTarFile = segmentTarFile;
      _metadataFileMap = metadataFileMap;
      _offset = _streamPartitionMsgOffsetFactory.create(offset);
      _buildTimeMillis = buildTimeMillis;
      _waitTimeMillis = waitTimeMillis;
      _segmentSizeBytes = segmentSizeBytes;
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
  }

  private static final long TIME_THRESHOLD_FOR_LOG_MINUTES = 1;
  private static final long TIME_EXTENSION_ON_EMPTY_SEGMENT_HOURS = 1;
  private static final int MSG_COUNT_THRESHOLD_FOR_LOG = 100000;
  private static final int BUILD_TIME_LEASE_SECONDS = 30;
  private static final int MAX_CONSECUTIVE_ERROR_COUNT = 5;

  private final LLCRealtimeSegmentZKMetadata _segmentZKMetadata;
  private final TableConfig _tableConfig;
  private final RealtimeTableDataManager _realtimeTableDataManager;
  private final StreamMessageDecoder _messageDecoder;
  private final int _segmentMaxRowCount;
  private final String _resourceDataDir;
  private final IndexLoadingConfig _indexLoadingConfig;
  private final Schema _schema;
  // Semaphore for each partitionGroupId only, which is to prevent two different stream consumers
  // from consuming with the same partitionGroupId in parallel in the same host.
  // See the comments in {@link RealtimeTableDataManager}.
  private final Semaphore _partitionGroupConsumerSemaphore;
  // A boolean flag to check whether the current thread has acquired the semaphore.
  // This boolean is needed because the semaphore is shared by threads; every thread holding this semaphore can
  // modify the permit. This boolean make sure the semaphore gets released only once when the partition group stops consuming.
  private final AtomicBoolean _acquiredConsumerSemaphore;
  private final String _metricKeyName;
  private final ServerMetrics _serverMetrics;
  private final MutableSegmentImpl _realtimeSegment;
  private StreamPartitionMsgOffset _currentOffset;
  private volatile State _state;
  private volatile int _numRowsConsumed = 0;
  private volatile int _numRowsIndexed = 0; // Can be different from _numRowsConsumed when metrics update is enabled.
  private volatile int _numRowsErrored = 0;
  private volatile int consecutiveErrorCount = 0;
  private long _startTimeMs = 0;
  private final String _segmentNameStr;
  private final SegmentVersion _segmentVersion;
  private final SegmentBuildTimeLeaseExtender _leaseExtender;
  private SegmentBuildDescriptor _segmentBuildDescriptor;
  private final StreamConsumerFactory _streamConsumerFactory;
  private final StreamPartitionMsgOffsetFactory _streamPartitionMsgOffsetFactory;

  // Segment end criteria
  private volatile long _consumeEndTime = 0;
  private volatile boolean _endOfPartitionGroup = false;
  private StreamPartitionMsgOffset _finalOffset; // Used when we want to catch up to this one
  private volatile boolean _shouldStop = false;

  // It takes 30s to locate controller leader, and more if there are multiple controller failures.
  // For now, we let 31s pass for this state transition.
  private static final int MAX_TIME_FOR_CONSUMING_TO_ONLINE_IN_SECONDS = 31;

  private Thread _consumerThread;
  private final String _streamTopic;
  private final int _partitionGroupId;
  private final PartitionGroupConsumptionStatus _partitionGroupConsumptionStatus;
  final String _clientId;
  private final LLCSegmentName _llcSegmentName;
  private final RecordTransformer _recordTransformer;
  private final ComplexTypeTransformer _complexTypeTransformer;
  private PartitionGroupConsumer _partitionGroupConsumer = null;
  private StreamMetadataProvider _streamMetadataProvider = null;
  private final File _resourceTmpDir;
  private final String _tableNameWithType;
  private final List<String> _invertedIndexColumns;
  private final List<String> _textIndexColumns;
  private final List<String> _fstIndexColumns;
  private final List<String> _noDictionaryColumns;
  private final List<String> _varLengthDictionaryColumns;
  private final String _sortedColumn;
  private final Logger segmentLogger;
  private final String _tableStreamName;
  private final PinotDataBufferMemoryManager _memoryManager;
  private final AtomicLong _lastUpdatedRowsIndexed = new AtomicLong(0);
  private final String _instanceId;
  private final ServerSegmentCompletionProtocolHandler _protocolHandler;
  private final long _consumeStartTime;
  private final StreamPartitionMsgOffset _startOffset;
  private final PartitionLevelStreamConfig _partitionLevelStreamConfig;

  private long _lastLogTime = 0;
  private int _lastConsumedCount = 0;
  private String _stopReason = null;
  private final Semaphore _segBuildSemaphore;
  private final boolean _isOffHeap;
  private final boolean _nullHandlingEnabled;
  private final SegmentCommitterFactory _segmentCommitterFactory;

  // TODO each time this method is called, we print reason for stop. Good to print only once.
  private boolean endCriteriaReached() {
    Preconditions.checkState(_state.shouldConsume(), "Incorrect state %s", _state);
    long now = now();
    switch (_state) {
      case INITIAL_CONSUMING:
        // The segment has been created, and we have not posted a segmentConsumed() message on the controller yet.
        // We need to consume as much data as available, until we have either reached the max number of rows or
        // the max time we are allowed to consume.
        if (now >= _consumeEndTime) {
          if (_realtimeSegment.getNumDocsIndexed() == 0) {
            segmentLogger.info("No events came in, extending time by {} hours", TIME_EXTENSION_ON_EMPTY_SEGMENT_HOURS);
            _consumeEndTime += TimeUnit.HOURS.toMillis(TIME_EXTENSION_ON_EMPTY_SEGMENT_HOURS);
            return false;
          }
          segmentLogger
              .info("Stopping consumption due to time limit start={} now={} numRowsConsumed={} numRowsIndexed={}",
                  _startTimeMs, now, _numRowsConsumed, _numRowsIndexed);
          _stopReason = SegmentCompletionProtocol.REASON_TIME_LIMIT;
          return true;
        } else if (_numRowsIndexed >= _segmentMaxRowCount) {
          segmentLogger.info("Stopping consumption due to row limit nRows={} numRowsIndexed={}, numRowsConsumed={}",
              _numRowsIndexed, _numRowsConsumed, _segmentMaxRowCount);
          _stopReason = SegmentCompletionProtocol.REASON_ROW_LIMIT;
          return true;
        } else if (_endOfPartitionGroup) {
          segmentLogger.info(
              "Stopping consumption due to end of partitionGroup reached nRows={} numRowsIndexed={}, numRowsConsumed={}",
              _numRowsIndexed, _numRowsConsumed, _segmentMaxRowCount);
          _stopReason = SegmentCompletionProtocol.REASON_END_OF_PARTITION_GROUP;
          return true;
        }
        return false;

      case CATCHING_UP:
        _stopReason = null;
        // We have posted segmentConsumed() at least once, and the controller is asking us to catch up to a certain offset.
        // There is no time limit here, so just check to see that we are still within the offset we need to reach.
        // Going past the offset is an exception.
        if (_currentOffset.compareTo(_finalOffset) == 0) {
          segmentLogger.info("Caught up to offset={}, state={}", _finalOffset, _state.toString());
          return true;
        }
        if (_currentOffset.compareTo(_finalOffset) > 0) {
          segmentLogger.error("Offset higher in state={}, current={}, final={}", _state.toString(), _currentOffset,
              _finalOffset);
          throw new RuntimeException("Past max offset");
        }
        return false;

      case CONSUMING_TO_ONLINE:
        // We are attempting to go from CONSUMING to ONLINE state. We are making a last attempt to catch up to the
        // target offset. We have a time constraint, and need to stop consuming if we cannot get to the target offset
        // within that time.
        if (_currentOffset.compareTo(_finalOffset) == 0) {
          segmentLogger.info("Caught up to offset={}, state={}", _finalOffset, _state.toString());
          return true;
        } else if (now >= _consumeEndTime) {
          segmentLogger.info("Past max time budget: offset={}, state={}", _currentOffset, _state.toString());
          return true;
        }
        if (_currentOffset.compareTo(_finalOffset) > 0) {
          segmentLogger.error("Offset higher in state={}, current={}, final={}", _state.toString(), _currentOffset,
              _finalOffset);
          throw new RuntimeException("Past max offset");
        }
        return false;
      default:
        segmentLogger.error("Illegal state {}" + _state.toString());
        throw new RuntimeException("Illegal state to consume");
    }
  }

  private void handleTransientStreamErrors(Exception e)
      throws Exception {
    consecutiveErrorCount++;
    if (consecutiveErrorCount > MAX_CONSECUTIVE_ERROR_COUNT) {
      segmentLogger.warn("Stream transient exception when fetching messages, stopping consumption after {} attempts",
          consecutiveErrorCount, e);
      throw e;
    } else {
      segmentLogger
          .warn("Stream transient exception when fetching messages, retrying (count={})", consecutiveErrorCount, e);
      Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
      makeStreamConsumer("Too many transient errors");
    }
  }

  protected boolean consumeLoop()
      throws Exception {
    _numRowsErrored = 0;
    final long idlePipeSleepTimeMillis = 100;
    final long maxIdleCountBeforeStatUpdate = (3 * 60 * 1000) / (idlePipeSleepTimeMillis + _partitionLevelStreamConfig
        .getFetchTimeoutMillis());  // 3 minute count
    StreamPartitionMsgOffset lastUpdatedOffset = _streamPartitionMsgOffsetFactory
        .create(_currentOffset);  // so that we always update the metric when we enter this method.
    long consecutiveIdleCount = 0;
    // At this point, we know that we can potentially move the offset, so the old saved segment file is not valid
    // anymore. Remove the file if it exists.
    removeSegmentFile();

    segmentLogger.info("Starting consumption loop start offset {}, finalOffset {}", _currentOffset, _finalOffset);
    while (!_shouldStop && !endCriteriaReached()) {
      // Consume for the next readTime ms, or we get to final offset, whichever happens earlier,
      // Update _currentOffset upon return from this method
      MessageBatch messageBatch;
      try {
        messageBatch = _partitionGroupConsumer
            .fetchMessages(_currentOffset, null, _partitionLevelStreamConfig.getFetchTimeoutMillis());
        _endOfPartitionGroup = messageBatch.isEndOfPartitionGroup();
        consecutiveErrorCount = 0;
      } catch (TimeoutException e) {
        handleTransientStreamErrors(e);
        continue;
      } catch (TransientConsumerException e) {
        handleTransientStreamErrors(e);
        continue;
      } catch (PermanentConsumerException e) {
        segmentLogger.warn("Permanent exception from stream when fetching messages, stopping consumption", e);
        throw e;
      } catch (Exception e) {
        // Unknown exception from stream. Treat as a transient exception.
        // One such exception seen so far is java.net.SocketTimeoutException
        handleTransientStreamErrors(e);
        continue;
      }

      processStreamEvents(messageBatch, idlePipeSleepTimeMillis);

      if (_currentOffset.compareTo(lastUpdatedOffset) != 0) {
        consecutiveIdleCount = 0;
        // We consumed something. Update the highest stream offset as well as partition-consuming metric.
        // TODO Issue 5359 Need to find a way to bump metrics without getting actual offset value.
//        _serverMetrics.setValueOfTableGauge(_metricKeyName, ServerGauge.HIGHEST_KAFKA_OFFSET_CONSUMED, _currentOffset.getOffset());
//        _serverMetrics.setValueOfTableGauge(_metricKeyName, ServerGauge.HIGHEST_STREAM_OFFSET_CONSUMED, _currentOffset.getOffset());
        _serverMetrics.setValueOfTableGauge(_metricKeyName, ServerGauge.LLC_PARTITION_CONSUMING, 1);
        lastUpdatedOffset = _streamPartitionMsgOffsetFactory.create(_currentOffset);
      } else {
        // We did not consume any rows. Update the partition-consuming metric only if we have been idling for a long time.
        // Create a new stream consumer wrapper, in case we are stuck on something.
        if (++consecutiveIdleCount > maxIdleCountBeforeStatUpdate) {
          _serverMetrics.setValueOfTableGauge(_metricKeyName, ServerGauge.LLC_PARTITION_CONSUMING, 1);
          consecutiveIdleCount = 0;
          makeStreamConsumer("Idle for too long");
        }
      }
    }

    if (_numRowsErrored > 0) {
      _serverMetrics.addMeteredTableValue(_metricKeyName, ServerMeter.ROWS_WITH_ERRORS, _numRowsErrored);
      _serverMetrics.addMeteredTableValue(_tableStreamName, ServerMeter.ROWS_WITH_ERRORS, _numRowsErrored);
    }
    return true;
  }

  private void processStreamEvents(MessageBatch messagesAndOffsets, long idlePipeSleepTimeMillis) {
    PinotMeter realtimeRowsConsumedMeter = null;
    PinotMeter realtimeRowsDroppedMeter = null;

    int indexedMessageCount = 0;
    int streamMessageCount = 0;
    boolean canTakeMore = true;

    GenericRow reuse = new GenericRow();
    for (int index = 0; index < messagesAndOffsets.getMessageCount(); index++) {
      if (_shouldStop || endCriteriaReached()) {
        break;
      }
      if (!canTakeMore) {
        // The RealtimeSegmentImpl that we are pushing rows into has indicated that it cannot accept any more
        // rows. This can happen in one of two conditions:
        // 1. We are in INITIAL_CONSUMING state, and we somehow exceeded the max number of rows we are allowed to consume
        //    for this row. Something is seriously wrong, because endCriteriaReached() should have returned true when
        //    we hit the row limit.
        //    Throw an exception.
        //
        // 2. We are in CATCHING_UP state, and we legally hit this error due to unclean leader election where
        //    offsets get changed with higher generation numbers for some pinot servers but not others. So, if another
        //    server (who got a larger stream offset) asked us to catch up to that offset, but we are connected to a
        //    broker who has smaller offsets, then we may try to push more rows into the buffer than maximum. This
        //    is a rare case, and we really don't know how to handle this at this time.
        //    Throw an exception.
        //
        segmentLogger
            .error("Buffer full with {} rows consumed (row limit {}, indexed {})", _numRowsConsumed, _numRowsIndexed,
                _segmentMaxRowCount);
        throw new RuntimeException("Realtime segment full");
      }

      // Index each message
      reuse.clear();
      // retrieve metadata from the message batch if available
      // this can be overridden by the decoder if there is a better indicator in the message payload
      RowMetadata msgMetadata = messagesAndOffsets.getMetadataAtIndex(index);

      GenericRow decodedRow = _messageDecoder
          .decode(messagesAndOffsets.getMessageAtIndex(index), messagesAndOffsets.getMessageOffsetAtIndex(index),
              messagesAndOffsets.getMessageLengthAtIndex(index), reuse);
      if (decodedRow != null) {
        try {
          if (_complexTypeTransformer != null) {
            // TODO: consolidate complex type transformer into composite type transformer
            decodedRow = _complexTypeTransformer.transform(decodedRow);
          }
          if (decodedRow.getValue(GenericRow.MULTIPLE_RECORDS_KEY) != null) {
            for (Object singleRow : (Collection) decodedRow.getValue(GenericRow.MULTIPLE_RECORDS_KEY)) {
              GenericRow transformedRow = _recordTransformer.transform((GenericRow) singleRow);
              if (transformedRow != null && IngestionUtils.shouldIngestRow(transformedRow)) {
                realtimeRowsConsumedMeter = _serverMetrics
                    .addMeteredTableValue(_metricKeyName, ServerMeter.REALTIME_ROWS_CONSUMED, 1,
                        realtimeRowsConsumedMeter);
                indexedMessageCount++;
                canTakeMore = _realtimeSegment.index(transformedRow, msgMetadata);
              } else {
                realtimeRowsDroppedMeter = _serverMetrics
                    .addMeteredTableValue(_metricKeyName, ServerMeter.INVALID_REALTIME_ROWS_DROPPED, 1,
                        realtimeRowsDroppedMeter);
              }
            }
          } else {
            GenericRow transformedRow = _recordTransformer.transform(decodedRow);
            if (transformedRow != null && IngestionUtils.shouldIngestRow(transformedRow)) {
              realtimeRowsConsumedMeter = _serverMetrics
                  .addMeteredTableValue(_metricKeyName, ServerMeter.REALTIME_ROWS_CONSUMED, 1,
                      realtimeRowsConsumedMeter);
              indexedMessageCount++;
              canTakeMore = _realtimeSegment.index(transformedRow, msgMetadata);
            } else {
              realtimeRowsDroppedMeter = _serverMetrics
                  .addMeteredTableValue(_metricKeyName, ServerMeter.INVALID_REALTIME_ROWS_DROPPED, 1,
                      realtimeRowsDroppedMeter);
            }
          }
        } catch (Exception e) {
          String errorMessage = String.format("Caught exception while transforming the record: %s", decodedRow);
          segmentLogger.error(errorMessage, e);
          _numRowsErrored++;
          _realtimeTableDataManager
              .addSegmentError(_segmentNameStr, new SegmentErrorInfo(System.currentTimeMillis(), errorMessage, e));
        }
      } else {
        realtimeRowsDroppedMeter = _serverMetrics
            .addMeteredTableValue(_metricKeyName, ServerMeter.INVALID_REALTIME_ROWS_DROPPED, 1,
                realtimeRowsDroppedMeter);
      }

      _currentOffset = messagesAndOffsets.getNextStreamParitionMsgOffsetAtIndex(index);
      _numRowsIndexed = _realtimeSegment.getNumDocsIndexed();
      _numRowsConsumed++;
      streamMessageCount++;
    }
    updateCurrentDocumentCountMetrics();
    if (streamMessageCount != 0) {
      segmentLogger.debug("Indexed {} messages ({} messages read from stream) current offset {}", indexedMessageCount,
          streamMessageCount, _currentOffset);
    } else {
      // If there were no messages to be fetched from stream, wait for a little bit as to avoid hammering the stream
      Uninterruptibles.sleepUninterruptibly(idlePipeSleepTimeMillis, TimeUnit.MILLISECONDS);
    }
  }

  public class PartitionConsumer implements Runnable {
    public void run() {
      long initialConsumptionEnd = 0L;
      long lastCatchUpStart = 0L;
      long catchUpTimeMillis = 0L;
      _startTimeMs = now();
      try {
        while (!_state.isFinal()) {
          if (_state.shouldConsume()) {
            consumeLoop();  // Consume until we reached the end criteria, or we are stopped.
          }
          _serverMetrics.setValueOfTableGauge(_metricKeyName, ServerGauge.LLC_PARTITION_CONSUMING, 0);
          if (_shouldStop) {
            break;
          }

          if (_state == State.INITIAL_CONSUMING) {
            initialConsumptionEnd = now();
            _serverMetrics.setValueOfTableGauge(_metricKeyName,
                ServerGauge.LAST_REALTIME_SEGMENT_INITIAL_CONSUMPTION_DURATION_SECONDS,
                TimeUnit.MILLISECONDS.toSeconds(initialConsumptionEnd - _startTimeMs));
          } else if (_state == State.CATCHING_UP) {
            catchUpTimeMillis += now() - lastCatchUpStart;
            _serverMetrics
                .setValueOfTableGauge(_metricKeyName, ServerGauge.LAST_REALTIME_SEGMENT_CATCHUP_DURATION_SECONDS,
                    TimeUnit.MILLISECONDS.toSeconds(catchUpTimeMillis));
          }

          // If we are sending segmentConsumed() to the controller, we are in HOLDING state.
          _state = State.HOLDING;
          SegmentCompletionProtocol.Response response = postSegmentConsumedMsg();
          SegmentCompletionProtocol.ControllerResponseStatus status = response.getStatus();
          StreamPartitionMsgOffset rspOffset = extractOffset(response);
          boolean success;
          switch (status) {
            case NOT_LEADER:
              // Retain the same state
              segmentLogger.warn("Got not leader response");
              hold();
              break;
            case CATCH_UP:
              if (rspOffset.compareTo(_currentOffset) <= 0) {
                // Something wrong with the controller. Back off and try again.
                segmentLogger.error("Invalid catchup offset {} in controller response, current offset {}", rspOffset,
                    _currentOffset);
                hold();
              } else {
                _state = State.CATCHING_UP;
                _finalOffset = rspOffset;
                lastCatchUpStart = now();
                // We will restart consumption when we loop back above.
              }
              break;
            case HOLD:
              hold();
              break;
            case DISCARD:
              // Keep this in memory, but wait for the online transition, and download when it comes in.
              _state = State.DISCARDED;
              break;
            case KEEP:
              _state = State.RETAINING;
              CompletionMode segmentCompletionMode = getSegmentCompletionMode();
              switch (segmentCompletionMode) {
                case DOWNLOAD:
                  _state = State.DISCARDED;
                  break;
                case DEFAULT:
                  success = buildSegmentAndReplace();
                  if (success) {
                    _state = State.RETAINED;
                  } else {
                    // Could not build segment for some reason. We can only download it.
                    _state = State.ERROR;
                    _realtimeTableDataManager.addSegmentError(_segmentNameStr,
                        new SegmentErrorInfo(System.currentTimeMillis(), "Could not build segment", null));
                  }
                  break;
              }
              break;
            case COMMIT:
              _state = State.COMMITTING;
              long buildTimeSeconds = response.getBuildTimeSeconds();
              buildSegmentForCommit(buildTimeSeconds * 1000L);
              if (_segmentBuildDescriptor == null) {
                // We could not build the segment. Go into error state.
                _state = State.ERROR;
                _realtimeTableDataManager.addSegmentError(_segmentNameStr,
                    new SegmentErrorInfo(System.currentTimeMillis(), "Could not build segment", null));
              } else {
                success = commitSegment(response.getControllerVipUrl(),
                    response.isSplitCommit() && _indexLoadingConfig.isEnableSplitCommit());
                if (success) {
                  _state = State.COMMITTED;
                } else {
                  // If for any reason commit failed, we don't want to be in COMMITTING state when we hold.
                  // Change the state to HOLDING before looping around.
                  _state = State.HOLDING;
                  segmentLogger.info("Could not commit segment. Retrying after hold");
                  hold();
                }
              }
              break;
            default:
              segmentLogger.error("Holding after response from Controller: {}", response.toJsonString());
              hold();
              break;
          }
        }
      } catch (Exception e) {
        String errorMessage = "Exception while in work";
        segmentLogger.error(errorMessage, e);
        postStopConsumedMsg(e.getClass().getName());
        _state = State.ERROR;
        _realtimeTableDataManager
            .addSegmentError(_segmentNameStr, new SegmentErrorInfo(System.currentTimeMillis(), errorMessage, e));
        _serverMetrics.setValueOfTableGauge(_metricKeyName, ServerGauge.LLC_PARTITION_CONSUMING, 0);
        return;
      }

      removeSegmentFile();

      if (initialConsumptionEnd != 0L) {
        _serverMetrics
            .setValueOfTableGauge(_metricKeyName, ServerGauge.LAST_REALTIME_SEGMENT_COMPLETION_DURATION_SECONDS,
                TimeUnit.MILLISECONDS.toSeconds(now() - initialConsumptionEnd));
      }
      _serverMetrics.setValueOfTableGauge(_metricKeyName, ServerGauge.LLC_PARTITION_CONSUMING, 0);
    }
  }

  /**
   * Fetches the completion mode for the segment completion for the given realtime table
   */
  private CompletionMode getSegmentCompletionMode() {
    CompletionConfig completionConfig = _tableConfig.getValidationConfig().getCompletionConfig();
    if (completionConfig != null) {
      if (CompletionMode.DOWNLOAD.toString().equalsIgnoreCase(completionConfig.getCompletionMode())) {
        return CompletionMode.DOWNLOAD;
      }
    }
    return CompletionMode.DEFAULT;
  }

  @VisibleForTesting
  protected StreamPartitionMsgOffset extractOffset(SegmentCompletionProtocol.Response response) {
    if (response.getStreamPartitionMsgOffset() != null) {
      return _streamPartitionMsgOffsetFactory.create(response.getStreamPartitionMsgOffset());
    } else {
      // TODO Issue 5359 Remove this once the protocol is upgraded on server and controller
      return _streamPartitionMsgOffsetFactory.create(Long.toString(response.getOffset()));
    }
  }

  // Side effect: Modifies _segmentBuildDescriptor if we do not have a valid built segment file and we
  // built the segment successfully.
  protected void buildSegmentForCommit(long buildTimeLeaseMs) {
    try {
      if (_segmentBuildDescriptor != null && _segmentBuildDescriptor.getOffset().compareTo(_currentOffset) == 0) {
        // Double-check that we have the file, just in case.
        File segmentTarFile = _segmentBuildDescriptor.getSegmentTarFile();
        if (segmentTarFile != null && segmentTarFile.exists()) {
          return;
        }
      }
      removeSegmentFile();
      if (buildTimeLeaseMs <= 0) {
        if (_segBuildSemaphore == null) {
          buildTimeLeaseMs = SegmentCompletionProtocol.getDefaultMaxSegmentCommitTimeSeconds() * 1000L;
        } else {
          // We know we are going to use a semaphore to limit number of segment builds, and could be
          // blocked for a long time. The controller has not provided a lease time, so set one to
          // some reasonable guess here.
          buildTimeLeaseMs = BUILD_TIME_LEASE_SECONDS * 1000;
        }
      }
      _leaseExtender.addSegment(_segmentNameStr, buildTimeLeaseMs, _currentOffset);
      _segmentBuildDescriptor = buildSegmentInternal(true);
    } finally {
      _leaseExtender.removeSegment(_segmentNameStr);
    }
  }

  @Override
  public Map<String, String> getPartitionToCurrentOffset() {
    Map<String, String> partitionToCurrentOffset = new HashMap<>();
    partitionToCurrentOffset.put(String.valueOf(_partitionGroupId), _currentOffset.toString());
    return partitionToCurrentOffset;
  }

  @Override
  public ConsumerState getConsumerState() {
    return _state == State.ERROR ? ConsumerState.NOT_CONSUMING : ConsumerState.CONSUMING;
  }

  @Override
  public long getLastConsumedTimestamp() {
    return _lastLogTime;
  }

  @VisibleForTesting
  protected StreamPartitionMsgOffset getCurrentOffset() {
    return _currentOffset;
  }

  @VisibleForTesting
  protected SegmentBuildDescriptor getSegmentBuildDescriptor() {
    return _segmentBuildDescriptor;
  }

  @VisibleForTesting
  protected Semaphore getPartitionGroupConsumerSemaphore() {
    return _partitionGroupConsumerSemaphore;
  }

  @VisibleForTesting
  protected AtomicBoolean getAcquiredConsumerSemaphore() {
    return _acquiredConsumerSemaphore;
  }

  protected SegmentBuildDescriptor buildSegmentInternal(boolean forCommit) {
    closeStreamConsumers();
    try {
      final long startTimeMillis = now();
      if (_segBuildSemaphore != null) {
        segmentLogger.info("Waiting to acquire semaphore for building segment");
        _segBuildSemaphore.acquire();
      }
      // Increment llc simultaneous segment builds.
      _serverMetrics.addValueToGlobalGauge(ServerGauge.LLC_SIMULTANEOUS_SEGMENT_BUILDS, 1L);

      final long lockAcquireTimeMillis = now();
      // Build a segment from in-memory rows.If buildTgz is true, then build the tar.gz file as well
      // TODO Use an auto-closeable object to delete temp resources.
      File tempSegmentFolder = new File(_resourceTmpDir, "tmp-" + _segmentNameStr + "-" + now());
      // lets convert the segment now
      RealtimeSegmentConverter converter =
          new RealtimeSegmentConverter(_realtimeSegment, tempSegmentFolder.getAbsolutePath(), _schema,
              _tableNameWithType, _tableConfig, _segmentZKMetadata.getSegmentName(), _sortedColumn,
              _invertedIndexColumns, _textIndexColumns, _fstIndexColumns, _noDictionaryColumns,
              _varLengthDictionaryColumns, _nullHandlingEnabled);
      segmentLogger.info("Trying to build segment");
      try {
        converter.build(_segmentVersion, _serverMetrics);
      } catch (Exception e) {
        segmentLogger.error("Could not build segment", e);
        FileUtils.deleteQuietly(tempSegmentFolder);
        return null;
      }
      final long buildTimeMillis = now() - lockAcquireTimeMillis;
      final long waitTimeMillis = lockAcquireTimeMillis - startTimeMillis;
      segmentLogger
          .info("Successfully built segment in {} ms, after lockWaitTime {} ms", buildTimeMillis, waitTimeMillis);

      File dataDir = new File(_resourceDataDir);
      File indexDir = new File(dataDir, _segmentNameStr);
      FileUtils.deleteQuietly(indexDir);

      File[] tempFiles = tempSegmentFolder.listFiles();
      assert tempFiles != null;
      File tempIndexDir = tempFiles[0];
      try {
        FileUtils.moveDirectory(tempIndexDir, indexDir);
      } catch (IOException e) {
        String errorMessage =
            String.format("Caught exception while moving index directory from: %s to: %s", tempIndexDir, indexDir);
        segmentLogger.error(errorMessage, e);
        _realtimeTableDataManager
            .addSegmentError(_segmentNameStr, new SegmentErrorInfo(System.currentTimeMillis(), errorMessage, e));
        return null;
      } finally {
        FileUtils.deleteQuietly(tempSegmentFolder);
      }

      long segmentSizeBytes = FileUtils.sizeOfDirectory(indexDir);
      _serverMetrics.setValueOfTableGauge(_metricKeyName, ServerGauge.LAST_REALTIME_SEGMENT_CREATION_DURATION_SECONDS,
          TimeUnit.MILLISECONDS.toSeconds(buildTimeMillis));
      _serverMetrics.setValueOfTableGauge(_metricKeyName, ServerGauge.LAST_REALTIME_SEGMENT_CREATION_WAIT_TIME_SECONDS,
          TimeUnit.MILLISECONDS.toSeconds(waitTimeMillis));

      if (forCommit) {
        File segmentTarFile = new File(dataDir, _segmentNameStr + TarGzCompressionUtils.TAR_GZ_FILE_EXTENSION);
        try {
          TarGzCompressionUtils.createTarGzFile(indexDir, segmentTarFile);
        } catch (IOException e) {
          String errorMessage =
              String.format("Caught exception while taring index directory from: %s to: %s", indexDir, segmentTarFile);
          segmentLogger.error(errorMessage, e);
          _realtimeTableDataManager
              .addSegmentError(_segmentNameStr, new SegmentErrorInfo(System.currentTimeMillis(), errorMessage, e));
          return null;
        }

        File metadataFile = SegmentDirectoryPaths.findMetadataFile(indexDir);
        if (metadataFile == null) {
          segmentLogger
              .error("Failed to find file: {} under index directory: {}", V1Constants.MetadataKeys.METADATA_FILE_NAME,
                  indexDir);
          return null;
        }
        File creationMetaFile = SegmentDirectoryPaths.findCreationMetaFile(indexDir);
        if (creationMetaFile == null) {
          segmentLogger
              .error("Failed to find file: {} under index directory: {}", V1Constants.SEGMENT_CREATION_META, indexDir);
          return null;
        }
        Map<String, File> metadataFiles = new HashMap<>();
        metadataFiles.put(V1Constants.MetadataKeys.METADATA_FILE_NAME, metadataFile);
        metadataFiles.put(V1Constants.SEGMENT_CREATION_META, creationMetaFile);

        return new SegmentBuildDescriptor(segmentTarFile, metadataFiles, _currentOffset, buildTimeMillis,
            waitTimeMillis, segmentSizeBytes);
      } else {
        return new SegmentBuildDescriptor(null, null, _currentOffset, buildTimeMillis, waitTimeMillis,
            segmentSizeBytes);
      }
    } catch (InterruptedException e) {
      segmentLogger.error("Interrupted while waiting for semaphore");
      return null;
    } finally {
      if (_segBuildSemaphore != null) {
        _segBuildSemaphore.release();
      }
      // Decrement llc simultaneous segment builds.
      _serverMetrics.addValueToGlobalGauge(ServerGauge.LLC_SIMULTANEOUS_SEGMENT_BUILDS, -1L);
    }
  }

  protected boolean commitSegment(String controllerVipUrl, boolean isSplitCommit) {
    File segmentTarFile = _segmentBuildDescriptor.getSegmentTarFile();
    if (segmentTarFile == null || !segmentTarFile.exists()) {
      throw new RuntimeException("Segment file does not exist: " + segmentTarFile);
    }
    SegmentCompletionProtocol.Response commitResponse = commit(controllerVipUrl, isSplitCommit);

    if (!commitResponse.getStatus().equals(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_SUCCESS)) {
      return false;
    }

    _realtimeTableDataManager.replaceLLSegment(_segmentNameStr, _indexLoadingConfig);
    removeSegmentFile();
    return true;
  }

  protected SegmentCompletionProtocol.Response commit(String controllerVipUrl, boolean isSplitCommit) {
    SegmentCompletionProtocol.Request.Params params = new SegmentCompletionProtocol.Request.Params();

    params.withSegmentName(_segmentNameStr).withStreamPartitionMsgOffset(_currentOffset.toString())
        .withNumRows(_numRowsConsumed).withInstanceId(_instanceId)
        .withBuildTimeMillis(_segmentBuildDescriptor.getBuildTimeMillis())
        .withSegmentSizeBytes(_segmentBuildDescriptor.getSegmentSizeBytes())
        .withWaitTimeMillis(_segmentBuildDescriptor.getWaitTimeMillis());
    if (_isOffHeap) {
      params.withMemoryUsedBytes(_memoryManager.getTotalAllocatedBytes());
    }

    SegmentCommitter segmentCommitter;
    try {
      segmentCommitter = _segmentCommitterFactory.createSegmentCommitter(isSplitCommit, params, controllerVipUrl);
    } catch (URISyntaxException e) {
      segmentLogger.error("Failed to create a segment committer: ", e);
      return SegmentCompletionProtocol.RESP_NOT_SENT;
    }
    return segmentCommitter.commit(_segmentBuildDescriptor);
  }

  protected boolean buildSegmentAndReplace() {
    SegmentBuildDescriptor descriptor = buildSegmentInternal(false);
    if (descriptor == null) {
      return false;
    }

    _realtimeTableDataManager.replaceLLSegment(_segmentNameStr, _indexLoadingConfig);
    return true;
  }

  private void closeStreamConsumers() {
    closePartitionGroupConsumer();
    closeStreamMetadataProvider();
    if (_acquiredConsumerSemaphore.compareAndSet(true, false)) {
      _partitionGroupConsumerSemaphore.release();
    }
  }

  private void closePartitionGroupConsumer() {
    try {
      _partitionGroupConsumer.close();
    } catch (Exception e) {
      segmentLogger.warn("Could not close stream consumer", e);
    }
  }

  private void closeStreamMetadataProvider() {
    try {
      _streamMetadataProvider.close();
    } catch (Exception e) {
      segmentLogger.warn("Could not close stream metadata provider", e);
    }
  }

  /**
   * Cleans up the metrics that reflects the state of the realtime segment.
   * This step is essential as the instance may not be the target location for some of the partitions.
   * E.g. if the number of partitions increases, or a host swap is needed, the target location for some partitions may change,
   * and the current host remains to run. In this case, the current server would still keep the state of the old partitions,
   * which no longer resides in this host any more, thus causes false positive information to the metric system.
   */
  private void cleanupMetrics() {
    _serverMetrics.removeTableGauge(_metricKeyName, ServerGauge.LLC_PARTITION_CONSUMING);
  }

  protected void hold() {
    try {
      Thread.sleep(SegmentCompletionProtocol.MAX_HOLD_TIME_MS);
    } catch (InterruptedException e) {
      segmentLogger.warn("Interrupted while holding");
    }
  }

  // Inform the controller that the server had to stop consuming due to an error.
  protected void postStopConsumedMsg(String reason) {
    do {
      SegmentCompletionProtocol.Request.Params params = new SegmentCompletionProtocol.Request.Params();
      params.withStreamPartitionMsgOffset(_currentOffset.toString()).withReason(reason).withSegmentName(_segmentNameStr)
          .withInstanceId(_instanceId);

      SegmentCompletionProtocol.Response response = _protocolHandler.segmentStoppedConsuming(params);
      if (response.getStatus() == SegmentCompletionProtocol.ControllerResponseStatus.PROCESSED) {
        segmentLogger.info("Got response {}", response.toJsonString());
        break;
      }
      Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);
      segmentLogger.info("Retrying after response {}", response.toJsonString());
    } while (!_shouldStop);
  }

  protected SegmentCompletionProtocol.Response postSegmentConsumedMsg() {
    // Post segmentConsumed to current leader.
    // Retry maybe once if leader is not found.
    SegmentCompletionProtocol.Request.Params params = new SegmentCompletionProtocol.Request.Params();
    params.withStreamPartitionMsgOffset(_currentOffset.toString()).withSegmentName(_segmentNameStr)
        .withReason(_stopReason).withNumRows(_numRowsConsumed).withInstanceId(_instanceId);
    if (_isOffHeap) {
      params.withMemoryUsedBytes(_memoryManager.getTotalAllocatedBytes());
    }
    return _protocolHandler.segmentConsumed(params);
  }

  private void removeSegmentFile() {
    if (_segmentBuildDescriptor != null) {
      _segmentBuildDescriptor.deleteSegmentFile();
      _segmentBuildDescriptor = null;
    }
  }

  public void goOnlineFromConsuming(RealtimeSegmentZKMetadata metadata)
      throws InterruptedException {
    _serverMetrics.setValueOfTableGauge(_metricKeyName, ServerGauge.LLC_PARTITION_CONSUMING, 0);
    try {
      LLCRealtimeSegmentZKMetadata llcMetadata = (LLCRealtimeSegmentZKMetadata) metadata;
      // Remove the segment file before we do anything else.
      removeSegmentFile();
      _leaseExtender.removeSegment(_segmentNameStr);
      final StreamPartitionMsgOffset endOffset = _streamPartitionMsgOffsetFactory.create(llcMetadata.getEndOffset());
      segmentLogger
          .info("State: {}, transitioning from CONSUMING to ONLINE (startOffset: {}, endOffset: {})", _state.toString(),
              _startOffset, endOffset);
      stop();
      segmentLogger.info("Consumer thread stopped in state {}", _state.toString());

      switch (_state) {
        case COMMITTED:
        case RETAINED:
          // Nothing to do. we already built local segment and swapped it with in-memory data.
          segmentLogger.info("State {}. Nothing to do", _state.toString());
          break;
        case DISCARDED:
        case ERROR:
          segmentLogger.info("State {}. Downloading to replace", _state.toString());
          downloadSegmentAndReplace(llcMetadata);
          break;
        case CATCHING_UP:
        case HOLDING:
        case INITIAL_CONSUMING:
          CompletionMode segmentCompletionMode = getSegmentCompletionMode();
          switch (segmentCompletionMode) {
            case DOWNLOAD:
              segmentLogger.info("State {}. CompletionMode {}. Downloading to replace", _state.toString(),
                  segmentCompletionMode);
              downloadSegmentAndReplace(llcMetadata);
              break;
            case DEFAULT:
              // Allow to catch up upto final offset, and then replace.
              if (_currentOffset.compareTo(endOffset) > 0) {
                // We moved ahead of the offset that is committed in ZK.
                segmentLogger
                    .warn("Current offset {} ahead of the offset in zk {}. Downloading to replace", _currentOffset,
                        endOffset);
                downloadSegmentAndReplace(llcMetadata);
              } else if (_currentOffset.compareTo(endOffset) == 0) {
                segmentLogger
                    .info("Current offset {} matches offset in zk {}. Replacing segment", _currentOffset, endOffset);
                buildSegmentAndReplace();
              } else {
                segmentLogger.info("Attempting to catch up from offset {} to {} ", _currentOffset, endOffset);
                boolean success = catchupToFinalOffset(endOffset,
                    TimeUnit.MILLISECONDS.convert(MAX_TIME_FOR_CONSUMING_TO_ONLINE_IN_SECONDS, TimeUnit.SECONDS));
                if (success) {
                  segmentLogger.info("Caught up to offset {}", _currentOffset);
                  buildSegmentAndReplace();
                } else {
                  segmentLogger
                      .info("Could not catch up to offset (current = {}). Downloading to replace", _currentOffset);
                  downloadSegmentAndReplace(llcMetadata);
                }
              }
              break;
          }
          break;
        default:
          segmentLogger.info("Downloading to replace segment while in state {}", _state.toString());
          downloadSegmentAndReplace(llcMetadata);
          break;
      }
    } catch (Exception e) {
      Utils.rethrowException(e);
    } finally {
      _serverMetrics.setValueOfTableGauge(_metricKeyName, ServerGauge.LLC_PARTITION_CONSUMING, 0);
    }
  }

  protected void downloadSegmentAndReplace(LLCRealtimeSegmentZKMetadata metadata) {
    closeStreamConsumers();
    _realtimeTableDataManager.downloadAndReplaceSegment(_segmentNameStr, metadata, _indexLoadingConfig, _tableConfig);
  }

  protected long now() {
    return System.currentTimeMillis();
  }

  private boolean catchupToFinalOffset(StreamPartitionMsgOffset endOffset, long timeoutMs) {
    _finalOffset = endOffset;
    _consumeEndTime = now() + timeoutMs;
    _state = State.CONSUMING_TO_ONLINE;
    _shouldStop = false;
    try {
      consumeLoop();
    } catch (Exception e) {
      // We will end up downloading the segment, so this is not a serious problem
      segmentLogger.warn("Exception when catching up to final offset", e);
      return false;
    } finally {
      _serverMetrics.setValueOfTableGauge(_metricKeyName, ServerGauge.LLC_PARTITION_CONSUMING, 0);
    }
    if (_currentOffset.compareTo(endOffset) != 0) {
      // Timeout?
      segmentLogger.error("Could not consume up to {} (current offset {})", endOffset, _currentOffset);
      return false;
    }

    return true;
  }

  public void destroy() {
    try {
      stop();
    } catch (InterruptedException e) {
      segmentLogger.error("Could not stop consumer thread");
    }
    _realtimeSegment.destroy();
    closeStreamConsumers();
    cleanupMetrics();
  }

  protected void start() {
    _consumerThread = new Thread(new PartitionConsumer(), _segmentNameStr);
    segmentLogger.info("Created new consumer thread {} for {}", _consumerThread, this.toString());
    _consumerThread.start();
  }

  /**
   * Stop the consuming thread.
   */
  public void stop()
      throws InterruptedException {
    _shouldStop = true;
    // This method could be called either when we get an ONLINE transition or
    // when we commit a segment and replace the realtime segment with a committed
    // one. In the latter case, we don't want to call join.
    if (Thread.currentThread() != _consumerThread) {
      Uninterruptibles.joinUninterruptibly(_consumerThread, 10, TimeUnit.MINUTES);

      if (_consumerThread.isAlive()) {
        segmentLogger.warn("Failed to stop consumer thread within 10 minutes");
      }
    }
  }

  // Assume that this is called only on OFFLINE to CONSUMING transition.
  // If the transition is OFFLINE to ONLINE, the caller should have downloaded the segment and we don't reach here.
  public LLRealtimeSegmentDataManager(RealtimeSegmentZKMetadata segmentZKMetadata, TableConfig tableConfig,
      RealtimeTableDataManager realtimeTableDataManager, String resourceDataDir, IndexLoadingConfig indexLoadingConfig,
      Schema schema, LLCSegmentName llcSegmentName, Semaphore partitionGroupConsumerSemaphore,
      ServerMetrics serverMetrics, @Nullable PartitionUpsertMetadataManager partitionUpsertMetadataManager) {
    _segBuildSemaphore = realtimeTableDataManager.getSegmentBuildSemaphore();
    _segmentZKMetadata = (LLCRealtimeSegmentZKMetadata) segmentZKMetadata;
    _tableConfig = tableConfig;
    _tableNameWithType = _tableConfig.getTableName();
    _realtimeTableDataManager = realtimeTableDataManager;
    _resourceDataDir = resourceDataDir;
    _indexLoadingConfig = indexLoadingConfig;
    _schema = schema;
    _serverMetrics = serverMetrics;
    _segmentVersion = indexLoadingConfig.getSegmentVersion();
    _instanceId = _realtimeTableDataManager.getServerInstance();
    _leaseExtender = SegmentBuildTimeLeaseExtender.getLeaseExtender(_tableNameWithType);
    _protocolHandler = new ServerSegmentCompletionProtocolHandler(_serverMetrics, _tableNameWithType);

    String timeColumnName = tableConfig.getValidationConfig().getTimeColumnName();
    // TODO Validate configs
    IndexingConfig indexingConfig = _tableConfig.getIndexingConfig();
    _partitionLevelStreamConfig =
        new PartitionLevelStreamConfig(_tableNameWithType, IngestionConfigUtils.getStreamConfigMap(_tableConfig));
    _streamConsumerFactory = StreamConsumerFactoryProvider.create(_partitionLevelStreamConfig);
    _streamPartitionMsgOffsetFactory =
        StreamConsumerFactoryProvider.create(_partitionLevelStreamConfig).createStreamMsgOffsetFactory();
    _streamTopic = _partitionLevelStreamConfig.getTopicName();
    _segmentNameStr = _segmentZKMetadata.getSegmentName();
    _llcSegmentName = llcSegmentName;
    _partitionGroupId = _llcSegmentName.getPartitionGroupId();
    _partitionGroupConsumptionStatus =
        new PartitionGroupConsumptionStatus(_partitionGroupId, _llcSegmentName.getSequenceNumber(),
            _streamPartitionMsgOffsetFactory.create(_segmentZKMetadata.getStartOffset()),
            _segmentZKMetadata.getEndOffset() == null ? null
                : _streamPartitionMsgOffsetFactory.create(_segmentZKMetadata.getEndOffset()),
            _segmentZKMetadata.getStatus().toString());
    _partitionGroupConsumerSemaphore = partitionGroupConsumerSemaphore;
    _acquiredConsumerSemaphore = new AtomicBoolean(false);
    _metricKeyName = _tableNameWithType + "-" + _streamTopic + "-" + _partitionGroupId;
    segmentLogger = LoggerFactory.getLogger(LLRealtimeSegmentDataManager.class.getName() + "_" + _segmentNameStr);
    _tableStreamName = _tableNameWithType + "_" + _streamTopic;
    _memoryManager = getMemoryManager(realtimeTableDataManager.getConsumerDir(), _segmentNameStr,
        indexLoadingConfig.isRealtimeOffHeapAllocation(), indexLoadingConfig.isDirectRealtimeOffHeapAllocation(),
        serverMetrics);

    List<String> sortedColumns = indexLoadingConfig.getSortedColumns();
    if (sortedColumns.isEmpty()) {
      segmentLogger.info("RealtimeDataResourceZKMetadata contains no information about sorted column for segment {}",
          _llcSegmentName);
      _sortedColumn = null;
    } else {
      String firstSortedColumn = sortedColumns.get(0);
      if (_schema.hasColumn(firstSortedColumn)) {
        segmentLogger.info("Setting sorted column name: {} from RealtimeDataResourceZKMetadata for segment {}",
            firstSortedColumn, _llcSegmentName);
        _sortedColumn = firstSortedColumn;
      } else {
        segmentLogger
            .warn("Sorted column name: {} from RealtimeDataResourceZKMetadata is not existed in schema for segment {}.",
                firstSortedColumn, _llcSegmentName);
        _sortedColumn = null;
      }
    }

    // Inverted index columns
    Set<String> invertedIndexColumns = indexLoadingConfig.getInvertedIndexColumns();
    // We need to add sorted column into inverted index columns because when we convert realtime in memory segment into
    // offline segment, we use sorted column's inverted index to maintain the order of the records so that the records
    // are sorted on the sorted column.
    if (_sortedColumn != null) {
      invertedIndexColumns.add(_sortedColumn);
    }
    _invertedIndexColumns = new ArrayList<>(invertedIndexColumns);

    // No dictionary Columns
    _noDictionaryColumns = new ArrayList<>(indexLoadingConfig.getNoDictionaryColumns());

    _varLengthDictionaryColumns = new ArrayList<>(indexLoadingConfig.getVarLengthDictionaryColumns());

    // Read the max number of rows
    int segmentMaxRowCount = _partitionLevelStreamConfig.getFlushThresholdRows();
    if (0 < segmentZKMetadata.getSizeThresholdToFlushSegment()) {
      segmentMaxRowCount = segmentZKMetadata.getSizeThresholdToFlushSegment();
    }
    _segmentMaxRowCount = segmentMaxRowCount;

    _isOffHeap = indexLoadingConfig.isRealtimeOffHeapAllocation();

    _nullHandlingEnabled = indexingConfig.isNullHandlingEnabled();

    Set<String> textIndexColumns = indexLoadingConfig.getTextIndexColumns();
    _textIndexColumns = new ArrayList<>(textIndexColumns);

    Set<String> fstIndexColumns = indexLoadingConfig.getFSTIndexColumns();
    _fstIndexColumns = new ArrayList<>(fstIndexColumns);

    // Start new realtime segment
    String consumerDir = realtimeTableDataManager.getConsumerDir();
    RealtimeSegmentConfig.Builder realtimeSegmentConfigBuilder =
        new RealtimeSegmentConfig.Builder().setTableNameWithType(_tableNameWithType).setSegmentName(_segmentNameStr)
            .setStreamName(_streamTopic).setSchema(_schema).setTimeColumnName(timeColumnName)
            .setCapacity(_segmentMaxRowCount).setAvgNumMultiValues(indexLoadingConfig.getRealtimeAvgMultiValueCount())
            .setNoDictionaryColumns(indexLoadingConfig.getNoDictionaryColumns())
            .setVarLengthDictionaryColumns(indexLoadingConfig.getVarLengthDictionaryColumns())
            .setInvertedIndexColumns(invertedIndexColumns).setTextIndexColumns(textIndexColumns)
            .setFSTIndexColumns(fstIndexColumns).setJsonIndexColumns(indexLoadingConfig.getJsonIndexColumns())
            .setH3IndexConfigs(indexLoadingConfig.getH3IndexConfigs()).setRealtimeSegmentZKMetadata(segmentZKMetadata)
            .setOffHeap(_isOffHeap).setMemoryManager(_memoryManager)
            .setStatsHistory(realtimeTableDataManager.getStatsHistory())
            .setAggregateMetrics(indexingConfig.isAggregateMetrics()).setNullHandlingEnabled(_nullHandlingEnabled)
            .setConsumerDir(consumerDir).setUpsertMode(tableConfig.getUpsertMode())
            .setPartitionUpsertMetadataManager(partitionUpsertMetadataManager)
            .setPartitionUpsertMetadataManager(partitionUpsertMetadataManager)
            .setGlobalUpsertStrategy(tableConfig.getUpsertConfig().getGlobalUpsertStrategy())
            .setPartialUpsertStrategy(tableConfig.getUpsertConfig().getPartialUpsertStrategy())
            .setCustomUpsertStrategy(tableConfig.getUpsertConfig().getCustomUpsertStrategy());
    ;

    // Create message decoder
    Set<String> fieldsToRead = IngestionUtils.getFieldsForRecordExtractor(_tableConfig.getIngestionConfig(), _schema);
    _messageDecoder = StreamDecoderProvider.create(_partitionLevelStreamConfig, fieldsToRead);
    _clientId = _streamTopic + "-" + _partitionGroupId;

    // Create record transformer
    _recordTransformer = CompositeTransformer.getDefaultTransformer(tableConfig, schema);

    // Create complex type transformer
    _complexTypeTransformer = ComplexTypeTransformer.getComplexTypeTransformer(tableConfig);

    // Acquire semaphore to create stream consumers
    try {
      _partitionGroupConsumerSemaphore.acquire();
      _acquiredConsumerSemaphore.set(true);
    } catch (InterruptedException e) {
      String errorMsg = "InterruptedException when acquiring the partitionConsumerSemaphore";
      segmentLogger.error(errorMsg);
      throw new RuntimeException(errorMsg + " for segment: " + _segmentNameStr);
    }
    makeStreamConsumer("Starting");
    makeStreamMetadataProvider("Starting");

    SegmentPartitionConfig segmentPartitionConfig = indexingConfig.getSegmentPartitionConfig();
    if (segmentPartitionConfig != null) {
      Map<String, ColumnPartitionConfig> columnPartitionMap = segmentPartitionConfig.getColumnPartitionMap();
      if (columnPartitionMap.size() == 1) {
        Map.Entry<String, ColumnPartitionConfig> entry = columnPartitionMap.entrySet().iterator().next();
        String partitionColumn = entry.getKey();
        ColumnPartitionConfig columnPartitionConfig = entry.getValue();
        String partitionFunctionName = columnPartitionConfig.getFunctionName();

        // NOTE: Here we compare the number of partitions from the config and the stream, and log a warning and emit a
        //       metric when they don't match, but use the one from the stream. The mismatch could happen when the
        //       stream partitions are changed, but the table config has not been updated to reflect the change. In such
        //       case, picking the number of partitions from the stream can keep the segment properly partitioned as
        //       long as the partition function is not changed.
        int numPartitions = columnPartitionConfig.getNumPartitions();
        try {
          // TODO: currentPartitionGroupConsumptionStatus should be fetched from idealState + segmentZkMetadata,
          //  so that we get back accurate partitionGroups info
          //  However this is not an issue for Kafka, since partitionGroups never expire and every partitionGroup has a single partition
          //  Fix this before opening support for partitioning in Kinesis
          int numPartitionGroups = _streamMetadataProvider
              .computePartitionGroupMetadata(_clientId, _partitionLevelStreamConfig,
                  Collections.emptyList(), /*maxWaitTimeMs=*/5000).size();

          if (numPartitionGroups != numPartitions) {
            segmentLogger.warn(
                "Number of stream partitions: {} does not match number of partitions in the partition config: {}, using number of stream partitions",
                numPartitionGroups, numPartitions);
            _serverMetrics.addMeteredTableValue(_tableNameWithType, ServerMeter.REALTIME_PARTITION_MISMATCH, 1);
            numPartitions = numPartitionGroups;
          }
        } catch (Exception e) {
          segmentLogger.warn(
              "Failed to get number of stream partitions in 5s, using number of partitions in the partition config: {}",
              numPartitions, e);
          makeStreamMetadataProvider("Timeout getting number of stream partitions");
        }

        realtimeSegmentConfigBuilder.setPartitionColumn(partitionColumn);
        realtimeSegmentConfigBuilder
            .setPartitionFunction(PartitionFunctionFactory.getPartitionFunction(partitionFunctionName, numPartitions));
        realtimeSegmentConfigBuilder.setPartitionId(_partitionGroupId);
      } else {
        segmentLogger.warn("Cannot partition on multiple columns: {}", columnPartitionMap.keySet());
      }
    }

    _realtimeSegment = new MutableSegmentImpl(realtimeSegmentConfigBuilder.build(), serverMetrics);
    _startOffset = _streamPartitionMsgOffsetFactory.create(_segmentZKMetadata.getStartOffset());
    _currentOffset = _streamPartitionMsgOffsetFactory.create(_startOffset);
    _resourceTmpDir = new File(resourceDataDir, "_tmp");
    if (!_resourceTmpDir.exists()) {
      _resourceTmpDir.mkdirs();
    }
    _state = State.INITIAL_CONSUMING;

    long now = now();
    _consumeStartTime = now;
    long maxConsumeTimeMillis = _partitionLevelStreamConfig.getFlushThresholdTimeMillis();
    _consumeEndTime = segmentZKMetadata.getCreationTime() + maxConsumeTimeMillis;

    // When we restart a server, the consuming segments retain their creationTime (derived from segment
    // metadata), but a couple of corner cases can happen:
    // (1) The server was down for a very long time, and the consuming segment is not yet completed.
    // (2) The consuming segment was just about to be completed, but the server went down.
    // In either of these two cases, if a different replica could not complete the segment, it is possible
    // that we get a value for _consumeEndTime that is in the very near future, or even in the past. In such
    // cases, we let some minimum consumption happen before we attempt to complete the segment (unless, of course
    // the max consumption time has been configured to be less than the minimum time we use in this class).
    long minConsumeTimeMillis =
        Math.min(maxConsumeTimeMillis, TimeUnit.MILLISECONDS.convert(MINIMUM_CONSUME_TIME_MINUTES, TimeUnit.MINUTES));
    if (_consumeEndTime - now < minConsumeTimeMillis) {
      _consumeEndTime = now + minConsumeTimeMillis;
    }

    _segmentCommitterFactory =
        new SegmentCommitterFactory(segmentLogger, _protocolHandler, tableConfig, indexLoadingConfig, serverMetrics);

    segmentLogger
        .info("Starting consumption on realtime consuming segment {} maxRowCount {} maxEndTime {}", _llcSegmentName,
            _segmentMaxRowCount, new DateTime(_consumeEndTime, DateTimeZone.UTC).toString());
    start();
  }

  /**
   * Creates a new stream consumer
   */
  private void makeStreamConsumer(String reason) {
    if (_partitionGroupConsumer != null) {
      closePartitionGroupConsumer();
    }
    segmentLogger.info("Creating new stream consumer, reason: {}", reason);
    _partitionGroupConsumer =
        _streamConsumerFactory.createPartitionGroupConsumer(_clientId, _partitionGroupConsumptionStatus);
  }

  /**
   * Creates a new stream metadata provider
   */
  private void makeStreamMetadataProvider(String reason) {
    if (_streamMetadataProvider != null) {
      closeStreamMetadataProvider();
    }
    segmentLogger.info("Creating new stream metadata provider, reason: {}", reason);
    _streamMetadataProvider = _streamConsumerFactory.createStreamMetadataProvider(_clientId);
  }

  // This should be done during commit? We may not always commit when we build a segment....
  // TODO Call this method when we are loading the segment, which we do from table datamanager afaik
  private void updateCurrentDocumentCountMetrics() {

    // When updating of metrics is enabled, numRowsIndexed can be <= numRowsConsumed. This is because when in this
    // case when a new row with existing dimension combination comes in, we find the existing row and update metrics.

    // Number of rows indexed should be used for DOCUMENT_COUNT metric, and also for segment flush. Whereas,
    // Number of rows consumed should be used for consumption metric.
    long rowsIndexed = _numRowsIndexed - _lastUpdatedRowsIndexed.get();
    _serverMetrics.addValueToTableGauge(_tableNameWithType, ServerGauge.DOCUMENT_COUNT, rowsIndexed);
    _lastUpdatedRowsIndexed.set(_numRowsIndexed);
    final long now = now();
    final int rowsConsumed = _numRowsConsumed - _lastConsumedCount;
    final long prevTime = _lastConsumedCount == 0 ? _consumeStartTime : _lastLogTime;
    // Log every minute or 100k events
    if (now - prevTime > TimeUnit.MINUTES.toMillis(TIME_THRESHOLD_FOR_LOG_MINUTES)
        || rowsConsumed >= MSG_COUNT_THRESHOLD_FOR_LOG) {
      segmentLogger.info(
          "Consumed {} events from (rate:{}/s), currentOffset={}, numRowsConsumedSoFar={}, numRowsIndexedSoFar={}",
          rowsConsumed, (float) (rowsConsumed) * 1000 / (now - prevTime), _currentOffset, _numRowsConsumed,
          _numRowsIndexed);
      _lastConsumedCount = _numRowsConsumed;
      _lastLogTime = now;
    }
  }

  @Override
  public MutableSegment getSegment() {
    return _realtimeSegment;
  }

  @Override
  public String getSegmentName() {
    return _segmentNameStr;
  }
}
