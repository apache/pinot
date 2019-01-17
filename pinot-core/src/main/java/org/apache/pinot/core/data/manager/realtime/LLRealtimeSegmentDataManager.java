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
import com.yammer.metrics.core.Meter;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.Utils;
import org.apache.pinot.common.config.IndexingConfig;
import org.apache.pinot.common.config.SegmentPartitionConfig;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.common.data.StarTreeIndexSpec;
import org.apache.pinot.common.metadata.instance.InstanceZKMetadata;
import org.apache.pinot.common.metadata.segment.LLCRealtimeSegmentZKMetadata;
import org.apache.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.protocols.SegmentCompletionProtocol;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.NetUtil;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.core.data.GenericRow;
import org.apache.pinot.core.data.recordtransformer.CompoundTransformer;
import org.apache.pinot.core.data.recordtransformer.RecordTransformer;
import org.apache.pinot.core.indexsegment.generator.SegmentVersion;
import org.apache.pinot.core.indexsegment.mutable.MutableSegment;
import org.apache.pinot.core.indexsegment.mutable.MutableSegmentImpl;
import org.apache.pinot.core.io.readerwriter.PinotDataBufferMemoryManager;
import org.apache.pinot.core.realtime.converter.RealtimeSegmentConverter;
import org.apache.pinot.core.realtime.impl.RealtimeSegmentConfig;
import org.apache.pinot.core.realtime.stream.MessageBatch;
import org.apache.pinot.core.realtime.stream.PartitionLevelConsumer;
import org.apache.pinot.core.realtime.stream.PartitionLevelStreamConfig;
import org.apache.pinot.core.realtime.stream.PermanentConsumerException;
import org.apache.pinot.core.realtime.stream.StreamConsumerFactory;
import org.apache.pinot.core.realtime.stream.StreamConsumerFactoryProvider;
import org.apache.pinot.core.realtime.stream.StreamDecoderProvider;
import org.apache.pinot.core.realtime.stream.StreamMessageDecoder;
import org.apache.pinot.core.realtime.stream.StreamMetadataProvider;
import org.apache.pinot.core.realtime.stream.TransientConsumerException;
import org.apache.pinot.core.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.server.realtime.ServerSegmentCompletionProtocolHandler;
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

  protected class SegmentBuildDescriptor {
    final String _segmentTarFilePath;
    final long _offset;
    final long _waitTimeMillis;
    final long _buildTimeMillis;
    final String _segmentDirPath;
    final long _segmentSizeBytes;
    SegmentBuildDescriptor(String segmentTarFilePath, long offset, String segmentDirPath, long buildTimeMillis,
        long waitTimeMillis, long segmentSizeBytes) {
      _segmentTarFilePath = segmentTarFilePath;
      _offset = offset;
      _buildTimeMillis = buildTimeMillis;
      _waitTimeMillis = waitTimeMillis;
      _segmentDirPath = segmentDirPath;
      _segmentSizeBytes = segmentSizeBytes;
    }

    public long getOffset() {
      return _offset;
    }

    public long getBuildTimeMillis() {
      return _buildTimeMillis;
    }

    public long getWaitTimeMillis() {
      return _waitTimeMillis;
    }

    public String getSegmentTarFilePath() {
      return _segmentTarFilePath;
    }

    public long getSegmentSizeBytes() {
      return _segmentSizeBytes;
    }

    public void deleteSegmentFile() {
      // If segment build fails with an exception then we will not be able to create a segment file and
      // the file name will be null.
      if (_segmentTarFilePath != null) {
        FileUtils.deleteQuietly(new File(_segmentTarFilePath));
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
  private final String _metricKeyName;
  private final ServerMetrics _serverMetrics;
  private final MutableSegmentImpl _realtimeSegment;
  private volatile long _currentOffset;
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
  private StreamConsumerFactory _streamConsumerFactory;

  // Segment end criteria
  private volatile long _consumeEndTime = 0;
  private volatile long _finalOffset = -1;
  private volatile boolean _shouldStop = false;

  // It takes 30s to locate controller leader, and more if there are multiple controller failures.
  // For now, we let 31s pass for this state transition.
  private static final int MAX_TIME_FOR_CONSUMING_TO_ONLINE_IN_SECONDS = 31;

  private Thread _consumerThread;
  private final String _streamTopic;
  private final int _streamPartitionId;
  final String _clientId;
  private final LLCSegmentName _segmentName;
  private final RecordTransformer _recordTransformer;
  private PartitionLevelConsumer _partitionLevelConsumer = null;
  private StreamMetadataProvider _streamMetadataProvider = null;
  private final File _resourceTmpDir;
  private final String _tableName;
  private final String _timeColumnName;
  private final List<String> _invertedIndexColumns;
  private final List<String> _noDictionaryColumns;
  private final StarTreeIndexSpec _starTreeIndexSpec;
  private final String _sortedColumn;
  private Logger segmentLogger;
  private final String _tableStreamName;
  private final PinotDataBufferMemoryManager _memoryManager;
  private AtomicLong _lastUpdatedRowsIndexed = new AtomicLong(0);
  private final String _instanceId;
  private final ServerSegmentCompletionProtocolHandler _protocolHandler;
  private final long _consumeStartTime;
  private final long _startOffset;
  private final PartitionLevelStreamConfig _partitionLevelStreamConfig;

  private long _lastLogTime = 0;
  private int _lastConsumedCount = 0;
  private String _stopReason = null;
  private final Semaphore _segBuildSemaphore;
  private final boolean _isOffHeap;


  // TODO each time this method is called, we print reason for stop. Good to print only once.
  private boolean endCriteriaReached() {
    Preconditions.checkState(_state.shouldConsume(), "Incorrect state %s", _state);
    long now = now();
    switch(_state) {
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
          segmentLogger.info(
              "Stopping consumption due to time limit start={} now={} numRowsConsumed={} numRowsIndexed={}",
              _startTimeMs, now, _numRowsConsumed, _numRowsIndexed);
          _stopReason = SegmentCompletionProtocol.REASON_TIME_LIMIT;
          return true;
        } else if (_numRowsIndexed >= _segmentMaxRowCount) {
          segmentLogger.info("Stopping consumption due to row limit nRows={} numRowsIndexed={}, numRowsConsumed={}",
              _numRowsIndexed, _numRowsConsumed, _segmentMaxRowCount);
          _stopReason = SegmentCompletionProtocol.REASON_ROW_LIMIT;
          return true;
        }
        return false;

      case CATCHING_UP:
        _stopReason = null;
        // We have posted segmentConsumed() at least once, and the controller is asking us to catch up to a certain offset.
        // There is no time limit here, so just check to see that we are still within the offset we need to reach.
        // Going past the offset is an exception.
        if (_currentOffset == _finalOffset) {
          segmentLogger.info("Caught up to offset={}, state={}", _finalOffset, _state.toString());
          return true;
        }
        if (_currentOffset > _finalOffset) {
          segmentLogger.error("Offset higher in state={}, current={}, final={}", _state.toString(), _currentOffset, _finalOffset);
          throw new RuntimeException("Past max offset");
        }
        return false;

      case CONSUMING_TO_ONLINE:
        // We are attempting to go from CONSUMING to ONLINE state. We are making a last attempt to catch up to the
        // target offset. We have a time constraint, and need to stop consuming if we cannot get to the target offset
        // within that time.
        if (_currentOffset == _finalOffset) {
          segmentLogger.info("Caught up to offset={}, state={}", _finalOffset, _state.toString());
          return true;
        } else if (now >= _consumeEndTime) {
          segmentLogger.info("Past max time budget: offset={}, state={}", _currentOffset, _state.toString());
          return true;
        }
        if (_currentOffset > _finalOffset) {
          segmentLogger.error("Offset higher in state={}, current={}, final={}", _state.toString(), _currentOffset, _finalOffset);
          throw new RuntimeException("Past max offset");
        }
        return false;
      default:
        segmentLogger.error("Illegal state {}" + _state.toString());
        throw new RuntimeException("Illegal state to consume");
    }
  }

  private void handleTransientStreamErrors(Exception e) throws Exception {
    consecutiveErrorCount++;
    if (consecutiveErrorCount > MAX_CONSECUTIVE_ERROR_COUNT) {
      segmentLogger.warn("Stream transient exception when fetching messages, stopping consumption after {} attempts", consecutiveErrorCount, e);
      throw e;
    } else {
      segmentLogger.warn("Stream transient exception when fetching messages, retrying (count={})", consecutiveErrorCount, e);
      Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
      makeStreamConsumer("Too many transient errors");
    }
  }

  protected boolean consumeLoop() throws Exception {
    _numRowsErrored = 0;
    final long idlePipeSleepTimeMillis = 100;
    final long maxIdleCountBeforeStatUpdate = (3 * 60 * 1000)/(idlePipeSleepTimeMillis + _partitionLevelStreamConfig.getFetchTimeoutMillis());  // 3 minute count
    long lastUpdatedOffset = _currentOffset;  // so that we always update the metric when we enter this method.
    long idleCount = 0;
    // At this point, we know that we can potentially move the offset, so the old saved segment file is not valid
    // anymore. Remove the file if it exists.
    removeSegmentFile();

    final long _endOffset = Long.MAX_VALUE; // No upper limit on stream offset
    segmentLogger.info("Starting consumption loop start offset {}, finalOffset {}", _currentOffset, _finalOffset);
    while(!_shouldStop && !endCriteriaReached()) {
      // Consume for the next readTime ms, or we get to final offset, whichever happens earlier,
      // Update _currentOffset upon return from this method
      MessageBatch messageBatch;
      try {
        messageBatch = _partitionLevelConsumer.fetchMessages(_currentOffset, _endOffset,
            _partitionLevelStreamConfig.getFetchTimeoutMillis());
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

      if (_currentOffset != lastUpdatedOffset) {
        // We consumed something. Update the highest stream offset as well as partition-consuming metric.
        _serverMetrics.setValueOfTableGauge(_metricKeyName, ServerGauge.HIGHEST_KAFKA_OFFSET_CONSUMED, _currentOffset);
        _serverMetrics.setValueOfTableGauge(_metricKeyName, ServerGauge.HIGHEST_STREAM_OFFSET_CONSUMED, _currentOffset);
        _serverMetrics.setValueOfTableGauge(_metricKeyName, ServerGauge.LLC_PARTITION_CONSUMING, 1);
        lastUpdatedOffset = _currentOffset;
      } else {
        // We did not consume any rows. Update the partition-consuming metric only if we have been idling for a long time.
        // Create a new stream consumer wrapper, in case we are stuck on something.
        if (++idleCount > maxIdleCountBeforeStatUpdate) {
          _serverMetrics.setValueOfTableGauge(_metricKeyName, ServerGauge.LLC_PARTITION_CONSUMING, 1);
          idleCount = 0;
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
    Meter realtimeRowsConsumedMeter = null;
    Meter realtimeRowsDroppedMeter = null;

    int indexedMessageCount = 0;
    int streamMessageCount = 0;
    boolean canTakeMore = true;
    GenericRow decodedRow = null;
    for (int index = 0; index < messagesAndOffsets.getMessageCount(); index ++) {
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
        segmentLogger.error("Buffer full with {} rows consumed (row limit {}, indexed {})", _numRowsConsumed,
            _numRowsIndexed, _segmentMaxRowCount);
        throw new RuntimeException("Realtime segment full");
      }

      // Index each message
      decodedRow = GenericRow.createOrReuseRow(decodedRow);

      decodedRow = _messageDecoder
          .decode(messagesAndOffsets.getMessageAtIndex(index), messagesAndOffsets.getMessageOffsetAtIndex(index),
              messagesAndOffsets.getMessageLengthAtIndex(index), decodedRow);

      if (decodedRow != null) {
        try {
          GenericRow transformedRow = _recordTransformer.transform(decodedRow);

          if (transformedRow != null) {
            realtimeRowsConsumedMeter =
                _serverMetrics.addMeteredTableValue(_metricKeyName, ServerMeter.REALTIME_ROWS_CONSUMED, 1,
                    realtimeRowsConsumedMeter);
            indexedMessageCount++;
          } else {
            realtimeRowsDroppedMeter =
                _serverMetrics.addMeteredTableValue(_metricKeyName, ServerMeter.INVALID_REALTIME_ROWS_DROPPED, 1,
                    realtimeRowsDroppedMeter);
          }

          canTakeMore = _realtimeSegment.index(transformedRow);
        } catch (Exception e) {
          segmentLogger.debug("Caught exception while transforming the record: {}", decodedRow, e);
          _numRowsErrored++;
        }
      } else {
        realtimeRowsDroppedMeter =
            _serverMetrics.addMeteredTableValue(_metricKeyName, ServerMeter.INVALID_REALTIME_ROWS_DROPPED, 1,
                realtimeRowsDroppedMeter);
      }

      _currentOffset = messagesAndOffsets.getNextStreamMessageOffsetAtIndex(index);
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
            _serverMetrics.setValueOfTableGauge(_metricKeyName,
                ServerGauge.LAST_REALTIME_SEGMENT_CATCHUP_DURATION_SECONDS,
                TimeUnit.MILLISECONDS.toSeconds(catchUpTimeMillis));
          }

          // If we are sending segmentConsumed() to the controller, we are in HOLDING state.
          _state = State.HOLDING;
          SegmentCompletionProtocol.Response response = postSegmentConsumedMsg();
          SegmentCompletionProtocol.ControllerResponseStatus status = response.getStatus();
          long rspOffset = response.getOffset();
          boolean success;
          switch (status) {
            case NOT_LEADER:
              // Retain the same state
              segmentLogger.warn("Got not leader response");
              hold();
              break;
            case CATCH_UP:
              if (rspOffset <= _currentOffset) {
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
              success = buildSegmentAndReplace();
              if (success) {
                _state = State.RETAINED;
              } else {
                // Could not build segment for some reason. We can only download it.
                _state = State.ERROR;
              }
              break;
            case COMMIT:
              _state = State.COMMITTING;
              long buildTimeSeconds = response.getBuildTimeSeconds();
              buildSegmentForCommit(buildTimeSeconds * 1000L);
              if (_segmentBuildDescriptor == null) {
                // We could not build the segment. Go into error state.
                _state = State.ERROR;
              } else {
                success = commitSegment(response);
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
        segmentLogger.error("Exception while in work", e);
        postStopConsumedMsg(e.getClass().getName());
        _state = State.ERROR;
        _serverMetrics.setValueOfTableGauge(_metricKeyName, ServerGauge.LLC_PARTITION_CONSUMING, 0);
        return;
      }

      removeSegmentFile();

      if (initialConsumptionEnd != 0L) {
        _serverMetrics.setValueOfTableGauge(_metricKeyName,
            ServerGauge.LAST_REALTIME_SEGMENT_COMPLETION_DURATION_SECONDS,
            TimeUnit.MILLISECONDS.toSeconds(now() - initialConsumptionEnd));
      }
      _serverMetrics.setValueOfTableGauge(_metricKeyName, ServerGauge.LLC_PARTITION_CONSUMING, 0);
    }
  }

  private File makeSegmentDirPath() {
    return new File(_resourceDataDir, _segmentZKMetadata.getSegmentName());
  }

  // Side effect: Modifies _segmentBuildDescriptor if we do not have a valid built segment file and we
  // built the segment successfully.
  protected void buildSegmentForCommit(long buildTimeLeaseMs) {
    try {
      if (_segmentBuildDescriptor != null && _segmentBuildDescriptor.getOffset() == _currentOffset) {
        // Double-check that we have the file, just in case.
        String segTarFile = _segmentBuildDescriptor.getSegmentTarFilePath();
        if (new File(segTarFile).exists()) {
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

  @VisibleForTesting
  protected long getCurrentOffset() {
    return _currentOffset;
  }

  @VisibleForTesting
  protected SegmentBuildDescriptor getSegmentBuildDescriptor() {
    return _segmentBuildDescriptor;
  }

  protected SegmentBuildDescriptor buildSegmentInternal(boolean forCommit) {
    try {
      final long startTimeMillis = now();
      if (_segBuildSemaphore != null) {
        segmentLogger.info("Waiting to acquire semaphore for building segment");
        _segBuildSemaphore.acquire();
      }
      // Increment llc simultaneous segment builds.
      _serverMetrics.addValueToGlobalGauge(ServerGauge.LLC_SIMULTANEOUS_SEGMENT_BUILDS, 1L);

      final long lockAquireTimeMillis = now();
      // Build a segment from in-memory rows.If buildTgz is true, then build the tar.gz file as well
      // TODO Use an auto-closeable object to delete temp resources.
      File tempSegmentFolder = new File(_resourceTmpDir, "tmp-" + _segmentNameStr + "-" + String.valueOf(now()));
      // lets convert the segment now
      RealtimeSegmentConverter converter =
          new RealtimeSegmentConverter(_realtimeSegment, tempSegmentFolder.getAbsolutePath(), _schema,
              _segmentZKMetadata.getTableName(), _timeColumnName, _segmentZKMetadata.getSegmentName(), _sortedColumn,
              _invertedIndexColumns, _noDictionaryColumns, _starTreeIndexSpec);
      segmentLogger.info("Trying to build segment");
      try {
        converter.build(_segmentVersion, _serverMetrics);
      } catch (Exception e) {
        segmentLogger.error("Could not build segment", e);
        FileUtils.deleteQuietly(tempSegmentFolder);
        return null;
      }
      final long buildTimeMillis = now() - lockAquireTimeMillis;
      final long waitTimeMillis = lockAquireTimeMillis - startTimeMillis;
      segmentLogger.info("Successfully built segment in {} ms, after lockWaitTime {} ms",
          buildTimeMillis, waitTimeMillis);
      File destDir = makeSegmentDirPath();
      FileUtils.deleteQuietly(destDir);
      try {
        FileUtils.moveDirectory(tempSegmentFolder.listFiles()[0], destDir);
        if (forCommit) {
          TarGzCompressionUtils.createTarGzOfDirectory(destDir.getAbsolutePath());
        }
      } catch (IOException e) {
        segmentLogger.error("Exception during move/tar segment", e);
        FileUtils.deleteQuietly(tempSegmentFolder);
        return null;
      }
      final long segmentSizeBytes = FileUtils.sizeOfDirectory(destDir);
      FileUtils.deleteQuietly(tempSegmentFolder);

      _serverMetrics.setValueOfTableGauge(_metricKeyName, ServerGauge.LAST_REALTIME_SEGMENT_CREATION_DURATION_SECONDS,
          TimeUnit.MILLISECONDS.toSeconds(buildTimeMillis));
      _serverMetrics.setValueOfTableGauge(_metricKeyName, ServerGauge.LAST_REALTIME_SEGMENT_CREATION_WAIT_TIME_SECONDS,
          TimeUnit.MILLISECONDS.toSeconds(waitTimeMillis));

      if (forCommit) {
        return new SegmentBuildDescriptor(
            destDir.getAbsolutePath() + TarGzCompressionUtils.TAR_GZ_FILE_EXTENSION, _currentOffset,
            null, buildTimeMillis, waitTimeMillis, segmentSizeBytes);
      }
      return new SegmentBuildDescriptor( null, _currentOffset, destDir.getAbsolutePath(),
          buildTimeMillis, waitTimeMillis, segmentSizeBytes);
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

  private SegmentCompletionProtocol.Response doSplitCommit(SegmentCompletionProtocol.Response prevResponse) {
    final File segmentTarFile = new File(_segmentBuildDescriptor.getSegmentTarFilePath());
    SegmentCompletionProtocol.Request.Params params = new SegmentCompletionProtocol.Request.Params();

    params.withSegmentName(_segmentNameStr).withOffset(_currentOffset).withNumRows(_numRowsConsumed)
        .withInstanceId(_instanceId)
        .withBuildTimeMillis(_segmentBuildDescriptor.getBuildTimeMillis())
        .withSegmentSizeBytes(_segmentBuildDescriptor.getSegmentSizeBytes())
        .withWaitTimeMillis(_segmentBuildDescriptor.getWaitTimeMillis());
    if (_isOffHeap) {
      params.withMemoryUsedBytes(_memoryManager.getTotalAllocatedBytes());
    }
    SegmentCompletionProtocol.Response segmentCommitStartResponse = _protocolHandler.segmentCommitStart(params);
    if (!segmentCommitStartResponse.getStatus().equals(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_CONTINUE)) {
      segmentLogger.warn("CommitStart failed  with response {}", segmentCommitStartResponse.toJsonString());
      return SegmentCompletionProtocol.RESP_FAILED;
    }

    params = new SegmentCompletionProtocol.Request.Params();
    params.withOffset(_currentOffset).withSegmentName(_segmentNameStr).withInstanceId(_instanceId);
    SegmentCompletionProtocol.Response segmentCommitUploadResponse = _protocolHandler.segmentCommitUpload(params,
        segmentTarFile, prevResponse.getControllerVipUrl());
    if (!segmentCommitUploadResponse.getStatus().equals(SegmentCompletionProtocol.ControllerResponseStatus.UPLOAD_SUCCESS)) {
      segmentLogger.warn("Segment upload failed  with response {}", segmentCommitUploadResponse.toJsonString());
      return SegmentCompletionProtocol.RESP_FAILED;
    }

    params = new SegmentCompletionProtocol.Request.Params();
    params.withInstanceId(_instanceId).withOffset(_currentOffset).withSegmentName(_segmentNameStr)
        .withSegmentLocation(segmentCommitUploadResponse.getSegmentLocation())
        .withNumRows(_numRowsConsumed)
        .withBuildTimeMillis(_segmentBuildDescriptor.getBuildTimeMillis())
        .withSegmentSizeBytes(_segmentBuildDescriptor.getSegmentSizeBytes())
        .withWaitTimeMillis(_segmentBuildDescriptor.getWaitTimeMillis());
    if (_isOffHeap) {
      params.withMemoryUsedBytes(_memoryManager.getTotalAllocatedBytes());
    }
    SegmentCompletionProtocol.Response commitEndResponse =  _protocolHandler.segmentCommitEnd(params);
    if (!commitEndResponse.getStatus().equals(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_SUCCESS))  {
      segmentLogger.warn("CommitEnd failed  with response {}", commitEndResponse.toJsonString());
      return SegmentCompletionProtocol.RESP_FAILED;
    }
    return commitEndResponse;
  }

  protected boolean commitSegment(SegmentCompletionProtocol.Response response) {
    final String segTarFileName = _segmentBuildDescriptor.getSegmentTarFilePath();
    File segTarFile = new File(segTarFileName);
    if (!segTarFile.exists()) {
      throw new RuntimeException("Segment file does not exist:" + segTarFileName);
    }
    SegmentCompletionProtocol.Response returnedResponse;
    if (response.isSplitCommit() && _indexLoadingConfig.isEnableSplitCommit()) {
      // Send segmentStart, segmentUpload, & segmentCommitEnd to the controller
      // if that succeeds, swap in-memory segment with the one built.
      returnedResponse = doSplitCommit(response);
    } else {
      // Send segmentCommit() to the controller
      // if that succeeds, swap in-memory segment with the one built.
      returnedResponse = postSegmentCommitMsg();
    }

    if (!returnedResponse.getStatus().equals(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_SUCCESS)) {
      return false;
    }

    _realtimeTableDataManager.replaceLLSegment(_segmentNameStr, _indexLoadingConfig);
    removeSegmentFile();
    return true;
  }

  protected SegmentCompletionProtocol.Response postSegmentCommitMsg() {
    final File segmentTarFile = new File(_segmentBuildDescriptor.getSegmentTarFilePath());
    SegmentCompletionProtocol.Request.Params params = new SegmentCompletionProtocol.Request.Params();
    params.withInstanceId(_instanceId).withOffset(_currentOffset).withSegmentName(_segmentNameStr)
        .withNumRows(_numRowsConsumed)
        .withInstanceId(_instanceId).withBuildTimeMillis(_segmentBuildDescriptor.getBuildTimeMillis())
        .withSegmentSizeBytes(_segmentBuildDescriptor.getSegmentSizeBytes())
        .withWaitTimeMillis(_segmentBuildDescriptor.getWaitTimeMillis());
    if (_isOffHeap) {
      params.withMemoryUsedBytes(_memoryManager.getTotalAllocatedBytes());
    }
    SegmentCompletionProtocol.Response response = _protocolHandler.segmentCommit(params, segmentTarFile);
    if (!response.getStatus().equals(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_SUCCESS)) {
      segmentLogger.warn("Commit failed  with response {}", response.toJsonString());
    }
    return response;
  }

  protected boolean buildSegmentAndReplace() {
    SegmentBuildDescriptor descriptor = buildSegmentInternal(false);
    if (descriptor == null) {
      return false;
    }

    _realtimeTableDataManager.replaceLLSegment(_segmentNameStr, _indexLoadingConfig);
    return true;
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
      params.withOffset(_currentOffset).withReason(reason).withSegmentName(_segmentNameStr).withInstanceId(_instanceId);

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
    params.withOffset(_currentOffset).withSegmentName(_segmentNameStr).withReason(_stopReason)
      .withNumRows(_numRowsConsumed)
      .withInstanceId(_instanceId);
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

  public void goOnlineFromConsuming(RealtimeSegmentZKMetadata metadata) throws InterruptedException {
    _serverMetrics.setValueOfTableGauge(_metricKeyName, ServerGauge.LLC_PARTITION_CONSUMING, 0);
    try {
      LLCRealtimeSegmentZKMetadata llcMetadata = (LLCRealtimeSegmentZKMetadata)metadata;
      // Remove the segment file before we do anything else.
      removeSegmentFile();
      _leaseExtender.removeSegment(_segmentNameStr);
      final long endOffset = llcMetadata.getEndOffset();
      segmentLogger.info("State: {}, transitioning from CONSUMING to ONLINE (startOffset: {}, endOffset: {})", _state.toString(), _startOffset, endOffset);
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
          // Allow to catch up upto final offset, and then replace.
          if (_currentOffset > endOffset) {
            // We moved ahead of the offset that is committed in ZK.
            segmentLogger.warn("Current offset {} ahead of the offset in zk {}. Downloading to replace", _currentOffset,
                endOffset);
            downloadSegmentAndReplace(llcMetadata);
          } else if (_currentOffset == endOffset) {
            segmentLogger.info("Current offset {} matches offset in zk {}. Replacing segment", _currentOffset, endOffset);
            buildSegmentAndReplace();
          } else {
            segmentLogger.info("Attempting to catch up from offset {} to {} ", _currentOffset, endOffset);
            boolean success = catchupToFinalOffset(endOffset,
                TimeUnit.MILLISECONDS.convert(MAX_TIME_FOR_CONSUMING_TO_ONLINE_IN_SECONDS, TimeUnit.SECONDS));
            if (success) {
              segmentLogger.info("Caught up to offset {}", _currentOffset);
              buildSegmentAndReplace();
            } else {
              segmentLogger.info("Could not catch up to offset (current = {}). Downloading to replace", _currentOffset);
              downloadSegmentAndReplace(llcMetadata);
            }
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
    _realtimeTableDataManager.downloadAndReplaceSegment(_segmentNameStr, metadata, _indexLoadingConfig);
  }

  protected long now() {
    return System.currentTimeMillis();
  }

  private boolean catchupToFinalOffset(long endOffset, long timeoutMs) {
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
    if (_currentOffset != endOffset) {
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
    try {
      _partitionLevelConsumer.close();
    } catch (Exception e) {
      segmentLogger.warn("Could not close stream consumer", e);
    }
    try {
      _streamMetadataProvider.close();
    } catch (Exception e) {
      segmentLogger.warn("Could not close stream metadata provider", e);
    }
  }

  protected void start() {
    _consumerThread = new Thread(new PartitionConsumer(), _segmentNameStr);
    segmentLogger.info("Created new consumer thread {} for {}", _consumerThread, this.toString());
    _consumerThread.start();
  }

  /**
   * Stop the consuming thread.
   */
  public void stop() throws InterruptedException {
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
      InstanceZKMetadata instanceZKMetadata, RealtimeTableDataManager realtimeTableDataManager, String resourceDataDir,
      IndexLoadingConfig indexLoadingConfig, Schema schema, ServerMetrics serverMetrics)
      throws Exception {
    _segBuildSemaphore = realtimeTableDataManager.getSegmentBuildSemaphore();
    _segmentZKMetadata = (LLCRealtimeSegmentZKMetadata) segmentZKMetadata;
    _tableConfig = tableConfig;
    _realtimeTableDataManager = realtimeTableDataManager;
    _resourceDataDir = resourceDataDir;
    _indexLoadingConfig = indexLoadingConfig;
    _schema = schema;
    _serverMetrics = serverMetrics;
    _segmentVersion = indexLoadingConfig.getSegmentVersion();
    _instanceId = _realtimeTableDataManager.getServerInstance();
    _leaseExtender = SegmentBuildTimeLeaseExtender.getLeaseExtender(_instanceId);
    _protocolHandler = new ServerSegmentCompletionProtocolHandler(_serverMetrics);

    // TODO Validate configs
    IndexingConfig indexingConfig = _tableConfig.getIndexingConfig();
    _partitionLevelStreamConfig = new PartitionLevelStreamConfig(indexingConfig.getStreamConfigs());
    _streamConsumerFactory = StreamConsumerFactoryProvider.create(_partitionLevelStreamConfig);
    _streamTopic = _partitionLevelStreamConfig.getTopicName();
    _segmentNameStr = _segmentZKMetadata.getSegmentName();
    _segmentName = new LLCSegmentName(_segmentNameStr);
    _streamPartitionId = _segmentName.getPartitionId();
    _tableName = _tableConfig.getTableName();
    _timeColumnName = tableConfig.getValidationConfig().getTimeColumnName();
    _metricKeyName = _tableName + "-" + _streamTopic + "-" + _streamPartitionId;
    segmentLogger = LoggerFactory.getLogger(LLRealtimeSegmentDataManager.class.getName() +
        "_" + _segmentNameStr);
    _tableStreamName = _tableName + "_" + _streamTopic;
    _memoryManager = getMemoryManager(realtimeTableDataManager.getConsumerDir(), _segmentNameStr,
        indexLoadingConfig.isRealtimeOffheapAllocation(), indexLoadingConfig.isDirectRealtimeOffheapAllocation(),
        serverMetrics);

    List<String> sortedColumns = indexLoadingConfig.getSortedColumns();
    if (sortedColumns.isEmpty()) {
      segmentLogger.info("RealtimeDataResourceZKMetadata contains no information about sorted column for segment {}",
          _segmentName);
      _sortedColumn = null;
    } else {
      String firstSortedColumn = sortedColumns.get(0);
      if (_schema.hasColumn(firstSortedColumn)) {
        segmentLogger.info("Setting sorted column name: {} from RealtimeDataResourceZKMetadata for segment {}",
            firstSortedColumn, _segmentName);
        _sortedColumn = firstSortedColumn;
      } else {
        segmentLogger.warn(
            "Sorted column name: {} from RealtimeDataResourceZKMetadata is not existed in schema for segment {}.",
            firstSortedColumn, _segmentName);
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

    // Read the star tree config
    _starTreeIndexSpec = indexingConfig.getStarTreeIndexSpec();

    // Read the max number of rows
    int segmentMaxRowCount = _partitionLevelStreamConfig.getFlushThresholdRows();
    if (0 < segmentZKMetadata.getSizeThresholdToFlushSegment()) {
      segmentMaxRowCount = segmentZKMetadata.getSizeThresholdToFlushSegment();
    }
    _segmentMaxRowCount = segmentMaxRowCount;

    _isOffHeap = indexLoadingConfig.isRealtimeOffheapAllocation();

    // Start new realtime segment
    RealtimeSegmentConfig.Builder realtimeSegmentConfigBuilder =
        new RealtimeSegmentConfig.Builder().setSegmentName(_segmentNameStr)
            .setStreamName(_streamTopic)
            .setSchema(schema)
            .setCapacity(_segmentMaxRowCount)
            .setAvgNumMultiValues(indexLoadingConfig.getRealtimeAvgMultiValueCount())
            .setNoDictionaryColumns(indexLoadingConfig.getNoDictionaryColumns())
            .setInvertedIndexColumns(invertedIndexColumns)
            .setRealtimeSegmentZKMetadata(segmentZKMetadata)
            .setOffHeap(_isOffHeap)
            .setMemoryManager(_memoryManager)
            .setStatsHistory(realtimeTableDataManager.getStatsHistory())
            .setAggregateMetrics(indexingConfig.isAggregateMetrics());

    // Create message decoder
    _messageDecoder = StreamDecoderProvider.create(_partitionLevelStreamConfig, _schema);
    _clientId = _streamPartitionId + "-" + NetUtil.getHostnameOrAddress();

    // Create record transformer
    _recordTransformer = CompoundTransformer.getDefaultTransformer(schema);
    makeStreamConsumer("Starting");
    makeStreamMetadataProvider("Starting");

    SegmentPartitionConfig segmentPartitionConfig = indexingConfig.getSegmentPartitionConfig();
    if (segmentPartitionConfig != null) {
      try {
        int nPartitions = _streamMetadataProvider.fetchPartitionCount(/*maxWaitTimeMs=*/5000L);
        segmentPartitionConfig.setNumPartitions(nPartitions);
        realtimeSegmentConfigBuilder.setSegmentPartitionConfig(segmentPartitionConfig);
      } catch (Exception e) {
        segmentLogger.warn("Couldn't get number of partitions in 5s, not using partition config {}", e.getMessage());
        makeStreamMetadataProvider("Timeout getting number of partitions");
      }
    }

    _realtimeSegment = new MutableSegmentImpl(realtimeSegmentConfigBuilder.build());
    _startOffset = _segmentZKMetadata.getStartOffset();
    _currentOffset = _startOffset;
    _resourceTmpDir = new File(resourceDataDir, "_tmp");
    if (!_resourceTmpDir.exists()) {
      _resourceTmpDir.mkdirs();
    }
    _state = State.INITIAL_CONSUMING;

    long now = now();
    _consumeStartTime = now;
    _consumeEndTime = now + _partitionLevelStreamConfig.getFlushThresholdTimeMillis();

    segmentLogger.info("Starting consumption on realtime consuming segment {} maxRowCount {} maxEndTime {}",
        _segmentName, _segmentMaxRowCount, new DateTime(_consumeEndTime, DateTimeZone.UTC).toString());
    start();
  }

  /**
   * Creates a new stream consumer
   * @param reason
   */
  private void makeStreamConsumer(String reason) {
    if (_partitionLevelConsumer != null) {
      try {
        _partitionLevelConsumer.close();
      } catch (Exception e) {
        segmentLogger.warn("Could not close stream consumer");
      }
    }
    segmentLogger.info("Creating new stream consumer, reason: {}", reason);
    _partitionLevelConsumer = _streamConsumerFactory.createPartitionLevelConsumer(_clientId, _streamPartitionId);
  }

  /**
   * Creates a new stream metadata provider
   * @param reason
   */
  private void makeStreamMetadataProvider(String reason) {
    if (_streamMetadataProvider != null) {
      try {
        _streamMetadataProvider.close();
      } catch (Exception e) {
        segmentLogger.warn("Could not close stream metadata provider");
      }
    }
    segmentLogger.info("Creating new stream metadata provider, reason: {}", reason);
    _streamMetadataProvider = _streamConsumerFactory.createPartitionMetadataProvider(_clientId, _streamPartitionId);
  }

  // This should be done during commit? We may not always commit when we build a segment....
  // TODO Call this method when we are loading the segment, which we do from table datamanager afaik
  private void updateCurrentDocumentCountMetrics() {

    // When updating of metrics is enabled, numRowsIndexed can be <= numRowsConsumed. This is because when in this
    // case when a new row with existing dimension combination comes in, we find the existing row and update metrics.

    // Number of rows indexed should be used for DOCUMENT_COUNT metric, and also for segment flush. Whereas,
    // Number of rows consumed should be used for consumption metric.
    long rowsIndexed = _numRowsIndexed - _lastUpdatedRowsIndexed.get();
    _serverMetrics.addValueToTableGauge(_tableName, ServerGauge.DOCUMENT_COUNT,
        rowsIndexed);
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
