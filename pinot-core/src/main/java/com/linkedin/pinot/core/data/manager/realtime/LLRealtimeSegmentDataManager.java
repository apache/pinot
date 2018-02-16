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

package com.linkedin.pinot.core.data.manager.realtime;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Uninterruptibles;
import com.linkedin.pinot.common.config.IndexingConfig;
import com.linkedin.pinot.common.config.SegmentPartitionConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.StarTreeIndexSpec;
import com.linkedin.pinot.common.metadata.instance.InstanceZKMetadata;
import com.linkedin.pinot.common.metadata.segment.LLCRealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.stream.KafkaStreamMetadata;
import com.linkedin.pinot.common.metrics.ServerGauge;
import com.linkedin.pinot.common.metrics.ServerMeter;
import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.common.protocols.SegmentCompletionProtocol;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.LLCSegmentName;
import com.linkedin.pinot.common.utils.NetUtil;
import com.linkedin.pinot.common.utils.TarGzCompressionUtils;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.extractors.FieldExtractorFactory;
import com.linkedin.pinot.core.data.extractors.PlainFieldExtractor;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.io.readerwriter.PinotDataBufferMemoryManager;
import com.linkedin.pinot.core.realtime.converter.RealtimeSegmentConverter;
import com.linkedin.pinot.core.realtime.impl.RealtimeSegmentConfig;
import com.linkedin.pinot.core.realtime.impl.RealtimeSegmentImpl;
import com.linkedin.pinot.core.realtime.impl.kafka.KafkaLowLevelStreamProviderConfig;
import com.linkedin.pinot.core.realtime.impl.kafka.KafkaMessageDecoder;
import com.linkedin.pinot.core.realtime.impl.kafka.MessageBatch;
import com.linkedin.pinot.core.realtime.impl.kafka.PinotKafkaConsumer;
import com.linkedin.pinot.core.realtime.impl.kafka.PinotKafkaConsumerFactory;
import com.linkedin.pinot.core.realtime.impl.kafka.SimpleConsumerWrapper;
import com.linkedin.pinot.core.segment.index.loader.IndexLoadingConfig;
import com.linkedin.pinot.server.realtime.ServerSegmentCompletionProtocolHandler;
import com.yammer.metrics.core.Meter;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Segment data manager for low level consumer realtime segments, which manages consumption and segment completion.
 */
public class LLRealtimeSegmentDataManager extends RealtimeSegmentDataManager {
  protected enum State {
    // The state machine starts off with this state. While in this state we consume kafka events
    // and index them in memory. We continue to be in this state until the end criteria is satisfied
    // (time or number of rows)
    INITIAL_CONSUMING,

    // In this state, we consume from kafka until we reach the _finalOffset (exclusive)
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
    // anymore rows from kafka. We wait for a helix transition to go ONLINE, at which point, we can download the
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

  private class SegmentFileAndOffset {
    final String _segmentFile;
    final long _offset;
    SegmentFileAndOffset(String segmentFile, long offset) {
      _segmentFile = segmentFile;
      _offset = offset;
    }
    public long getOffset() {
      return _offset;
    }

    public String getSegmentFile() {
      return _segmentFile;
    }

    public void deleteSegmentFile() {
      // If segment build fails with an exception then we will not be able to create a segment file and
      // the file name will be null.
      if (_segmentFile != null) {
        FileUtils.deleteQuietly(new File(_segmentFile));
      }
    }
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(LLRealtimeSegmentDataManager.class);
  private static final long TIME_THRESHOLD_FOR_LOG_MINUTES = 1;
  private static final long TIME_EXTENSION_ON_EMPTY_SEGMENT_HOURS = 1;
  private static final int MSG_COUNT_THRESHOLD_FOR_LOG = 100000;
  private static final int BUILD_TIME_LEASE_SECONDS = 30;
  private static final int MAX_CONSECUTIVE_ERROR_COUNT = 5;

  private final LLCRealtimeSegmentZKMetadata _segmentZKMetadata;
  private final TableConfig _tableConfig;
  private final RealtimeTableDataManager _realtimeTableDataManager;
  private final KafkaMessageDecoder _messageDecoder;
  private final int _segmentMaxRowCount;
  private final String _resourceDataDir;
  private final IndexLoadingConfig _indexLoadingConfig;
  private final Schema _schema;
  private final String _metricKeyName;
  private final ServerMetrics _serverMetrics;
  private final RealtimeSegmentImpl _realtimeSegment;
  private volatile long _currentOffset;
  private volatile State _state;
  private volatile int _numRowsConsumed = 0;
  private volatile int consecutiveErrorCount = 0;
  private long _startTimeMs = 0;
  private final String _segmentNameStr;
  private final SegmentVersion _segmentVersion;
  private final SegmentBuildTimeLeaseExtender _leaseExtender;
  private SegmentFileAndOffset _segmentFileAndOffset;
  private PinotKafkaConsumerFactory _pinotKafkaConsumerFactory;

  // Segment end criteria
  private volatile long _consumeEndTime = 0;
  private volatile long _finalOffset = -1;
  private volatile boolean _shouldStop = false;

  // It takes 30s to locate controller leader, and more if there are multiple controller failures.
  // For now, we let 31s pass for this state transition.
  private static final int MAX_TIME_FOR_CONSUMING_TO_ONLINE_IN_SECONDS = 31;

  private Thread _consumerThread;
  private final String _kafkaTopic;
  private final int _kafkaPartitionId;
  final String _clientId;
  private final LLCSegmentName _segmentName;
  private final PlainFieldExtractor _fieldExtractor;
  private PinotKafkaConsumer _consumerWrapper = null;
  private final File _resourceTmpDir;
  private final String _tableName;
  private final List<String> _invertedIndexColumns;
  private final List<String> _noDictionaryColumns;
  private final StarTreeIndexSpec _starTreeIndexSpec;
  private final String _sortedColumn;
  private Logger segmentLogger = LOGGER;
  private final String _tableStreamName;
  private final PinotDataBufferMemoryManager _memoryManager;
  private AtomicLong _lastUpdatedRawDocuments = new AtomicLong(0);
  private final String _instanceId;
  private final ServerSegmentCompletionProtocolHandler _protocolHandler;
  private final long _consumeStartTime;
  private final long _startOffset;
  private final KafkaStreamMetadata _kafkaStreamMetadata;
  private final String _kafkaBootstrapNodes;

  private long _lastLogTime = 0;
  private int _lastConsumedCount = 0;
  private String _stopReason = null;
  private final Semaphore _segBuildSemaphore;


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
          segmentLogger.info("Stopping consumption due to time limit start={} now={} numRows={}", _startTimeMs, now, _numRowsConsumed);
          _stopReason = SegmentCompletionProtocol.REASON_TIME_LIMIT;
          return true;
        } else if (_numRowsConsumed >= _segmentMaxRowCount) {
          segmentLogger.info("Stopping consumption due to row limit nRows={} maxNRows={}", _numRowsConsumed,
              _segmentMaxRowCount);
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

  private void handleTransientKafkaErrors(Exception e) throws  Exception {
    consecutiveErrorCount++;
    if (consecutiveErrorCount > MAX_CONSECUTIVE_ERROR_COUNT) {
      segmentLogger.warn("Kafka transient exception when fetching messages, stopping consumption after {} attempts", consecutiveErrorCount, e);
      throw e;
    } else {
      segmentLogger.warn("Kafka transient exception when fetching messages, retrying (count={})", consecutiveErrorCount, e);
      Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
      makeConsumerWrapper("Too many transient errors");
    }
  }

  protected boolean consumeLoop() throws Exception {
    _fieldExtractor.resetCounters();
    final long idlePipeSleepTimeMillis = 100;
    final long maxIdleCountBeforeStatUpdate = (3 * 60 * 1000)/(idlePipeSleepTimeMillis + _kafkaStreamMetadata.getKafkaFetchTimeoutMillis());  // 3 minute count
    long lastUpdatedOffset = _currentOffset;  // so that we always update the metric when we enter this method.
    long idleCount = 0;
    // At this point, we know that we can potentially move the offset, so the old saved segment file is not valid
    // anymore. Remove the file if it exists.
    removeSegmentFile();

    final long _endOffset = Long.MAX_VALUE; // No upper limit on Kafka offset
    segmentLogger.info("Starting consumption loop start offset {}, finalOffset {}", _currentOffset, _finalOffset);
    while(!_shouldStop && !endCriteriaReached()) {
      // Consume for the next _kafkaReadTime ms, or we get to final offset, whichever happens earlier,
      // Update _currentOffset upon return from this method
      MessageBatch messageBatch = null;
      try {
        messageBatch = _consumerWrapper.fetchMessages(_currentOffset, _endOffset,
            _kafkaStreamMetadata.getKafkaFetchTimeoutMillis());
        consecutiveErrorCount = 0;
      } catch (TimeoutException e) {
        handleTransientKafkaErrors(e);
        continue;
      } catch (SimpleConsumerWrapper.TransientConsumerException e) {
        handleTransientKafkaErrors(e);
        continue;
      } catch (SimpleConsumerWrapper.PermanentConsumerException e) {
        segmentLogger.warn("Kafka permanent exception when fetching messages, stopping consumption", e);
        throw e;
      } catch (Exception e) {
        // Unknown exception from Kafka. Treat as a transient exception.
        // One such exception seen so far is java.net.SocketTimeoutException
        handleTransientKafkaErrors(e);
        continue;
      }

      processKafkaEvents(messageBatch, idlePipeSleepTimeMillis);

      if (_currentOffset != lastUpdatedOffset) {
        // We consumed something. Update the highest kafka offset as well as partition-consuming metric.
        _serverMetrics.setValueOfTableGauge(_metricKeyName, ServerGauge.HIGHEST_KAFKA_OFFSET_CONSUMED, _currentOffset);
        _serverMetrics.setValueOfTableGauge(_metricKeyName, ServerGauge.LLC_PARTITION_CONSUMING, 1);
        lastUpdatedOffset = _currentOffset;
      } else {
        // We did not consume any rows. Update the partition-consuming metric only if we have been idling for a long time.
        // Create a new kafka consumer wrapper, in case we are stuck on something.
        if (++idleCount > maxIdleCountBeforeStatUpdate) {
          _serverMetrics.setValueOfTableGauge(_metricKeyName, ServerGauge.LLC_PARTITION_CONSUMING, 1);
          idleCount = 0;
          makeConsumerWrapper("Idle for too long");
        }
      }
    }

    _serverMetrics.addMeteredTableValue(_metricKeyName, ServerMeter.ROWS_WITH_ERRORS,
        (long) _fieldExtractor.getTotalErrors());
    _serverMetrics.addMeteredTableValue(_metricKeyName, ServerMeter.ROWS_NEEDING_CONVERSIONS,
        (long) _fieldExtractor.getTotalConversions());
    _serverMetrics.addMeteredTableValue(_metricKeyName, ServerMeter.ROWS_WITH_NULL_VALUES,
        (long) _fieldExtractor.getTotalNulls());
    _serverMetrics.addMeteredTableValue(_metricKeyName, ServerMeter.COLUMNS_WITH_NULL_VALUES,
        (long) _fieldExtractor.getTotalNullCols());
    return true;
  }

  private void processKafkaEvents(MessageBatch messagesAndOffsets, long idlePipeSleepTimeMillis) {
    Meter realtimeRowsConsumedMeter = null;
    Meter realtimeRowsDroppedMeter = null;

    int indexedMessageCount = 0;
    int kafkaMessageCount = 0;
    boolean canTakeMore = true;
    GenericRow decodedRow = null;
    GenericRow transformedRow = null;
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
        // 2. We are in CATCHING_UP state, and we legally hit this error due to Kafka unclean leader election where
        //    offsets get changed with higher generation numbers for some pinot servers but not others. So, if another
        //    server (who got a larger kafka offset) asked us to catch up to that offset, but we are connected to a
        //    broker who has smaller offsets, then we may try to push more rows into the buffer than maximum. This
        //    is a rare case, and we really don't know how to handle this at this time.
        //    Throw an exception.
        //
        segmentLogger.error("Buffer full with {} rows consumed (row limit {})", _numRowsConsumed, _segmentMaxRowCount);
        throw new RuntimeException("Realtime segment full");
      }

      // Index each message
      decodedRow = GenericRow.createOrReuseRow(decodedRow);

      decodedRow = _messageDecoder
          .decode(messagesAndOffsets.getMessageAtIndex(index), messagesAndOffsets.getMessageOffsetAtIndex(index),
              messagesAndOffsets.getMessageLengthAtIndex(index), decodedRow);

      if (decodedRow != null) {
        transformedRow = GenericRow.createOrReuseRow(transformedRow);
        transformedRow = _fieldExtractor.transform(decodedRow, transformedRow);

        if (transformedRow != null) {
          realtimeRowsConsumedMeter = _serverMetrics
              .addMeteredTableValue(_metricKeyName, ServerMeter.REALTIME_ROWS_CONSUMED, 1, realtimeRowsConsumedMeter);
          indexedMessageCount++;
        } else {
          realtimeRowsDroppedMeter = _serverMetrics
              .addMeteredTableValue(_metricKeyName, ServerMeter.INVALID_REALTIME_ROWS_DROPPED, 1,
                  realtimeRowsDroppedMeter);
        }

        canTakeMore = _realtimeSegment.index(transformedRow);
      } else {
        realtimeRowsDroppedMeter = _serverMetrics
            .addMeteredTableValue(_metricKeyName, ServerMeter.INVALID_REALTIME_ROWS_DROPPED, 1,
                realtimeRowsDroppedMeter);
      }

      _currentOffset = messagesAndOffsets.getNextKafkaMessageOffsetAtIndex(index);
      _numRowsConsumed++;
      kafkaMessageCount++;
    }
    updateCurrentDocumentCountMetrics();
    if (kafkaMessageCount != 0) {
      segmentLogger.debug("Indexed {} messages ({} messages read from Kafka) current offset {}", indexedMessageCount,
          kafkaMessageCount, _currentOffset);
    } else {
      // If there were no messages to be fetched from Kafka, wait for a little bit as to avoid hammering the
      // Kafka broker
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
              _serverMetrics.addMeteredTableValue(_metricKeyName, ServerMeter.LLC_CONTROLLER_RESPONSE_NOT_LEADER, 1);
              // Retain the same state
              segmentLogger.warn("Got not leader response");
              hold();
              break;
            case CATCH_UP:
              _serverMetrics.addMeteredTableValue(_metricKeyName, ServerMeter.LLC_CONTROLLER_RESPONSE_CATCH_UP, 1);
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
              _serverMetrics.addMeteredTableValue(_metricKeyName, ServerMeter.LLC_CONTROLLER_RESPONSE_HOLD, 1);
              hold();
              break;
            case DISCARD:
              _serverMetrics.addMeteredTableValue(_metricKeyName, ServerMeter.LLC_CONTROLLER_RESPONSE_DISCARD, 1);
              // Keep this in memory, but wait for the online transition, and download when it comes in.
              _state = State.DISCARDED;
              break;
            case KEEP:
              _serverMetrics.addMeteredTableValue(_metricKeyName, ServerMeter.LLC_CONTROLLER_RESPONSE_KEEP, 1);
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
              _serverMetrics.addMeteredTableValue(_metricKeyName, ServerMeter.LLC_CONTROLLER_RESPONSE_COMMIT, 1);
              _state = State.COMMITTING;
              long buildTimeSeconds = response.getBuildTimeSeconds();
              final String segmentTarFile = buildSegmentForCommit(buildTimeSeconds * 1000L);
              if (segmentTarFile == null) {
                // We could not build the segment. Go into error state.
                _state = State.ERROR;
              } else {
                success = commitSegment(segmentTarFile, response);
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

  protected String buildSegmentForCommit(long buildTimeLeaseMs) {
    try {
      if (_segmentFileAndOffset != null) {
        if (_segmentFileAndOffset.getOffset() == _currentOffset) {
          // Double-check that we have the file, just in case.
          String segTarFile = _segmentFileAndOffset.getSegmentFile();
          if (new File(segTarFile).exists()) {
            return _segmentFileAndOffset.getSegmentFile();
          } else {
            _segmentFileAndOffset = null;
          }
        }
        removeSegmentFile();
      }
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
      String segTarFile =  buildSegmentInternal(true);
      _segmentFileAndOffset = new SegmentFileAndOffset(segTarFile, _currentOffset);
      return segTarFile;
    } finally {
      _leaseExtender.removeSegment(_segmentNameStr);
    }
  }

  protected String buildSegmentInternal(boolean forCommit) {
    try {
      if (_segBuildSemaphore != null) {
        segmentLogger.info("Waiting to acquire semaphore for building segment");
        _segBuildSemaphore.acquire();
      }
      long startTimeMillis = System.currentTimeMillis();
      // Build a segment from in-memory rows.If buildTgz is true, then build the tar.gz file as well
      // TODO Use an auto-closeable object to delete temp resources.
      File tempSegmentFolder = new File(_resourceTmpDir, "tmp-" + _segmentNameStr + "-" + String.valueOf(now()));
      // lets convert the segment now
      RealtimeSegmentConverter converter =
          new RealtimeSegmentConverter(_realtimeSegment, tempSegmentFolder.getAbsolutePath(), _schema,
              _segmentZKMetadata.getTableName(), _segmentZKMetadata.getSegmentName(), _sortedColumn,
              _invertedIndexColumns, _noDictionaryColumns, _starTreeIndexSpec);
      logStatistics();
      segmentLogger.info("Trying to build segment");
      final long buildStartTime = now();
      try {
        converter.build(_segmentVersion, _serverMetrics);
      } catch (Exception e) {
        segmentLogger.error("Could not build segment", e);
        FileUtils.deleteQuietly(tempSegmentFolder);
        return null;
      }
      final long buildEndTime = now();
      segmentLogger.info("Successfully built segment in {} ms", (buildEndTime - buildStartTime));
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
      FileUtils.deleteQuietly(tempSegmentFolder);
      long endTimeMillis = System.currentTimeMillis();

      _serverMetrics.setValueOfTableGauge(_metricKeyName, ServerGauge.LAST_REALTIME_SEGMENT_CREATION_DURATION_SECONDS,
          TimeUnit.MILLISECONDS.toSeconds(endTimeMillis - startTimeMillis));

      if (forCommit) {
        return destDir.getAbsolutePath() + TarGzCompressionUtils.TAR_GZ_FILE_EXTENTION;
      }
      return destDir.getAbsolutePath();
    } catch (InterruptedException e) {
      segmentLogger.error("Interrupted while waiting for semaphore");
      return null;
    } finally {
      if (_segBuildSemaphore != null) {
        _segBuildSemaphore.release();
      }
    }
  }

  protected SegmentCompletionProtocol.Response doSplitCommit(File segmentTarFile, SegmentCompletionProtocol.Response prevResponse) {
    SegmentCompletionProtocol.Response segmentCommitStartResponse = _protocolHandler.segmentCommitStart(_currentOffset, _segmentNameStr);
    if (!segmentCommitStartResponse.getStatus().equals(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_CONTINUE)) {
      segmentLogger.warn("CommitStart failed  with response {}", segmentCommitStartResponse.toJsonString());
      return SegmentCompletionProtocol.RESP_FAILED;
    }

    SegmentCompletionProtocol.Response segmentCommitUploadResponse = _protocolHandler.segmentCommitUpload(
        _currentOffset, _segmentNameStr, segmentTarFile, prevResponse.getControllerVipUrl());
    if (!segmentCommitUploadResponse.getStatus().equals(SegmentCompletionProtocol.ControllerResponseStatus.UPLOAD_SUCCESS)) {
      segmentLogger.warn("Segment upload failed  with response {}", segmentCommitUploadResponse.toJsonString());
      return SegmentCompletionProtocol.RESP_FAILED;
    }

    SegmentCompletionProtocol.Response commitEndResponse =  _protocolHandler.segmentCommitEnd(_currentOffset,
        _segmentNameStr, segmentCommitUploadResponse.getSegmentLocation(), _memoryManager.getTotalAllocatedBytes());
    if (!commitEndResponse.getStatus().equals(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_SUCCESS))  {
      segmentLogger.warn("CommitEnd failed  with response {}", commitEndResponse.toJsonString());
      return SegmentCompletionProtocol.RESP_FAILED;
    }
    return commitEndResponse;
  }

  protected boolean commitSegment(final String segTarFileName, SegmentCompletionProtocol.Response response) {
    File segTarFile = new File(segTarFileName);
    if (!segTarFile.exists()) {
      throw new RuntimeException("Segment file does not exist:" + segTarFileName);
    }
    SegmentCompletionProtocol.Response returnedResponse;
    if (response.getIsSplitCommit() && _indexLoadingConfig.isEnableSplitCommit()) {
      // Send segmentStart, segmentUpload, & segmentCommitEnd to the controller
      // if that succeeds, swap in-memory segment with the one built.
      returnedResponse = doSplitCommit(segTarFile, response);
    } else {
      // Send segmentCommit() to the controller
      // if that succeeds, swap in-memory segment with the one built.
      returnedResponse = postSegmentCommitMsg(segTarFile);
    }
    if (!returnedResponse.getStatus().equals(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_SUCCESS)) {
      return false;
    }
    _realtimeTableDataManager.replaceLLSegment(_segmentNameStr, _indexLoadingConfig);
    removeSegmentFile();
    return true;
  }

  protected SegmentCompletionProtocol.Response postSegmentCommitMsg(File segmentTarFile) {
    SegmentCompletionProtocol.Response response = _protocolHandler.segmentCommit(_currentOffset, _segmentNameStr,
        _memoryManager.getTotalAllocatedBytes(), segmentTarFile);
    if (!response.getStatus().equals(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_SUCCESS)) {
      segmentLogger.warn("Commit failed  with response {}", response.toJsonString());
    }
    return response;
  }

  protected boolean buildSegmentAndReplace() {
    String segmentDirName = buildSegmentInternal(false);
    if (segmentDirName == null) {
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
      params.withOffset(_currentOffset).withReason(reason).withSegmentName(_segmentNameStr);

      SegmentCompletionProtocol.Response response = _protocolHandler.segmentStoppedConsuming(params);
      if (response.getStatus() == SegmentCompletionProtocol.ControllerResponseStatus.PROCESSED) {
        LOGGER.info("Got response {}", response.toJsonString());
        break;
      }
      Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);
      LOGGER.info("Retrying after response {}", response.toJsonString());
    } while (!_shouldStop);
  }

  protected SegmentCompletionProtocol.Response postSegmentConsumedMsg() {
    // Post segmentConsumed to current leader.
    // Retry maybe once if leader is not found.
    SegmentCompletionProtocol.Request.Params params = new SegmentCompletionProtocol.Request.Params();
    params.withOffset(_currentOffset).withSegmentName(_segmentNameStr).withReason(_stopReason);
    return _protocolHandler.segmentConsumed(params);
  }

  private void removeSegmentFile() {
    if (_segmentFileAndOffset != null) {
      _segmentFileAndOffset.deleteSegmentFile();
      _segmentFileAndOffset = null;
    }
  }

  public void goOnlineFromConsuming(RealtimeSegmentZKMetadata metadata) throws InterruptedException {
    LLCRealtimeSegmentZKMetadata llcMetadata = (LLCRealtimeSegmentZKMetadata)metadata;
    // Remove the segment file before we do anything else.
    removeSegmentFile();
    _leaseExtender.removeSegment(_segmentNameStr);
    final long endOffset = llcMetadata.getEndOffset();
    segmentLogger.info("State: {}, transitioning from CONSUMING to ONLINE (startOffset: {}, endOffset: {})",
        _state.toString(), _startOffset, endOffset);
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
      _consumerWrapper.close();
    } catch (Exception e) {
      segmentLogger.warn("Could not close consumer wrapper", e);
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

  // TODO Make this a factory class.
  protected KafkaLowLevelStreamProviderConfig createStreamProviderConfig() {
    return new KafkaLowLevelStreamProviderConfig();
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
    _protocolHandler = new ServerSegmentCompletionProtocolHandler(_instanceId);

    // TODO Validate configs
    IndexingConfig indexingConfig = _tableConfig.getIndexingConfig();
    _kafkaStreamMetadata = new KafkaStreamMetadata(indexingConfig.getStreamConfigs());
    _pinotKafkaConsumerFactory = PinotKafkaConsumerFactory.create(_kafkaStreamMetadata);
    KafkaLowLevelStreamProviderConfig kafkaStreamProviderConfig = createStreamProviderConfig();
    kafkaStreamProviderConfig.init(tableConfig, instanceZKMetadata, schema);
    _kafkaBootstrapNodes = indexingConfig.getStreamConfigs()
        .get(CommonConstants.Helix.DataSource.STREAM_PREFIX + "."
            + CommonConstants.Helix.DataSource.Realtime.Kafka.KAFKA_BROKER_LIST);
    _kafkaTopic = kafkaStreamProviderConfig.getTopicName();
    _segmentNameStr = _segmentZKMetadata.getSegmentName();
    _segmentName = new LLCSegmentName(_segmentNameStr);
    _kafkaPartitionId = _segmentName.getPartitionId();
    _tableName = _tableConfig.getTableName();
    _metricKeyName = _tableName + "-" + _kafkaTopic + "-" + _kafkaPartitionId;
    segmentLogger = LoggerFactory.getLogger(LLRealtimeSegmentDataManager.class.getName() +
        "_" + _segmentNameStr);
    _tableStreamName = _tableName + "_" + kafkaStreamProviderConfig.getStreamName();
    _memoryManager = getMemoryManager(realtimeTableDataManager.getConsumerDir(), _segmentNameStr,
        indexLoadingConfig.isRealtimeOffheapAllocation(), indexLoadingConfig.isDirectRealtimeOffheapAllocation(),
        realtimeTableDataManager.getServerMetrics());

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
    int segmentMaxRowCount = kafkaStreamProviderConfig.getSizeThresholdToFlushSegment();

    if (0 < segmentZKMetadata.getSizeThresholdToFlushSegment()) {
      segmentMaxRowCount = segmentZKMetadata.getSizeThresholdToFlushSegment();
    }

    _segmentMaxRowCount = segmentMaxRowCount;

    // Start new realtime segment
    RealtimeSegmentConfig.Builder realtimeSegmentConfigBuilder =
        new RealtimeSegmentConfig.Builder().setSegmentName(_segmentNameStr)
            .setStreamName(_kafkaTopic)
            .setSchema(schema)
            .setCapacity(_segmentMaxRowCount)
            .setAvgNumMultiValues(indexLoadingConfig.getRealtimeAvgMultiValueCount())
            .setNoDictionaryColumns(indexLoadingConfig.getNoDictionaryColumns())
            .setInvertedIndexColumns(invertedIndexColumns)
            .setRealtimeSegmentZKMetadata(segmentZKMetadata)
            .setOffHeap(indexLoadingConfig.isRealtimeOffheapAllocation())
            .setMemoryManager(_memoryManager)
            .setStatsHistory(realtimeTableDataManager.getStatsHistory());

    // Create message decoder
    _messageDecoder = _pinotKafkaConsumerFactory.getDecoder(kafkaStreamProviderConfig);
    _clientId = _kafkaPartitionId + "-" + NetUtil.getHostnameOrAddress();

    // Create field extractor
    _fieldExtractor = FieldExtractorFactory.getPlainFieldExtractor(schema);
    makeConsumerWrapper("Starting");

    SegmentPartitionConfig segmentPartitionConfig = indexingConfig.getSegmentPartitionConfig();
    if (segmentPartitionConfig != null) {
      try {
        int nPartitions = _consumerWrapper.getPartitionCount(_kafkaTopic, /*maxWaitTimeMs=*/5000L);
        segmentPartitionConfig.setNumPartitions(nPartitions);
        realtimeSegmentConfigBuilder.setSegmentPartitionConfig(segmentPartitionConfig);
      } catch (Exception e) {
        segmentLogger.warn("Couldn't get number of partitions in 5s, not using partition config {}", e.getMessage());
        makeConsumerWrapper("Timeout getting number of partitions");
      }
    }

    _realtimeSegment = new RealtimeSegmentImpl(realtimeSegmentConfigBuilder.build());
    _startOffset = _segmentZKMetadata.getStartOffset();
    _currentOffset = _startOffset;
    _resourceTmpDir = new File(resourceDataDir, "_tmp");
    if (!_resourceTmpDir.exists()) {
      _resourceTmpDir.mkdirs();
    }
    _state = State.INITIAL_CONSUMING;
    long now = now();
    _consumeStartTime = now;
    _consumeEndTime = now + kafkaStreamProviderConfig.getTimeThresholdToFlushSegment();
    LOGGER.info("Starting consumption on realtime consuming segment {} maxRowCount {} maxEndTime {}",
        _segmentName, _segmentMaxRowCount, new DateTime(_consumeEndTime, DateTimeZone.UTC).toString());
    start();
  }

  private void logStatistics() {
    int numErrors, numConversions, numNulls, numNullCols;
    if ((numErrors = _fieldExtractor.getTotalErrors()) > 0) {
      _serverMetrics.addMeteredTableValue(_tableStreamName,
          ServerMeter.ROWS_WITH_ERRORS, (long) numErrors);
    }
    Map<String, Integer> errorCount = _fieldExtractor.getErrorCount();
    for (String column : errorCount.keySet()) {
      if ((numErrors = errorCount.get(column)) > 0) {
        segmentLogger.warn("Column {} had {} rows with errors", column, numErrors);
      }
    }
    if ((numConversions = _fieldExtractor.getTotalConversions()) > 0) {
      _serverMetrics.addMeteredTableValue(_tableStreamName,
          ServerMeter.ROWS_NEEDING_CONVERSIONS, (long) numConversions);
      segmentLogger.info("{} rows needed conversions ", numConversions);
    }
    if ((numNulls = _fieldExtractor.getTotalNulls()) > 0) {
      _serverMetrics.addMeteredTableValue(_tableStreamName,
          ServerMeter.ROWS_WITH_NULL_VALUES, (long) numNulls);
      segmentLogger.info("{} rows had null columns", numNulls);
    }
    if ((numNullCols = _fieldExtractor.getTotalNullCols()) > 0) {
      _serverMetrics.addMeteredTableValue(_tableStreamName,
          ServerMeter.COLUMNS_WITH_NULL_VALUES, (long) numNullCols);
      segmentLogger.info("{} columns had null values", numNullCols);
    }
  }

  private void makeConsumerWrapper(String reason) {
    if (_consumerWrapper != null) {
      try {
        _consumerWrapper.close();
      } catch (Exception e) {
        segmentLogger.warn("Could not close Kafka consumer wrapper");
      }
    }
    segmentLogger.info("Creating new Kafka consumer wrapper, reason: {}", reason);
    _consumerWrapper = _pinotKafkaConsumerFactory.buildConsumer(_clientId, _kafkaPartitionId, _kafkaStreamMetadata);
  }

  // This should be done during commit? We may not always commit when we build a segment....
  // TODO Call this method when we are loading the segment, which we do from table datamanager afaik
  private void updateCurrentDocumentCountMetrics() {
    int currentRawDocs = _realtimeSegment.getNumDocsIndexed();
    _serverMetrics.addValueToTableGauge(_tableName, ServerGauge.DOCUMENT_COUNT, (currentRawDocs - _lastUpdatedRawDocuments
        .get()));
    _lastUpdatedRawDocuments.set(currentRawDocs);
    final long now = now();
    final int rowsConsumed = _numRowsConsumed - _lastConsumedCount;
    final long prevTime = _lastConsumedCount == 0 ? _consumeStartTime : _lastLogTime;
    // Log every minute or 100k events
    if (now - prevTime > TimeUnit.MINUTES.toMillis(TIME_THRESHOLD_FOR_LOG_MINUTES) || rowsConsumed >= MSG_COUNT_THRESHOLD_FOR_LOG) {
      segmentLogger.info("Consumed {} events from (rate:{}/s), currentOffset={}, numRowsSoFar={}", rowsConsumed,
            (float) (rowsConsumed) * 1000 / (now - prevTime), _currentOffset, _numRowsConsumed);
      _lastConsumedCount = _numRowsConsumed;
      _lastLogTime = now;
    }
  }

  @Override
  public IndexSegment getSegment() {
    return _realtimeSegment;
  }

  @Override
  public String getSegmentName() {
    return _segmentNameStr;
  }
}
