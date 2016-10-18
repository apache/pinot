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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Uninterruptibles;
import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.common.config.IndexingConfig;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metadata.instance.InstanceZKMetadata;
import com.linkedin.pinot.common.metadata.segment.LLCRealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
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
import com.linkedin.pinot.core.data.manager.offline.SegmentDataManager;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.realtime.converter.RealtimeSegmentConverter;
import com.linkedin.pinot.core.realtime.impl.RealtimeSegmentImpl;
import com.linkedin.pinot.core.realtime.impl.kafka.KafkaHighLevelStreamProviderConfig;
import com.linkedin.pinot.core.realtime.impl.kafka.KafkaMessageDecoder;
import com.linkedin.pinot.core.realtime.impl.kafka.KafkaSimpleConsumerFactoryImpl;
import com.linkedin.pinot.core.realtime.impl.kafka.SimpleConsumerWrapper;
import com.linkedin.pinot.server.realtime.ServerSegmentCompletionProtocolHandler;
import kafka.message.MessageAndOffset;


/**
 * Segment data manager for low level consumer realtime segments, which manages consumption and segment completion.
 */
public class LLRealtimeSegmentDataManager extends SegmentDataManager {
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
      if (this.equals(INITIAL_CONSUMING) || this.equals(CATCHING_UP) || this.equals(CONSUMING_TO_ONLINE)) {
        return true;
      }
      return false;
    }

    public boolean isFinal() {
      if (this.equals(ERROR) || this.equals(COMMITTED) || this.equals(RETAINED) || this.equals(DISCARDED)) {
        return true;
      }
      return false;
    }
  }
  private static final Logger LOGGER = LoggerFactory.getLogger(LLRealtimeSegmentDataManager.class);
  private static final int KAFKA_MAX_FETCH_TIME_MILLIS = 1000;
  private static final long TIME_THRESHOLD_FOR_LOG_MINUTES = 1;
  private static final long TIME_EXTENSION_ON_EMPTY_SEGMENT_HOURS = 1;
  private static final int MSG_COUNT_THRESHOLD_FOR_LOG = 100000;

  private final LLCRealtimeSegmentZKMetadata _segmentZKMetadata;
  private final AbstractTableConfig _tableConfig;
  private final RealtimeTableDataManager _realtimeTableDataManager;
  private final KafkaMessageDecoder _messageDecoder;
  private final int _segmentMaxRowCount;
  private final String _resourceDataDir;
  private final Schema _schema;
  private final String _metricKeyName;
  private final ServerMetrics _serverMetrics;
  private final RealtimeSegmentImpl _realtimeSegment;
  private volatile long _currentOffset;
  private volatile State _state;
  private volatile int _numRowsConsumed = 0;
  private long _startTimeMs = 0;
  private final String _segmentNameStr;
  private final SegmentVersion _segmentVersion;

  // Segment end criteria
  private volatile long _consumeEndTime = 0;
  private volatile long _finalOffset = -1;
  private volatile boolean _receivedStop = false;

  // It takes 30s to locate controller leader, and more if there are multiple controller failures.
  // For now, we let 31s pass for this state transition.
  private final int _maxTimeForConsumingToOnlineSec = 31;

  private Thread _consumerThread;
  private final String _kafkaTopic;
  private final int _kafkaPartitionId;
  final String _clientId;
  private final LLCSegmentName _segmentName;
  private final PlainFieldExtractor _fieldExtractor;
  private final SimpleConsumerWrapper _consumerWrapper;
  private final File _resourceTmpDir;
  private final String _tableName;
  private final List<String> _invertedIndexColumns;
  private final String _sortedColumn;
  private Logger segmentLogger = LOGGER;
  private final String _tableStreamName;
  private AtomicLong _lastUpdatedRawDocuments = new AtomicLong(0);
  private final String _instance;
  private final ServerSegmentCompletionProtocolHandler _protocolHandler;
  private final long _consumeStartTime;
  private final long _startOffset;

  private long _lastLogTime = 0;
  private int _lastConsumedCount = 0;


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
          if (_realtimeSegment.getRawDocumentCount() == 0) {
            segmentLogger.info("No events came in, extending time by {} hours", TIME_EXTENSION_ON_EMPTY_SEGMENT_HOURS);
            _consumeEndTime += TimeUnit.HOURS.toMillis(TIME_EXTENSION_ON_EMPTY_SEGMENT_HOURS);
            return false;
          }
          segmentLogger.info("Stopping consumption due to time limit start={} now={} numRows={}", _startTimeMs, now, _numRowsConsumed);
          return true;
        } else if (_numRowsConsumed >= _segmentMaxRowCount) {
          segmentLogger.info("Stopping consumption due to row limit nRows={} maxNRows={}", _numRowsConsumed,
              _segmentMaxRowCount);
          return true;
        }
        return false;

      case CATCHING_UP:
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

  protected void consumeLoop() {
    _fieldExtractor.resetCounters();

    final long _endOffset = Long.MAX_VALUE; // No upper limit on Kafka offset
    segmentLogger.info("Starting consumption loop start offset {}, finalOffset {}", _currentOffset, _finalOffset);
    while(!_receivedStop && !endCriteriaReached()) {
      // Consume for the next _kafkaReadTime ms, or we get to final offset, whichever happens earlier,
      // Update _currentOffset upon return from this method
      Iterable<MessageAndOffset> messagesAndOffsets = null;
      Long highWatermark = null;
      try {
        Pair<Iterable<MessageAndOffset>, Long> messagesAndWatermark =
            _consumerWrapper.fetchMessagesAndHighWatermark(_currentOffset, _endOffset, KAFKA_MAX_FETCH_TIME_MILLIS);
        messagesAndOffsets = messagesAndWatermark.getLeft();
        highWatermark = messagesAndWatermark.getRight();
      } catch (TimeoutException e) {
        segmentLogger.warn("Timed out when fetching messages from Kafka, retrying");
        continue;
      }

      Iterator<MessageAndOffset> msgIterator = messagesAndOffsets.iterator();

      int indexedMessageCount = 0;
      int kafkaMessageCount = 0;
      while (!_receivedStop && !endCriteriaReached() && msgIterator.hasNext()) {
        // Get a batch of messages from Kafka
        // Index each message
        MessageAndOffset messageAndOffset = msgIterator.next();
        byte[] array = messageAndOffset.message().payload().array();
        int offset = messageAndOffset.message().payload().arrayOffset();
        int length = messageAndOffset.message().payloadSize();
        GenericRow row = _messageDecoder.decode(array, offset, length);

        // Update lag metric on the first message of each batch
        if (kafkaMessageCount == 0) {
          long messageOffset = messageAndOffset.offset();
          long offsetDifference = highWatermark - messageOffset;
          _serverMetrics.setValueOfTableGauge(_metricKeyName, ServerGauge.KAFKA_PARTITION_OFFSET_LAG, offsetDifference);
        }

        if (row != null) {
          row = _fieldExtractor.transform(row);

          if (row != null) {
            _serverMetrics.addMeteredTableValue(_metricKeyName, ServerMeter.REALTIME_ROWS_CONSUMED, 1);
            indexedMessageCount++;
          } else {
            _serverMetrics.addMeteredTableValue(_metricKeyName, ServerMeter.INVALID_REALTIME_ROWS_DROPPED, 1);
          }

          boolean canTakeMore = _realtimeSegment.index(row);  // Ignore the boolean return
          if (!canTakeMore) {
            //TODO
            // This condition can happen when we are catching up, (due to certain failure scenarios in kafka where
            // offsets get changed with higher generation numbers for some pinot servers but not others).
            // Also, it may be that we push in a row into the realtime segment, but it fails to index that row
            // for some reason., so we may end up with less number of rows in the real segment. Actually, even 0 rows.
            // In that case, we will see an exception when generating the segment.
            // TODO We need to come up with how the system behaves in these cases and document/handle them
            segmentLogger.warn("We got full during indexing");
          }
        } else {
          _serverMetrics.addMeteredTableValue(_metricKeyName, ServerMeter.INVALID_REALTIME_ROWS_DROPPED, 1);
        }

        _currentOffset = messageAndOffset.nextOffset();
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
        Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
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
          if (_receivedStop) {
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
              success = buildSegment(true);
              if (!success) {
                // We could not build the segment. Go into error state.
                _state = State.ERROR;
              } else {
                success = commitSegment();
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
        _state = State.ERROR;
      }

      if (initialConsumptionEnd != 0L) {
        _serverMetrics.setValueOfTableGauge(_metricKeyName,
            ServerGauge.LAST_REALTIME_SEGMENT_COMPLETION_DURATION_SECONDS,
            TimeUnit.MILLISECONDS.toSeconds(now() - initialConsumptionEnd));
      }
    }
  }

  private File makeSegmentDirPath() {
    return new File(_resourceDataDir, _segmentZKMetadata.getSegmentName());
  }

  /**
   *
   * @param buildTgz true if you want the method to also build tgz file
   * @return true if all succeeds.
   */
  protected boolean buildSegment(boolean buildTgz) {
    long startTimeMillis = System.currentTimeMillis();
    // Build a segment from in-memory rows.If buildTgz is true, then build the tar.gz file as well
    // TODO Use an auto-closeable object to delete temp resources.
    File tempSegmentFolder = new File(_resourceTmpDir, "tmp-" + _segmentNameStr + "-" + String.valueOf(now()));

    // lets convert the segment now
    RealtimeSegmentConverter converter =
        new RealtimeSegmentConverter(_realtimeSegment, tempSegmentFolder.getAbsolutePath(), _schema,
            _segmentZKMetadata.getTableName(), _segmentZKMetadata.getSegmentName(), _sortedColumn, _invertedIndexColumns);

    logStatistics();
    segmentLogger.info("Trying to build segment");
    final long buildStartTime = now();
    try {
      converter.build(_segmentVersion);
    } catch (Exception e) {
      segmentLogger.error("Could not build segment", e);
      FileUtils.deleteQuietly(tempSegmentFolder);
      return false;
    }
    final long buildEndTime = now();
    segmentLogger.info("Successfully built segment in {} ms", (buildEndTime - buildStartTime));
    File destDir = makeSegmentDirPath();
    FileUtils.deleteQuietly(destDir);
    try {
      FileUtils.moveDirectory(tempSegmentFolder.listFiles()[0], destDir);
      if (buildTgz) {
        TarGzCompressionUtils.createTarGzOfDirectory(destDir.getAbsolutePath());
      }
    } catch (IOException e) {
      segmentLogger.error("Exception during move/tar segment", e);
      FileUtils.deleteQuietly(tempSegmentFolder);
      return false;
    }
    FileUtils.deleteQuietly(tempSegmentFolder);
    long endTimeMillis = System.currentTimeMillis();

    _serverMetrics.setValueOfTableGauge(_metricKeyName, ServerGauge.LAST_REALTIME_SEGMENT_CREATION_DURATION_SECONDS,
        TimeUnit.MILLISECONDS.toSeconds(endTimeMillis - startTimeMillis));

    return true;
  }

  protected boolean commitSegment() {
    // Send segmentCommit() to the controller
    // if that succeeds, swap in-memory segment with the one built.
    File destSeg = makeSegmentDirPath();
    final String segTarFileName = destSeg.getAbsolutePath() + TarGzCompressionUtils.TAR_GZ_FILE_EXTENTION;
    try {
      SegmentCompletionProtocol.Response response = _protocolHandler.segmentCommit(_currentOffset, _segmentNameStr,
          new File(segTarFileName));
      if (!response.getStatus().equals(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_SUCCESS)) {
        segmentLogger.warn("Received controller response {}", response);
        return false;
      }
      _realtimeTableDataManager.replaceLLSegment(_segmentNameStr);
      FileUtils.deleteQuietly(new File(segTarFileName));
    } catch (FileNotFoundException e) {
      segmentLogger.error("Tar file {} not found", segTarFileName, e);
      return false;
    }
    return true;
  }

  protected boolean buildSegmentAndReplace() {
    boolean success = buildSegment(false);
    if (!success) {
      return success;
    }
    _realtimeTableDataManager.replaceLLSegment(_segmentZKMetadata.getSegmentName());
    return true;
  }

  protected void hold() {
    try {
      Thread.sleep(SegmentCompletionProtocol.MAX_HOLD_TIME_MS);
    } catch (InterruptedException e) {
      segmentLogger.warn("Interrupted while holding");
    }
  }

  protected SegmentCompletionProtocol.Response postSegmentConsumedMsg() {
    // Post segmentConsumed to current leader.
    // Retry maybe once if leader is not found.
    return _protocolHandler.segmentConsumed(_segmentNameStr, _currentOffset);
  }

  public void goOnlineFromConsuming(RealtimeSegmentZKMetadata metadata) throws InterruptedException {
    LLCRealtimeSegmentZKMetadata llcMetadata = (LLCRealtimeSegmentZKMetadata)metadata;
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
              TimeUnit.MILLISECONDS.convert(_maxTimeForConsumingToOnlineSec, TimeUnit.SECONDS));
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
    _realtimeTableDataManager.downloadAndReplaceSegment(_segmentNameStr, metadata);
  }

  protected long now() {
    return System.currentTimeMillis();
  }

  private boolean catchupToFinalOffset(long endOffset, long timeoutMs) {
    _finalOffset = endOffset;
    _consumeEndTime = now() + timeoutMs;
    _state = State.CONSUMING_TO_ONLINE;
    _receivedStop = false;
    consumeLoop();
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
    _receivedStop = true;
    _consumerThread.join();
  }

  // TODO Make this a factory class.
  protected KafkaHighLevelStreamProviderConfig createStreamProviderConfig() {
    return new KafkaHighLevelStreamProviderConfig();
  }

  // Assume that this is called only on OFFLINE to CONSUMING transition.
  // If the transition is OFFLINE to ONLINE, the caller should have downloaded the segment and we don't reach here.
  public LLRealtimeSegmentDataManager(RealtimeSegmentZKMetadata segmentZKMetadata, AbstractTableConfig tableConfig,
      InstanceZKMetadata instanceZKMetadata, RealtimeTableDataManager realtimeTableDataManager, String resourceDataDir,
      Schema schema, ServerMetrics serverMetrics) throws Exception {
    _segmentZKMetadata = (LLCRealtimeSegmentZKMetadata) segmentZKMetadata;
    _tableConfig = tableConfig;
    _realtimeTableDataManager = realtimeTableDataManager;
    _resourceDataDir = resourceDataDir;
    _schema = schema;
    _serverMetrics = serverMetrics;
    _segmentVersion = SegmentVersion.fromStringOrDefault(tableConfig.getIndexingConfig().getSegmentFormatVersion());
    _instance = _realtimeTableDataManager.getServerInstance();
    _protocolHandler = new ServerSegmentCompletionProtocolHandler(_instance);

    // TODO Validate configs
    IndexingConfig indexingConfig = _tableConfig.getIndexingConfig();
    KafkaHighLevelStreamProviderConfig kafkaStreamProviderConfig = createStreamProviderConfig();
    kafkaStreamProviderConfig.init(tableConfig, instanceZKMetadata, schema);
    final String bootstrapNodes = indexingConfig.getStreamConfigs()
        .get(CommonConstants.Helix.DataSource.STREAM_PREFIX + "." + CommonConstants.Helix.DataSource.Realtime.Kafka.KAFKA_BROKER_LIST);
    _kafkaTopic = kafkaStreamProviderConfig.getTopicName();
    _segmentNameStr = _segmentZKMetadata.getSegmentName();
    _segmentName = new LLCSegmentName(_segmentNameStr);
    _kafkaPartitionId = _segmentName.getPartitionId();
    _segmentMaxRowCount = kafkaStreamProviderConfig.getSizeThresholdToFlushSegment();
    _tableName = _tableConfig.getTableName();
    _metricKeyName = _tableName + "-" + _kafkaTopic + "-" + _kafkaPartitionId;
    segmentLogger = LoggerFactory.getLogger(LLRealtimeSegmentDataManager.class.getName() +
        "_" + _segmentNameStr);
    if (indexingConfig.getSortedColumn().isEmpty()) {
      segmentLogger.info("RealtimeDataResourceZKMetadata contains no information about sorted column for segment {}",
          _segmentName);
      _sortedColumn = null;
    } else {
      String firstSortedColumn = indexingConfig.getSortedColumn().get(0);
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
    //inverted index columns
    _invertedIndexColumns = indexingConfig.getInvertedIndexColumns();
    _tableStreamName = _tableName + "_" + kafkaStreamProviderConfig.getStreamName();


    List<String> invertedIndexColumns = indexingConfig.getInvertedIndexColumns();
    if (_sortedColumn != null && !invertedIndexColumns.contains(_sortedColumn)) {
      invertedIndexColumns.add(_sortedColumn);
    }
    // Start new realtime segment
    _realtimeSegment = new RealtimeSegmentImpl(schema, _segmentMaxRowCount, tableConfig.getTableName(),
        segmentZKMetadata.getSegmentName(), _kafkaTopic, _serverMetrics, invertedIndexColumns);
    _realtimeSegment.setSegmentMetadata(segmentZKMetadata, schema);

    // Create message decoder
    _messageDecoder = kafkaStreamProviderConfig.getDecoder();
    _clientId = _kafkaPartitionId + "-" + NetUtil.getHostnameOrAddress();

    // Create field extractor
    _fieldExtractor = (PlainFieldExtractor) FieldExtractorFactory.getPlainFieldExtractor(schema);
    _consumerWrapper = SimpleConsumerWrapper.forPartitionConsumption(new KafkaSimpleConsumerFactoryImpl(),
        bootstrapNodes, _clientId, _kafkaTopic, _kafkaPartitionId);
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

  // This should be done during commit? We may not always commit when we build a segment....
  // TODO Call this method when we are loading the segment, which we do from table datamanager afaik
  private void updateCurrentDocumentCountMetrics() {
    int currentRawDocs = _realtimeSegment.getRawDocumentCount();
    _serverMetrics.addValueToTableGauge(_tableName, ServerGauge.DOCUMENT_COUNT, (currentRawDocs - _lastUpdatedRawDocuments
        .get()));
    _lastUpdatedRawDocuments.set(currentRawDocs);
    final long now = now();
    final int rowsConsumed = _numRowsConsumed - _lastConsumedCount;
    final long prevTime = _lastConsumedCount == 0 ? _consumeStartTime : _lastLogTime;
    // Log every minute or 100k events
    if (now - prevTime > TimeUnit.MINUTES.toMillis(TIME_THRESHOLD_FOR_LOG_MINUTES) || rowsConsumed >= MSG_COUNT_THRESHOLD_FOR_LOG) {
      segmentLogger.info("Consumed {} events from (rate:{}/s), currentOffset={}", rowsConsumed,
            (float) (rowsConsumed) * 1000 / (now - prevTime), _currentOffset);
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

  public int getMaxTimeForConsumingToOnlineSec() {
    return _maxTimeForConsumingToOnlineSec;
  }
}
