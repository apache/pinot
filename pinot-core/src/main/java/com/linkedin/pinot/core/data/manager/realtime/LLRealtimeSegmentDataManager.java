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
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.io.FileUtils;
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
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.segment.fetcher.SegmentFetcherFactory;
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
  private enum State {
    // The state machine starts off with this state. While in this state we consume kafka events
    // and index them in memory. We continue to be in this state until the end criteria is satisfied
    // (time or number of rows)
    INITIAL_CONSUMING,

    // In this state, we consume from kafka until we reach the _finalOffset (exclusive)
    CATCHING_UP,

    // In this state, we sleep for MAX_HOLDING_TIME_MS, and the make a segmentConsumed() call to the
    // controller.
    HOLDING,

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
      if (this.equals(INITIAL_CONSUMING) || this.equals(CATCHING_UP)) {
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

  private final LLCRealtimeSegmentZKMetadata _segmentZKMetadata;
  private final AbstractTableConfig _tableConfig;
  private final RealtimeTableDataManager _realtimeTableDataManager;
  private final String _decoderClassName;
  private final KafkaMessageDecoder _messageDecoder;
  private final int _segmentMaxRowCount;
  private final String _resourceDataDir;
  private final Schema _schema;
  private final ServerMetrics _serverMetrics;
  private final RealtimeSegmentImpl _realtimeSegment;
  private long _endOffset = Long.MAX_VALUE; // No upper limit on Kafka offset
  private long _currentOffset;
  private volatile State _state;
  private int _numRowsConsumed = 0;
  private long _startOffset = 0;
  private long _startTimeMs = 0;
  private final String _segmentNameStr;
  private final SegmentVersion _segmentVersion;

  // Segment end criteria
  private long _consumeEndTime = 0;
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


  private boolean endCriteriaReached() {
    Preconditions.checkState(_state.shouldConsume(), "Incorrect state %s", _state);
    if (_state.equals(State.INITIAL_CONSUMING)) {
      if (_consumeEndTime > 0) {
        // We have a time-based constraint.
        long now = now();
        if (now >= _consumeEndTime) {
          segmentLogger.info("Stopping consumption due to time limit start={} now={} numRows={}", _startTimeMs, now, _numRowsConsumed);
          return true;
        }
      } else {
        // We have a number-of-rows based constraint.
        if (_numRowsConsumed >= _segmentMaxRowCount) {
          segmentLogger.info("Stopping consumption due to row limit nRows={} maxNRows={}", _numRowsConsumed,
              _segmentMaxRowCount);
          return true;
        }
      }
      return false;
    } else if (_state.equals(State.CATCHING_UP)) {
      if (_currentOffset == _finalOffset) {
        segmentLogger.info("Caught up to offset {}", _finalOffset);
        return true;
      }
      if (_currentOffset > _finalOffset) {
        throw new RuntimeException("Offset higher in catchup mode: current " + _currentOffset + " final " + _finalOffset);
      }
    }
    throw new RuntimeException("Unreachable");
  }

  private void consumeLoop() {
    while(!_receivedStop && !endCriteriaReached()) {
      // Consume for the next _kafkaReadTime ms, or we get to final offset, whichever happens earlier,
      // Update _currentOffset upon return from this method
      Iterable<MessageAndOffset> messagesAndOffsets = _consumerWrapper.fetchMessages(_currentOffset, _endOffset, KAFKA_MAX_FETCH_TIME_MILLIS);
      Iterator<MessageAndOffset> msgIterator = messagesAndOffsets.iterator();

      int batchSize = 0;
      while (!_receivedStop && !endCriteriaReached() && msgIterator.hasNext()) {
        // Get a batch of messages from Kafka
        // Index each message
        MessageAndOffset messageAndOffset = msgIterator.next();
        byte[] array = messageAndOffset.message().payload().array();
        int offset = messageAndOffset.message().payload().arrayOffset();
        int length = messageAndOffset.message().payloadSize();
        GenericRow row = _messageDecoder.decode(array, offset, length);

        if (row != null) {
          row = _fieldExtractor.transform(row);
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
          batchSize++;
        }
        _currentOffset = messageAndOffset.nextOffset();
        _numRowsConsumed++;
      }
      if (batchSize != 0) {
        segmentLogger.debug("Indexed {} messages current offset {}", batchSize, _currentOffset);
      } else {
        // If there were no messages to be fetched from Kafka, wait for a little bit as to avoid hammering the
        // Kafka broker
        Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
      }
    }
  }

  public class PartitionConsumer implements Runnable {
    public void run() {
      _startTimeMs = now();
      try {
        while (!_state.isFinal()) {
          if (_state.shouldConsume()) {
            consumeLoop();  // Consume until we reached the end criteria, or we are stopped.
          }
          if (_receivedStop) {
            break;
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
              success = buildSegment(true);
              if (!success) {
                // We could not build the segment. Go into error state.
                _state = State.ERROR;
              } else {
                success = commitSegment();
                if (success) {
                  _state = State.COMMITTED;
                } else {
                  // Back off and retry from holding state again.
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
  private boolean buildSegment(boolean buildTgz) {
    // Build a segment from in-memory rows.If buildTgz is true, then build the tar.gz file as well
    // TODO Use an auto-closeable objec to delete temp resources.
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
    return true;
  }

  private boolean commitSegment() {
    // Send segmentCommit() to the controller
    // if that succeeds, swap in-memory segment with the one built.
    File destSeg = makeSegmentDirPath();
    final String segTarFileName = destSeg.getAbsolutePath() + TarGzCompressionUtils.TAR_GZ_FILE_EXTENTION;
    try {
      _protocolHandler.segmentCommit(_currentOffset, _segmentNameStr, new File(segTarFileName));
    } catch (FileNotFoundException e) {
      segmentLogger.error("Tar file {} not found", segTarFileName, e);
      return false;
    }
    return true;
  }

  private boolean buildSegmentAndReplace() {
    boolean success = buildSegment(false);
    if (!success) {
      return success;
    }
    _realtimeTableDataManager.replaceLLSegment(_segmentZKMetadata.getSegmentName());
    return true;
  }

  private void hold() {
    try {
      Thread.sleep(SegmentCompletionProtocol.MAX_HOLD_TIME_MS);
    } catch (InterruptedException e) {
      segmentLogger.warn("Interrupted while holding");
    }
  }

  private SegmentCompletionProtocol.Response postSegmentConsumedMsg() {
    // Post segmentConsumed to current leader.
    // Retry maybe once if leader is not found.
    return _protocolHandler.segmentConsumed(_segmentNameStr, _currentOffset);
  }

  public void goOnlineFromConsuming(RealtimeSegmentZKMetadata metadata) throws InterruptedException {
    // Takes 30s max to locate a new controller (more if there are multiple controller failures)
    // set the timeout to 31s;
    final long transitionStartTimeMs = now();
    long timeoutMs =_maxTimeForConsumingToOnlineSec * 1000L;
    LLCRealtimeSegmentZKMetadata llcMetadata = (LLCRealtimeSegmentZKMetadata)metadata;
    final long endOffset = llcMetadata.getEndOffset();
    stop(timeoutMs);
    long now = now();
    timeoutMs -= (now - transitionStartTimeMs);
    if (timeoutMs <= 0) {
      // we could not get it to stop. Throw an exception and let it go to error state.
      throw new RuntimeException("Could not get to stop consumer thread" + _consumerThread);
    }

    switch (_state) {
      case COMMITTED:
      case RETAINED:
        // Nothing to do. we already built local segment and swapped it with in-memory data.
        break;
      case DISCARDED:
      case ERROR:
        downloadSegmentAndReplace(llcMetadata);
        break;
      case CATCHING_UP:
      case HOLDING:
        // Allow to catch up upto final offset, and then replace.
        if (_currentOffset > endOffset) {
          // We moved ahead of the offset that is committed in ZK.
          segmentLogger.warn("Current offset {} ahead of the offset in zk {}", _currentOffset, endOffset);
          downloadSegmentAndReplace(llcMetadata);
        } else {
          boolean success = catchupToFinalOffset(endOffset, timeoutMs);
          if (success) {
            buildSegmentAndReplace();
          } else {
            downloadSegmentAndReplace(llcMetadata);
          }
        }
        break;
      default:
        downloadSegmentAndReplace(llcMetadata);
        break;
    }
  }

  private void downloadSegmentAndReplace(LLCRealtimeSegmentZKMetadata metadata) {
    File tempSegmentFolder = new File(_resourceTmpDir, "tmp-" + String.valueOf(now()));
    File tempFile = new File(_resourceTmpDir, _segmentNameStr + ".tar.gz");
    String uri = metadata.getDownloadUrl();
    try {
      SegmentFetcherFactory.getSegmentFetcherBasedOnURI(uri).fetchSegmentToLocal(uri, tempFile);
      segmentLogger.info("Downloaded file from {} to {}; Length of downloaded file: {}", uri, tempFile,
          tempFile.length());
      TarGzCompressionUtils.unTar(tempFile, tempSegmentFolder);
      FileUtils.deleteQuietly(tempFile);
      FileUtils.moveDirectory(tempSegmentFolder, new File(_resourceTmpDir, _segmentNameStr));
      _realtimeTableDataManager.replaceLLSegment(_segmentNameStr);
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      FileUtils.deleteQuietly(tempSegmentFolder);
    }
  }

  private long now() {
    return System.currentTimeMillis();
  }

  private boolean catchupToFinalOffset(long endOffset, long timeoutMs) {
    _finalOffset = endOffset;
    _consumeEndTime = now() + timeoutMs;
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
      stop(0);
    } catch (InterruptedException e) {
      segmentLogger.error("Could not stop consumer thread");
    }
    _realtimeSegment.destroy();
  }

  public void start() {
    _state = State.INITIAL_CONSUMING;
    _consumerThread = new Thread(new PartitionConsumer(), "consumer_" + _kafkaTopic + "_" + _kafkaPartitionId + "_" + _startOffset);
    segmentLogger.info("Created new consumer thread {} for {}", _consumerThread, this.toString());
    _consumerThread.start();
  }

  /**
   * Stop the consuming thread.
   * @param ms max number of millis allowed to wait. If 0, then no limit.
   */
  public void stop(long ms) throws InterruptedException {
    _receivedStop = true;
    _consumerThread.join(ms);
  }

  // Assume that this is called only on OFFLINE to CONSUMING transition.
  // If the transition is OFFLINE to ONLINE, the caller should have downloaded the segment and we don't reach here.
  public LLRealtimeSegmentDataManager(RealtimeSegmentZKMetadata segmentZKMetadata, AbstractTableConfig tableConfig,
      InstanceZKMetadata instanceZKMetadata, RealtimeTableDataManager realtimeTableDataManager, String resourceDataDir,
      ReadMode readMode, Schema schema, ServerMetrics serverMetrics) throws Exception {
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
    KafkaHighLevelStreamProviderConfig kafkaStreamProviderConfig = new KafkaHighLevelStreamProviderConfig();
    kafkaStreamProviderConfig.init(tableConfig, instanceZKMetadata, schema);
    final String bootstrapNodes = indexingConfig.getStreamConfigs()
        .get(CommonConstants.Helix.DataSource.STREAM_PREFIX + "." + CommonConstants.Helix.DataSource.Realtime.Kafka.KAFKA_BROKER_LIST);
    _kafkaTopic = kafkaStreamProviderConfig.getTopicName();
    _segmentNameStr = _segmentZKMetadata.getSegmentName();
    _segmentName = new LLCSegmentName(_segmentNameStr);
    _kafkaPartitionId = _segmentName.getPartitionId();
    _decoderClassName = indexingConfig.getStreamConfigs()
        .get(CommonConstants.Helix.DataSource.STREAM_PREFIX + "." + CommonConstants.Helix.DataSource.Realtime.Kafka.DECODER_CLASS);
    _segmentMaxRowCount = Integer.parseInt(_tableConfig.getIndexingConfig().getStreamConfigs().get(
        CommonConstants.Helix.DataSource.Realtime.REALTIME_SEGMENT_FLUSH_SIZE));
    _tableName = _tableConfig.getTableName();
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
    if (!indexingConfig.getSortedColumn().isEmpty()) {
      invertedIndexColumns.add(indexingConfig.getSortedColumn().get(0));
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
    _currentOffset = _segmentZKMetadata.getStartOffset();
    _resourceTmpDir = new File(resourceDataDir, "_tmp");
    if (!_resourceTmpDir.exists()) {
      _resourceTmpDir.mkdirs();
    }
    start();
  }

  private void logStatistics() {
    int numErrors, numConversions, numNulls, numNullCols;
    if ((numErrors = _fieldExtractor.getTotalErrors()) > 0) {
      _serverMetrics.addMeteredTableValue(_tableStreamName,
          ServerMeter.ROWS_WITH_ERRORS, (long) numErrors);
    }
    Map<String, Integer> errorCount = _fieldExtractor.getError_count();
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
