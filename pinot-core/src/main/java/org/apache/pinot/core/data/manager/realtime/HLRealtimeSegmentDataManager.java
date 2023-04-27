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

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Uninterruptibles;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metadata.instance.InstanceZKMetadata;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.indexsegment.mutable.MutableSegmentImpl;
import org.apache.pinot.segment.local.realtime.converter.ColumnIndicesForRealtimeTable;
import org.apache.pinot.segment.local.realtime.converter.RealtimeSegmentConverter;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentConfig;
import org.apache.pinot.segment.local.recordtransformer.CompositeTransformer;
import org.apache.pinot.segment.local.recordtransformer.RecordTransformer;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.utils.IngestionUtils;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.metrics.PinotMeter;
import org.apache.pinot.spi.stream.ConsumerPartitionState;
import org.apache.pinot.spi.stream.PartitionLagState;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConsumerFactory;
import org.apache.pinot.spi.stream.StreamConsumerFactoryProvider;
import org.apache.pinot.spi.stream.StreamLevelConsumer;
import org.apache.pinot.spi.utils.CommonConstants.ConsumerState;
import org.apache.pinot.spi.utils.CommonConstants.Segment.Realtime.Status;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HLRealtimeSegmentDataManager extends RealtimeSegmentDataManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(HLRealtimeSegmentDataManager.class);
  private final static long ONE_MINUTE_IN_MILLSEC = 1000 * 60;

  private final String _tableNameWithType;
  private final String _segmentName;
  private final String _timeColumnName;
  private final TimeUnit _timeType;
  private final RecordTransformer _recordTransformer;

  private final StreamLevelConsumer _streamLevelConsumer;
  private final File _resourceTmpDir;
  private final MutableSegmentImpl _realtimeSegment;
  private final String _tableStreamName;
  private final StreamConfig _streamConfig;

  private final long _start = System.currentTimeMillis();
  private long _segmentEndTimeThreshold;
  private AtomicLong _lastUpdatedRawDocuments = new AtomicLong(0);

  private volatile boolean _keepIndexing = true;
  private volatile boolean _isShuttingDown = false;

  private final TimerTask _segmentStatusTask;
  private final ServerMetrics _serverMetrics;
  private final RealtimeTableDataManager _notifier;
  private Thread _indexingThread;

  private final String _sortedColumn;
  private final List<String> _invertedIndexColumns;
  private final Logger _segmentLogger;
  private final SegmentVersion _segmentVersion;

  private PinotMeter _tableAndStreamRowsConsumed = null;
  private PinotMeter _tableRowsConsumed = null;

  // An instance of this class exists only for the duration of the realtime segment that is currently being consumed.
  // Once the segment is committed, the segment is handled by OfflineSegmentDataManager
  public HLRealtimeSegmentDataManager(final SegmentZKMetadata segmentZKMetadata, final TableConfig tableConfig,
      InstanceZKMetadata instanceMetadata, final RealtimeTableDataManager realtimeTableDataManager,
      final String resourceDataDir, final IndexLoadingConfig indexLoadingConfig, final Schema schema,
      final ServerMetrics serverMetrics)
      throws Exception {
    super();
    _segmentVersion = indexLoadingConfig.getSegmentVersion();
    _recordTransformer = CompositeTransformer.getDefaultTransformer(tableConfig, schema);
    _serverMetrics = serverMetrics;
    _segmentName = segmentZKMetadata.getSegmentName();
    _tableNameWithType = tableConfig.getTableName();
    _timeColumnName = tableConfig.getValidationConfig().getTimeColumnName();
    Preconditions
        .checkNotNull(_timeColumnName, "Must provide valid timeColumnName in tableConfig for realtime table {}",
            _tableNameWithType);
    DateTimeFieldSpec dateTimeFieldSpec = schema.getSpecForTimeColumn(_timeColumnName);
    Preconditions.checkNotNull(dateTimeFieldSpec, "Must provide field spec for time column {}", _timeColumnName);
    _timeType = dateTimeFieldSpec.getFormatSpec().getColumnUnit();

    List<String> sortedColumns = indexLoadingConfig.getSortedColumns();
    if (sortedColumns.isEmpty()) {
      LOGGER.info("RealtimeDataResourceZKMetadata contains no information about sorted column for segment {}",
          _segmentName);
      _sortedColumn = null;
    } else {
      String firstSortedColumn = sortedColumns.get(0);
      if (schema.hasColumn(firstSortedColumn)) {
        LOGGER.info("Setting sorted column name: {} from RealtimeDataResourceZKMetadata for segment {}",
            firstSortedColumn, _segmentName);
        _sortedColumn = firstSortedColumn;
      } else {
        LOGGER
            .warn("Sorted column name: {} from RealtimeDataResourceZKMetadata is not existed in schema for segment {}.",
                firstSortedColumn, _segmentName);
        _sortedColumn = null;
      }
    }

    // Inverted index columns
    // We need to add sorted column into inverted index columns because when we convert realtime in memory segment into
    // offline segment, we use sorted column's inverted index to maintain the order of the records so that the records
    // are sorted on the sorted column.
    if (_sortedColumn != null) {
      indexLoadingConfig.addInvertedIndexColumns(_sortedColumn);
    }
    Set<String> invertedIndexColumns = indexLoadingConfig.getInvertedIndexColumns();
    _invertedIndexColumns = new ArrayList<>(invertedIndexColumns);
    _streamConfig = new StreamConfig(_tableNameWithType, IngestionConfigUtils.getStreamConfigMap(tableConfig));

    _segmentLogger = LoggerFactory.getLogger(
        HLRealtimeSegmentDataManager.class.getName() + "_" + _segmentName + "_" + _streamConfig.getTopicName());
    _segmentLogger.info("Created segment data manager with Sorted column:{}, invertedIndexColumns:{}", _sortedColumn,
        invertedIndexColumns);

    _segmentEndTimeThreshold = _start + _streamConfig.getFlushThresholdTimeMillis();
    _resourceTmpDir = new File(resourceDataDir, RESOURCE_TEMP_DIR_NAME);
    if (!_resourceTmpDir.exists()) {
      _resourceTmpDir.mkdirs();
    }
    // create and init stream level consumer
    StreamConsumerFactory streamConsumerFactory = StreamConsumerFactoryProvider.create(_streamConfig);
    String clientId = HLRealtimeSegmentDataManager.class.getSimpleName() + "-" + _streamConfig.getTopicName();
    Set<String> fieldsToRead = IngestionUtils.getFieldsForRecordExtractor(tableConfig.getIngestionConfig(), schema);
    _streamLevelConsumer = streamConsumerFactory.createStreamLevelConsumer(clientId, _tableNameWithType, fieldsToRead,
        instanceMetadata.getGroupId(_tableNameWithType));
    _streamLevelConsumer.start();
    _tableStreamName = _tableNameWithType + "_" + _streamConfig.getTopicName();

    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    if (indexingConfig != null && indexingConfig.isAggregateMetrics()) {
      LOGGER.warn("Updating of metrics only supported for LLC consumer, ignoring.");
    }

    // lets create a new realtime segment
    _segmentLogger.info("Started {} stream provider", _streamConfig.getType());
    final int capacity = _streamConfig.getFlushThresholdRows();
    boolean nullHandlingEnabled = indexingConfig != null && indexingConfig.isNullHandlingEnabled();
    RealtimeSegmentConfig realtimeSegmentConfig =
        new RealtimeSegmentConfig.Builder(indexLoadingConfig).setTableNameWithType(_tableNameWithType)
            .setSegmentName(_segmentName)
            .setStreamName(_streamConfig.getTopicName()).setSchema(schema).setTimeColumnName(_timeColumnName)
            .setCapacity(capacity).setAvgNumMultiValues(indexLoadingConfig.getRealtimeAvgMultiValueCount())
            .setSegmentZKMetadata(segmentZKMetadata)
            .setOffHeap(indexLoadingConfig.isRealtimeOffHeapAllocation()).setMemoryManager(
            getMemoryManager(realtimeTableDataManager.getConsumerDir(), _segmentName,
                indexLoadingConfig.isRealtimeOffHeapAllocation(),
                indexLoadingConfig.isDirectRealtimeOffHeapAllocation(), serverMetrics))
            .setStatsHistory(realtimeTableDataManager.getStatsHistory())
            .setNullHandlingEnabled(nullHandlingEnabled).build();
    _realtimeSegment = new MutableSegmentImpl(realtimeSegmentConfig, serverMetrics);

    _notifier = realtimeTableDataManager;

    LOGGER.info("Starting consumption on realtime consuming segment {} maxRowCount {} maxEndTime {}", _segmentName,
        capacity, new DateTime(_segmentEndTimeThreshold, DateTimeZone.UTC).toString());
    _segmentStatusTask = new TimerTask() {
      @Override
      public void run() {
        computeKeepIndexing();
      }
    };

    // start the indexing thread
    _indexingThread = new Thread(new Runnable() {
      @Override
      public void run() {
        // continue indexing until criteria is met
        boolean notFull = true;
        long exceptionSleepMillis = 50L;
        _segmentLogger.info("Starting to collect rows");

        int numRowsErrored = 0;
        GenericRow reuse = new GenericRow();
        do {
          reuse.clear();
          try {
            GenericRow consumedRow;
            try {
              consumedRow = _streamLevelConsumer.next(reuse);
              _tableAndStreamRowsConsumed = serverMetrics
                  .addMeteredTableValue(_tableStreamName, ServerMeter.REALTIME_ROWS_CONSUMED, 1L,
                      _tableAndStreamRowsConsumed);
              _tableRowsConsumed =
                  serverMetrics.addMeteredGlobalValue(ServerMeter.REALTIME_ROWS_CONSUMED, 1L, _tableRowsConsumed);
            } catch (Exception e) {
              _segmentLogger.warn("Caught exception while consuming row, sleeping for {} ms", exceptionSleepMillis, e);
              numRowsErrored++;
              serverMetrics.addMeteredTableValue(_tableStreamName, ServerMeter.REALTIME_CONSUMPTION_EXCEPTIONS, 1L);
              serverMetrics.addMeteredGlobalValue(ServerMeter.REALTIME_CONSUMPTION_EXCEPTIONS, 1L);

              // Sleep for a short time as to avoid filling the logs with exceptions too quickly
              Uninterruptibles.sleepUninterruptibly(exceptionSleepMillis, TimeUnit.MILLISECONDS);
              exceptionSleepMillis = Math.min(60000L, exceptionSleepMillis * 2);
              continue;
            }
            if (consumedRow != null) {
              try {
                GenericRow transformedRow = _recordTransformer.transform(consumedRow);
                // FIXME: handle MULTIPLE_RECORDS_KEY for HLL
                if (transformedRow != null && IngestionUtils.shouldIngestRow(transformedRow)) {
                  // we currently do not get ingestion data through stream-consumer
                  notFull = _realtimeSegment.index(transformedRow, null);
                  exceptionSleepMillis = 50L;
                }
              } catch (Exception e) {
                _segmentLogger.warn("Caught exception while indexing row, sleeping for {} ms, row contents {}",
                    exceptionSleepMillis, consumedRow, e);
                numRowsErrored++;

                // Sleep for a short time as to avoid filling the logs with exceptions too quickly
                Uninterruptibles.sleepUninterruptibly(exceptionSleepMillis, TimeUnit.MILLISECONDS);
                exceptionSleepMillis = Math.min(60000L, exceptionSleepMillis * 2);
              }
            }
          } catch (Error e) {
            _segmentLogger.error("Caught error in indexing thread", e);
            throw e;
          }
        } while (notFull && _keepIndexing && (!_isShuttingDown));

        if (_isShuttingDown) {
          _segmentLogger.info("Shutting down indexing thread!");
          return;
        }
        try {
          if (numRowsErrored > 0) {
            serverMetrics.addMeteredTableValue(_tableStreamName, ServerMeter.ROWS_WITH_ERRORS, numRowsErrored);
          }
          _segmentLogger.info("Indexing threshold reached, proceeding with index conversion");
          // kill the timer first
          _segmentStatusTask.cancel();
          updateCurrentDocumentCountMetrics();
          _segmentLogger.info("Indexed {} raw events", _realtimeSegment.getNumDocsIndexed());
          File tempSegmentFolder = new File(_resourceTmpDir, "tmp-" + System.currentTimeMillis());
          ColumnIndicesForRealtimeTable columnIndicesForRealtimeTable =
              new ColumnIndicesForRealtimeTable(_sortedColumn, _invertedIndexColumns, Collections.emptyList(),
                  Collections.emptyList(), new ArrayList<>(indexLoadingConfig.getNoDictionaryColumns()),
                  new ArrayList<>(indexLoadingConfig.getVarLengthDictionaryColumns()));
          // lets convert the segment now
          RealtimeSegmentConverter converter =
              new RealtimeSegmentConverter(_realtimeSegment, null, tempSegmentFolder.getAbsolutePath(),
                  schema, _tableNameWithType, tableConfig, segmentZKMetadata.getSegmentName(),
                  columnIndicesForRealtimeTable, indexingConfig.isNullHandlingEnabled());

          _segmentLogger.info("Trying to build segment");
          final long buildStartTime = System.nanoTime();
          converter.build(_segmentVersion, serverMetrics);
          final long buildEndTime = System.nanoTime();
          _segmentLogger.info("Built segment in {} ms",
              TimeUnit.MILLISECONDS.convert((buildEndTime - buildStartTime), TimeUnit.NANOSECONDS));
          File destDir = new File(resourceDataDir, segmentZKMetadata.getSegmentName());
          FileUtils.deleteQuietly(destDir);
          FileUtils.moveDirectory(tempSegmentFolder.listFiles()[0], destDir);

          FileUtils.deleteQuietly(tempSegmentFolder);
          long segStartTime = _realtimeSegment.getMinTime();
          long segEndTime = _realtimeSegment.getMaxTime();

          _segmentLogger.info("Committing {} offsets", _streamConfig.getType());
          boolean commitSuccessful = false;
          try {
            _streamLevelConsumer.commit();
            commitSuccessful = true;
            _streamLevelConsumer.shutdown();
            _segmentLogger
                .info("Successfully committed {} offsets, consumer release requested.", _streamConfig.getType());
            serverMetrics.addMeteredTableValue(_tableStreamName, ServerMeter.REALTIME_OFFSET_COMMITS, 1L);
            serverMetrics.addMeteredGlobalValue(ServerMeter.REALTIME_OFFSET_COMMITS, 1L);
          } catch (Throwable e) {
            // If we got here, it means that either the commit or the shutdown failed. Considering that the
            // KafkaConsumerManager delays shutdown and only adds the consumer to be released in a deferred way, this
            // likely means that writing the Kafka offsets failed.
            //
            // The old logic (mark segment as done, then commit offsets and shutdown the consumer immediately) would die
            // in a terrible way, leaving the consumer open and causing us to only get half the records from that point
            // on. In this case, because we keep the consumer open for a little while, we should be okay if the
            // controller reassigns us a new segment before the consumer gets released. Hopefully by the next time that
            // we get to committing the offsets, the transient ZK failure that caused the write to fail will not
            // happen again and everything will be good.
            //
            // Several things can happen:
            // - The controller reassigns us a new segment before we release the consumer (KafkaConsumerManager will
            //   keep the consumer open for about a minute, which should be enough time for the controller to reassign
            //   us a new segment) and the next time we close the segment the offsets commit successfully; we're good.
            // - The controller reassigns us a new segment, but after we released the consumer (if the controller was
            //   down or there was a ZK failure on writing the Kafka offsets but not the Helix state). We lose whatever
            //   data was in this segment. Not good.
            // - The server crashes after this comment and before we mark the current segment as done; if the Kafka
            //   offsets didn't get written, then when the server restarts it'll start consuming the current segment
            //   from the previously committed offsets; we're good.
            // - The server crashes after this comment, the Kafka offsets were written but the segment wasn't marked as
            //   done in Helix, but we got a failure (or not) on the commit; we lose whatever data was in this segment
            //   if we restart the server (not good). If we manually mark the segment as done in Helix by editing the
            //   state in ZK, everything is good, we'll consume a new segment that starts from the correct offsets.
            //
            // This is still better than the previous logic, which would have these failure modes:
            // - Consumer was left open and the controller reassigned us a new segment; consume only half the events
            //   (because there are two consumers and Kafka will try to rebalance partitions between those two)
            // - We got a segment assigned to us before we got around to committing the offsets, reconsume the data that
            //   we got in this segment again, as we're starting consumption from the previously committed offset (eg.
            //   duplicate data).
            //
            // This is still not very satisfactory, which is why this part is due for a redesign.
            //
            // Assuming you got here because the realtime offset commit metric has fired, check the logs to determine
            // which of the above scenarios happened. If you're in one of the good scenarios, then there's nothing to
            // do. If you're not, then based on how critical it is to get those rows back, then your options are:
            // - Wipe the realtime table and reconsume everything (mark the replica as disabled so that clients don't
            //   see query results from partially consumed data, then re-enable it when this replica has caught up)
            // - Accept that those rows are gone in this replica and move on (they'll be replaced by good offline data
            //   soon anyway)
            // - If there's a replica that has consumed properly, you could shut it down, copy its segments onto this
            //   replica, assign a new consumer group id to this replica, rename the copied segments and edit their
            //   metadata to reflect the new consumer group id, copy the Kafka offsets from the shutdown replica onto
            //   the new consumer group id and then restart both replicas. This should get you the missing rows.

            _segmentLogger
                .error("FATAL: Exception committing or shutting down consumer commitSuccessful={}", commitSuccessful,
                    e);
            serverMetrics.addMeteredTableValue(_tableNameWithType, ServerMeter.REALTIME_OFFSET_COMMIT_EXCEPTIONS, 1L);
            if (!commitSuccessful) {
              _streamLevelConsumer.shutdown();
            }
          }

          try {
            _segmentLogger.info("Marking current segment as completed in Helix");
            SegmentZKMetadata metadataToOverwrite = new SegmentZKMetadata(segmentZKMetadata.getSegmentName());
            metadataToOverwrite.setStatus(Status.DONE);
            metadataToOverwrite.setStartTime(segStartTime);
            metadataToOverwrite.setEndTime(segEndTime);
            metadataToOverwrite.setTimeUnit(_timeType);
            metadataToOverwrite.setTotalDocs(_realtimeSegment.getNumDocsIndexed());
            _notifier.replaceHLSegment(metadataToOverwrite, indexLoadingConfig);
            _segmentLogger
                .info("Completed write of segment completion to Helix, waiting for controller to assign a new segment");
          } catch (Exception e) {
            if (commitSuccessful) {
              _segmentLogger.error(
                  "Offsets were committed to Kafka but we were unable to mark this segment as completed in Helix. "
                      + "Manually mark the segment as completed in Helix; restarting this instance will result in "
                      + "data loss.", e);
            } else {
              _segmentLogger.warn(
                  "Caught exception while marking segment as completed in Helix. Offsets were not written, restarting"
                      + " the instance should be safe.", e);
            }
          }
        } catch (Exception e) {
          _segmentLogger.error("Caught exception in the realtime indexing thread", e);
        }
      }
    });

    _indexingThread.start();
    serverMetrics.addValueToTableGauge(_tableNameWithType, ServerGauge.SEGMENT_COUNT, 1L);
    _segmentLogger.debug("scheduling keepIndexing timer check");
    // start a schedule timer to keep track of the segment
    TimerService.TIMER.schedule(_segmentStatusTask, ONE_MINUTE_IN_MILLSEC, ONE_MINUTE_IN_MILLSEC);
    _segmentLogger.info("finished scheduling keepIndexing timer check");
  }

  @Override
  public MutableSegment getSegment() {
    return _realtimeSegment;
  }

  @Override
  public Map<String, String> getPartitionToCurrentOffset() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ConsumerState getConsumerState() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getLastConsumedTimestamp() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<String, ConsumerPartitionState> getConsumerPartitionState() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<String, PartitionLagState> getPartitionToLagState(
      Map<String, ConsumerPartitionState> consumerPartitionStateMap) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getSegmentName() {
    return _segmentName;
  }

  private void computeKeepIndexing() {
    if (_keepIndexing) {
      _segmentLogger.debug("Current indexed {} raw events", _realtimeSegment.getNumDocsIndexed());
      if ((System.currentTimeMillis() >= _segmentEndTimeThreshold)
          || _realtimeSegment.getNumDocsIndexed() >= _streamConfig.getFlushThresholdRows()) {
        if (_realtimeSegment.getNumDocsIndexed() == 0) {
          _segmentLogger.info("no new events coming in, extending the end time by another hour");
          _segmentEndTimeThreshold = System.currentTimeMillis() + _streamConfig.getFlushThresholdTimeMillis();
          return;
        }
        _segmentLogger.info(
            "Stopped indexing due to reaching segment limit: {} raw documents indexed, segment is aged {} minutes",
            _realtimeSegment.getNumDocsIndexed(), ((System.currentTimeMillis() - _start) / (ONE_MINUTE_IN_MILLSEC)));
        _keepIndexing = false;
      }
    }
    updateCurrentDocumentCountMetrics();
  }

  private void updateCurrentDocumentCountMetrics() {
    int currentRawDocs = _realtimeSegment.getNumDocsIndexed();
    _serverMetrics.addValueToTableGauge(_tableNameWithType, ServerGauge.DOCUMENT_COUNT,
        (currentRawDocs - _lastUpdatedRawDocuments.get()));
    _lastUpdatedRawDocuments.set(currentRawDocs);
  }

  @Override
  protected void doDestroy() {
    LOGGER.info("Trying to shutdown RealtimeSegmentDataManager : {}!", _segmentName);
    _isShuttingDown = true;
    try {
      _streamLevelConsumer.shutdown();
    } catch (Exception e) {
      LOGGER.error("Failed to shutdown stream consumer!", e);
    }
    _keepIndexing = false;
    _segmentStatusTask.cancel();
    _realtimeSegment.destroy();
  }
}
