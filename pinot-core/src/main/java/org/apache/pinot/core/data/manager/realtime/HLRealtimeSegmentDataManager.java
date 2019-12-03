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

import com.google.common.util.concurrent.Uninterruptibles;
import com.yammer.metrics.core.Meter;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.config.IndexingConfig;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.common.metadata.instance.InstanceZKMetadata;
import org.apache.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.CommonConstants.Segment.Realtime.Status;
import org.apache.pinot.common.utils.CommonConstants.Segment.SegmentType;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.core.data.recordtransformer.CompositeTransformer;
import org.apache.pinot.core.data.recordtransformer.RecordTransformer;
import org.apache.pinot.core.indexsegment.generator.SegmentVersion;
import org.apache.pinot.core.indexsegment.mutable.MutableSegment;
import org.apache.pinot.core.indexsegment.mutable.MutableSegmentImpl;
import org.apache.pinot.core.realtime.converter.RealtimeSegmentConverter;
import org.apache.pinot.core.realtime.impl.RealtimeSegmentConfig;
import org.apache.pinot.core.realtime.stream.StreamConfig;
import org.apache.pinot.core.realtime.stream.StreamConsumerFactory;
import org.apache.pinot.core.realtime.stream.StreamConsumerFactoryProvider;
import org.apache.pinot.core.realtime.stream.StreamLevelConsumer;
import org.apache.pinot.core.segment.index.loader.IndexLoadingConfig;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HLRealtimeSegmentDataManager extends RealtimeSegmentDataManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(HLRealtimeSegmentDataManager.class);
  private final static long ONE_MINUTE_IN_MILLSEC = 1000 * 60;

  private final String tableNameWithType;
  private final String segmentName;
  private final Schema schema;
  private final String timeColumnName;
  private final RecordTransformer _recordTransformer;
  private final RealtimeSegmentZKMetadata segmentMetatdaZk;

  private final StreamConsumerFactory _streamConsumerFactory;
  private final StreamLevelConsumer _streamLevelConsumer;
  private final File resourceDir;
  private final File resourceTmpDir;
  private final MutableSegmentImpl realtimeSegment;
  private final String tableStreamName;
  private final StreamConfig _streamConfig;

  private final long start = System.currentTimeMillis();
  private long segmentEndTimeThreshold;
  private AtomicLong lastUpdatedRawDocuments = new AtomicLong(0);

  private volatile boolean keepIndexing = true;
  private volatile boolean isShuttingDown = false;

  private TimerTask segmentStatusTask;
  private final ServerMetrics serverMetrics;
  private final RealtimeTableDataManager notifier;
  private Thread indexingThread;

  private final String sortedColumn;
  private final List<String> invertedIndexColumns;
  private final List<String> noDictionaryColumns;
  private final List<String> varLengthDictionaryColumns;
  private Logger segmentLogger = LOGGER;
  private final SegmentVersion _segmentVersion;

  private Meter tableAndStreamRowsConsumed = null;
  private Meter tableRowsConsumed = null;

  // An instance of this class exists only for the duration of the realtime segment that is currently being consumed.
  // Once the segment is committed, the segment is handled by OfflineSegmentDataManager
  public HLRealtimeSegmentDataManager(final RealtimeSegmentZKMetadata realtimeSegmentZKMetadata,
      final TableConfig tableConfig, InstanceZKMetadata instanceMetadata,
      final RealtimeTableDataManager realtimeTableDataManager, final String resourceDataDir,
      final IndexLoadingConfig indexLoadingConfig, final Schema schema, final ServerMetrics serverMetrics)
      throws Exception {
    super();
    _segmentVersion = indexLoadingConfig.getSegmentVersion();
    this.schema = schema;
    _recordTransformer = CompositeTransformer.getDefaultTransformer(schema);
    this.serverMetrics = serverMetrics;
    this.segmentName = realtimeSegmentZKMetadata.getSegmentName();
    this.tableNameWithType = tableConfig.getTableName();
    this.timeColumnName = tableConfig.getValidationConfig().getTimeColumnName();

    List<String> sortedColumns = indexLoadingConfig.getSortedColumns();
    if (sortedColumns.isEmpty()) {
      LOGGER.info("RealtimeDataResourceZKMetadata contains no information about sorted column for segment {}",
          segmentName);
      this.sortedColumn = null;
    } else {
      String firstSortedColumn = sortedColumns.get(0);
      if (this.schema.hasColumn(firstSortedColumn)) {
        LOGGER.info("Setting sorted column name: {} from RealtimeDataResourceZKMetadata for segment {}",
            firstSortedColumn, segmentName);
        this.sortedColumn = firstSortedColumn;
      } else {
        LOGGER
            .warn("Sorted column name: {} from RealtimeDataResourceZKMetadata is not existed in schema for segment {}.",
                firstSortedColumn, segmentName);
        this.sortedColumn = null;
      }
    }

    // Inverted index columns
    Set<String> invertedIndexColumns = indexLoadingConfig.getInvertedIndexColumns();
    // We need to add sorted column into inverted index columns because when we convert realtime in memory segment into
    // offline segment, we use sorted column's inverted index to maintain the order of the records so that the records
    // are sorted on the sorted column.
    if (sortedColumn != null) {
      invertedIndexColumns.add(sortedColumn);
    }
    this.invertedIndexColumns = new ArrayList<>(invertedIndexColumns);

    this.segmentMetatdaZk = realtimeSegmentZKMetadata;

    // No DictionaryColumns
    noDictionaryColumns = new ArrayList<>(indexLoadingConfig.getNoDictionaryColumns());

    varLengthDictionaryColumns = new ArrayList<>(indexLoadingConfig.getVarLengthDictionaryColumns());

    _streamConfig = new StreamConfig(tableNameWithType, tableConfig.getIndexingConfig().getStreamConfigs());

    segmentLogger = LoggerFactory.getLogger(
        HLRealtimeSegmentDataManager.class.getName() + "_" + segmentName + "_" + _streamConfig.getTopicName());
    segmentLogger.info("Created segment data manager with Sorted column:{}, invertedIndexColumns:{}", sortedColumn,
        this.invertedIndexColumns);

    segmentEndTimeThreshold = start + _streamConfig.getFlushThresholdTimeMillis();

    this.resourceDir = new File(resourceDataDir);
    this.resourceTmpDir = new File(resourceDataDir, "_tmp");
    if (!resourceTmpDir.exists()) {
      resourceTmpDir.mkdirs();
    }
    // create and init stream level consumer
    _streamConsumerFactory = StreamConsumerFactoryProvider.create(_streamConfig);
    String clientId = HLRealtimeSegmentDataManager.class.getSimpleName() + "-" + _streamConfig.getTopicName();
    _streamLevelConsumer = _streamConsumerFactory
        .createStreamLevelConsumer(clientId, tableNameWithType, schema, instanceMetadata.getGroupId(tableNameWithType));
    _streamLevelConsumer.start();

    tableStreamName = tableNameWithType + "_" + _streamConfig.getTopicName();

    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    if (indexingConfig != null && indexingConfig.isAggregateMetrics()) {
      LOGGER.warn("Updating of metrics only supported for LLC consumer, ignoring.");
    }

    // lets create a new realtime segment
    segmentLogger.info("Started {} stream provider", _streamConfig.getType());
    final int capacity = _streamConfig.getFlushThresholdRows();
    RealtimeSegmentConfig realtimeSegmentConfig =
        new RealtimeSegmentConfig.Builder().setSegmentName(segmentName).setStreamName(_streamConfig.getTopicName())
            .setSchema(schema).setCapacity(capacity)
            .setAvgNumMultiValues(indexLoadingConfig.getRealtimeAvgMultiValueCount())
            .setNoDictionaryColumns(indexLoadingConfig.getNoDictionaryColumns())
            .setVarLengthDictionaryColumns(indexLoadingConfig.getVarLengthDictionaryColumns())
            .setInvertedIndexColumns(invertedIndexColumns).setRealtimeSegmentZKMetadata(realtimeSegmentZKMetadata)
            .setOffHeap(indexLoadingConfig.isRealtimeOffheapAllocation()).setMemoryManager(
            getMemoryManager(realtimeTableDataManager.getConsumerDir(), segmentName,
                indexLoadingConfig.isRealtimeOffheapAllocation(),
                indexLoadingConfig.isDirectRealtimeOffheapAllocation(), serverMetrics))
            .setStatsHistory(realtimeTableDataManager.getStatsHistory())
            .setNullHandlingEnabled(indexingConfig.isNullHandlingEnabled()).build();
    realtimeSegment = new MutableSegmentImpl(realtimeSegmentConfig);

    notifier = realtimeTableDataManager;

    LOGGER.info("Starting consumption on realtime consuming segment {} maxRowCount {} maxEndTime {}", segmentName,
        capacity, new DateTime(segmentEndTimeThreshold, DateTimeZone.UTC).toString());
    segmentStatusTask = new TimerTask() {
      @Override
      public void run() {
        computeKeepIndexing();
      }
    };

    // start the indexing thread
    indexingThread = new Thread(new Runnable() {
      @Override
      public void run() {
        // continue indexing until criteria is met
        boolean notFull = true;
        long exceptionSleepMillis = 50L;
        segmentLogger.info("Starting to collect rows");

        int numRowsErrored = 0;
        GenericRow reuse = new GenericRow();
        do {
          reuse.clear();
          try {
            GenericRow consumedRow;
            try {
              consumedRow = _streamLevelConsumer.next(reuse);
              tableAndStreamRowsConsumed = serverMetrics
                  .addMeteredTableValue(tableStreamName, ServerMeter.REALTIME_ROWS_CONSUMED, 1L,
                      tableAndStreamRowsConsumed);
              tableRowsConsumed =
                  serverMetrics.addMeteredGlobalValue(ServerMeter.REALTIME_ROWS_CONSUMED, 1L, tableRowsConsumed);
            } catch (Exception e) {
              segmentLogger.warn("Caught exception while consuming row, sleeping for {} ms", exceptionSleepMillis, e);
              numRowsErrored++;
              serverMetrics.addMeteredTableValue(tableStreamName, ServerMeter.REALTIME_CONSUMPTION_EXCEPTIONS, 1L);
              serverMetrics.addMeteredGlobalValue(ServerMeter.REALTIME_CONSUMPTION_EXCEPTIONS, 1L);

              // Sleep for a short time as to avoid filling the logs with exceptions too quickly
              Uninterruptibles.sleepUninterruptibly(exceptionSleepMillis, TimeUnit.MILLISECONDS);
              exceptionSleepMillis = Math.min(60000L, exceptionSleepMillis * 2);
              continue;
            }
            if (consumedRow != null) {
              try {
                GenericRow transformedRow = _recordTransformer.transform(consumedRow);
                if (transformedRow != null) {
                  // we currently do not get ingestion data through stream-consumer
                  notFull = realtimeSegment.index(transformedRow, null);
                  exceptionSleepMillis = 50L;
                }
              } catch (Exception e) {
                segmentLogger.warn("Caught exception while indexing row, sleeping for {} ms, row contents {}",
                    exceptionSleepMillis, consumedRow, e);
                numRowsErrored++;

                // Sleep for a short time as to avoid filling the logs with exceptions too quickly
                Uninterruptibles.sleepUninterruptibly(exceptionSleepMillis, TimeUnit.MILLISECONDS);
                exceptionSleepMillis = Math.min(60000L, exceptionSleepMillis * 2);
              }
            }
          } catch (Error e) {
            segmentLogger.error("Caught error in indexing thread", e);
            throw e;
          }
        } while (notFull && keepIndexing && (!isShuttingDown));

        if (isShuttingDown) {
          segmentLogger.info("Shutting down indexing thread!");
          return;
        }
        try {
          if (numRowsErrored > 0) {
            serverMetrics.addMeteredTableValue(tableStreamName, ServerMeter.ROWS_WITH_ERRORS, numRowsErrored);
          }
          segmentLogger.info("Indexing threshold reached, proceeding with index conversion");
          // kill the timer first
          segmentStatusTask.cancel();
          updateCurrentDocumentCountMetrics();
          segmentLogger.info("Indexed {} raw events", realtimeSegment.getNumDocsIndexed());
          File tempSegmentFolder = new File(resourceTmpDir, "tmp-" + System.currentTimeMillis());

          // lets convert the segment now
          RealtimeSegmentConverter converter =
              new RealtimeSegmentConverter(realtimeSegment, tempSegmentFolder.getAbsolutePath(), schema,
                  tableNameWithType, timeColumnName, realtimeSegmentZKMetadata.getSegmentName(), sortedColumn,
                  HLRealtimeSegmentDataManager.this.invertedIndexColumns, noDictionaryColumns,
                  varLengthDictionaryColumns, null/*StarTreeIndexSpec*/, indexingConfig.isNullHandlingEnabled()); // Star tree not supported for HLC.

          segmentLogger.info("Trying to build segment");
          final long buildStartTime = System.nanoTime();
          converter.build(_segmentVersion, serverMetrics);
          final long buildEndTime = System.nanoTime();
          segmentLogger.info("Built segment in {} ms",
              TimeUnit.MILLISECONDS.convert((buildEndTime - buildStartTime), TimeUnit.NANOSECONDS));
          File destDir = new File(resourceDataDir, realtimeSegmentZKMetadata.getSegmentName());
          FileUtils.deleteQuietly(destDir);
          FileUtils.moveDirectory(tempSegmentFolder.listFiles()[0], destDir);

          FileUtils.deleteQuietly(tempSegmentFolder);
          long segStartTime = realtimeSegment.getMinTime();
          long segEndTime = realtimeSegment.getMaxTime();

          segmentLogger.info("Committing {} offsets", _streamConfig.getType());
          boolean commitSuccessful = false;
          try {
            _streamLevelConsumer.commit();
            commitSuccessful = true;
            _streamLevelConsumer.shutdown();
            segmentLogger
                .info("Successfully committed {} offsets, consumer release requested.", _streamConfig.getType());
            serverMetrics.addMeteredTableValue(tableStreamName, ServerMeter.REALTIME_OFFSET_COMMITS, 1L);
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

            segmentLogger
                .error("FATAL: Exception committing or shutting down consumer commitSuccessful={}", commitSuccessful,
                    e);
            serverMetrics.addMeteredTableValue(tableNameWithType, ServerMeter.REALTIME_OFFSET_COMMIT_EXCEPTIONS, 1L);
            if (!commitSuccessful) {
              _streamLevelConsumer.shutdown();
            }
          }

          try {
            segmentLogger.info("Marking current segment as completed in Helix");
            RealtimeSegmentZKMetadata metadataToOverwrite = new RealtimeSegmentZKMetadata();
            metadataToOverwrite.setTableName(tableNameWithType);
            metadataToOverwrite.setSegmentName(realtimeSegmentZKMetadata.getSegmentName());
            metadataToOverwrite.setSegmentType(SegmentType.OFFLINE);
            metadataToOverwrite.setStatus(Status.DONE);
            metadataToOverwrite.setStartTime(segStartTime);
            metadataToOverwrite.setEndTime(segEndTime);
            metadataToOverwrite.setTimeUnit(schema.getOutgoingTimeUnit());
            metadataToOverwrite.setTotalRawDocs(realtimeSegment.getNumDocsIndexed());
            notifier.replaceHLSegment(metadataToOverwrite, indexLoadingConfig);
            segmentLogger
                .info("Completed write of segment completion to Helix, waiting for controller to assign a new segment");
          } catch (Exception e) {
            if (commitSuccessful) {
              segmentLogger.error(
                  "Offsets were committed to Kafka but we were unable to mark this segment as completed in Helix. Manually mark the segment as completed in Helix; restarting this instance will result in data loss.",
                  e);
            } else {
              segmentLogger.warn(
                  "Caught exception while marking segment as completed in Helix. Offsets were not written, restarting the instance should be safe.",
                  e);
            }
          }
        } catch (Exception e) {
          segmentLogger.error("Caught exception in the realtime indexing thread", e);
        }
      }
    });

    indexingThread.start();
    serverMetrics.addValueToTableGauge(tableNameWithType, ServerGauge.SEGMENT_COUNT, 1L);
    segmentLogger.debug("scheduling keepIndexing timer check");
    // start a schedule timer to keep track of the segment
    TimerService.timer.schedule(segmentStatusTask, ONE_MINUTE_IN_MILLSEC, ONE_MINUTE_IN_MILLSEC);
    segmentLogger.info("finished scheduling keepIndexing timer check");
  }

  @Override
  public MutableSegment getSegment() {
    return realtimeSegment;
  }

  @Override
  public String getSegmentName() {
    return segmentName;
  }

  private void computeKeepIndexing() {
    if (keepIndexing) {
      segmentLogger.debug("Current indexed {} raw events", realtimeSegment.getNumDocsIndexed());
      if ((System.currentTimeMillis() >= segmentEndTimeThreshold)
          || realtimeSegment.getNumDocsIndexed() >= _streamConfig.getFlushThresholdRows()) {
        if (realtimeSegment.getNumDocsIndexed() == 0) {
          segmentLogger.info("no new events coming in, extending the end time by another hour");
          segmentEndTimeThreshold = System.currentTimeMillis() + _streamConfig.getFlushThresholdTimeMillis();
          return;
        }
        segmentLogger.info(
            "Stopped indexing due to reaching segment limit: {} raw documents indexed, segment is aged {} minutes",
            realtimeSegment.getNumDocsIndexed(), ((System.currentTimeMillis() - start) / (ONE_MINUTE_IN_MILLSEC)));
        keepIndexing = false;
      }
    }
    updateCurrentDocumentCountMetrics();
  }

  private void updateCurrentDocumentCountMetrics() {
    int currentRawDocs = realtimeSegment.getNumDocsIndexed();
    serverMetrics.addValueToTableGauge(tableNameWithType, ServerGauge.DOCUMENT_COUNT,
        (currentRawDocs - lastUpdatedRawDocuments.get()));
    lastUpdatedRawDocuments.set(currentRawDocs);
  }

  @Override
  public void destroy() {
    LOGGER.info("Trying to shutdown RealtimeSegmentDataManager : {}!", this.segmentName);
    isShuttingDown = true;
    try {
      _streamLevelConsumer.shutdown();
    } catch (Exception e) {
      LOGGER.error("Failed to shutdown stream consumer!", e);
    }
    keepIndexing = false;
    segmentStatusTask.cancel();
    realtimeSegment.destroy();
  }
}
