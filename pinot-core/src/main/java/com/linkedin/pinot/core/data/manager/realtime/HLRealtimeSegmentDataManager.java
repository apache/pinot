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

import com.google.common.util.concurrent.Uninterruptibles;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metadata.instance.InstanceZKMetadata;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.metrics.ServerGauge;
import com.linkedin.pinot.common.metrics.ServerMeter;
import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.common.utils.CommonConstants.Segment.Realtime.Status;
import com.linkedin.pinot.common.utils.CommonConstants.Segment.SegmentType;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.extractors.FieldExtractorFactory;
import com.linkedin.pinot.core.data.extractors.PlainFieldExtractor;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentLoader;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.realtime.StreamProvider;
import com.linkedin.pinot.core.realtime.StreamProviderConfig;
import com.linkedin.pinot.core.realtime.StreamProviderFactory;
import com.linkedin.pinot.core.realtime.converter.RealtimeSegmentConverter;
import com.linkedin.pinot.core.realtime.impl.RealtimeSegmentConfig;
import com.linkedin.pinot.core.realtime.impl.RealtimeSegmentImpl;
import com.linkedin.pinot.core.realtime.impl.kafka.KafkaHighLevelStreamProviderConfig;
import com.linkedin.pinot.core.segment.index.loader.IndexLoadingConfig;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HLRealtimeSegmentDataManager extends RealtimeSegmentDataManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(HLRealtimeSegmentDataManager.class);
  private final static long ONE_MINUTE_IN_MILLSEC = 1000 * 60;

  private final String tableName;
  private final String segmentName;
  private final Schema schema;
  private final PlainFieldExtractor extractor;
  private final RealtimeSegmentZKMetadata segmentMetatdaZk;

  private final StreamProviderConfig kafkaStreamProviderConfig;
  private final StreamProvider kafkaStreamProvider;
  private final File resourceDir;
  private final File resourceTmpDir;
  private final RealtimeSegmentImpl realtimeSegment;
  private final String tableStreamName;

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
  private Logger segmentLogger = LOGGER;
  private final SegmentVersion _segmentVersion;

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
    this.extractor = FieldExtractorFactory.getPlainFieldExtractor(schema);
    this.serverMetrics =serverMetrics;
    this.segmentName = realtimeSegmentZKMetadata.getSegmentName();
    this.tableName = tableConfig.getTableName();

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
        LOGGER.warn(
            "Sorted column name: {} from RealtimeDataResourceZKMetadata is not existed in schema for segment {}.",
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

    // create and init stream provider config
    // TODO : ideally resourceMetatda should create and give back a streamProviderConfig
    this.kafkaStreamProviderConfig = new KafkaHighLevelStreamProviderConfig();
    this.kafkaStreamProviderConfig.init(tableConfig, instanceMetadata, schema);
    segmentLogger = LoggerFactory.getLogger(HLRealtimeSegmentDataManager.class.getName() +
            "_" + segmentName +
            "_" + kafkaStreamProviderConfig.getStreamName()
    );
    segmentLogger.info("Created segment data manager with Sorted column:{}, invertedIndexColumns:{}", sortedColumn,
        this.invertedIndexColumns);

    segmentEndTimeThreshold = start + kafkaStreamProviderConfig.getTimeThresholdToFlushSegment();

    this.resourceDir = new File(resourceDataDir);
    this.resourceTmpDir = new File(resourceDataDir, "_tmp");
    if (!resourceTmpDir.exists()) {
      resourceTmpDir.mkdirs();
    }
    // create and init stream provider
    final String tableName = tableConfig.getTableName();
    this.kafkaStreamProvider = StreamProviderFactory.buildStreamProvider();
    this.kafkaStreamProvider.init(kafkaStreamProviderConfig, tableName, serverMetrics);
    this.kafkaStreamProvider.start();
    this.tableStreamName = tableName + "_" + kafkaStreamProviderConfig.getStreamName();

    // lets create a new realtime segment
    segmentLogger.info("Started kafka stream provider");
    final int capacity = kafkaStreamProviderConfig.getSizeThresholdToFlushSegment();
    RealtimeSegmentConfig realtimeSegmentConfig = new RealtimeSegmentConfig.Builder().setSegmentName(segmentName)
        .setStreamName(kafkaStreamProviderConfig.getStreamName())
        .setSchema(schema)
        .setCapacity(capacity)
        .setAvgNumMultiValues(indexLoadingConfig.getRealtimeAvgMultiValueCount())
        .setNoDictionaryColumns(indexLoadingConfig.getNoDictionaryColumns())
        .setInvertedIndexColumns(invertedIndexColumns)
        .setRealtimeSegmentZKMetadata(realtimeSegmentZKMetadata)
        .setOffHeap(indexLoadingConfig.isRealtimeOffheapAllocation())
        .setMemoryManager(getMemoryManager(realtimeTableDataManager.getConsumerDir(), segmentName,
            indexLoadingConfig.isRealtimeOffheapAllocation(), indexLoadingConfig.isDirectRealtimeOffheapAllocation(),
            serverMetrics))
        .setStatsHistory(realtimeTableDataManager.getStatsHistory())
        .build();
    realtimeSegment = new RealtimeSegmentImpl(realtimeSegmentConfig);

    notifier = realtimeTableDataManager;

    LOGGER.info("Starting consumption on realtime consuming segment {} maxRowCount {} maxEndTime {}",
        segmentName, capacity, new DateTime(segmentEndTimeThreshold, DateTimeZone.UTC).toString());
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

        do {
          GenericRow readRow = null;
          GenericRow transformedRow = null;
          GenericRow row = null;
          try {
            readRow = GenericRow.createOrReuseRow(readRow);
            readRow = kafkaStreamProvider.next(readRow);
            row = readRow;

            if (readRow != null) {
              transformedRow = GenericRow.createOrReuseRow(transformedRow);
              transformedRow = extractor.transform(readRow, transformedRow);
              row = transformedRow;
              notFull = realtimeSegment.index(transformedRow);
              exceptionSleepMillis = 50L;
            }
          } catch (Exception e) {
            segmentLogger.warn("Caught exception while indexing row, sleeping for {} ms, row contents {}",
                exceptionSleepMillis, row, e);

            // Sleep for a short time as to avoid filling the logs with exceptions too quickly
            Uninterruptibles.sleepUninterruptibly(exceptionSleepMillis, TimeUnit.MILLISECONDS);
            exceptionSleepMillis = Math.min(60000L, exceptionSleepMillis * 2);
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
          int numErrors, numConversions, numNulls, numNullCols;
          if ((numErrors = extractor.getTotalErrors()) > 0) {
            serverMetrics.addMeteredTableValue(tableStreamName,
                ServerMeter.ROWS_WITH_ERRORS, (long) numErrors);
          }
          Map<String, Integer> errorCount = extractor.getErrorCount();
          for (String column : errorCount.keySet()) {
            if ((numErrors = errorCount.get(column)) > 0) {
              segmentLogger.warn("Column {} had {} rows with errors", column, numErrors);
            }
          }
          if ((numConversions = extractor.getTotalConversions()) > 0) {
            serverMetrics.addMeteredTableValue(tableStreamName,
                ServerMeter.ROWS_NEEDING_CONVERSIONS, (long) numConversions);
            segmentLogger.info("{} rows needed conversions ", numConversions);
          }
          if ((numNulls = extractor.getTotalNulls()) > 0) {
            serverMetrics.addMeteredTableValue(tableStreamName,
                ServerMeter.ROWS_WITH_NULL_VALUES, (long) numNulls);
            segmentLogger.info("{} rows had null columns", numNulls);
          }
          if ((numNullCols = extractor.getTotalNullCols()) > 0) {
            serverMetrics.addMeteredTableValue(tableStreamName,
                ServerMeter.COLUMNS_WITH_NULL_VALUES, (long) numNullCols);
            segmentLogger.info("{} columns had null values", numNullCols);
          }
          segmentLogger.info("Indexing threshold reached, proceeding with index conversion");
          // kill the timer first
          segmentStatusTask.cancel();
          updateCurrentDocumentCountMetrics();
          segmentLogger.info("Indexed {} raw events", realtimeSegment.getNumDocsIndexed());
          File tempSegmentFolder = new File(resourceTmpDir, "tmp-" + String.valueOf(System.currentTimeMillis()));

          // lets convert the segment now
          RealtimeSegmentConverter converter =
              new RealtimeSegmentConverter(realtimeSegment, tempSegmentFolder.getAbsolutePath(), schema,
                  realtimeSegmentZKMetadata.getTableName(), realtimeSegmentZKMetadata.getSegmentName(), sortedColumn,
                  HLRealtimeSegmentDataManager.this.invertedIndexColumns,
                  noDictionaryColumns, null/*StarTreeIndexSpec*/); // Star tree not supported for HLC.

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

          TimeUnit timeUnit = schema.getTimeFieldSpec().getOutgoingGranularitySpec().getTimeType();
          IndexSegment segment =
              ColumnarSegmentLoader.load(new File(resourceDir, segmentMetatdaZk.getSegmentName()), indexLoadingConfig);

          segmentLogger.info("Committing Kafka offsets");
          boolean commitSuccessful = false;
          try {
            kafkaStreamProvider.commit();
            commitSuccessful = true;
            kafkaStreamProvider.shutdown();
            segmentLogger.info("Successfully committed Kafka offsets, consumer release requested.");
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

            segmentLogger.error("FATAL: Exception committing or shutting down consumer commitSuccessful={}",
                commitSuccessful, e);
            serverMetrics.addMeteredTableValue(tableName, ServerMeter.REALTIME_OFFSET_COMMIT_EXCEPTIONS, 1L);
            if (!commitSuccessful) {
              kafkaStreamProvider.shutdown();
            }
          }

          try {
            segmentLogger.info("Marking current segment as completed in Helix");
            RealtimeSegmentZKMetadata metadataToOverwrite = new RealtimeSegmentZKMetadata();
            metadataToOverwrite.setTableName(realtimeSegmentZKMetadata.getTableName());
            metadataToOverwrite.setSegmentName(realtimeSegmentZKMetadata.getSegmentName());
            metadataToOverwrite.setSegmentType(SegmentType.OFFLINE);
            metadataToOverwrite.setStatus(Status.DONE);
            metadataToOverwrite.setStartTime(segStartTime);
            metadataToOverwrite.setEndTime(segEndTime);
            metadataToOverwrite.setTimeUnit(timeUnit);
            metadataToOverwrite.setTotalRawDocs(realtimeSegment.getNumDocsIndexed());
            notifier.notifySegmentCommitted(metadataToOverwrite, segment);
            segmentLogger.info("Completed write of segment completion to Helix, waiting for controller to assign a new segment");
          } catch (Exception e) {
            if (commitSuccessful) {
              segmentLogger.error("Offsets were committed to Kafka but we were unable to mark this segment as completed in Helix. Manually mark the segment as completed in Helix; restarting this instance will result in data loss.", e);
            } else {
              segmentLogger.warn("Caught exception while marking segment as completed in Helix. Offsets were not written, restarting the instance should be safe.", e);
            }
          }
        } catch (Exception e) {
          segmentLogger.error("Caught exception in the realtime indexing thread", e);
        }
      }
    });

    indexingThread.start();
    serverMetrics.addValueToTableGauge(tableName, ServerGauge.SEGMENT_COUNT, 1L);
    segmentLogger.debug("scheduling keepIndexing timer check");
    // start a schedule timer to keep track of the segment
    TimerService.timer.schedule(segmentStatusTask, ONE_MINUTE_IN_MILLSEC, ONE_MINUTE_IN_MILLSEC);
    segmentLogger.info("finished scheduling keepIndexing timer check");
  }

  @Override
  public IndexSegment getSegment() {
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
          || realtimeSegment.getNumDocsIndexed() >= kafkaStreamProviderConfig.getSizeThresholdToFlushSegment()) {
        if (realtimeSegment.getNumDocsIndexed() == 0) {
          segmentLogger.info("no new events coming in, extending the end time by another hour");
          segmentEndTimeThreshold =
              System.currentTimeMillis() + kafkaStreamProviderConfig.getTimeThresholdToFlushSegment();
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
    serverMetrics.addValueToTableGauge(tableName, ServerGauge.DOCUMENT_COUNT, (currentRawDocs - lastUpdatedRawDocuments.get()));
    lastUpdatedRawDocuments.set(currentRawDocs);
  }

  @Override
  public void destroy() {
    LOGGER.info("Trying to shutdown RealtimeSegmentDataManager : {}!", this.segmentName);
    isShuttingDown = true;
    try {
      kafkaStreamProvider.shutdown();
    } catch (Exception e) {
      LOGGER.error("Failed to shutdown kafka stream provider!", e);
    }
    keepIndexing = false;
    segmentStatusTask.cancel();
    realtimeSegment.destroy();
  }
}
