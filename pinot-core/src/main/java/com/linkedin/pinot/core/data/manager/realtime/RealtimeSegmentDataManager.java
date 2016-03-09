/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.common.metrics.ServerMetrics;
import java.io.File;
import java.util.List;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.plist.PropertyListConfiguration;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.util.concurrent.Uninterruptibles;
import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.common.config.IndexingConfig;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metadata.instance.InstanceZKMetadata;
import com.linkedin.pinot.common.metadata.segment.IndexLoadingConfigMetadata;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.utils.CommonConstants.Segment.Realtime.Status;
import com.linkedin.pinot.common.utils.CommonConstants.Segment.SegmentType;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.manager.offline.SegmentDataManager;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.realtime.StreamProvider;
import com.linkedin.pinot.core.realtime.StreamProviderConfig;
import com.linkedin.pinot.core.realtime.StreamProviderFactory;
import com.linkedin.pinot.core.realtime.converter.RealtimeSegmentConverter;
import com.linkedin.pinot.core.realtime.impl.RealtimeSegmentImpl;
import com.linkedin.pinot.core.realtime.impl.kafka.KafkaHighLevelStreamProviderConfig;
import com.linkedin.pinot.core.segment.index.loader.Loaders;


public class RealtimeSegmentDataManager extends SegmentDataManager {
  private static final Logger GLOBAL_LOGGER = LoggerFactory.getLogger(RealtimeSegmentDataManager.class);
  private final static long ONE_MINUTE_IN_MILLSEC = 1000 * 60;

  private final String segmentName;
  private final Schema schema;
  private final RealtimeSegmentZKMetadata segmentMetatdaZk;

  private final StreamProviderConfig kafkaStreamProviderConfig;
  private final StreamProvider kafkaStreamProvider;
  private final File resourceDir;
  private final File resourceTmpDir;
  private final Object lock = new Object();
  private RealtimeSegmentImpl realtimeSegment;

  private final long start = System.currentTimeMillis();
  private long segmentEndTimeThreshold;

  private volatile boolean keepIndexing = true;
  private TimerTask segmentStatusTask;
  private final RealtimeTableDataManager notifier;
  private Thread indexingThread;

  private final String sortedColumn;
  private final List<String> invertedIndexColumns;
  private Logger LOGGER = GLOBAL_LOGGER;

  // An instance of this class exists only for the duration of the realtime segment that is currently being consumed.
  // Once the segment is committed, the segment is handled by OfflineSegmentDataManager
  public RealtimeSegmentDataManager(final RealtimeSegmentZKMetadata segmentMetadata,
      final AbstractTableConfig tableConfig, InstanceZKMetadata instanceMetadata,
      RealtimeTableDataManager realtimeResourceManager, final String resourceDataDir, final ReadMode mode,
      final Schema schema, final ServerMetrics serverMetrics) throws Exception {
    super();
    this.schema = schema;
    this.segmentName = segmentMetadata.getSegmentName();
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    if (indexingConfig.getSortedColumn().isEmpty()) {
      GLOBAL_LOGGER.info("RealtimeDataResourceZKMetadata contains no information about sorted column for segment {}",
          segmentName);
      this.sortedColumn = null;
    } else {
      String firstSortedColumn = indexingConfig.getSortedColumn().get(0);
      if (this.schema.isExisted(firstSortedColumn)) {
        GLOBAL_LOGGER.info("Setting sorted column name: {} from RealtimeDataResourceZKMetadata for segment {}",
            firstSortedColumn, segmentName);
        this.sortedColumn = firstSortedColumn;
      } else {
        GLOBAL_LOGGER.warn(
            "Sorted column name: {} from RealtimeDataResourceZKMetadata is not existed in schema for segment {}.",
            firstSortedColumn, segmentName);
        this.sortedColumn = null;
      }
    }
    //inverted index columns
    invertedIndexColumns = indexingConfig.getInvertedIndexColumns();

    this.segmentMetatdaZk = segmentMetadata;

    // create and init stream provider config
    // TODO : ideally resourceMetatda should create and give back a streamProviderConfig
    this.kafkaStreamProviderConfig = new KafkaHighLevelStreamProviderConfig();
    this.kafkaStreamProviderConfig.init(tableConfig, instanceMetadata, schema);
    LOGGER = LoggerFactory.getLogger(RealtimeSegmentDataManager.class.getName() +
            "_" + segmentName +
            "_" + kafkaStreamProviderConfig.getStreamName()
    );
    LOGGER.info("Created segment data manager with Sorted column:{}, invertedIndexColumns:{}", sortedColumn,
        invertedIndexColumns);

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
    // lets create a new realtime segment
    LOGGER.info("Started kafka stream provider");
    realtimeSegment = new RealtimeSegmentImpl(schema, kafkaStreamProviderConfig.getSizeThresholdToFlushSegment(), tableName,
        segmentMetadata.getSegmentName(), kafkaStreamProviderConfig.getStreamName(), serverMetrics);
    realtimeSegment.setSegmentMetadata(segmentMetadata, this.schema);
    notifier = realtimeResourceManager;

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
        LOGGER.info("Starting to collect rows");

        do {
          GenericRow row = null;
          try {
            row = kafkaStreamProvider.next();

            if (row != null) {
              notFull = realtimeSegment.index(row);
              exceptionSleepMillis = 50L;
            }
          } catch (Exception e) {
            LOGGER.warn("Caught exception while indexing row, sleeping for {} ms, row contents {}",
                exceptionSleepMillis, row, e);

            // Sleep for a short time as to avoid filling the logs with exceptions too quickly
            Uninterruptibles.sleepUninterruptibly(exceptionSleepMillis, TimeUnit.MILLISECONDS);
            exceptionSleepMillis = Math.min(60000L, exceptionSleepMillis * 2);
          } catch (Error e) {
            LOGGER.error("Caught error in indexing thread", e);
            throw e;
          }
        } while (notFull && keepIndexing);

        try {
          LOGGER.info("Indexing threshold reached, proceeding with index conversion");
          // kill the timer first
          segmentStatusTask.cancel();
          LOGGER.info("Indexed {} raw events, current number of docs = {}",
              realtimeSegment.getRawDocumentCount(), realtimeSegment.getSegmentMetadata().getTotalDocs());
          File tempSegmentFolder = new File(resourceTmpDir, "tmp-" + String.valueOf(System.currentTimeMillis()));

          // lets convert the segment now
          RealtimeSegmentConverter converter =
              new RealtimeSegmentConverter(realtimeSegment, tempSegmentFolder.getAbsolutePath(), schema,
                  segmentMetadata.getTableName(), segmentMetadata.getSegmentName(), sortedColumn, invertedIndexColumns);

          LOGGER.info("Trying to build segment {}", segmentName);
          final long buildStartTime = System.nanoTime();
          converter.build();
          final long buildEndTime = System.nanoTime();
          LOGGER.info("Built segment in {}ms",
              TimeUnit.MILLISECONDS.convert((buildEndTime - buildStartTime), TimeUnit.NANOSECONDS));
          File destDir = new File(resourceDataDir, segmentMetadata.getSegmentName());
          FileUtils.deleteQuietly(destDir);
          FileUtils.moveDirectory(tempSegmentFolder.listFiles()[0], destDir);

          FileUtils.deleteQuietly(tempSegmentFolder);
          long segStartTime = realtimeSegment.getMinTime();
          long segEndTime = realtimeSegment.getMaxTime();

          TimeUnit timeUnit = schema.getTimeFieldSpec().getOutgoingGranularitySpec().getTimeType();
          RealtimeSegmentZKMetadata metadaToOverrite = new RealtimeSegmentZKMetadata();
          metadaToOverrite.setTableName(segmentMetadata.getTableName());
          metadaToOverrite.setSegmentName(segmentMetadata.getSegmentName());
          metadaToOverrite.setSegmentType(SegmentType.OFFLINE);
          metadaToOverrite.setStatus(Status.DONE);
          metadaToOverrite.setStartTime(segStartTime);
          metadaToOverrite.setEndTime(segEndTime);
          metadaToOverrite.setTotalRawDocs(realtimeSegment.getSegmentMetadata().getTotalDocs());
          metadaToOverrite.setTimeUnit(timeUnit);
          Configuration configuration = new PropertyListConfiguration();
          configuration.setProperty(IndexLoadingConfigMetadata.KEY_OF_LOADING_INVERTED_INDEX, invertedIndexColumns);
          IndexLoadingConfigMetadata configMetadata = new IndexLoadingConfigMetadata(configuration);
          IndexSegment segment = Loaders.IndexSegment.load(new File(resourceDir, segmentMetatdaZk.getSegmentName()), mode, configMetadata);
          notifier.notifySegmentCommitted(metadaToOverrite, segment);

          LOGGER.info("Committing Kafka offset");
          boolean commitSuccessful = false;
          try {
            kafkaStreamProvider.commit();
            commitSuccessful = true;
            kafkaStreamProvider.shutdown();
          } catch (Throwable e) {
            // We have already marked this segment as "DONE", so the controller will be giving us a new segment.
            // The new segment manager will start consumption with the same kafka group Id. One of the two scenarios can
            // happen:
            // a. We committed kafka offset successfully.  In that case, the new segment manager will consume events from
            //    the new check-point, but will get only half the events if we have not shut down this consumer correctly.
            // b. Kafka commit failed. In this case, we have not yet shutdown the consumer, so both consumers will be active,
            //    getting half the number of events.
            // Manual action needs to be taken at this point.
            // The server needs to be cleared of all segments, and consumption started afresh.
            // It is enough to start consumption from latest (since there are other servers that may be
            // serving recent data.
            LOGGER.error("FATAL: Exception committing or shutting down consumer commitSuccessful={}",
                commitSuccessful, e);
            if (!commitSuccessful) {
              kafkaStreamProvider.shutdown();
            }
            throw e;
          }
          LOGGER.info("Successfully closed and committed kafka", segmentName);
        } catch (Exception e) {
          LOGGER.error("Caught exception in the realtime indexing thread", e);
        }
      }
    });

    indexingThread.start();

    LOGGER.debug("scheduling keepIndexing timer check");
    // start a schedule timer to keep track of the segment
    TimerService.timer.schedule(segmentStatusTask, ONE_MINUTE_IN_MILLSEC, ONE_MINUTE_IN_MILLSEC);
    LOGGER.info("finished scheduling keepIndexing timer check");
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
      LOGGER.debug(
          "Current indexed " + realtimeSegment.getRawDocumentCount() + " raw events, success = " + realtimeSegment
              .getSuccessIndexedCount() + " docs, total = " + realtimeSegment.getSegmentMetadata().getTotalDocs()
              + " docs in realtime segment");
      if ((System.currentTimeMillis() >= segmentEndTimeThreshold)
          || realtimeSegment.getRawDocumentCount() >= kafkaStreamProviderConfig.getSizeThresholdToFlushSegment()) {
        if (realtimeSegment.getRawDocumentCount() == 0) {
          LOGGER.info("no new events coming in, extending the end time by another hour");
          segmentEndTimeThreshold =
              System.currentTimeMillis() + kafkaStreamProviderConfig.getTimeThresholdToFlushSegment();
          return;
        }
        LOGGER.info(
            "Stopped indexing due to reaching segment limit: {} raw documents indexed, segment is aged {}"
                + realtimeSegment.getRawDocumentCount() + ((System.currentTimeMillis() - start)
                / (ONE_MINUTE_IN_MILLSEC)) + " minutes");
        keepIndexing = false;
      }
    }
  }
}
