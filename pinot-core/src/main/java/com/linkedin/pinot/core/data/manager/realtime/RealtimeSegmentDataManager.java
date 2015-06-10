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

import java.io.File;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metadata.instance.InstanceZKMetadata;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.utils.CommonConstants.Segment.Realtime.Status;
import com.linkedin.pinot.common.utils.CommonConstants.Segment.SegmentType;
import com.linkedin.pinot.core.data.manager.offline.SegmentDataManager;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.realtime.StreamProvider;
import com.linkedin.pinot.core.realtime.StreamProviderConfig;
import com.linkedin.pinot.core.realtime.converter.RealtimeSegmentConverter;
import com.linkedin.pinot.core.realtime.impl.RealtimeSegmentImpl;
import com.linkedin.pinot.core.realtime.impl.kafka.KafkaHighLevelConsumerStreamProvider;
import com.linkedin.pinot.core.realtime.impl.kafka.KafkaHighLevelStreamProviderConfig;
import com.linkedin.pinot.core.segment.index.loader.Loaders;


public class RealtimeSegmentDataManager implements SegmentDataManager {
  private static final int FIVE_MILLION = 5000000;
  private static final Logger LOGGER = LoggerFactory.getLogger(RealtimeSegmentDataManager.class);
  private final static long ONE_MINUTE_IN_MILLSEC = 1000 * 60;

  private final static String CONFIG_TIME_IN_MILLIS_TO_STOP_INDEXING =
      "metadata.realtime.segment.timeInMillisToStopIndexing";
  private final static String CONFIG_NUM_INDEXED_EVENTS_TO_STOP_INDEXING =
      "metadata.realtime.segment.numIndexedEventsToStopIndexing";
  private final static long DEFAULT_TIME_IN_MILLIS_TO_STOP_INDEXING = ONE_MINUTE_IN_MILLSEC * 60;
  private final static long DEFAULT_NUM_INDEXED_EVENTS_TO_STOP_INDEXING = 5000000;

  private final String segmentName;
  private final Schema schema;
  private final ReadMode mode;
  private final RealtimeSegmentZKMetadata segmentMetatdaZk;

  private final StreamProviderConfig kafkaStreamProviderConfig;
  private final StreamProvider kafkaStreamProvider;
  private final File resourceDir;
  private final File resourceTmpDir;
  private final Object lock = new Object();
  private IndexSegment realtimeSegment;

  private final long start = System.currentTimeMillis();
  private long segmentEndTimeThreshold;

  private volatile boolean keepIndexing = true;
  private TimerTask segmentStatusTask;
  private final RealtimeTableDataManager notifier;
  private Thread indexingThread;
  private long timeInMillisToStopIndexing = DEFAULT_TIME_IN_MILLIS_TO_STOP_INDEXING;
  private long numIndexedEventsToStopIndexing = DEFAULT_NUM_INDEXED_EVENTS_TO_STOP_INDEXING;
  private final String sortedColumn;

  public RealtimeSegmentDataManager(final RealtimeSegmentZKMetadata segmentMetadata,
      final AbstractTableConfig tableConfig, InstanceZKMetadata instanceMetadata,
      RealtimeTableDataManager realtimeResourceManager, final String resourceDataDir, final ReadMode mode,
      final Schema schema) throws Exception {
    this.timeInMillisToStopIndexing = DEFAULT_TIME_IN_MILLIS_TO_STOP_INDEXING;
    segmentEndTimeThreshold = start + this.timeInMillisToStopIndexing;
    this.numIndexedEventsToStopIndexing = DEFAULT_NUM_INDEXED_EVENTS_TO_STOP_INDEXING;
    this.schema = schema;
    if (tableConfig.getIndexingConfig().getSortedColumn().isEmpty()) {
      LOGGER.info("RealtimeDataResourceZKMetadata contains no information about sorted column");
      this.sortedColumn = null;
    } else {
      String firstSortedColumn = tableConfig.getIndexingConfig().getSortedColumn().get(0);
      if (this.schema.isExisted(firstSortedColumn)) {
        LOGGER.info("Setting sorted column name: {} from RealtimeDataResourceZKMetadata.", firstSortedColumn);
        this.sortedColumn = firstSortedColumn;
      } else {
        LOGGER.warn("Sorted column name: {} from RealtimeDataResourceZKMetadata is not existed in schema.",
            firstSortedColumn);
        this.sortedColumn = null;
      }
    }
    this.segmentMetatdaZk = segmentMetadata;
    this.segmentName = segmentMetadata.getSegmentName();

    // create and init stream provider config
    // TODO : ideally resourceMetatda should create and give back a streamProviderConfig
    this.kafkaStreamProviderConfig = new KafkaHighLevelStreamProviderConfig();
    this.kafkaStreamProviderConfig.init(tableConfig, instanceMetadata, schema);
    this.resourceDir = new File(resourceDataDir);
    this.resourceTmpDir = new File(resourceDataDir, "_tmp");
    if (!resourceTmpDir.exists()) {
      resourceTmpDir.mkdirs();
    }
    this.mode = mode;
    // create and init stream provider
    this.kafkaStreamProvider = new KafkaHighLevelConsumerStreamProvider();
    this.kafkaStreamProvider.init(kafkaStreamProviderConfig);
    this.kafkaStreamProvider.start();
    // lets create a new realtime segment
    realtimeSegment = new RealtimeSegmentImpl(schema, FIVE_MILLION);
    ((RealtimeSegmentImpl) (realtimeSegment)).setSegmentName(segmentMetadata.getSegmentName());
    ((RealtimeSegmentImpl) (realtimeSegment)).setSegmentMetadata(segmentMetadata, this.schema);
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
        // continue indexing until critertia is met
        while (((RealtimeSegmentImpl) realtimeSegment).index(kafkaStreamProvider.next()) && keepIndexing) {
        }

        LOGGER.info("Indexing threshold reached, proceeding with index conversion");
        // kill the timer first
        segmentStatusTask.cancel();
        LOGGER.info("Trying to persist a realtimeSegment - " + realtimeSegment.getSegmentName());
        LOGGER.info("Indexed " + ((RealtimeSegmentImpl) realtimeSegment).getRawDocumentCount()
            + " raw events, current number of docs = " + ((RealtimeSegmentImpl) realtimeSegment).getTotalDocs());
        File tempSegmentFolder = new File(resourceTmpDir, "tmp-" + String.valueOf(System.currentTimeMillis()));

        // lets convert the segment now
        RealtimeSegmentConverter conveter =
            new RealtimeSegmentConverter((RealtimeSegmentImpl) realtimeSegment, tempSegmentFolder.getAbsolutePath(),
                schema, segmentMetadata.getTableName(), segmentMetadata.getSegmentName(), sortedColumn);
        try {
          LOGGER.info("Trying to build segment!");
          conveter.build();
          File destDir = new File(resourceDataDir, segmentMetadata.getSegmentName());
          FileUtils.deleteQuietly(destDir);
          FileUtils.moveDirectory(tempSegmentFolder.listFiles()[0], destDir);

          FileUtils.deleteQuietly(tempSegmentFolder);
          long startTime = ((RealtimeSegmentImpl) realtimeSegment).getMinTime();
          long endTime = ((RealtimeSegmentImpl) realtimeSegment).getMaxTime();

          TimeUnit timeUnit = schema.getTimeFieldSpec().getOutgoingGranularitySpec().getTimeType();
          swap();
          RealtimeSegmentZKMetadata metadaToOverrite = new RealtimeSegmentZKMetadata();
          metadaToOverrite.setTableName(segmentMetadata.getTableName());
          metadaToOverrite.setSegmentName(segmentMetadata.getSegmentName());
          metadaToOverrite.setSegmentType(SegmentType.OFFLINE);
          metadaToOverrite.setStatus(Status.DONE);
          metadaToOverrite.setStartTime(startTime);
          metadaToOverrite.setEndTime(endTime);
          metadaToOverrite.setTotalDocs(realtimeSegment.getTotalDocs());
          metadaToOverrite.setTimeUnit(timeUnit);
          notifier.notify(metadaToOverrite);

          kafkaStreamProvider.commit();
          kafkaStreamProvider.shutdown();
        } catch (Exception e) {
          LOGGER.error("Caught exception in the realtime indexing thread", e);
        }
      }
    });

    indexingThread.start();

    LOGGER.debug("scheduling keepIndexing timer check");
    // start a schedule timer to keep track of the segment
    TimerService.timer.schedule(segmentStatusTask, ONE_MINUTE_IN_MILLSEC, ONE_MINUTE_IN_MILLSEC);
    LOGGER.debug("finished scheduling keepIndexing timer check");

  }

  public void swap() throws Exception {
    IndexSegment segment = Loaders.IndexSegment.load(new File(resourceDir, segmentMetatdaZk.getSegmentName()), mode);
    synchronized (lock) {
      this.realtimeSegment = segment;
    }
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
      LOGGER.info("Current indexed " + ((RealtimeSegmentImpl) realtimeSegment).getRawDocumentCount()
          + " raw events, success = " + ((RealtimeSegmentImpl) realtimeSegment).getSuccessIndexedCount()
          + " docs, total = " + ((RealtimeSegmentImpl) realtimeSegment).getTotalDocs() + " docs in realtime segment");
      if ((System.currentTimeMillis() >= segmentEndTimeThreshold)
          || ((RealtimeSegmentImpl) realtimeSegment).getRawDocumentCount() >= numIndexedEventsToStopIndexing) {
        if (((RealtimeSegmentImpl) realtimeSegment).getRawDocumentCount() == 0) {
          LOGGER.info("no new events coming in, extending the end time by another hour");
          segmentEndTimeThreshold = System.currentTimeMillis() + this.timeInMillisToStopIndexing;
          return;
        }
        LOGGER.info("Stopped indexing due to reaching segment limit: "
            + ((RealtimeSegmentImpl) realtimeSegment).getRawDocumentCount()
            + " raw documents indexed, segment is aged "
            + ((System.currentTimeMillis() - start) / (ONE_MINUTE_IN_MILLSEC)) + " minutes");
        keepIndexing = false;
      }
    }
  }
}
