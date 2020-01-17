/*
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

package org.apache.pinot.thirdeye.anomaly.detection.trigger;

import java.util.List;
import org.apache.pinot.thirdeye.anomaly.detection.trigger.filter.DataAvailabilityEventFilter;
import org.apache.pinot.thirdeye.anomaly.detection.trigger.utils.DatasetTriggerInfoRepo;
import org.apache.pinot.thirdeye.anomaly.utils.ThirdeyeMetricsUtil;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is to listen to Kafka trigger events and update metadata in the metadata store based on the events,
 * so that new anomaly detection can be trigger accordingly.
 */
public class DataAvailabilityEventListener implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(DataAvailabilityEventListener.class);
  private DataAvailabilityKafkaConsumer consumer;
  private final List<DataAvailabilityEventFilter> filters;
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();
  private DatasetTriggerInfoRepo datasetTriggerInfoRepo;
  private DatasetConfigManager datasetConfigManager;
  private long sleepTimeInMilli;
  private long pollTimeInMilli;

  public DataAvailabilityEventListener(DataAvailabilityKafkaConsumer consumer, List<DataAvailabilityEventFilter> filters,
      long sleepTimeInMilli, long pollTimeInMilli) {
    this.consumer = consumer;
    this.filters = filters;
    this.datasetConfigManager = DAO_REGISTRY.getDatasetConfigDAO();
    this.datasetTriggerInfoRepo = DatasetTriggerInfoRepo.getInstance();
    this.sleepTimeInMilli = sleepTimeInMilli;
    this.pollTimeInMilli = pollTimeInMilli;
  }

  @Override
  public void run() {
    try {
      while (!(Thread.interrupted())) {
        processOneBatch();
      }
    } catch (Exception e) {
      LOG.error("Caught an exception while processing event.", e);
    } finally {
      consumer.close();
    }
    LOG.info("DataAvailabilityEventListener under thread {} is closed.", Thread.currentThread().getName());
  }

  public void close() {
    datasetTriggerInfoRepo.close();
  }

  void processOneBatch() throws InterruptedException {
    List<DataAvailabilityEvent> events = consumer.poll(pollTimeInMilli);
    ThirdeyeMetricsUtil.triggerEventCounter.inc(events.size());
    for (DataAvailabilityEvent event : events) {
      if (checkAllFiltersPassed(event)) {
        try {
          LOG.info("Processing event: " + event.getDatasetName() + " with watermark " + event.getHighWatermark());
          String dataset = event.getDatasetName();
          datasetTriggerInfoRepo.setLastUpdateTimestamp(dataset, event.getHighWatermark());
          //Note: Batch update the timestamps of dataset if the event traffic spikes
          datasetConfigManager.updateLastRefreshTime(dataset, event.getHighWatermark(), System.currentTimeMillis());
          ThirdeyeMetricsUtil.processedTriggerEventCounter.inc();
          LOG.debug("Finished processing event: " + event.getDatasetName());
        } catch (Exception e) {
          LOG.error("Error in processing event for {}, so skipping...", event.getDatasetName(), e);
        }
      }
    }
    if (!events.isEmpty()) {
      consumer.commitSync();
    } else if (sleepTimeInMilli > 0) {
      LOG.info("Going to sleep and wake up in next " + sleepTimeInMilli + " milliseconds...");
      Thread.sleep(sleepTimeInMilli);
    }
  }

  private boolean checkAllFiltersPassed(DataAvailabilityEvent event) {
    for (DataAvailabilityEventFilter filter : filters) {
      if (!filter.isPassed(event))
        return false;
    }
    return true;
  }
}
