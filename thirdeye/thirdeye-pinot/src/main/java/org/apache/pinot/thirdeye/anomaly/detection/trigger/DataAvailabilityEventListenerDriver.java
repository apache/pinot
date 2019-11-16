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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.pinot.thirdeye.anomaly.detection.trigger.filter.DataAvailabilityEventFilter;
import org.apache.pinot.thirdeye.anomaly.detection.trigger.utils.DatasetTriggerInfoRepo;
import org.apache.pinot.thirdeye.anomaly.detection.trigger.utils.DataAvailabilitySchedulingConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class is to start DataAvailabilityEventListener based on configuration.
 */
public class DataAvailabilityEventListenerDriver {
  private static final Logger LOG = LoggerFactory.getLogger(DataAvailabilityEventListenerDriver.class);
  private ExecutorService executorService;
  private DataAvailabilitySchedulingConfiguration config;
  private Properties consumerProps;
  private List<DataAvailabilityEventListener> listeners;

  public DataAvailabilityEventListenerDriver(DataAvailabilitySchedulingConfiguration config) throws IOException {
    String rootDir = System.getProperty("dw.rootDir");
    this.config = config;
    this.executorService = Executors.newFixedThreadPool(this.config.getNumParallelConsumer(),
        new ThreadFactoryBuilder().setNameFormat("data-avail-event-consumer-%d").build());
    this.consumerProps = new Properties();
    this.consumerProps.load(new FileInputStream(rootDir + "/" + this.config.getKafkaConsumerPropPath()));
    this.listeners = new ArrayList<>();
    DatasetTriggerInfoRepo.init(config.getDatasetWhitelistUpdateFreqInMin(), config.getDataSourceWhitelist());
  }

  public void start() {
    for (int i = 0; i < config.getNumParallelConsumer(); i++) {
      DataAvailabilityKafkaConsumer consumer = loadConsumer();
      List<DataAvailabilityEventFilter> filters = loadFilters();
      DataAvailabilityEventListener listener = new DataAvailabilityEventListener(consumer, filters,
          config.getSleepTimeWhenNoEventInMilli(), config.getConsumerPollTimeInMilli());
      listeners.add(listener);
      executorService.submit(listener);
    }
    LOG.info("Started {} DataAvailabilityEventListener...", listeners.size());
  }

  public void shutdown() {
    listeners.forEach(DataAvailabilityEventListener::close);
    executorService.shutdown();
    LOG.info("Successfully shut down all listeners.");
  }

  private DataAvailabilityKafkaConsumer loadConsumer() {
    String className = config.getConsumerClass();
    try {
      Constructor<?> constructor = Class.forName(className)
          .getConstructor(String.class, String.class, String.class, Properties.class);
      LOG.info("Loaded consumer class: {}", className);
      return (DataAvailabilityKafkaConsumer) constructor.newInstance(config.getKafkaTopic(),
          config.getKafkaConsumerGroupId(), config.getKafkaBootstrapServers(), consumerProps);
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to initialize consumer.", e.getCause());
    }
  }

  private List<DataAvailabilityEventFilter> loadFilters() {
    List<DataAvailabilityEventFilter> filters = new ArrayList<>(config.getFilterClassList().size());
    for (String filterClassName :  config.getFilterClassList()) {
      try {
        DataAvailabilityEventFilter filter = (DataAvailabilityEventFilter) Class.forName(filterClassName).newInstance();
        filters.add(filter);
        LOG.info("Loaded event filter: {}", filterClassName);
      } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
        throw new IllegalArgumentException("Failed to initialize trigger event filter.", e.getCause());
      }
    }
    return filters;
  }
}
