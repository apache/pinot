package com.linkedin.thirdeye.managed;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.impl.storage.DataUpdateManager;
import com.linkedin.thirdeye.realtime.ThirdEyeKafkaConfig;
import com.linkedin.thirdeye.realtime.ThirdEyeKafkaStats;
import com.linkedin.thirdeye.realtime.ThirdEyeKafkaConsumer;
import io.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;

public class KafkaConsumerManager implements Managed {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerManager.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());
  static {
    OBJECT_MAPPER.registerModule(new JodaModule());
  }

  private final File rootDir;
  private final StarTreeManager starTreeManager;
  private final DataUpdateManager dataUpdateManager;
  private final MetricRegistry metricRegistry;
  private final Map<String, ThirdEyeKafkaConsumer> consumers;

  public KafkaConsumerManager(File rootDir,
                              StarTreeManager starTreeManager,
                              DataUpdateManager dataUpdateManager,
                              MetricRegistry metricRegistry) {
    this.rootDir = rootDir;
    this.starTreeManager = starTreeManager;
    this.dataUpdateManager = dataUpdateManager;
    this.metricRegistry = metricRegistry;
    this.consumers = new HashMap<>();
  }

  @Override
  public synchronized void start() throws Exception {
    for (String collection : starTreeManager.getCollections()) {
      try {
        start(collection);
      } catch (FileNotFoundException e) {
        LOG.warn("No {} for {}", StarTreeConstants.KAFKA_CONFIG_FILE_NAME, collection);
      } catch (Exception e) {
        LOG.error("Could not start consumer for {}", collection, e);
      }
    }
  }

  @Override
  public synchronized void stop() throws Exception {
    for (String collection : consumers.keySet()) {
      try {
        stop(collection);
      } catch (FileNotFoundException e) {
        LOG.warn("No {} for {}", StarTreeConstants.KAFKA_CONFIG_FILE_NAME, collection);
      } catch (Exception e) {
        LOG.error("Could not stop consumer for {}", collection, e);
      }
    }
  }

  public synchronized Map<String, ThirdEyeKafkaStats> getStats() {
    Map<String, ThirdEyeKafkaStats> stats = new HashMap<>();
    for (Map.Entry<String, ThirdEyeKafkaConsumer> entry : consumers.entrySet()) {
      stats.put(entry.getKey(), entry.getValue().getStats());
    }
    return stats;
  }

  public synchronized void start(String collection) throws Exception {
    File collectionDir = new File(rootDir, collection);
    File kafkaFile = new File(collectionDir, StarTreeConstants.KAFKA_CONFIG_FILE_NAME);
    ThirdEyeKafkaConfig kafkaConfig = OBJECT_MAPPER.readValue(kafkaFile, ThirdEyeKafkaConfig.class);
    ThirdEyeKafkaConsumer consumer = new ThirdEyeKafkaConsumer(
        starTreeManager.getMutableStarTree(collection),
        kafkaConfig,
        Executors.newSingleThreadExecutor(),
        Executors.newSingleThreadScheduledExecutor(),
        dataUpdateManager,
        metricRegistry);
    consumers.put(collection, consumer);
    consumer.start();
    LOG.info("Started Kafka consumer for {}", collection);
  }

  public synchronized void stop(String collection) throws Exception {
    ThirdEyeKafkaConsumer consumer = consumers.get(collection);
    if (consumer == null) {
      throw new IllegalArgumentException("No consumer for " + collection);
    }
    consumer.stop();
    LOG.info("Stopped Kafka consumer for {}", collection);
  }
}
