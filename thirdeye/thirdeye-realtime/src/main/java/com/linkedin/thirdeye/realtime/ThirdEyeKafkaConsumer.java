package com.linkedin.thirdeye.realtime;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.thirdeye.api.*;
import com.linkedin.thirdeye.impl.StarTreeImpl;
import com.linkedin.thirdeye.impl.StarTreeRecordStoreFactoryHashMapImpl;
import com.linkedin.thirdeye.impl.storage.DataUpdateManager;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ThirdEyeKafkaConsumer {
  private static final Logger LOG = LoggerFactory.getLogger(ThirdEyeKafkaConsumer.class);

  private final StarTreeConfig starTreeConfig;
  private final ThirdEyeKafkaConfig kafkaConfig;
  private final ExecutorService consumerExecutors;
  private final ScheduledExecutorService taskScheduler;
  private final DataUpdateManager dataUpdateManager;
  private final ThirdEyeKafkaStats stats;

  private final AtomicBoolean isStarted;
  private final ReadWriteLock lock;
  private final AtomicReference<Runnable> persistTask;

  /**
   * Consumes data from Kafka for one collection.
   *
   * @param starTreeConfig
   *  The collection index configuration
   * @param kafkaConfig
   *  The kafka configuration (e.g. zk address, topic, etc.)
   * @param consumerExecutors
   *  Used to consume messages from Kafka
   * @param taskScheduler
   *  Used to schedule persist task
   * @param dataUpdateManager
   *  Utility to persist segments
   * @param metricRegistry
   *  The server's metric registry for reporting
   */
  public ThirdEyeKafkaConsumer(StarTreeConfig starTreeConfig,
                               ThirdEyeKafkaConfig kafkaConfig,
                               ExecutorService consumerExecutors,
                               ScheduledExecutorService taskScheduler,
                               DataUpdateManager dataUpdateManager,
                               MetricRegistry metricRegistry) {
    if (starTreeConfig == null) {
      throw new NullPointerException("Provided star tree config is null");
    }
    if (kafkaConfig == null) {
      throw new NullPointerException("Provided kafka config is null");
    }

    this.starTreeConfig = starTreeConfig;
    this.kafkaConfig = kafkaConfig;
    this.consumerExecutors = consumerExecutors;
    this.taskScheduler = taskScheduler;
    this.dataUpdateManager = dataUpdateManager;
    this.isStarted = new AtomicBoolean();
    this.lock = new ReentrantReadWriteLock();
    this.stats = new ThirdEyeKafkaStats(starTreeConfig.getCollection(), kafkaConfig.getTopicName(), metricRegistry);
    this.persistTask = new AtomicReference<>();
  }

  public ThirdEyeKafkaStats getStats() {
    return stats;
  }

  public void start() throws Exception {
    if (!isStarted.getAndSet(true)) {
      // Create an in-memory tree
      LOG.info("Creating in-memory star tree");
      StarTreeConfig inMemoryConfig = new StarTreeConfig(starTreeConfig.getCollection(),
          StarTreeRecordStoreFactoryHashMapImpl.class.getCanonicalName(),
          new Properties(),
          starTreeConfig.getAnomalyDetectionFunctionClass(),
          starTreeConfig.getAnomalyDetectionFunctionConfig(),
          starTreeConfig.getAnomalyHandlerClass(),
          starTreeConfig.getAnomalyHandlerConfig(),
          starTreeConfig.getAnomalyDetectionMode(),
          starTreeConfig.getDimensions(),
          starTreeConfig.getMetrics(),
          starTreeConfig.getTime(),
          starTreeConfig.getJoinSpec(),
          starTreeConfig.getRollup(),
          starTreeConfig.getSplit(),
          false);
      final StarTree starTree = new StarTreeImpl(inMemoryConfig);
      starTree.open();

      // Construct decoder
      LOG.info("Constructing decoder {}", kafkaConfig.getDecoderClass());
      Class<?> decoderClass = Class.forName(kafkaConfig.getDecoderClass());
      ThirdEyeKafkaDecoder decoder = (ThirdEyeKafkaDecoder) decoderClass.getConstructor().newInstance();
      decoder.init(starTreeConfig, kafkaConfig);

      // Configure Kafka consumer
      Properties props = new Properties();
      props.put("zookeeper.connect", kafkaConfig.getZkAddress());
      props.put("group.id", kafkaConfig.getGroupId());
      props.put("auto.commit.enable", "false");
      props.put("auto.offset.reset", "smallest");
      kafka.consumer.ConsumerConfig consumerConfig = new ConsumerConfig(props);
      ConsumerConnector consumer = Consumer.createJavaConsumerConnector(consumerConfig);
      LOG.info("Configuring kafka with {}", props);

      // Create stream
      LOG.info("Creating Kafka message streams");
      Map<String, Integer> topicMap = Collections.singletonMap(kafkaConfig.getTopicName(), 1);
      Map<String, List<KafkaStream<byte[], byte[]>>> streams = consumer.createMessageStreams(topicMap);
      if (streams.size() != 1) {
        throw new IllegalStateException("Can only consume one stream for " + kafkaConfig.getTopicName());
      }
      List<KafkaStream<byte[], byte[]>> topicStreams = streams.values().iterator().next();
      if (topicStreams.size() != 1) {
        throw new IllegalStateException("Can only consume one stream for " + kafkaConfig.getTopicName());
      }
      KafkaStream<byte[], byte[]> stream = topicStreams.get(0);

      // Consume messages
      LOG.info("Starting event iterator for topic {}", kafkaConfig.getTopicName());
      EventIterator eventIterator = new EventIterator(starTree, decoder, stream);
      consumerExecutors.submit(eventIterator);

      // Schedule persist task
      persistTask.set(new PersistTask(consumer, starTree));
      TimeGranularity interval = kafkaConfig.getPersistInterval();
      LOG.info("Scheduling persist task at {}", interval);
      taskScheduler.scheduleAtFixedRate(persistTask.get(), interval.getSize(), interval.getSize(), interval.getUnit());
    }
  }

  /** Stops consumer and shuts down the provided executors. */
  public void stop() throws Exception {
    if (isStarted.getAndSet(false)) {
      LOG.info("Shutting down Kafka consumer persist scheduler for {}", starTreeConfig.getCollection());
      taskScheduler.shutdown();
      LOG.info("Shutting down Kafka consumer executors for {}", starTreeConfig.getCollection());
      consumerExecutors.shutdown();

      // Persist any remaining
      LOG.info("Persisting any remaining data");
      if (persistTask.get() != null) {
        persistTask.get().run();
      }

      LOG.info("Shut down kafka consumer for {}", starTreeConfig.getCollection());
    }
  }

  private class EventIterator implements Runnable {
    private final StarTree starTree;
    private final ThirdEyeKafkaDecoder decoder;
    private final KafkaStream<byte[], byte[]> stream;

    EventIterator(StarTree starTree, ThirdEyeKafkaDecoder decoder, KafkaStream<byte[], byte[]> stream) {
      this.starTree = starTree;
      this.decoder = decoder;
      this.stream = stream;
    }

    @Override
    public void run() {
      ConsumerIterator<byte[], byte[]> itr = stream.iterator();
      while (isStarted.get() && itr.hasNext()) {
        try {
          // Consume
          MessageAndMetadata<byte[], byte[]> next = itr.next();
          long currentTime = System.currentTimeMillis();
          stats.getBytesRead().mark(next.message().length);
          stats.getLastConsumedRecordTimeMillis().set(currentTime);

          // Decode
          StarTreeRecord record = decoder.decode(next.message());
          if (record == null) {
            stats.getRecordsSkippedInvalid().mark();
            continue;
          }

          // Check recorded time
          long minTimeMillis = getMinCollectionTimeMillis(record.getMetricTimeSeries());
          if (minTimeMillis < kafkaConfig.getStartTime().getMillis()) {
            stats.getRecordsSkippedExpired().mark();
            continue;
          }

          // Add record
          lock.readLock().lock(); // n.b. we are locking tree w.r.t clearing, not adding new records
          try {
            starTree.add(record);
            stats.getRecordsAdded().mark();
          } finally {
            lock.readLock().unlock();
          }

          // Update lag / data time stats
          if (!record.getMetricTimeSeries().getTimeWindowSet().isEmpty()) {
            long maxTimeMillis = getMaxCollectionTimeMillis(record.getMetricTimeSeries());
            if (maxTimeMillis > stats.getDataTimeMillis().get()) {
              stats.getDataTimeMillis().set(maxTimeMillis);
            }
          }
        } catch (Exception e) {
          LOG.error("Error consuming message from kafka for {}", starTreeConfig.getCollection(), e);
        }
      }
    }
  }

  private class PersistTask implements Runnable {
    private final ConsumerConnector consumer;
    private final StarTree starTree;

    PersistTask(ConsumerConnector consumer, StarTree starTree) {
      this.consumer = consumer;
      this.starTree = starTree;
    }

    @Override
    public void run() {
      String collection = starTreeConfig.getCollection();
      try {
        long currentTime = System.currentTimeMillis();
        TimeGranularity scheduleGranularity = kafkaConfig.getPersistInterval();
        String schedule = String.format("KAFKA-%d-%s", scheduleGranularity.getSize(), scheduleGranularity.getUnit());
        DateTime minTime = new DateTime(stats.getLastPersistTimeMillis().get(), DateTimeZone.UTC);
        DateTime maxTime = new DateTime(currentTime, DateTimeZone.UTC);

        LOG.info("Beginning persist data from {} to {} for {}", minTime, maxTime, collection);
        lock.writeLock().lock();
        try {
          // Write to disk
          dataUpdateManager.persistTree(collection, schedule, minTime, maxTime, starTree);
          starTree.clear();
          stats.getLastPersistTimeMillis().set(currentTime);
          LOG.info("Successfully persisted data from {} to {} for {}", minTime, maxTime, collection);
          // TODO: We may want to periodically wipe the tree structure as well
          consumer.commitOffsets();
        } finally {
          lock.writeLock().unlock();
        }
      } catch (Exception e) {
        LOG.error("Could not persist star tree", collection, e);
      }
    }
  }

  private long getMinCollectionTimeMillis(MetricTimeSeries timeSeries) {
    long minTime = Collections.min(timeSeries.getTimeWindowSet());
    TimeGranularity bucket = starTreeConfig.getTime().getBucket();
    return TimeUnit.MILLISECONDS.convert(minTime * bucket.getSize(), bucket.getUnit());
  }

  private long getMaxCollectionTimeMillis(MetricTimeSeries timeSeries) {
    long maxTime = Collections.max(timeSeries.getTimeWindowSet());
    TimeGranularity bucket = starTreeConfig.getTime().getBucket();
    return TimeUnit.MILLISECONDS.convert(maxTime * bucket.getSize(), bucket.getUnit());
  }
}
