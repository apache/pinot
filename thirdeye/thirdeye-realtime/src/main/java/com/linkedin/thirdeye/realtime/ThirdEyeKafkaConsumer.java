package com.linkedin.thirdeye.realtime;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.thirdeye.api.*;
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
  private static final String SCHEDULE = "KAFKA";

  private final StarTree starTree;
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
   * @param starTree
   *          The mutable index
   * @param kafkaConfig
   *          The kafka configuration (e.g. zk address, topic, etc.)
   * @param consumerExecutors
   *          Used to consume messages from Kafka
   * @param taskScheduler
   *          Used to schedule persist task
   * @param dataUpdateManager
   *          Utility to persist segments
   * @param metricRegistry
   *          The server's metric registry for reporting
   */
  public ThirdEyeKafkaConsumer(StarTree starTree, ThirdEyeKafkaConfig kafkaConfig,
      ExecutorService consumerExecutors, ScheduledExecutorService taskScheduler,
      DataUpdateManager dataUpdateManager, MetricRegistry metricRegistry) {
    if (starTree == null) {
      throw new NullPointerException("Provided star tree is null");
    }
    if (kafkaConfig == null) {
      throw new NullPointerException("Provided kafka config is null");
    }

    String collection = starTree.getConfig().getCollection();
    String topic = kafkaConfig.getTopicName();

    this.starTree = starTree;
    this.kafkaConfig = kafkaConfig;
    this.consumerExecutors = consumerExecutors;
    this.taskScheduler = taskScheduler;
    this.dataUpdateManager = dataUpdateManager;
    this.isStarted = new AtomicBoolean();
    this.lock = new ReentrantReadWriteLock();
    this.stats = new ThirdEyeKafkaStats(collection, topic, metricRegistry);
    this.persistTask = new AtomicReference<>();
  }

  public ThirdEyeKafkaStats getStats() {
    return stats;
  }

  public void start() throws Exception {
    if (!isStarted.getAndSet(true)) {
      // Construct decoder
      LOG.info("Constructing decoder {}", kafkaConfig.getDecoderClass());
      Class<?> decoderClass = Class.forName(kafkaConfig.getDecoderClass());
      ThirdEyeKafkaDecoder decoder =
          (ThirdEyeKafkaDecoder) decoderClass.getConstructor().newInstance();
      decoder.init(starTree.getConfig(), kafkaConfig);

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
      Map<String, List<KafkaStream<byte[], byte[]>>> streams =
          consumer.createMessageStreams(topicMap);
      if (streams.size() != 1) {
        throw new IllegalStateException(
            "Can only consume one stream for " + kafkaConfig.getTopicName());
      }
      List<KafkaStream<byte[], byte[]>> topicStreams = streams.values().iterator().next();
      if (topicStreams.size() != 1) {
        throw new IllegalStateException(
            "Can only consume one stream for " + kafkaConfig.getTopicName());
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
      taskScheduler.scheduleAtFixedRate(persistTask.get(), interval.getSize(), interval.getSize(),
          interval.getUnit());
    }
  }

  /** Stops consumer and shuts down the provided executors. */
  public void stop() throws Exception {
    if (isStarted.getAndSet(false)) {
      String collection = starTree.getConfig().getCollection();
      LOG.info("Shutting down Kafka consumer persist scheduler for {}", collection);
      taskScheduler.shutdown();
      LOG.info("Shutting down Kafka consumer executors for {}", collection);
      consumerExecutors.shutdown();

      // Persist any remaining
      LOG.info("Persisting any remaining data");
      if (persistTask.get() != null) {
        persistTask.get().run();
      }

      LOG.info("Shut down kafka consumer for {}", collection);
    }
  }

  private class EventIterator implements Runnable {
    private final StarTree starTree;
    private final ThirdEyeKafkaDecoder decoder;
    private final KafkaStream<byte[], byte[]> stream;

    EventIterator(StarTree starTree, ThirdEyeKafkaDecoder decoder,
        KafkaStream<byte[], byte[]> stream) {
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
          String collection = starTree.getConfig().getCollection();
          LOG.error("Error consuming message from kafka for {}", collection, e);
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
      String collection = starTree.getConfig().getCollection();
      long currentTime = System.currentTimeMillis();
      DateTime minTime = new DateTime(stats.getLastPersistTimeMillis().get(), DateTimeZone.UTC);
      DateTime maxTime = new DateTime(currentTime, DateTimeZone.UTC);

      LOG.info("Beginning persist data from {} to {} for {}", minTime, maxTime, collection);
      lock.writeLock().lock();
      try {
        // Write to disk
        LOG.info("Locked {} to persist data...", lock);
        dataUpdateManager.persistTree(collection, SCHEDULE, minTime, maxTime, starTree);
        LOG.info("Clearing existing star tree metrics...");
        starTree.clear();
        LOG.info("Updating last persist time to {}", currentTime);
        stats.getLastPersistTimeMillis().set(currentTime);
        // TODO: We may want to periodically wipe the tree structure as well
        LOG.info("Committing offsets to Kafka for {}", collection);
        consumer.commitOffsets();
        LOG.info("Successfully persisted data from {} to {} for {}", minTime, maxTime, collection);
      } catch (Exception e) {
        LOG.error("Could not persist star tree for {}", collection, e);
      } finally {
        lock.writeLock().unlock();
        LOG.info("Unlocked {}", lock);
      }
    }
  }

  private long getMinCollectionTimeMillis(MetricTimeSeries timeSeries) {
    long minTime = Collections.min(timeSeries.getTimeWindowSet());
    TimeGranularity bucket = starTree.getConfig().getTime().getBucket();
    return TimeUnit.MILLISECONDS.convert(minTime * bucket.getSize(), bucket.getUnit());
  }

  private long getMaxCollectionTimeMillis(MetricTimeSeries timeSeries) {
    long maxTime = Collections.max(timeSeries.getTimeWindowSet());
    TimeGranularity bucket = starTree.getConfig().getTime().getBucket();
    return TimeUnit.MILLISECONDS.convert(maxTime * bucket.getSize(), bucket.getUnit());
  }
}
