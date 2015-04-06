package com.linkedin.thirdeye.realtime;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeRecord;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ThirdEyeKafkaConsumer
{
  private static final Logger LOG = LoggerFactory.getLogger(ThirdEyeKafkaConsumer.class);
  private static final String TMP_DIR_PREFIX = "kafka_load_";

  private final StarTree starTree;
  private final ThirdEyeKafkaConfig config;
  private final AtomicBoolean isShutdown;
  private final ExecutorService executorService;
  private final ScheduledExecutorService persistScheduler;
  private final MetricRegistry metricRegistry;
  private final File metricStoreDirectory;
  private final File tmpMetricStoreDirectory;
  private final ConcurrentMap<String, ThirdEyeKafkaStats> streamStats;
  private final ReadWriteLock persistLock;
  private final String schedule;
  private final File kafkaDataDir;

  public ThirdEyeKafkaConsumer(StarTree starTree,
                               ThirdEyeKafkaConfig config,
                               ExecutorService executorService,
                               ScheduledExecutorService persistScheduler,
                               MetricRegistry metricRegistry,
                               File kafkaDataDir)
  {
    this.starTree = starTree;
    this.config = config;
    this.executorService = executorService;
    this.persistScheduler = persistScheduler;
    this.isShutdown = new AtomicBoolean(true);
    this.metricRegistry = metricRegistry;
    this.streamStats = new ConcurrentHashMap<String, ThirdEyeKafkaStats>();
    this.persistLock = new ReentrantReadWriteLock(true);
    this.schedule = config.getPersistInterval().getSize() + "-" + config.getPersistInterval().getUnit();
    this.metricStoreDirectory = new File(kafkaDataDir, StarTreeConstants.METRIC_STORE);
    this.tmpMetricStoreDirectory = new File(kafkaDataDir, TMP_DIR_PREFIX + StarTreeConstants.METRIC_STORE);
    this.kafkaDataDir = kafkaDataDir;
  }

  public Map<String, ThirdEyeKafkaStats> getStreamStats()
  {
    return streamStats;
  }

  private void doPersist(ThirdEyeKafkaStats stats) throws IOException
  {
    persistLock.writeLock().lock();
    try
    {
      long persistTime = System.currentTimeMillis();
      DateTime minTime = new DateTime(stats.getLastPersistTimeMillis().get());
      DateTime maxTime = new DateTime(persistTime);

      if (tmpMetricStoreDirectory.exists())
      {
        FileUtils.forceDelete(tmpMetricStoreDirectory);
      }

      ThirdEyeKafkaPersistenceUtils.persistMetrics(starTree, tmpMetricStoreDirectory);
      prefixFilesWithTime(tmpMetricStoreDirectory, minTime, maxTime, schedule);
      moveAllFiles(tmpMetricStoreDirectory, metricStoreDirectory);

      if (tmpMetricStoreDirectory.exists())
      {
        FileUtils.forceDelete(tmpMetricStoreDirectory);
      }

      stats.getLastPersistTimeMillis().set(persistTime);
      starTree.clear();

      // Trigger watch on collection dir
      if (!kafkaDataDir.setLastModified(stats.getLastPersistTimeMillis().get()))
      {
        LOG.warn("Could not trigger watch on collection dir {}", kafkaDataDir.getParentFile());
      }
    }
    catch (Exception e)
    {
      LOG.error("Error persisting data from Kafka", e);
    }
    finally
    {
      persistLock.writeLock().unlock();
    }
  }

  public void start() throws Exception
  {
    if (isShutdown.getAndSet(false))
    {
      final ThirdEyeKafkaDecoder decoder
              = (ThirdEyeKafkaDecoder) Class.forName(config.getDecoderClass()).getConstructor().newInstance();
      decoder.init(starTree.getConfig(), config);

      Properties props = new Properties();
      props.put("zookeeper.connect", config.getZkAddress());
      props.put("group.id", config.getGroupId());
      props.put("auto.commit.enable", "false");
      props.put("auto.offset.reset", "smallest");

      if (config.getConsumerConfig() != null)
      {
        props.putAll(config.getConsumerConfig());
      }

      final ConsumerConnector consumer
              = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));

      Map<String, List<KafkaStream<byte[], byte[]>>> streams
              = consumer.createMessageStreams(Collections.singletonMap(config.getTopicName(), 1));

      for (final Map.Entry<String, List<KafkaStream<byte[], byte[]>>> entry : streams.entrySet())
      {
        for (final KafkaStream<byte[], byte[]> stream : entry.getValue())
        {
          final ThirdEyeKafkaStats stats
                  = new ThirdEyeKafkaStats(starTree.getConfig().getCollection(), entry.getKey(), metricRegistry);

          streamStats.put(entry.getKey(), stats);

          final ScheduledFuture persistFuture = persistScheduler.scheduleAtFixedRate(new Runnable()
          {
            @Override
            public void run()
            {
              try
              {
                doPersist(stats);
                consumer.commitOffsets();
              }
              catch (Exception e)
              {
                LOG.error("{}", e);
              }
            }
          }, config.getPersistInterval().getSize(), config.getPersistInterval().getSize(), config.getPersistInterval().getUnit());

          executorService.submit(new Runnable()
          {
            @Override
            public void run()
            {
              ConsumerIterator<byte[], byte[]> itr = stream.iterator();

              while (!isShutdown.get() && itr.hasNext())
              {
                try
                {
                  MessageAndMetadata<byte[], byte[]> next = itr.next();

                  long currentTime = System.currentTimeMillis();
                  stats.getBytesRead().mark(next.message().length);
                  stats.getLastConsumedRecordTimeMillis().set(currentTime);

                  StarTreeRecord record = decoder.decode(next.message());
                  if (record == null)
                  {
                    stats.getRecordsSkippedInvalid().mark();
                    continue;
                  }

                  // Check record time
                  long minTimeMillis = TimeUnit.MILLISECONDS.convert(
                          Collections.min(record.getMetricTimeSeries().getTimeWindowSet())
                                  * starTree.getConfig().getTime().getBucket().getSize(),
                          starTree.getConfig().getTime().getBucket().getUnit());
                  if (minTimeMillis < config.getStartTime().getMillis())
                  {
                    stats.getRecordsSkippedExpired().mark();
                    continue;
                  }

                  // Add record
                  persistLock.readLock().lock();
                  try
                  {
                    starTree.add(record);
                  }
                  finally
                  {
                    persistLock.readLock().unlock();
                  }
                  stats.getRecordsAdded().mark();

                  // Update lag / data time stats
                  if (!record.getMetricTimeSeries().getTimeWindowSet().isEmpty())
                  {
                    long maxTime = Collections.max(record.getMetricTimeSeries().getTimeWindowSet());
                    long maxTimeMillis = TimeUnit.MILLISECONDS.convert(
                            maxTime * starTree.getConfig().getTime().getInput().getSize(),
                            starTree.getConfig().getTime().getInput().getUnit());
                    if (maxTimeMillis > stats.getDataTimeMillis().get())
                    {
                      stats.getDataTimeMillis().set(maxTimeMillis);
                    }
                  }
                }
                catch (Exception e)
                {
                  LOG.error("Error consuming message from kafka for {}", starTree.getConfig().getCollection(), e);
                  stats.getRecordsError().mark();
                }
              }

              // Persist any remaining data we've consumed and commit the offsets
              try
              {
                doPersist(stats);
                consumer.commitOffsets();
              }
              catch (Exception e)
              {
                LOG.error("{}", e);
              }
            }
          });
        }
      }

      LOG.info("Started kafka consumer for {}", starTree.getConfig().getCollection());
    }
  }

  public void shutdown() throws Exception
  {
    if (!isShutdown.getAndSet(true))
    {
      LOG.info("Shutdown kafka consumer for {}", starTree.getConfig().getCollection());
    }
  }

  private static void prefixFilesWithTime(File dir,
                                          DateTime minTime,
                                          DateTime maxTime,
                                          String schedule) throws IOException
  {
    File[] files = dir.listFiles();

    if (files != null)
    {
      for (File file : files)
      {
        String minTimeComponent = StarTreeConstants.DATE_TIME_FORMATTER.print(minTime);
        String maxTimeComponent = StarTreeConstants.DATE_TIME_FORMATTER.print(maxTime);
        File renamed = new File(file.getParent(), schedule + "_" + minTimeComponent + "_" + maxTimeComponent + "_" + file.getName());
        FileUtils.moveFile(file, renamed);
      }
    }
  }

  private static void moveAllFiles(File srcMetricDir, File dstMetricDir) throws IOException
  {
    File[] metricFiles = srcMetricDir.listFiles();
    if (metricFiles != null)
    {
      for (File file : metricFiles)
      {
        FileUtils.moveFile(file, new File(dstMetricDir, file.getName()));
      }
    }
  }
}
