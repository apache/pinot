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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
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

  private final StarTree starTree;
  private final ThirdEyeKafkaConfig config;
  private final AtomicBoolean isShutdown;
  private final ExecutorService executorService;
  private final ScheduledExecutorService persistScheduler;
  private final MetricRegistry metricRegistry;
  private final File metricStoreDirectory;
  private final ConcurrentMap<String, ThirdEyeKafkaStats> streamStats;
  private final ReadWriteLock persistLock;

  public ThirdEyeKafkaConsumer(StarTree starTree,
                               ThirdEyeKafkaConfig config,
                               ExecutorService executorService,
                               ScheduledExecutorService persistScheduler,
                               MetricRegistry metricRegistry,
                               File rootDir)
  {
    this.starTree = starTree;
    this.config = config;
    this.executorService = executorService;
    this.persistScheduler = persistScheduler;
    this.isShutdown = new AtomicBoolean(true);
    this.metricRegistry = metricRegistry;
    this.streamStats = new ConcurrentHashMap<String, ThirdEyeKafkaStats>();
    this.persistLock = new ReentrantReadWriteLock(true);

    this.metricStoreDirectory = new File(rootDir.getAbsolutePath()
            + File.separator + starTree.getConfig().getCollection()
            + File.separator + StarTreeConstants.DATA_DIR_NAME
            + File.separator + StarTreeConstants.METRIC_STORE);
  }

  public Map<String, ThirdEyeKafkaStats> getStreamStats()
  {
    return streamStats;
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
              persistLock.writeLock().lock();
              try
              {
                ThirdEyeKafkaPersistenceUtils.persistMetrics(starTree, metricStoreDirectory);
                stats.getLastPersistTimeMillis().set(System.currentTimeMillis());
                starTree.clear();
                consumer.commitOffsets();
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
          }, config.getPersistIntervalMillis(), config.getPersistIntervalMillis(), TimeUnit.MILLISECONDS);

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

                  // Add record
                  StarTreeRecord record = decoder.decode(next.message());
                  if (record == null)
                  {
                    stats.getRecordsSkipped().mark();
                  }
                  else
                  {
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

                  // Update record stats
                  stats.getBytesRead().mark(next.message().length);
                  stats.getLastConsumedRecordTimeMillis().set(currentTime);
                }
                catch (Exception e)
                {
                  LOG.error("Error consuming message from kafka for {}", starTree.getConfig().getCollection(), e);
                  stats.getRecordsError().mark();
                }
              }

              // Persist any remaining data we've consumed and commit the offsets
              persistLock.writeLock().lock();
              try
              {
                persistFuture.cancel(true);
                ThirdEyeKafkaPersistenceUtils.persistMetrics(starTree, metricStoreDirectory);
                stats.getLastPersistTimeMillis().set(System.currentTimeMillis());
                starTree.clear();
                consumer.commitOffsets();
                LOG.info("Persisted all data before shutdown for {}", starTree.getConfig().getCollection());
              }
              catch (Exception e)
              {
                LOG.error("Error persisting data during shutdown for {}", starTree.getConfig().getCollection(), e);
              }
              finally
              {
                persistLock.writeLock().unlock();
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
}
