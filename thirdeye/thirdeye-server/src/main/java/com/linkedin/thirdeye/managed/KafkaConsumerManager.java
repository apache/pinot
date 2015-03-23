package com.linkedin.thirdeye.managed;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.impl.StarTreeImpl;
import com.linkedin.thirdeye.impl.storage.StarTreeRecordStoreFactoryDefaultImpl;
import com.linkedin.thirdeye.realtime.ThirdEyeKafkaConfig;
import com.linkedin.thirdeye.realtime.ThirdEyeKafkaConsumer;
import com.linkedin.thirdeye.realtime.ThirdEyeKafkaStats;
import io.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public class KafkaConsumerManager implements Managed
{
  private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerManager.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());

  private final StarTreeManager starTreeManager;
  private final File rootDir;
  private final ExecutorService executorService;
  private final ScheduledExecutorService persistScheduler;
  private final MetricRegistry metricRegistry;
  private final Map<String, ThirdEyeKafkaConsumer> kafkaConsumers;

  private boolean isShutdown;

  public KafkaConsumerManager(StarTreeManager starTreeManager,
                              File rootDir,
                              ExecutorService executorService,
                              ScheduledExecutorService persistScheduler,
                              MetricRegistry metricRegistry)
  {
    this.starTreeManager = starTreeManager;
    this.rootDir = rootDir;
    this.executorService = executorService;
    this.persistScheduler = persistScheduler;
    this.metricRegistry = metricRegistry;
    this.kafkaConsumers = new HashMap<String, ThirdEyeKafkaConsumer>();
  }

  public Map<String, Map<String, ThirdEyeKafkaStats>> getStats()
  {
    Map<String, Map<String, ThirdEyeKafkaStats>> stats = new HashMap<String, Map<String, ThirdEyeKafkaStats>>();

    synchronized (kafkaConsumers)
    {
      for (Map.Entry<String, ThirdEyeKafkaConsumer> entry : kafkaConsumers.entrySet())
      {
        stats.put(entry.getKey(), entry.getValue().getStreamStats());
      }
    }

    return stats;
  }

  @Override
  public void start() throws Exception
  {
    synchronized (kafkaConsumers)
    {
      File[] collectionDirs = rootDir.listFiles();
      if (collectionDirs != null)
      {
        boolean startedOne = false;

        for (File collectionDir : collectionDirs)
        {
          File kafkaFile = new File(collectionDir, StarTreeConstants.KAFKA_CONFIG_FILE_NAME);
          if (kafkaFile.exists())
          {
            start(collectionDir.getName());
            startedOne = true;
          }
          else
          {
            LOG.warn("No kafka.yml for {}, will not start", collectionDir.getName());
          }
        }

        if (startedOne)
        {
          LOG.info("Started all kafka consumers");
        }
      }
    }
  }

  @Override
  public void stop() throws Exception
  {
    synchronized (kafkaConsumers)
    {
      for (String collection : kafkaConsumers.keySet())
      {
        stop(collection);
      }
      LOG.info("Stopped all kafka consumers");
    }
  }

  public void reset() throws Exception
  {
    synchronized (kafkaConsumers)
    {
      stop();
      start();
    }
  }

  public void start(String collection) throws Exception
  {
    synchronized (kafkaConsumers)
    {
      if (!starTreeManager.getCollections().contains(collection))
      {
        throw new IllegalArgumentException("No collection " + collection);
      }

      File collectionDir = new File(rootDir, collection);
      File kafkaFile = new File(collectionDir, StarTreeConstants.KAFKA_CONFIG_FILE_NAME);
      if (kafkaFile.exists())
      {
        StarTreeConfig starTreeConfig
                = OBJECT_MAPPER.readValue(new File(new File(rootDir, collection), StarTreeConstants.CONFIG_FILE_NAME), StarTreeConfig.class);
        starTreeConfig.getRecordStoreFactoryConfig()
                      .setProperty(StarTreeRecordStoreFactoryDefaultImpl.PROP_METRIC_STORE_MUTABLE, "true");

        // Read tree structure
        ObjectInputStream inputStream = new ObjectInputStream(
                new FileInputStream(new File(collectionDir, StarTreeConstants.TREE_FILE_NAME)));
        StarTreeNode root = (StarTreeNode) inputStream.readObject();
        final StarTree mutableStarTree = new StarTreeImpl(starTreeConfig, new File(collectionDir, StarTreeConstants.DATA_DIR_NAME), root);
        mutableStarTree.open();

        ThirdEyeKafkaConsumer kafkaConsumer
                = new ThirdEyeKafkaConsumer(mutableStarTree,
                                            OBJECT_MAPPER.readValue(kafkaFile, ThirdEyeKafkaConfig.class),
                                            executorService,
                                            persistScheduler,
                                            metricRegistry,
                                            rootDir);

        kafkaConsumers.put(collection, kafkaConsumer);

        kafkaConsumer.start();

        LOG.info("Started kafka consumer for {}", collection);
      }
      else
      {
        throw new IllegalArgumentException(
                "Could not find " + StarTreeConstants.KAFKA_CONFIG_FILE_NAME + " for collection " + collection);
      }
    }
  }

  public void stop(String collection) throws Exception
  {
    synchronized (kafkaConsumers)
    {
      ThirdEyeKafkaConsumer kafkaConsumer = kafkaConsumers.remove(collection);
      if (kafkaConsumer != null)
      {
        kafkaConsumer.shutdown();
        LOG.info("Stopped kafka consumer for {}", collection);
      }
    }
  }

  public void reset(String collection) throws Exception
  {
    synchronized (kafkaConsumers)
    {
      stop(collection);
      start(collection);
    }
  }
}
