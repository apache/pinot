package com.linkedin.thirdeye.managed;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.impl.StarTreeImpl;
import com.linkedin.thirdeye.impl.storage.StarTreeRecordStoreFactoryDefaultImpl;
import com.linkedin.thirdeye.impl.storage.StorageUtils;
import com.linkedin.thirdeye.realtime.ThirdEyeKafkaConfig;
import com.linkedin.thirdeye.realtime.ThirdEyeKafkaConsumer;
import com.linkedin.thirdeye.realtime.ThirdEyeKafkaStats;
import io.dropwizard.lifecycle.Managed;
import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.ObjectInputStream;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public class ThirdEyeKafkaConsumerManager implements Managed
{
  private static final Logger LOG = LoggerFactory.getLogger(ThirdEyeKafkaConsumerManager.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());

  static
  {
    OBJECT_MAPPER.registerModule(new JodaModule());
  }

  private final StarTreeManager starTreeManager;
  private final File rootDir;
  private final ExecutorService executorService;
  private final ScheduledExecutorService persistScheduler;
  private final MetricRegistry metricRegistry;
  private final Map<String, ThirdEyeKafkaConsumer> kafkaConsumers;

  public ThirdEyeKafkaConsumerManager(StarTreeManager starTreeManager,
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
            = OBJECT_MAPPER.readValue(new File(collectionDir, StarTreeConstants.CONFIG_FILE_NAME), StarTreeConfig.class);
        ThirdEyeKafkaConfig kafkaConfig
            = OBJECT_MAPPER.readValue(kafkaFile, ThirdEyeKafkaConfig.class);

        starTreeConfig.getRecordStoreFactoryConfig()
                      .setProperty(StarTreeRecordStoreFactoryDefaultImpl.PROP_METRIC_STORE_MUTABLE, "true");

        // Get tree
        File latestDataDir = StorageUtils.findLatestDataDir(collectionDir);
        if (latestDataDir == null)
        {
          throw new IllegalStateException("No available star tree");
        }
        File starTreeFile = new File(latestDataDir, StarTreeConstants.TREE_FILE_NAME);
        FileInputStream fis = new FileInputStream(new File(latestDataDir, StarTreeConstants.TREE_FILE_NAME));
        ObjectInputStream ois = new ObjectInputStream(fis);
        StarTreeNode root = (StarTreeNode) ois.readObject();
        LOG.info("Using tree {} from {} for collection {}", root.getId(), latestDataDir, collection);

        // Create data directory for kafka consumer
        File kafkaDataDir = new File(
            collectionDir, StorageUtils.getDataDirName(root.getId().toString(), "KAFKA", new DateTime(), null));
        FileUtils.forceMkdir(kafkaDataDir);

        // Copy the dimension store and tree
        FileUtils.copyFile(
            starTreeFile,
            new File(kafkaDataDir, StarTreeConstants.TREE_FILE_NAME));
        FileUtils.copyDirectory(
            new File(latestDataDir, StarTreeConstants.DIMENSION_STORE),
            new File(kafkaDataDir, StarTreeConstants.DIMENSION_STORE));
        LOG.info("Bootstrapped {} with tree / dimension store from {}", kafkaDataDir, latestDataDir);

        // Create and open tree
        StarTree mutableTree = new StarTreeImpl(starTreeConfig, kafkaDataDir, root);
        mutableTree.open();

        // Start consumer
        ThirdEyeKafkaConsumer kafkaConsumer
                = new ThirdEyeKafkaConsumer(mutableTree,
                                            kafkaConfig,
                                            executorService,
                                            persistScheduler,
                                            metricRegistry,
                                            kafkaDataDir);
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
