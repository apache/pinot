package com.linkedin.thirdeye.managed;

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.impl.NumberUtils;
import com.linkedin.thirdeye.impl.StarTreeRecordImpl;
import io.dropwizard.lifecycle.Managed;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaConsumerManager implements Managed
{
  private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerManager.class);

  private final StarTreeManager manager;
  private final File rootDir;
  private final String kafkaZooKeeperAddress;
  private final String kafkaGroupId;
  private final AtomicBoolean isStarted;
  private final ThreadLocal<BinaryDecoder> decoderThreadLocal;
  private final Object sync;

  private ExecutorService executorService;

  public KafkaConsumerManager(StarTreeManager manager,
                              File rootDir,
                              String kafkaZooKeeperAddress,
                              int kafkaGroupIdSuffix)
  {
    this.manager = manager;
    this.rootDir = rootDir;
    this.kafkaZooKeeperAddress = kafkaZooKeeperAddress;
    this.kafkaGroupId = String.format(KafkaConsumerManager.class.getCanonicalName() + "_" + kafkaGroupIdSuffix);
    this.isStarted = new AtomicBoolean();
    this.decoderThreadLocal = new ThreadLocal<BinaryDecoder>();
    this.sync = new Object();
  }

  @Override
  public void start() throws Exception
  {
    if (kafkaZooKeeperAddress == null)
    {
      // NOP
      return;
    }

    synchronized (sync)
    {
      if (!isStarted.getAndSet(true))
      {
        final Map<String, Long> startTimes = new HashMap<String, Long>();
        final Map<String, DatumReader<GenericRecord>> readers = new HashMap<String, DatumReader<GenericRecord>>();
        final Map<String, String> kafkaTopics = new HashMap<String, String>();
        final Map<String, Integer> topicCountMap = new HashMap<String, Integer>();

        for (String collection : manager.getCollections())
        {
          File schemaFile = new File(new File(rootDir, collection), StarTreeConstants.SCHEMA_FILE_NAME);
          if (schemaFile.exists())
          {
            Schema schema = new Schema.Parser().parse(schemaFile);
            String kafkaTopic = schema.getProp("kafkaTopic");
            if (kafkaTopic == null)
            {
              LOG.warn("Found schema with no kafkaTopic: {}", schemaFile);
            }
            else
            {
              readers.put(collection, new GenericDatumReader<GenericRecord>(schema));
              startTimes.put(collection, manager.getStarTree(collection).getStats().getMaxTime());
              topicCountMap.put(kafkaTopic, 1);
              kafkaTopics.put(kafkaTopic, collection);
            }
          }
        }

        executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

        Properties props = new Properties();
        props.put("zookeeper.connect", kafkaZooKeeperAddress);
        props.put("group.id", kafkaGroupId);
        props.put("auto.commit.enable", "false");
        props.put("auto.offset.reset", "smallest");

        final ConsumerConnector consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));

        Map<String, List<KafkaStream<byte[], byte[]>>> streams = consumer.createMessageStreams(topicCountMap);

        for (Map.Entry<String, List<KafkaStream<byte[], byte[]>>> entry : streams.entrySet())
        {
          for (final KafkaStream<byte[], byte[]> stream : entry.getValue())
          {
            executorService.submit(new Runnable()
            {
              @Override
              public void run()
              {
                ConsumerIterator<byte[], byte[]> itr = stream.iterator();

                while (isStarted.get() && itr.hasNext())
                {
                  try
                  {
                    MessageAndMetadata<byte[], byte[]> next = itr.next();
                    String collection = kafkaTopics.get(next.topic());
                    StarTree starTree = manager.getStarTree(collection);

                    // Decode Avro
                    DatumReader<GenericRecord> reader = readers.get(collection);
                    decoderThreadLocal.set(DecoderFactory.get().binaryDecoder(next.message(), decoderThreadLocal.get()));
                    GenericRecord avroRecord = reader.read(null, decoderThreadLocal.get());

                    // Translate to StarTreeRecord
                    StarTreeRecord record = convert(starTree.getConfig(), avroRecord);

                    // Update tree
                    if (!record.getMetricTimeSeries().getTimeWindowSet().isEmpty())
                    {
                      Long minTime = Collections.min(record.getMetricTimeSeries().getTimeWindowSet());

                      if (minTime > startTimes.get(collection))
                      {
                        starTree.add(record);
                      }
                    }
                  }
                  catch (IOException e)
                  {
                    throw new RuntimeException(e);
                  }
                }
              }
            });
          }
        }

        LOG.info("Started kafka consumption");
      }
    }
  }

  @Override
  public void stop() throws Exception
  {
    if (kafkaZooKeeperAddress == null)
    {
      // NOP
      return;
    }

    synchronized (sync)
    {
      if (isStarted.getAndSet(false))
      {
        if (executorService != null)
        {
          executorService.shutdown();
        }

        LOG.info("Shut down kafka consumption");
      }
    }
  }

  public void reset() throws Exception
  {
    synchronized (sync)
    {
      stop();
      start();
    }
  }

  private static StarTreeRecord convert(StarTreeConfig config, GenericRecord record)
  {
    // Dimensions
    String[] dimensionValues = new String[config.getDimensions().size()];
    for (int i = 0; i < config.getDimensions().size(); i++)
    {
      String dimensionName = config.getDimensions().get(i).getName();
      Object dimensionObj = record.get(dimensionName);
      if (dimensionObj == null)
      {
        dimensionObj = "";
      }
      dimensionValues[i] = dimensionObj.toString();
    }
    DimensionKey dimensionKey = new DimensionKey(dimensionValues);

    // Time
    Object timeObj = record.get(config.getTime().getColumnName());
    if (timeObj == null)
    {
      throw new IllegalArgumentException("Record has null time " + config.getTime().getColumnName() + ": " + record);
    }
    if (!(timeObj instanceof Number))
    {
      throw new IllegalArgumentException("Time must be numeric (it is " + timeObj.getClass() + ")");
    }
    Long time = ((Number) timeObj).longValue();

    // Metrics
    MetricTimeSeries timeSeries = new MetricTimeSeries(MetricSchema.fromMetricSpecs(config.getMetrics()));
    for (int i = 0; i < config.getMetrics().size(); i++)
    {
      MetricSpec metricSpec = config.getMetrics().get(i);
      Object metricObj = record.get(metricSpec.getName());
      if (metricObj == null)
      {
        metricObj = 0;
      }
      Number metricValue = NumberUtils.valueOf(metricObj.toString(), metricSpec.getType());
      timeSeries.set(time, metricSpec.getName(), metricValue);
    }

    return new StarTreeRecordImpl(config, dimensionKey, timeSeries);
  }
}
