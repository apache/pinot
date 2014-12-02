package com.linkedin.thirdeye.realtime;

import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.impl.StarTreeRecordImpl;
import com.linkedin.thirdeye.impl.StarTreeUtils;
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

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class ThirdEyeKafkaConsumer implements Runnable
{
  public static final String PROP_THREAD_COUNT = "thread.count";
  public static final String PROP_START_TIME = "start.time";

  public static final int DEFAULT_THREAD_COUNT = Runtime.getRuntime().availableProcessors();

  private static final Logger LOG = LoggerFactory.getLogger(ThirdEyeKafkaConsumer.class);

  private final ExecutorService executorService;
  private final String topic;
  private final Properties kafkaConfig;
  private final Schema avroSchema;
  private final StarTree starTree;
  private final ConsumerConnector consumerConnector;
  private final AtomicBoolean isShutdown;
  private final AtomicLong messagesReceived;
  private final Long startTime;

  public ThirdEyeKafkaConsumer(ExecutorService executorService,
                               String topic,
                               Properties kafkaConfig,
                               Schema avroSchema,
                               StarTree starTree)
  {
    this.executorService = executorService;
    this.topic = topic;
    this.kafkaConfig = kafkaConfig;
    this.avroSchema = avroSchema;
    this.starTree = starTree;
    this.consumerConnector = Consumer.createJavaConsumerConnector(new ConsumerConfig(kafkaConfig));
    this.isShutdown = new AtomicBoolean();
    this.messagesReceived = new AtomicLong();

    this.startTime = kafkaConfig.containsKey(PROP_START_TIME)
            ? Long.valueOf(kafkaConfig.getProperty(PROP_START_TIME)) : null;

    LOG.info("Creating kafka consumer with config:");
    for (Map.Entry<Object, Object> entry : kafkaConfig.entrySet())
    {
      LOG.info("\t{}={}", entry.getKey(), entry.getValue());
    }
  }

  public long getMessagesReceived()
  {
    return messagesReceived.get();
  }

  @Override
  public void run()
  {
    int threadCount = DEFAULT_THREAD_COUNT;
    if (kafkaConfig.getProperty(PROP_THREAD_COUNT) != null)
    {
      threadCount = Integer.valueOf(kafkaConfig.getProperty(PROP_THREAD_COUNT));
    }

    List<KafkaStream<byte[], byte[]>> streams
            = consumerConnector.createMessageStreams(Collections.singletonMap(topic, threadCount)).get(topic);

    for (KafkaStream<byte[], byte[]> stream : streams)
    {
      executorService.submit(new Worker(stream));
    }
  }

  public void shutdown()
  {
    if (!isShutdown.getAndSet(true))
    {
      LOG.info("Shutting down");
    }
  }

  private class Worker implements Runnable
  {
    private final KafkaStream<byte[], byte[]> stream;
    private final DatumReader<GenericRecord> datumReader;
    private final StarTreeRecordImpl.Builder builder;

    private BinaryDecoder reuseDecoder;
    private GenericRecord reuseRecord;

    Worker(KafkaStream<byte[], byte[]> stream)
    {
      this.stream = stream;
      this.datumReader = new GenericDatumReader<GenericRecord>(avroSchema);
      this.builder = new StarTreeRecordImpl.Builder();
    }

    @Override
    public void run()
    {
      ConsumerIterator<byte[], byte[]> itr = stream.iterator();

      while (!isShutdown.get() && itr.hasNext())
      {
        try
        {
          // Decode
          MessageAndMetadata<byte[], byte[]> next = itr.next();
          reuseDecoder = DecoderFactory.get().binaryDecoder(next.message(), reuseDecoder);
          reuseRecord = datumReader.read(reuseRecord, reuseDecoder);
          messagesReceived.incrementAndGet();

          // Convert
          StarTreeUtils.toStarTreeRecord(starTree.getConfig(), reuseRecord, builder);
          StarTreeRecord record = builder.build();

          // Update
          if (startTime == null || record.getTime() >= startTime)
          {
            starTree.add(record);
          }

          // Reset local state
          builder.clear();
        }
        catch (IOException e)
        {
          throw new RuntimeException(e);
        }
      }
    }
  }
}
