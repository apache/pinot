package com.linkedin.thirdeye.realtime;

import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeQuery;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.impl.StarTreeImpl;
import com.linkedin.thirdeye.impl.StarTreeQueryImpl;
import com.linkedin.thirdeye.impl.StarTreeRecordImpl;
import com.linkedin.thirdeye.impl.StarTreeRecordStoreFactoryLogBufferImpl;
import com.linkedin.thirdeye.impl.StarTreeUtils;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TestThirdEyeKafkaConsumer
{
  private static final Random RANDOM = new Random();
  private static final String TOPIC = "myTopic";

  private File tmpDir;
  private Schema schema;
  private StarTreeConfig config;
  private ExecutorService executorService;

  private File zkRoot;
  private int zkPort;
  private ZkServer zkServer;

  private File kafkaRoot;
  private int kafkaPort;
  private KafkaServerStartable kafkaServer;

  private Producer<byte[], byte[]> kafkaProducer;
  private ThirdEyeKafkaConsumer kafkaConsumer;

  @BeforeClass
  public void beforeClass() throws Exception
  {
    tmpDir = new File(System.getProperty("java.io.tmpdir"), TestThirdEyeKafkaConsumer.class.getSimpleName());
    try
    {
      FileUtils.forceDelete(tmpDir);
    }
    catch (Exception e)
    {
      // Ok
    }

    // Star tree config
    config = new StarTreeConfig.Builder()
            .setCollection(TOPIC)
            .setDimensionNames(Arrays.asList("A", "B", "C"))
            .setMetricNames(Arrays.asList("M"))
            .setMetricTypes(Arrays.asList("INT"))
            .setTime(new TimeSpec("T", null, null, null))
            .setRecordStoreFactoryClass(StarTreeRecordStoreFactoryLogBufferImpl.class.getCanonicalName())
            .build();


    // Parse schema
    schema = new Schema.Parser().parse(ClassLoader.getSystemResourceAsStream("MyRecord.avsc"));

    // Start ZK
    zkRoot = new File(tmpDir, "zk");
    zkPort = 40000;
    zkServer = new ZkServer(new File(zkRoot, "data").getAbsolutePath(),
                            new File(zkRoot, "log").getAbsolutePath(),
                            new IDefaultNameSpace()
    {
      @Override
      public void createDefaultNameSpace(ZkClient zkClient)
      {
        // Do nothing
      }
    }, zkPort);
    zkServer.start();

    // Start Kafka broker
    kafkaPort = 40001;
    kafkaRoot = new File(tmpDir, "kafka");
    Properties kafkaConfig = new Properties();
    kafkaConfig.load(ClassLoader.getSystemResourceAsStream("test-kafka-server.properties"));
    kafkaConfig.setProperty("port", String.valueOf(kafkaPort));
    kafkaConfig.setProperty("zookeeper.connect", "localhost:" + zkPort);
    kafkaConfig.setProperty("log.dirs", kafkaRoot.getAbsolutePath());
    kafkaServer = new KafkaServerStartable(new KafkaConfig(kafkaConfig));
    kafkaServer.startup();

    // Create a producer
    Properties producerConfig = new Properties();
    producerConfig.setProperty("metadata.broker.list", "localhost:" + kafkaPort);
    producerConfig.setProperty("request.required.acks", "-1");
    kafkaProducer = new Producer<byte[], byte[]>(new ProducerConfig(producerConfig));

    // Produce some records into stream
    GenericRecord record = null;
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
    BinaryEncoder encoder = null;
    for (int i = 0; i < 100; i++)
    {
      // Serialize Avro
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      record = generateRecord(record);
      encoder = EncoderFactory.get().binaryEncoder(baos, encoder);
      datumWriter.write(record, encoder);
      encoder.flush();

      // Produce record
      kafkaProducer.send(new KeyedMessage<byte[], byte[]>(TOPIC, baos.toByteArray()));
    }

    executorService =  Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
  }

  @AfterClass
  public void afterClass() throws Exception
  {
    try
    {
      kafkaProducer.close();

      // Stop consumer
      if (kafkaConsumer != null)
      {
        kafkaConsumer.shutdown();
      }

      // Stop kafka broker
      kafkaServer.shutdown();
      kafkaServer.awaitShutdown();

      // Stop ZK
      zkServer.shutdown();
    }
    finally
    {
      // Wipe data directories
      FileUtils.forceDelete(tmpDir);
    }
  }

  @Test
  public void testConsumer() throws Exception
  {
    // Create star tree
    StarTree starTree = new StarTreeImpl(config);
    starTree.open();

    // Create consumer
    Properties consumerConfig = new Properties();
    consumerConfig.setProperty("zookeeper.connect", "localhost:" + zkPort);
    consumerConfig.setProperty("group.id", "1000");
    consumerConfig.put("zookeeper.session.timeout.ms", "400");
    consumerConfig.put("zookeeper.sync.time.ms", "200");
    consumerConfig.put("auto.commit.interval.ms", "1000");
    consumerConfig.put("auto.offset.reset", "smallest"); // consume the messages we produced
    kafkaConsumer = new ThirdEyeKafkaConsumer(executorService, TOPIC, consumerConfig, schema, starTree);

    // Run consumer
    kafkaConsumer.run();

    // Wait until it consumes everything (or timeout)
    long start = System.currentTimeMillis();
    long timeout = 30000; // 30s
    while (System.currentTimeMillis() - start < timeout && kafkaConsumer.getMessagesReceived() < 100)
    {
      Thread.sleep(1000);
    }

    // Check that it did consume all
    Assert.assertEquals(kafkaConsumer.getMessagesReceived(), 100);

    // Check that queries reflect this
    StarTreeQuery query = new StarTreeQueryImpl.Builder()
            .setDimensionValue("A", "*")
            .setDimensionValue("B", "*")
            .setDimensionValue("C", "*")
            .build();
    StarTreeRecord starTreeRecord = starTree.getAggregate(query);
    Assert.assertEquals(starTreeRecord.getMetricValues().get("M").intValue(), 100);
  }

  private GenericRecord generateRecord(GenericRecord reuse)
  {
    StarTreeRecord record = new StarTreeRecordImpl.Builder()
            .setDimensionValue("A", "A" + RANDOM.nextInt(2))
            .setDimensionValue("B", "B" + RANDOM.nextInt(4))
            .setDimensionValue("C", "C" + RANDOM.nextInt(8))
            .setMetricValue("M", 1)
            .setMetricType("M", "INT")
            .setTime(0L)
            .build();

    return StarTreeUtils.toGenericRecord(config, schema, record, reuse);
  }
}
