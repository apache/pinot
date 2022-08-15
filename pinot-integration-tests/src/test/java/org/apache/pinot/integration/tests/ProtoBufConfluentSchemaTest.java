package org.apache.pinot.integration.tests;

import com.google.protobuf.Message;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.pinot.integration.tests.kafka.schemaregistry.SchemaRegistryStarter;
import org.apache.pinot.plugin.inputformat.protobuf.KafkaConfluentSchemaRegistryProtoBufMessageDecoder;
import org.apache.pinot.plugin.inputformat.protobuf.Sample;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.StreamDataServerStartable;
import org.apache.pinot.tools.utils.KafkaStarterUtils;
import org.junit.Rule;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class ProtoBufConfluentSchemaTest {
  SchemaRegistryStarter.KafkaSchemaRegistryInstance _schemaRegistry;

  @BeforeClass
  public void setup() {
    _schemaRegistry =  SchemaRegistryStarter.startLocalInstance(9093);

    Properties avroProducerProps = new Properties();
    avroProducerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, _schemaRegistry._kafkaContainer.getBootstrapServers());
    avroProducerProps.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, _schemaRegistry.getUrl());
    avroProducerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArraySerializer");
    avroProducerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer");
    Producer<byte[], Message> protoProducer = new KafkaProducer<>(avroProducerProps);

    for (int i = 0; i < 10; i++) {
      Sample.SampleRecord sampleRecord =
          Sample.SampleRecord.newBuilder().addFriends("foo").addFriends("bar").setEmail("foo@bar.com").setId(i)
              .setName("hello").build();

      protoProducer.send(new ProducerRecord<>("test_topic_proto", sampleRecord));
    }
  }

  @Test
  public void testSamplePinotConsumer() throws Exception {
    Properties consumerProps = new Properties();
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, _schemaRegistry._kafkaContainer.getBootstrapServers());
    consumerProps.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, _schemaRegistry.getUrl());
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "foo_bar");
    KafkaConsumer<byte[], byte[]> kafkaConsumer = new KafkaConsumer<byte[], byte[]>(consumerProps);
    kafkaConsumer.subscribe(Collections.singletonList("test_topic_proto"));
    ConsumerRecords<byte[], byte[]> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
    System.out.println("NUM RECORDS: " + consumerRecords.count());
    Iterator<ConsumerRecord<byte[], byte[]>> iter = consumerRecords.iterator();

    KafkaConfluentSchemaRegistryProtoBufMessageDecoder decoder = new KafkaConfluentSchemaRegistryProtoBufMessageDecoder();
    Map<String, String> decoderProps = new HashMap<>();
    decoderProps.put("schema.registry.rest.url", _schemaRegistry.getUrl());
    decoder.init(decoderProps, null, "test_topic_proto");
    GenericRow reuse = new GenericRow();
    while(iter.hasNext()){
      byte[] arr = iter.next().value();
      decoder.decode(arr, reuse);
      System.out.println(reuse.toString());
      reuse.clear();
    }

  }


  @AfterClass
  public void tearDown() {
    _schemaRegistry.stop();
  }
}
