/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.plugin.inputformat.protobuf;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.pinot.plugin.inputformat.protobuf.kafka.schemaregistry.SchemaRegistryStarter;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class ProtoBufConfluentSchemaTest {
  public static final String TOPIC_PROTO = "test_topic_proto";
  SchemaRegistryStarter.KafkaSchemaRegistryInstance _schemaRegistry;
  private Producer<byte[], Message> _protoProducer;

  @BeforeClass
  public void setup() {
    _schemaRegistry = SchemaRegistryStarter.startLocalInstance(9093);

    Properties protoBufProducerProps = new Properties();
    protoBufProducerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
        _schemaRegistry._kafkaContainer.getBootstrapServers());
    protoBufProducerProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, _schemaRegistry.getUrl());
    protoBufProducerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArraySerializer");
    protoBufProducerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer");
    _protoProducer = new KafkaProducer<>(protoBufProducerProps);
  }

  @Test
  public void testSamplePinotConsumer()
      throws Exception {
    int numRecords = 10;
    List<Sample.SampleRecord> recordList = new ArrayList<>();
    for (int i = 0; i < numRecords; i++) {
      Sample.SampleRecord.Builder builder = Sample.SampleRecord.newBuilder()
          .addFriends(UUID.randomUUID().toString())
          .addFriends(UUID.randomUUID().toString())
          .setEmail(UUID.randomUUID().toString())
          .setName(UUID.randomUUID().toString())
          .setId(i);
      if (i % 2 == 0) {
        builder.setOptionalField(UUID.randomUUID().toString());
      }
      Sample.SampleRecord sampleRecord = builder.build();

      _protoProducer.send(new ProducerRecord<>(TOPIC_PROTO, sampleRecord));
      recordList.add(sampleRecord);
    }

    Properties consumerProps = new Properties();
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, _schemaRegistry._kafkaContainer.getBootstrapServers());
    consumerProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, _schemaRegistry.getUrl());
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "foo_bar");
    KafkaConsumer<byte[], byte[]> kafkaConsumer = new KafkaConsumer<>(consumerProps);
    kafkaConsumer.subscribe(Collections.singletonList(TOPIC_PROTO));
    ConsumerRecords<byte[], byte[]> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
    Iterator<ConsumerRecord<byte[], byte[]>> iter = consumerRecords.iterator();

    Consumer<Message> onMessage = message -> {
      // we use this to verify we are not creating/consuming modified descriptors
      // older versions of confluent connectors (7.1.x and lower) used to rewrite proto3 optional as oneof at descriptor
      // level. Newer versions of confluent consumers support both alternatives.
      Descriptors.FieldDescriptor optionalField = message.getDescriptorForType().findFieldByName("optionalField");
      Assert.assertNull(optionalField.getRealContainingOneof(), "Received protobuf have been rewritten");
    };

    KafkaConfluentSchemaRegistryProtoBufMessageDecoder decoder =
        new KafkaConfluentSchemaRegistryProtoBufMessageDecoder(onMessage);
    Map<String, String> decoderProps = new HashMap<>();
    decoderProps.put("schema.registry.rest.url", _schemaRegistry.getUrl());
    decoder.init(decoderProps, null, TOPIC_PROTO);
    GenericRow reuse = new GenericRow();
    List<GenericRow> result = new ArrayList<>();
    while (iter.hasNext()) {
      byte[] arr = iter.next().value();
      decoder.decode(arr, reuse);
      result.add(reuse.copy());
      reuse.clear();
    }

    Assert.assertEquals(result.size(), numRecords);

    for (int i = 0; i < numRecords; i++) {
      Sample.SampleRecord originalValue = recordList.get(i);
      GenericRow decodedValue = result.get(i);

      Assert.assertEquals(decodedValue.getValue("name"), originalValue.getName(), "Unexpected 'name' value");
      Assert.assertEquals(decodedValue.getValue("id"), originalValue.getId(), "Unexpected 'id' value");
      Assert.assertEquals(decodedValue.getValue("email"), originalValue.getEmail(), "Unexpected 'email' value");

      Object[] expectedFriends = originalValue.getFriendsList()
          .asByteStringList()
          .stream()
          .map(ByteString::toStringUtf8)
          .toArray(Object[]::new);
      Assert.assertEquals(decodedValue.getValue("friends"), expectedFriends, "Unexpected 'friends' value");

      String expectedOptionalField = i % 2 == 0 ? originalValue.getOptionalField() : null;
      Assert.assertEquals(decodedValue.getValue("optionalField"), expectedOptionalField,
          "Unexpected 'optionalField' value");
    }
  }

  @AfterClass
  public void tearDown() {
    _schemaRegistry.stop();
  }
}
