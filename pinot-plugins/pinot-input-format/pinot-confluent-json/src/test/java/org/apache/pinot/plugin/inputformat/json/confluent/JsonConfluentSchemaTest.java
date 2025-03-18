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
package org.apache.pinot.plugin.inputformat.json.confluent;

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
import org.apache.pinot.plugin.inputformat.json.confluent.kafka.schemaregistry.SchemaRegistryStarter;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class JsonConfluentSchemaTest {
  public static final String TOPIC_JSON = "test_topic_json";
  SchemaRegistryStarter.KafkaSchemaRegistryInstance _schemaRegistry;
  private Producer<byte[], Map<String, Object>> _jsonProducer;

  @BeforeClass
  public void setup() {
    _schemaRegistry = SchemaRegistryStarter.startLocalInstance(9093);

    Properties jsonBufProducerProps = new Properties();
    jsonBufProducerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
        _schemaRegistry._kafkaContainer.getBootstrapServers());
    jsonBufProducerProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, _schemaRegistry.getUrl());
    jsonBufProducerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArraySerializer");
    jsonBufProducerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "io.confluent.kafka.serializers.json.KafkaJsonSerializer");
    _jsonProducer = new KafkaProducer<>(jsonBufProducerProps);
  }

  @Test
  public void testSamplePinotConsumer()
      throws Exception {
    int numRecords = 10;
    List<Map<String, Object>> recordList = new ArrayList<>();
    for (int i = 0; i < numRecords; i++) {
      Map<String, Object> sampleRecord = new HashMap<>();
      sampleRecord.put("name", UUID.randomUUID().toString());
      sampleRecord.put("email", UUID.randomUUID().toString());
      List<String> friends = new ArrayList<>();
      friends.add(UUID.randomUUID().toString());
      friends.add(UUID.randomUUID().toString());
      sampleRecord.put("friends", friends);
      sampleRecord.put("id", i);
      if (i % 2 == 0) {
        sampleRecord.put("optionalField", UUID.randomUUID().toString());
      }

      _jsonProducer.send(new ProducerRecord<>(TOPIC_JSON, sampleRecord));
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
    kafkaConsumer.subscribe(Collections.singletonList(TOPIC_JSON));
    ConsumerRecords<byte[], byte[]> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
    Iterator<ConsumerRecord<byte[], byte[]>> iter = consumerRecords.iterator();

    Consumer<Map<String, Object>> onMessage = message -> {
      String optionalField = (String) message.get("optionalField");
      Assert.assertNull(optionalField, "Received json have been rewritten");
    };

    KafkaConfluentSchemaRegistryJsonMessageDecoder decoder =
        new KafkaConfluentSchemaRegistryJsonMessageDecoder(onMessage);
    Map<String, String> decoderProps = new HashMap<>();
    decoderProps.put("schema.registry.rest.url", _schemaRegistry.getUrl());
    decoder.init(decoderProps, null, TOPIC_JSON);
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
      Map<String, Object> originalValue = recordList.get(i);
      GenericRow decodedValue = result.get(i);

      Assert.assertEquals(decodedValue.getValue("name"), originalValue.get("name"), "Unexpected 'name' value");
      Assert.assertEquals(decodedValue.getValue("id"), originalValue.get("id"), "Unexpected 'id' value");
      Assert.assertEquals(decodedValue.getValue("email"), originalValue.get("email"), "Unexpected 'email' value");

      Object[] expectedFriends = ((List<String>) originalValue.get("friends")).toArray(new String[0]);
      Assert.assertEquals(decodedValue.getValue("friends"), expectedFriends, "Unexpected 'friends' value");
      String expectedOptionalField = i % 2 == 0 ? (String) originalValue.get("optionalField") : null;
      Assert.assertEquals(decodedValue.getValue("optionalField"), expectedOptionalField, "Unexpected 'optionalField' value");
    }
  }

  @AfterClass
  public void tearDown() {
    _schemaRegistry.stop();
  }
}
