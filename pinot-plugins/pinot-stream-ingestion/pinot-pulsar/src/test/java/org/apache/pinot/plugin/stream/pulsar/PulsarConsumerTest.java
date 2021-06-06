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
package org.apache.pinot.plugin.stream.pulsar;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.spi.stream.MessageBatch;
import org.apache.pinot.spi.stream.PartitionLevelConsumer;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConsumerFactory;
import org.apache.pinot.spi.stream.StreamConsumerFactoryProvider;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class PulsarConsumerTest {

  public static final String TABLE_NAME_WITH_TYPE = "tableName_REALTIME";
  public static final String TEST_TOPIC = "test-topic";
  public static final int NUM_PARTITION = 3;
  public static final String MESSAGE_PREFIX = "sample_msg";
  public static final int NUM_RECORDS = 1000;
  public static final String CLIENT_ID = "clientId";
  private PulsarClient _pulsarClient;
  private PulsarStandaloneCluster _pulsarStandaloneCluster;

  @BeforeClass
  public void setUp()
      throws Exception {
    _pulsarStandaloneCluster = new PulsarStandaloneCluster();

    _pulsarStandaloneCluster.start();

    PulsarAdmin admin =
        PulsarAdmin.builder().serviceHttpUrl("http://localhost:" + _pulsarStandaloneCluster.getAdminPort()).build();

    String bootstrapServer = "pulsar://localhost:" + _pulsarStandaloneCluster.getBrokerPort();

    _pulsarClient = PulsarClient.builder().serviceUrl(bootstrapServer).build();

    admin.topics().createPartitionedTopic(TEST_TOPIC, NUM_PARTITION);

    publishRecords();
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    _pulsarStandaloneCluster.stop();
  }

  public void publishRecords()
      throws Exception {
    Producer<String> producer = _pulsarClient.newProducer(Schema.STRING).topic(TEST_TOPIC).create();
    for (int i = 0; i < NUM_RECORDS; i++) {
      producer.send(MESSAGE_PREFIX + "-" + i);
    }
  }

  public StreamConfig getStreamConfig() {
    String streamType = "pulsar";
    String streamPulsarBrokerList = "pulsar://localhost:" + _pulsarStandaloneCluster.getBrokerPort();
    String streamPulsarConsumerType = "simple";
    String tableNameWithType = TABLE_NAME_WITH_TYPE;

    Map<String, String> streamConfigMap = new HashMap<>();
    streamConfigMap.put("streamType", streamType);
    streamConfigMap.put("stream.pulsar.consumer.type", streamPulsarConsumerType);
    streamConfigMap.put("stream.pulsar.topic.name", TEST_TOPIC);
    streamConfigMap.put("stream.pulsar.bootstrap.servers", streamPulsarBrokerList);
    streamConfigMap.put("stream.pulsar.start_position", "earliest");
    streamConfigMap.put("stream.pulsar.consumer.factory.class.name", getPulsarConsumerFactoryName());
    streamConfigMap.put("stream.pulsar.decoder.class.name", "decoderClass");
    StreamConfig streamConfig = new StreamConfig(tableNameWithType, streamConfigMap);

    return streamConfig;
  }

  protected String getPulsarConsumerFactoryName() {
    return PulsarConsumerFactory.class.getName();
  }

  @Test
  public void testPartitionLevelConsumer()
      throws Exception {

    final StreamConsumerFactory streamConsumerFactory = StreamConsumerFactoryProvider.create(getStreamConfig());
    int numPartitions = new PulsarStreamMetadataProvider(CLIENT_ID, getStreamConfig()).fetchPartitionCount(10000);

    int totalMessagesReceived = 0;
    for (int partition = 0; partition < numPartitions; partition++) {
      final PartitionLevelConsumer consumer = streamConsumerFactory.createPartitionLevelConsumer(CLIENT_ID, partition);
      final MessageBatch messageBatch =
          consumer.fetchMessages(new MessageIdStreamOffset(MessageId.earliest), null, 10000);

      for (int i = 0; i < messageBatch.getMessageCount(); i++) {
        final byte[] msg = (byte[]) messageBatch.getMessageAtIndex(i);
        Assert.assertTrue(new String(msg).contains(MESSAGE_PREFIX));
        totalMessagesReceived++;
      }
    }

    Assert.assertEquals(totalMessagesReceived, NUM_RECORDS);
  }
}
