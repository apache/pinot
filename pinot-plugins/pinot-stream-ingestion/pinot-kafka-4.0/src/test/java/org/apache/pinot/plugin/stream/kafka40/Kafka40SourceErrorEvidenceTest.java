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
package org.apache.pinot.plugin.stream.kafka40;

import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.pinot.plugin.stream.kafka.BaseKafkaSourceErrorEvidenceTest;
import org.apache.pinot.plugin.stream.kafka40.utils.MiniKafkaCluster;
import org.apache.pinot.spi.stream.PartitionGroupConsumer;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import org.apache.pinot.spi.utils.retry.RetryPolicy;


/// Runs the Kafka source error evidence tests against the kafka-clients 4.x plugin and a containerized broker.
public class Kafka40SourceErrorEvidenceTest extends BaseKafkaSourceErrorEvidenceTest {
  private static final String CLIENT_ID = "clientId";
  // The handler's constructor calls the consumer hook before a subclass is initialized.
  private static final ThreadLocal<Consumer<Bytes, Bytes>> KAFKA_CONSUMER = new ThreadLocal<>();

  private MiniKafkaCluster _broker;

  @Override
  protected PartitionGroupConsumer createConsumer(StreamConfig streamConfig, RetryPolicy retryPolicy) {
    return new KafkaPartitionLevelConsumer(CLIENT_ID, streamConfig, 0, retryPolicy);
  }

  @Override
  protected PartitionGroupConsumer createConsumer(StreamConfig streamConfig, Consumer<Bytes, Bytes> kafkaConsumer) {
    KAFKA_CONSUMER.set(kafkaConsumer);
    try {
      return new KafkaPartitionLevelConsumer(CLIENT_ID, streamConfig, 0) {
        @Override
        protected Consumer<Bytes, Bytes> createConsumer(Properties consumerProp) {
          return KAFKA_CONSUMER.get();
        }
      };
    } finally {
      KAFKA_CONSUMER.remove();
    }
  }

  @Override
  protected StreamMetadataProvider createMetadataProvider(StreamConfig streamConfig) {
    return new KafkaStreamMetadataProvider(CLIENT_ID, streamConfig);
  }

  @Override
  protected String startBroker()
      throws Exception {
    _broker = new MiniKafkaCluster("0");
    _broker.start();
    return _broker.getKafkaServerAddress();
  }

  @Override
  protected void stopBroker() {
    _broker.stop();
  }
}
