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
package org.apache.pinot.plugin.stream.kafka09.server;

import java.util.Properties;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.pinot.spi.stream.StreamDataProducer;


public class KafkaDataProducer implements StreamDataProducer {
  private Producer<byte[], byte[]> _producer;

  @Override
  public void init(Properties props) {
    ProducerConfig producerConfig = new ProducerConfig(props);
    _producer = new Producer(producerConfig);
  }

  @Override
  public void produce(String topic, byte[] payload) {
    KeyedMessage<byte[], byte[]> data = new KeyedMessage<>(topic, payload);
    this.produce(data);
  }

  @Override
  public void produce(String topic, byte[] key, byte[] payload) {
    KeyedMessage<byte[], byte[]> data = new KeyedMessage<>(topic, key, payload);
    this.produce(data);
  }

  public void produce(KeyedMessage message) {
    _producer.send(message);
  }

  @Override
  public void close() {
    _producer.close();
  }
}
