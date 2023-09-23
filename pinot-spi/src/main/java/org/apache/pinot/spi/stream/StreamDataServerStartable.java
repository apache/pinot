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
package org.apache.pinot.spi.stream;

import java.util.Properties;


/**
 * StreamDataServerStartable is the interface for stream data sources.
 * Each stream data connector should implement a mock/wrapper of the data server.
 *
 * E.g. KafkaDataServerStartable is a wrapper class of Kafka 0.9 broker.
 *
 */
public interface StreamDataServerStartable {
  /**
   * Init the server.
   *
   * @param props
   */
  void init(Properties props);

  /**
   * Start the server
   */
  void start();

  /**
   * Stop the server
   */
  void stop();

  /**
   * Create a data stream (e.g Kafka topic) in the server.
   *
   * @param topic
   * @param topicProps
   */
  void createTopic(String topic, Properties topicProps);

  /**
   * Delete a data stream (e.g Kafka topic) in the server.
   *
   * @param topic
   */
  void deleteTopic(String topic);


  /**
   * Get the port of the server.
   */
  int getPort();
}
