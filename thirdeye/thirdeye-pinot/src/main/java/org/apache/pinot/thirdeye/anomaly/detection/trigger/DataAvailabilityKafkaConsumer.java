/*
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

package org.apache.pinot.thirdeye.anomaly.detection.trigger;

import java.util.List;
import java.util.Properties;

/**
 * This abstract class defines the framework for Kafka consumers that generates trigger event for anomaly detection.
 * By extending from this abstract class, an application-specific consumer can be used in {@code DataAvailabilityEventListener},
 * so that anomaly detection can be triggered regardless of the actual content of Kafka events. In order to be
 * initialized by {@code DataAvailabilityEventListener}, all extended consumer classes should have the same constructor of this
 * base class.
 */
public abstract class DataAvailabilityKafkaConsumer {
  protected final String topic;
  protected final String groupId;
  protected final String bootstrapServers;
  protected final Properties properties;

  public DataAvailabilityKafkaConsumer(String topic, String groupId, String bootstrapServers, Properties properties) {
    this.topic = topic;
    this.groupId = groupId;
    this.bootstrapServers = bootstrapServers;
    this.properties = properties;
  }

  /**
   * Get a batch of events from subscribed Kafka topic
   * @param poll poll time for Kafka consumer
   * @return list of events from Source
   */
  public abstract List<DataAvailabilityEvent> poll(long poll);

  /**
   * Commit a checkpoint
   */
  public abstract void commitSync();

  /**
   * Close the consumer
   */
  public abstract void close();
}
