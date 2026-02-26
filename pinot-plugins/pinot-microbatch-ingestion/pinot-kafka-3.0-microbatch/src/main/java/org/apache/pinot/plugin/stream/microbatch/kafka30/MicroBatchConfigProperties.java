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
package org.apache.pinot.plugin.stream.microbatch.kafka30;

import org.apache.pinot.spi.stream.StreamConfig;


/**
 * Configuration properties for MicroBatch Kafka consumer.
 *
 * <p>Config key format: {@code stream.microbatch.kafka.<property>}
 *
 * <p>Example configuration in table config:
 * <pre>
 * {
 *   "streamConfigs": {
 *     "streamType": "kafka",
 *     "stream.kafka.topic.name": "microbatch-topic",
 *     "stream.kafka.broker.list": "localhost:9092",
 *     "stream.kafka.consumer.factory.class.name":
 *       "org.apache.pinot.plugin.stream.microbatch.kafka30.KafkaMicroBatchConsumerFactory",
 *     "stream.microbatch.kafka.file.fetch.threads": "4"
 *   }
 * }
 * </pre>
 */
public final class MicroBatchConfigProperties {

  private MicroBatchConfigProperties() {
    // Utility class
  }

  /**
   * Config key prefix: stream.microbatch.kafka
   */
  public static final String CONFIG_PREFIX = "stream.microbatch.kafka.";

  /**
   * Number of parallel threads for fetching batch files from PinotFS.
   * Higher values provide better throughput but increase disk I/O and memory usage.
   *
   * <p>Key: stream.microbatch.kafka.file.fetch.threads
   * <p>Default: 2
   * <p>Range: 1 to N (recommended: 1-8 depending on disk I/O capacity)
   */
  public static final String FILE_FETCH_THREADS = "file.fetch.threads";

  /**
   * Default number of file fetch threads.
   */
  public static final int DEFAULT_FILE_FETCH_THREADS = 2;

  /**
   * Number of records to include in each MessageBatch returned from fetchMessages().
   * Matches Kafka's default max.poll.records (500), which is the typical MessageBatch size
   * returned in the direct streaming path.
   */
  public static final int DEFAULT_MESSAGE_BATCH_SIZE = 500;

  /**
   * Gets the value of a microbatch config property from the stream config.
   *
   * @param streamConfig the stream config
   * @param property the property name (e.g., "parallelDataloaders")
   * @return the property value, or null if not set
   */
  public static String getProperty(StreamConfig streamConfig, String property) {
    return streamConfig.getStreamConfigsMap().get(CONFIG_PREFIX + property);
  }

  /**
   * Gets the value of a microbatch config property as an integer.
   *
   * @param streamConfig the stream config
   * @param property the property name
   * @param defaultValue the default value if not set or invalid
   * @return the property value as an integer
   */
  public static int getIntProperty(StreamConfig streamConfig, String property, int defaultValue) {
    String value = getProperty(streamConfig, property);
    if (value == null) {
      return defaultValue;
    }
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }
}
