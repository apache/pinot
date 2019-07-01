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
package org.apache.pinot.core.realtime.impl.kafka2;

import com.google.common.base.Joiner;
import org.apache.pinot.core.realtime.stream.StreamConfigProperties;


/**
 * Property key definitions for all kafka stream related properties
 */
public class Kafka2StreamConfigProperties {
  public static final String DOT_SEPARATOR = ".";
  public static final String KAFKA_CONSUMER_PROP_PREFIX = "kafka.consumer.prop";

  /**
   * Helper method to create a property string for kafka stream
   * @param property
   * @return
   */
  public static String constructStreamProperty(String property) {
    return Joiner.on(DOT_SEPARATOR).join(StreamConfigProperties.STREAM_PREFIX, property);
  }

  public static class HighLevelConsumer {
    public static final String KAFKA_HLC_BOOTSTRAP_SERVER = "kafka.hlc.bootstrap.server";
    public static final String AUTO_COMMIT_ENABLE = "auto.commit.enable";
  }

  public static class LowLevelConsumer {
    public static final String KAFKA_BROKER_LIST = "kafka.broker.list";
    public static final String KAFKA_BUFFER_SIZE = "kafka.buffer.size";
    public static final int KAFKA_BUFFER_SIZE_DEFAULT = 512000;
    public static final String KAFKA_SOCKET_TIMEOUT = "kafka.socket.timeout";
    public static final int KAFKA_SOCKET_TIMEOUT_DEFAULT = 10000;
    public static final String KAFKA_FETCHER_SIZE_BYTES = "kafka.fetcher.size";
    public static final String KAFKA_FETCHER_MIN_BYTES = "kafka.fetcher.minBytes";
    public static final int KAFKA_FETCHER_MIN_BYTES_DEFAULT = 100000;
  }
}

