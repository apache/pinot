/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.realtime.impl.kafka;

import com.google.common.base.Joiner;
import com.linkedin.pinot.core.realtime.stream.StreamConfigProperties;


/**
 * Property key definitions for all kafka stream related properties
 */
public class KafkaStreamConfigProperties {
  public static final String DOT_SEPARATOR = ".";
  public static final String STREAM_TYPE = "kafka";

  public static class HighLevelConsumer {
    public static final String KAFKA_HLC_ZK_CONNECTION_STRING = "kafka.hlc.zk.connect.string";
    public static final String ZK_SESSION_TIMEOUT_MS = "zookeeper.session.timeout.ms";
    public static final String ZK_CONNECTION_TIMEOUT_MS = "zookeeper.connection.timeout.ms";
    public static final String ZK_SYNC_TIME_MS = "zookeeper.sync.time.ms";
    public static final String REBALANCE_MAX_RETRIES = "rebalance.max.retries";
    public static final String REBALANCE_BACKOFF_MS = "rebalance.backoff.ms";
    public static final String AUTO_COMMIT_ENABLE = "auto.commit.enable";
    public static final String AUTO_OFFSET_RESET = "auto.offset.reset";
  }

  public static class LowLevelConsumer {
    public static final String KAFKA_BROKER_LIST = "kafka.broker.list";
  }

  public static final String KAFKA_CONSUMER_PROP_PREFIX = "kafka.consumer.prop";

  /**
   * Helper method to create a property string for kafka stream
   * @param property
   * @return
   */
  public static String constructStreamProperty(String property) {
    return Joiner.on(DOT_SEPARATOR).join(StreamConfigProperties.STREAM_PREFIX, property);
  }
}

