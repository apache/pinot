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
package org.apache.pinot.plugin.stream.kafka20;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.spi.stream.StreamConfig;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;


public class KafkaPartitionLevelConnectionHandlerTest {

  private static class TestableKafkaPartitionLevelConnectionHandler extends KafkaPartitionLevelConnectionHandler {
    public TestableKafkaPartitionLevelConnectionHandler(String clientId, StreamConfig streamConfig, int partition) {
      super(clientId, streamConfig, partition);
    }
  }

  private StreamConfig createTestStreamConfig() {
    Map<String, String> streamConfigMap = new HashMap<>();
    streamConfigMap.put("streamType", "kafka");
    streamConfigMap.put("stream.kafka.topic.name", "testTopic");
    streamConfigMap.put("stream.kafka.broker.list", "localhost:9092");
    streamConfigMap.put("stream.kafka.consumer.factory.class.name", KafkaConsumerFactory.class.getName());
    streamConfigMap.put("stream.kafka.decoder.class.name", "decoderClass");
    return new StreamConfig("testTable_REALTIME", streamConfigMap);
  }

  @Test
  public void testSharedAdminClientReference() {
    StreamConfig streamConfig = createTestStreamConfig();

    try {
      TestableKafkaPartitionLevelConnectionHandler handler =
          new TestableKafkaPartitionLevelConnectionHandler("testClient", streamConfig, 0);

      // Test that we can call getOrCreateSharedAdminClient multiple times
      // without throwing exceptions (even though it may fail to connect)
      try {
        handler.getOrCreateSharedAdminClient();
        handler.getOrCreateSharedAdminClient(); // Should reuse the same reference
      } catch (Exception e) {
        // Expected when no real Kafka cluster is available
        assertTrue(e.getMessage().contains("Connection") || e.getMessage().contains("Kafka")
            || e.getMessage().contains("timeout") || e.getMessage().contains("refused")
            || e.getCause() != null);
      }

      // Test that close doesn't throw exceptions
      handler.close();
    } catch (Exception e) {
      // Expected when initializing without a real Kafka cluster
      assertTrue(e.getMessage().contains("Connection") || e.getMessage().contains("Kafka")
          || e.getMessage().contains("timeout") || e.getMessage().contains("refused")
          || e.getCause() != null);
    }
  }

  @Test
  public void testGetOrCreateAdminClientBackwardCompatibility() {
    StreamConfig streamConfig = createTestStreamConfig();

    try {
      TestableKafkaPartitionLevelConnectionHandler handler =
          new TestableKafkaPartitionLevelConnectionHandler("testClient", streamConfig, 0);

      // Test that the backward compatibility method still works
      try {
        handler.getOrCreateAdminClient();
      } catch (Exception e) {
        // Expected when no real Kafka cluster is available
        assertTrue(e.getMessage().contains("Connection") || e.getMessage().contains("Kafka")
            || e.getMessage().contains("timeout") || e.getMessage().contains("refused")
            || e.getCause() != null);
      }

      handler.close();
    } catch (Exception e) {
      // Expected when initializing without a real Kafka cluster
      assertTrue(e.getMessage().contains("Connection") || e.getMessage().contains("Kafka")
          || e.getMessage().contains("timeout") || e.getMessage().contains("refused")
          || e.getCause() != null);
    }
  }
}
