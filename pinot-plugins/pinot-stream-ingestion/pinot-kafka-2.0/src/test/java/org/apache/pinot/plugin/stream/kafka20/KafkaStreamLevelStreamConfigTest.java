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
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.testng.Assert;
import org.testng.annotations.Test;


public class KafkaStreamLevelStreamConfigTest {
  private static final String KAFKA_DECODER_CLASS_NAME =
      "org.apache.pinot.plugin.inputformat.avro.KafkaAvroMessageDecoder";

  private StreamConfig getStreamConfig(String topic, String bootstrapHosts, String buffer, String socketTimeout) {
    Map<String, String> streamConfigMap = new HashMap<>();
    String streamType = "kafka";
    String consumerType = StreamConfig.ConsumerType.LOWLEVEL.toString();
    String consumerFactoryClassName = KafkaConsumerFactory.class.getName();
    String tableNameWithType = "tableName_REALTIME";
    streamConfigMap.put(StreamConfigProperties.STREAM_TYPE, streamType);
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_TOPIC_NAME), topic);
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_TYPES),
        consumerType);
    streamConfigMap.put(StreamConfigProperties.constructStreamProperty(streamType,
        StreamConfigProperties.STREAM_CONSUMER_FACTORY_CLASS), consumerFactoryClassName);
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_DECODER_CLASS),
        KAFKA_DECODER_CLASS_NAME);
    streamConfigMap.put("stream.kafka.hlc.bootstrap.server", bootstrapHosts);
    if (buffer != null) {
      streamConfigMap.put("stream.kafka.buffer.size", buffer);
    }
    if (socketTimeout != null) {
      streamConfigMap.put("stream.kafka.socket.timeout", socketTimeout);
    }

    return new StreamConfig(tableNameWithType, streamConfigMap);
  }

  @Test
  public void testGroupIdWhenNull() {
    StreamConfig streamConfig = getStreamConfig("topic", "bootstrapHosts", "", "");
    KafkaStreamLevelStreamConfig config = new KafkaStreamLevelStreamConfig(streamConfig, "tableName", null);
    Assert.assertEquals("tableName", config.getStreamLevelGroupId());
  }

  @Test
  public void testGroupId() {
    StreamConfig streamConfig = getStreamConfig("topic", "", "", "");
    KafkaStreamLevelStreamConfig config = new KafkaStreamLevelStreamConfig(streamConfig, "tableName", "groupId");
    Assert.assertEquals("groupId", config.getStreamLevelGroupId());
  }
}
