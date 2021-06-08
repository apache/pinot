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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pulsar.client.api.MessageId;

public class PulsarConfig {
  public static final String STREAM_TYPE = "pulsar";
  public static final String PULSAR_PROP_PREFIX = "consumer.prop";
  public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
  public static final String START_POSITION = "start_position";
  public static final String DEFAULT_BOOTSTRAP_BROKERS = "pulsar://localhost:6650";

  private String _pulsarTopicName;
  private String _subscriberId;
  private String _bootstrapServers;
  private MessageId _initialMessageId = MessageId.latest;
  private Map<String, String> _pulsarConsumerProperties;

  public PulsarConfig(StreamConfig streamConfig, String subscriberId){
    Map<String, String> streamConfigMap = streamConfig.getStreamConfigsMap();
    _pulsarTopicName = streamConfig.getTopicName();
    _bootstrapServers = streamConfigMap.getOrDefault(BOOTSTRAP_SERVERS, DEFAULT_BOOTSTRAP_BROKERS);
    _subscriberId = subscriberId;

    String startPositionProperty = StreamConfigProperties.constructStreamProperty(STREAM_TYPE, START_POSITION);
    String startPosition = streamConfigMap.getOrDefault(startPositionProperty, "latest");
    if(startPosition.equals("earliest")){
      _initialMessageId =  MessageId.earliest;
    } else if(startPosition.equals("latest")) {
      _initialMessageId = MessageId.latest;
    } else {
      try {
        _initialMessageId = MessageId.fromByteArray(startPosition.getBytes());
      } catch (IOException e){
        throw new RuntimeException("Invalid start position found: " + startPosition);
      }
    }

    _pulsarConsumerProperties = new HashMap<>();

    String pulsarConsumerPropertyPrefix =
        StreamConfigProperties.constructStreamProperty(STREAM_TYPE, PULSAR_PROP_PREFIX);
    for (String key : streamConfigMap.keySet()) {
      if (key.startsWith(pulsarConsumerPropertyPrefix)) {
        _pulsarConsumerProperties
            .put(StreamConfigProperties.getPropertySuffix(key, pulsarConsumerPropertyPrefix), streamConfigMap.get(key));
      }
    }
  }

  public String getPulsarTopicName() {
    return _pulsarTopicName;
  }

  public String getSubscriberId() {
    return _subscriberId;
  }

  public String getBootstrapServers() {
    return _bootstrapServers;
  }

  public Properties getPulsarConsumerProperties() {
    Properties props = new Properties();
    for (String key : _pulsarConsumerProperties.keySet()) {
      props.put(key, _pulsarConsumerProperties.get(key));
    }

    return props;
  }

  public MessageId getInitialMessageId() {
    return _initialMessageId;
  }
}
