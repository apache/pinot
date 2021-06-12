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

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pulsar.client.api.MessageId;


/**
 * Pulsar specific stream config
 * contains pulsar brokers list, start offset and group id/subscriber id if using high level consumer/
 */
public class PulsarConfig {
  public static final String STREAM_TYPE = "pulsar";
  public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
  public static final String START_POSITION = "start.position";
  public static final String EARLIEST = "earliest";
  public static final String LATEST = "latest";

  private String _pulsarTopicName;
  private String _subscriberId;
  private String _bootstrapServers;
  private MessageId _initialMessageId;

  public PulsarConfig(StreamConfig streamConfig, String subscriberId) {
    Map<String, String> streamConfigMap = streamConfig.getStreamConfigsMap();
    _pulsarTopicName = streamConfig.getTopicName();
    _bootstrapServers = streamConfigMap.get(StreamConfigProperties.constructStreamProperty(STREAM_TYPE, BOOTSTRAP_SERVERS));
    _subscriberId = subscriberId;

    Preconditions.checkNotNull(_bootstrapServers, "No brokers provided in the config");

    String startPositionProperty = StreamConfigProperties.constructStreamProperty(STREAM_TYPE, START_POSITION);
    String startPosition = streamConfigMap.getOrDefault(startPositionProperty, "latest");
    if (startPosition.equalsIgnoreCase(EARLIEST)) {
      _initialMessageId = MessageId.earliest;
    } else if (startPosition.equalsIgnoreCase(LATEST)) {
      _initialMessageId = MessageId.latest;
    } else {
      try {
        _initialMessageId = MessageId.fromByteArray(startPosition.getBytes());
      } catch (IOException e) {
        throw new RuntimeException("Invalid start position found: " + startPosition);
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

  public MessageId getInitialMessageId() {
    return _initialMessageId;
  }
}
