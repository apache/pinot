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
package org.apache.pinot.plugin.stream.rocketmq;

import com.google.common.base.Preconditions;
import java.util.Map;
import org.apache.pinot.spi.stream.OffsetCriteria;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;


/**
 * RocketMQ specific stream config
 * contains RocketMQ NameServer list, start offset and group id/subscriber id if using high level consumer/
 */
public class RocketMQConfig {
  public static final String STREAM_TYPE = "rocketmq";
  public static final String NAME_SERVER_LIST = "nameserver.list";
  public static final String CONSUMER_PROP_NAMESPACE = "consumer.prop.namespace";

  private String _rocketmqTopicName;
  private String _consumerGroupId;
  private String _consumerNamespace;
  private String _nameServer;
  private ConsumeFromWhere _initialMessagePosition;
  private String _initialMessageTimestamp;

  private Map<String, String> _rocketmqConsumerProperties;

  public RocketMQConfig(StreamConfig streamConfig, String groupId) {
    Map<String, String> streamConfigMap = streamConfig.getStreamConfigsMap();
    _rocketmqTopicName = streamConfig.getTopicName();
    _nameServer =
        streamConfigMap.get(StreamConfigProperties.constructStreamProperty(STREAM_TYPE, NAME_SERVER_LIST));
    _consumerNamespace =
        streamConfigMap.get(StreamConfigProperties.constructStreamProperty(STREAM_TYPE, CONSUMER_PROP_NAMESPACE));
    _consumerGroupId = groupId;

    Preconditions.checkNotNull(_nameServer, "No NameServers provided in the config");

    OffsetCriteria offsetCriteria = streamConfig.getOffsetCriteria();

    if (offsetCriteria.isSmallest()) {
      _initialMessagePosition = ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET;
    } else if (offsetCriteria.isLargest()) {
      _initialMessagePosition = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;
    } else if (offsetCriteria.isCustom()) {
      _initialMessagePosition = ConsumeFromWhere.CONSUME_FROM_TIMESTAMP;
      _initialMessageTimestamp = offsetCriteria.getOffsetString();
    }
  }

  public ConsumeFromWhere getConsumerFromWhere() {
    return _initialMessagePosition;
  }

  public String getConsumeTimestamp() {
    return _initialMessageTimestamp;
  }

  public String getRocketMQTopicName() {
    return _rocketmqTopicName;
  }

  public String getConsumerGroupId() {
    return _consumerGroupId;
  }

  public String getConsumerNamespace() {
    return _consumerNamespace;
  }

  public String getNameServer() {
    return _nameServer;
  }
}
