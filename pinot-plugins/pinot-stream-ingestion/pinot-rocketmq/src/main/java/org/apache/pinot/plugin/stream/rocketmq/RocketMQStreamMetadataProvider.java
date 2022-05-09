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
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nonnull;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.OffsetCriteria;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.message.MessageQueue;


/**
 * A {@link StreamMetadataProvider} implementation for the RocketMQ stream
 */
public class RocketMQStreamMetadataProvider implements StreamMetadataProvider {
  protected final RocketMQConfig _config;
  protected final String _clientId;
  protected final String _topic;

  protected final MQClientInstance _client;

  public RocketMQStreamMetadataProvider(String clientId, StreamConfig streamConfig) {
    _config = new RocketMQConfig(streamConfig, clientId);
    _clientId = clientId;
    _topic = _config.getRocketMQTopicName();

    ClientConfig clientConfig = new ClientConfig();
    clientConfig.setNamesrvAddr(_config.getNameServer());
    clientConfig.setClientCallbackExecutorThreads(1);
    _client = MQClientManager.getInstance().getOrCreateMQClientInstance(clientConfig);
    try {
      _client.start();
    } catch (MQClientException e) {
      e.printStackTrace();
    }
  }

  @Override
  public int fetchPartitionCount(long timeoutMillis) {
    try {
      return _client.getMQAdminImpl().fetchSubscribeMessageQueues(_config.getRocketMQTopicName()).size();
    } catch (MQClientException e) {
      e.printStackTrace();
    }
    return 0;
  }

  @Override
  public StreamPartitionMsgOffset fetchStreamPartitionOffset(@Nonnull OffsetCriteria offsetCriteria, long timeoutMillis)
      throws TimeoutException {
    Preconditions.checkNotNull(offsetCriteria);
    Long offset = null;
    try {
      Set<MessageQueue> queues = _client.getMQAdminImpl().fetchSubscribeMessageQueues(_config.getRocketMQTopicName());
      if (queues.isEmpty()) {
        throw new RuntimeException("queues not found for topic: " + _config.getRocketMQTopicName());
      }

      MessageQueue queue = queues.iterator().next();
      if (offsetCriteria.isLargest()) {
        offset = _client.getMQAdminImpl().maxOffset(queue);
      } else if (offsetCriteria.isSmallest()) {
        offset = _client.getMQAdminImpl().minOffset(queue);
      } else if (offsetCriteria.isCustom()) {
        offset = _client.getMQAdminImpl().searchOffset(queue, Long.parseLong(offsetCriteria.getOffsetString()));
      } else {
        throw new IllegalArgumentException("Unknown initial offset value " + offsetCriteria.toString());
      }
    } catch (MQClientException e) {
      throw new RuntimeException("Failed to get offset for timestamp " + offsetCriteria.getOffsetString(), e);
    }
    return new LongMsgOffset(offset);
  }

  @Override
  public void close()
      throws IOException {
  }
}
