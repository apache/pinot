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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamDecoderProvider;
import org.apache.pinot.spi.stream.StreamLevelConsumer;
import org.apache.pinot.spi.stream.StreamMessageDecoder;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@link StreamLevelConsumer} implementation for the RocketMQ stream
 */
public class RocketMQStreamLevelConsumer implements StreamLevelConsumer {

  private StreamMessageDecoder _messageDecoder;
  private Logger _instanceLogger;

  private StreamConfig _streamConfig;
  private RocketMQConfig _rocketmqStreamLevelStreamConfig;

  private DefaultLitePullConsumer _consumer;
  private List<MessageExt> _consumerRecords;
  private Iterator<MessageExt> _rocketmqIterator;
  private Map<MessageQueue, Long> _consumerOffsets = new HashMap<>(); // tracking current consumed records offsets.

  private long _lastLogTime = 0;
  private long _lastCount = 0;
  private long _currentCount = 0L;

  public RocketMQStreamLevelConsumer(String clientId, String tableName, StreamConfig streamConfig,
      Set<String> sourceFields, String groupId) {
    _streamConfig = streamConfig;
    _rocketmqStreamLevelStreamConfig = new RocketMQConfig(streamConfig, groupId);

    _messageDecoder = StreamDecoderProvider.create(streamConfig, sourceFields);

    _instanceLogger =
        LoggerFactory.getLogger(RocketMQConfig.class.getName() + "_" + tableName + "_" + streamConfig.getTopicName());
    _instanceLogger.info("RocketMQStreamLevelConsumer: streamConfig : {}", _streamConfig);
  }

  @Override
  public void start()
      throws Exception {
    _consumer = RocketMQStreamLevelConsumerManager.acquireRocketMQConsumerForConfig(_rocketmqStreamLevelStreamConfig);
  }

  private void updateRocketMQIterator() {
    _consumerRecords = _consumer.poll(_streamConfig.getFetchTimeoutMillis());
    _rocketmqIterator = _consumerRecords.iterator();
  }

  private void resetOffsets() {
    for (MessageQueue queue : _consumerOffsets.keySet()) {
      long offsetToSeek = _consumerOffsets.get(queue);
      try {
        _consumer.seek(queue, offsetToSeek);
      } catch (MQClientException e) {
        _instanceLogger.warn("Caught exception while seek offset", e);
      }
    }
  }

  @Override
  public GenericRow next(GenericRow destination) {
    if (_rocketmqIterator == null || !_rocketmqIterator.hasNext()) {
      updateRocketMQIterator();
    }
    if (_rocketmqIterator.hasNext()) {
      try {
        final MessageExt record = _rocketmqIterator.next();
        updateOffsets(new MessageQueue(record.getTopic(), record.getBrokerName(), record.getQueueId()),
            record.getQueueOffset());
        destination = _messageDecoder.decode(record.getBody(), destination);

        _currentCount++;

        final long now = System.currentTimeMillis();
        // Log every minute or 100k events
        if (now - _lastLogTime > 60000 || _currentCount - _lastCount >= 100000) {
          if (_lastCount == 0) {
            _instanceLogger
                .info("Consumed {} events from rocketmq stream {}", _currentCount, _streamConfig.getTopicName());
          } else {
            _instanceLogger.info("Consumed {} events from rocketmq stream {} (rate:{}/s)", _currentCount - _lastCount,
                _streamConfig.getTopicName(), (float) (_currentCount - _lastCount) * 1000 / (now - _lastLogTime));
          }
          _lastCount = _currentCount;
          _lastLogTime = now;
        }
        return destination;
      } catch (Exception e) {
        _instanceLogger.warn("Caught exception while consuming events", e);
        throw e;
      }
    }
    return null;
  }

  private void updateOffsets(MessageQueue queue, long offset) {
    _consumerOffsets.put(queue, offset + 1);
  }

  @Override
  public void commit() {
    _consumer.commitSync();
    // Since the latest batch may not be consumed fully, so we need to reset rocketmq consumer's offset.
    resetOffsets();
    _consumerOffsets.clear();
  }

  @Override
  public void shutdown()
      throws Exception {
    if (_consumer != null) {
      // If offsets commit is not succeed, then reset the offsets here.
      resetOffsets();
      RocketMQStreamLevelConsumerManager.releaseRocketMQConsumer(_consumer);
      _consumer = null;
    }
  }

  public RocketMQConfig getRocketmqStreamLevelStreamConfig() {
    return _rocketmqStreamLevelStreamConfig;
  }
}
