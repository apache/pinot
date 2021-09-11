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
package org.apache.pinot.plugin.stream.kafka09;

import java.util.Set;
import kafka.consumer.ConsumerIterator;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamDecoderProvider;
import org.apache.pinot.spi.stream.StreamLevelConsumer;
import org.apache.pinot.spi.stream.StreamMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An implementation of a {@link StreamLevelConsumer} which consumes from the kafka stream
 */
public class KafkaStreamLevelConsumer implements StreamLevelConsumer {

  private StreamMessageDecoder _messageDecoder;
  private Logger _instanceLogger;

  private String _clientId;
  private String _tableAndStreamName;

  private StreamConfig _streamConfig;
  private KafkaHighLevelStreamConfig _kafkaHighLevelStreamConfig;

  private ConsumerConnector _consumer;
  private ConsumerIterator<byte[], byte[]> _kafkaIterator;
  private ConsumerAndIterator _consumerAndIterator;
  private long _lastLogTime = 0;
  private long _lastCount = 0;
  private long _currentCount = 0L;

  public KafkaStreamLevelConsumer(String clientId, String tableName, StreamConfig streamConfig,
      Set<String> fieldsToRead, String groupId) {
    _clientId = clientId;
    _streamConfig = streamConfig;
    _kafkaHighLevelStreamConfig = new KafkaHighLevelStreamConfig(streamConfig, tableName, groupId);

    _messageDecoder = StreamDecoderProvider.create(streamConfig, fieldsToRead);

    _tableAndStreamName = tableName + "-" + streamConfig.getTopicName();
    _instanceLogger = LoggerFactory
        .getLogger(KafkaStreamLevelConsumer.class.getName() + "_" + tableName + "_" + streamConfig.getTopicName());
  }

  @Override
  public void start()
      throws Exception {
    _consumerAndIterator = KafkaConsumerManager.acquireConsumerAndIteratorForConfig(_kafkaHighLevelStreamConfig);
    _kafkaIterator = _consumerAndIterator.getIterator();
    _consumer = _consumerAndIterator.getConsumer();
  }

  @Override
  public GenericRow next(GenericRow destination) {

    if (_kafkaIterator.hasNext()) {
      try {
        destination = _messageDecoder.decode(_kafkaIterator.next().message(), destination);
        ++_currentCount;

        final long now = System.currentTimeMillis();
        // Log every minute or 100k events
        if (now - _lastLogTime > 60000 || _currentCount - _lastCount >= 100000) {
          if (_lastCount == 0) {
            _instanceLogger
                .info("Consumed {} events from kafka stream {}", _currentCount, _streamConfig.getTopicName());
          } else {
            _instanceLogger.info("Consumed {} events from kafka stream {} (rate:{}/s)", _currentCount - _lastCount,
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

  @Override
  public void commit() {
    _consumer.commitOffsets();
  }

  @Override
  public void shutdown()
      throws Exception {
    if (_consumerAndIterator != null) {
      _kafkaIterator = null;
      _consumer = null;

      KafkaConsumerManager.releaseConsumerAndIterator(_consumerAndIterator);
      _consumerAndIterator = null;
    }
  }
}
