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

import java.util.Set;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamDecoderProvider;
import org.apache.pinot.spi.stream.StreamLevelConsumer;
import org.apache.pinot.spi.stream.StreamMessageDecoder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Reader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@link StreamLevelConsumer} implementation for the Pulsar stream
 */
public class PulsarStreamLevelConsumer implements StreamLevelConsumer {
  private Logger _logger;

  private StreamMessageDecoder _messageDecoder;

  private StreamConfig _streamConfig;
  private PulsarConfig _pulsarStreamLevelStreamConfig;

  private Reader<byte[]> _reader;

  private long _lastLogTime = 0;
  private long _lastCount = 0;
  private long _currentCount = 0L;

  public PulsarStreamLevelConsumer(String clientId, String tableName, StreamConfig streamConfig,
      Set<String> sourceFields, String subscriberId) {
    _streamConfig = streamConfig;
    _pulsarStreamLevelStreamConfig = new PulsarConfig(streamConfig, subscriberId);

    _messageDecoder = StreamDecoderProvider.create(streamConfig, sourceFields);

    _logger =
        LoggerFactory.getLogger(PulsarConfig.class.getName() + "_" + tableName + "_" + streamConfig.getTopicName());
    _logger.info("PulsarStreamLevelConsumer: streamConfig : {}", _streamConfig);
  }

  @Override
  public void start()
      throws Exception {
    _reader = PulsarStreamLevelConsumerManager.acquirePulsarConsumerForConfig(_pulsarStreamLevelStreamConfig);
  }

  /**
   * Get next {@link GenericRow} after decoding pulsar {@link Message}
   */
  @Override
  public GenericRow next(GenericRow destination) {
    try {
      if (_reader.hasMessageAvailable()) {
        final Message<byte[]> record = _reader.readNext();
        destination = _messageDecoder.decode(record.getData(), destination);

        _currentCount++;

        final long now = System.currentTimeMillis();
        // Log every minute or 100k events
        if (now - _lastLogTime > 60000 || _currentCount - _lastCount >= 100000) {
          if (_lastCount == 0) {
            _logger.info("Consumed {} events from pulsar stream {}", _currentCount, _streamConfig.getTopicName());
          } else {
            _logger.info("Consumed {} events from pulsar stream {} (rate:{}/s)", _currentCount - _lastCount,
                _streamConfig.getTopicName(), (float) (_currentCount - _lastCount) * 1000 / (now - _lastLogTime));
          }
          _lastCount = _currentCount;
          _lastLogTime = now;
        }
        return destination;
      }
    } catch (Exception e) {
      _logger.warn("Caught exception while consuming events", e);
    }
    return null;
  }

  @Override
  public void commit() {
  }

  @Override
  public void shutdown()
      throws Exception {
    if (_reader != null) {
      PulsarStreamLevelConsumerManager.releasePulsarConsumer(_reader);
      _reader = null;
    }
  }
}
