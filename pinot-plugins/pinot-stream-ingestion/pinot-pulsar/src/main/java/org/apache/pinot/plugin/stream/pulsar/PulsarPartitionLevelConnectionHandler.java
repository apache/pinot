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

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.List;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PulsarPartitionLevelConnectionHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(PulsarPartitionLevelConnectionHandler.class);

  protected final PulsarConfig _config;
  protected final String _clientId;
  protected final int _partition;
  protected final String _topic;
  protected PulsarClient _pulsarClient = null;
  protected Reader<byte[]> _reader = null;

  public PulsarPartitionLevelConnectionHandler(String clientId, StreamConfig streamConfig, int partition) {
    _config = new PulsarConfig(streamConfig, clientId);
    _clientId = clientId;
    _partition = partition;
    _topic = _config.getPulsarTopicName();

    try {
      _pulsarClient = PulsarClient.builder().serviceUrl(_config.getBootstrapServers()).build();

      _reader = _pulsarClient.newReader().topic(getPartitionedTopicName(partition))
          .startMessageId(_config.getInitialMessageId()).create();

      LOGGER.info("Created consumer with id {} for topic {}", _reader, _config.getPulsarTopicName());
    } catch (Exception e) {
      LOGGER.error("Could not create pulsar consumer", e);
    }
  }

  protected String getPartitionedTopicName(int partition)
      throws Exception {
    List<String> partitionTopicList = _pulsarClient.getPartitionsForTopic(_topic).get();
    return partitionTopicList.get(partition);
  }

  public void close()
      throws IOException {
    _reader.close();
  }
}
