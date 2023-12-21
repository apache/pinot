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
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationFactoryOAuth2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Manages the Pulsar client connection, given the partition id and {@link PulsarConfig}
 */
public class PulsarPartitionLevelConnectionHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(PulsarPartitionLevelConnectionHandler.class);

  protected final PulsarConfig _config;
  protected final String _clientId;
  protected PulsarClient _pulsarClient = null;
  protected final PulsarMetadataExtractor _pulsarMetadataExtractor;

  /**
   * Creates a new instance of {@link PulsarClient} and {@link Reader}
   */
  public PulsarPartitionLevelConnectionHandler(String clientId, StreamConfig streamConfig) {
    _config = new PulsarConfig(streamConfig, clientId);
    _clientId = clientId;
    _pulsarMetadataExtractor = PulsarMetadataExtractor.build(_config.isPopulateMetadata(), _config.getMetadataFields());
    try {
      ClientBuilder pulsarClientBuilder = PulsarClient.builder().serviceUrl(_config.getBootstrapServers());
      if (_config.getTlsTrustCertsFilePath() != null) {
        pulsarClientBuilder.tlsTrustCertsFilePath(_config.getTlsTrustCertsFilePath());
      }

      if (_config.getAuthenticationToken() != null) {
        Authentication authentication = AuthenticationFactory.token(_config.getAuthenticationToken());
        pulsarClientBuilder.authentication(authentication);
      }

      getAuthenticationFactory(_config).ifPresent(pulsarClientBuilder::authentication);
      _pulsarClient = pulsarClientBuilder.build();
      LOGGER.info("Created pulsar client {}", _pulsarClient);
    } catch (Exception e) {
      LOGGER.error("Could not create pulsar consumer", e);
    }
  }

  protected Optional<Authentication> getAuthenticationFactory(PulsarConfig pulsarConfig) {
    if (StringUtils.isNotBlank(pulsarConfig.getIssuerUrl())
        && StringUtils.isNotBlank(pulsarConfig.getAudience())
        && StringUtils.isNotBlank(pulsarConfig.getCredentialsFilePath())) {
      try {
        return Optional.of(AuthenticationFactoryOAuth2.clientCredentials(
            new URL(pulsarConfig.getIssuerUrl()),
            new URL(pulsarConfig.getCredentialsFilePath()),
            pulsarConfig.getAudience()));
      } catch (MalformedURLException mue) {
        LOGGER.error("Failed to create authentication factory for pulsar client with config: "
                + "issuer: {}, credential file path: {}, audience: {}",
            pulsarConfig.getIssuerUrl(),
            pulsarConfig.getCredentialsFilePath(),
            pulsarConfig.getAudience(),
            mue);
      }
    }
    return Optional.empty();
  }
  protected Reader<byte[]> createReaderForPartition(String topic, int partition, MessageId initialMessageId) {
    if (_pulsarClient == null) {
      throw new RuntimeException("Failed to create reader as no pulsar client found for topic " + topic);
    }
    try {
      return _pulsarClient.newReader().topic(getPartitionedTopicName(topic, partition)).startMessageId(initialMessageId)
          .startMessageIdInclusive().create();
    } catch (Exception e) {
      LOGGER.error("Failed to create pulsar consumer client for topic " + topic + " partition " + partition, e);
      return null;
    }
  }

  /**
   * A pulsar partitioned topic with N partitions is comprised of N topics with topicName as prefix and portitionId
   * as suffix.
   * The method fetches the names of N partitioned topic and returns the topic name of {@param partition}
   */
  protected String getPartitionedTopicName(String topic, int partition)
      throws Exception {
    List<String> partitionTopicList = _pulsarClient.getPartitionsForTopic(topic).get();
    return partitionTopicList.get(partition);
  }

  public void close()
      throws IOException {
    if (_pulsarClient != null) {
      _pulsarClient.close();
    }
  }
}
