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
package org.apache.pinot.plugin.stream.pulsar.server;

import com.google.common.base.Preconditions;
import java.util.Base64;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.stream.StreamDataProducer;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Pulsar data producer class
 *
 * TODO: Improve the current implementation that creates the producer object for each `produce()` call.
 *
 */
public class PulsarDataProducer implements StreamDataProducer {

  private static final Logger LOGGER = LoggerFactory.getLogger(PulsarDataProducer.class);

  public static final String BROKER_SERVICE_URL = "brokerServiceUrl";
  public static final String TOKEN = "token";

  private PulsarClient _pulsarClient;

  public PulsarDataProducer() {
  }

  @Override
  public void init(Properties props) {
    String brokerServiceUrl = props.getProperty(BROKER_SERVICE_URL);
    Preconditions.checkNotNull(brokerServiceUrl, "broker service url must be configured.");
    ClientBuilder clientBuilder = PulsarClient.builder().serviceUrl(brokerServiceUrl);

    String token = props.getProperty(TOKEN);
    if (StringUtils.isNotEmpty(token)) {
      clientBuilder.authentication(AuthenticationFactory.token(token));
    }

    try {
      _pulsarClient = clientBuilder.build();
    } catch (PulsarClientException e) {
      throw new IllegalArgumentException("Failed to create pulsar client", e);
    }
  }

  @Override
  public void produce(String topic, byte[] payload) {
    try (Producer<byte[]> producer = _pulsarClient.newProducer().topic(topic).create()) {
      producer.send(payload);
    } catch (PulsarClientException e) {
      LOGGER.error("Failed to produce message for topic: " + topic, e);
    }
  }

  @Override
  public void produce(String topic, byte[] key, byte[] payload) {
    try (Producer<byte[]> producer = _pulsarClient.newProducer().topic(topic).create()) {
      producer.newMessage().key(Base64.getEncoder().encodeToString(key)).value(payload).send();
    } catch (PulsarClientException e) {
      LOGGER.error("Failed to produce message for topic: " + topic, e);
    }
  }

  @Override
  public void close() {
    try {
      _pulsarClient.close();
    } catch (PulsarClientException e) {
      throw new RuntimeException("Failed to close pulsar client.", e);
    }
  }
}
