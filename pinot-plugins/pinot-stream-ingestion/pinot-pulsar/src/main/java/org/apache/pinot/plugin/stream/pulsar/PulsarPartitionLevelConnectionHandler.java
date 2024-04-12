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

import java.io.Closeable;
import java.io.IOException;
import java.net.URL;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationFactoryOAuth2;


/**
 * Manages the Pulsar client connection, given the partition id and {@link PulsarConfig}
 */
public class PulsarPartitionLevelConnectionHandler implements Closeable {
  protected final PulsarConfig _config;
  protected final String _clientId;
  protected final PulsarClient _pulsarClient;

  /**
   * Creates a new instance of {@link PulsarClient} and {@link Reader}
   */
  protected PulsarPartitionLevelConnectionHandler(String clientId, StreamConfig streamConfig) {
    _config = new PulsarConfig(streamConfig, clientId);
    _clientId = clientId;
    try {
      ClientBuilder pulsarClientBuilder = PulsarClient.builder().serviceUrl(_config.getBootstrapServers());
      String tlsTrustCertsFilePath = _config.getTlsTrustCertsFilePath();
      if (StringUtils.isNotBlank(tlsTrustCertsFilePath)) {
        pulsarClientBuilder.tlsTrustCertsFilePath(tlsTrustCertsFilePath);
      }
      String authenticationToken = _config.getAuthenticationToken();
      if (StringUtils.isNotBlank(authenticationToken)) {
        pulsarClientBuilder.authentication(AuthenticationFactory.token(authenticationToken));
      } else {
        String issuerUrl = _config.getIssuerUrl();
        String credentialsFilePath = _config.getCredentialsFilePath();
        String audience = _config.getAudience();
        if (StringUtils.isNotBlank(issuerUrl) && StringUtils.isNotBlank(credentialsFilePath) && StringUtils.isNotBlank(
            audience)) {
          pulsarClientBuilder.authentication(
              AuthenticationFactoryOAuth2.clientCredentials(new URL(issuerUrl), new URL(credentialsFilePath),
                  audience));
        }
      }
      _pulsarClient = pulsarClientBuilder.build();
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while creating Pulsar client", e);
    }
  }

  @Override
  public void close()
      throws IOException {
    _pulsarClient.close();
  }
}
