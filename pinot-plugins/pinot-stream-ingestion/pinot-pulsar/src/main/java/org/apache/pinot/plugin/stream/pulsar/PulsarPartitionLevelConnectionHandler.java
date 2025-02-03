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
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationFactoryOAuth2;

import static com.google.common.base.Preconditions.checkArgument;


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
    _pulsarClient = createPulsarClient();
  }

  private PulsarClient createPulsarClient() {
    ClientBuilder clientBuilder = PulsarClient.builder().serviceUrl(_config.getBootstrapServers());
    try {
      Optional.ofNullable(_config.getTlsTrustCertsFilePath())
          .filter(StringUtils::isNotBlank)
          .ifPresent(clientBuilder::tlsTrustCertsFilePath);
      Optional.ofNullable(authenticationConfig()).ifPresent(clientBuilder::authentication);
      return clientBuilder.build();
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while creating Pulsar client", e);
    }
  }

  protected PulsarAdmin createPulsarAdmin() {
    checkArgument(StringUtils.isNotBlank(_config.getServiceHttpUrl()),
        "Service HTTP URL must be provided to perform admin operations");

    PulsarAdminBuilder adminBuilder = PulsarAdmin.builder().serviceHttpUrl(_config.getServiceHttpUrl());
    try {
      Optional.ofNullable(_config.getTlsTrustCertsFilePath())
          .filter(StringUtils::isNotBlank)
          .ifPresent(adminBuilder::tlsTrustCertsFilePath);
      Optional.ofNullable(authenticationConfig()).ifPresent(adminBuilder::authentication);
      return adminBuilder.build();
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while creating Pulsar admin", e);
    }
  }

  /**
   * Creates and returns an {@link Authentication} object based on the configuration.
   *
   * @return an Authentication object
   */
  private Authentication authenticationConfig()
      throws MalformedURLException {
    String authenticationToken = _config.getAuthenticationToken();
    if (StringUtils.isNotBlank(authenticationToken)) {
      return AuthenticationFactory.token(authenticationToken);
    } else {
      return oAuth2AuthenticationConfig();
    }
  }

  /**
   * Creates and returns an OAuth2 {@link Authentication} object.
   *
   * @return an OAuth2 Authentication object
   */
  private Authentication oAuth2AuthenticationConfig()
      throws MalformedURLException {
    String issuerUrl = _config.getIssuerUrl();
    String credentialsFilePath = _config.getCredentialsFilePath();
    String audience = _config.getAudience();

    if (StringUtils.isNotBlank(issuerUrl) && StringUtils.isNotBlank(credentialsFilePath) && StringUtils.isNotBlank(
        audience)) {
      return AuthenticationFactoryOAuth2.clientCredentials(new URL(issuerUrl), new URL(credentialsFilePath),
          audience);
    }
    return null;
  }

  @Override
  public void close()
      throws IOException {
    if (_pulsarClient != null) {
      _pulsarClient.close();
    }
  }
}
