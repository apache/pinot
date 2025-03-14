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
package org.apache.pinot.plugin.provider;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.hc.client5.http.HttpRequestRetryStrategy;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.DefaultHttpRequestRetryStrategy;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.util.TimeValue;
import org.apache.hc.core5.util.Timeout;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.environmentprovider.PinotEnvironmentProvider;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Kubernetes Environment Provider used to retrieve Kubernetes specific instance configuration,
 * particularly the availability zone of the node the pod is running on.
 */
public class KubernetesEnvironmentProvider implements PinotEnvironmentProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(KubernetesEnvironmentProvider.class);

  protected static final String MAX_RETRY = "maxRetry";
  protected static final String KUBERNETES_API_ENDPOINT = "kubernetesApiEndpoint";
  protected static final String CONNECTION_TIMEOUT_MILLIS = "connectionTimeoutMillis";
  protected static final String REQUEST_TIMEOUT_MILLIS = "requestTimeoutMillis";
  protected static final String TOKEN_FILE_PATH = "tokenFilePath";
  protected static final String NODE_NAME_ENV_VAR = "NODE_NAME";
  protected static final String DEFAULT_TOKEN_FILE_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/token";
  protected static final String DEFAULT_KUBERNETES_API_ENDPOINT = "https://kubernetes.default.svc";
  protected static final int DEFAULT_MAX_RETRY = 3;
  protected static final int DEFAULT_CONNECTION_TIMEOUT_MILLIS = 5000;
  protected static final int DEFAULT_REQUEST_TIMEOUT_MILLIS = 5000;

  private static final String FAILURE_DOMAIN_BETA_LABEL = "failure-domain.beta.kubernetes.io/zone";
  private static final String TOPOLOGY_ZONE_LABEL = "topology.kubernetes.io/zone";

  private int _maxRetry;
  private String _kubernetesApiEndpoint;
  private String _tokenFilePath;
  private CloseableHttpClient _closeableHttpClient;

  public KubernetesEnvironmentProvider() {
  }

  @Override
  public void init(PinotConfiguration pinotConfiguration) {
    _maxRetry = pinotConfiguration.getProperty(MAX_RETRY, DEFAULT_MAX_RETRY);
    Preconditions.checkArgument(0 < _maxRetry, 
        "[KubernetesEnvironmentProvider]: " + MAX_RETRY + " cannot be less than or equal to 0");

    _kubernetesApiEndpoint = pinotConfiguration.getProperty(KUBERNETES_API_ENDPOINT, DEFAULT_KUBERNETES_API_ENDPOINT);
    Preconditions.checkArgument(!StringUtils.isBlank(_kubernetesApiEndpoint),
        "[KubernetesEnvironmentProvider]: " + KUBERNETES_API_ENDPOINT + " should not be null or empty");

    _tokenFilePath = pinotConfiguration.getProperty(TOKEN_FILE_PATH, DEFAULT_TOKEN_FILE_PATH);
    
    int connectionTimeoutMillis = pinotConfiguration.getProperty(CONNECTION_TIMEOUT_MILLIS, DEFAULT_CONNECTION_TIMEOUT_MILLIS);
    int requestTimeoutMillis = pinotConfiguration.getProperty(REQUEST_TIMEOUT_MILLIS, DEFAULT_REQUEST_TIMEOUT_MILLIS);

    final RequestConfig requestConfig =
        RequestConfig.custom().setConnectTimeout(Timeout.of(connectionTimeoutMillis, TimeUnit.MILLISECONDS))
            .setResponseTimeout(Timeout.of(requestTimeoutMillis, TimeUnit.MILLISECONDS)).build();

    final HttpRequestRetryStrategy httpRequestRetry = new DefaultHttpRequestRetryStrategy(
        _maxRetry,
        TimeValue.ofSeconds(1));

    _closeableHttpClient =
        HttpClients.custom().setDefaultRequestConfig(requestConfig).setRetryStrategy(httpRequestRetry).build();
    
    LOGGER.info("Initialized KubernetesEnvironmentProvider with endpoint: {}", _kubernetesApiEndpoint);
  }

  // Constructor for test purposes.
  @VisibleForTesting
  public KubernetesEnvironmentProvider(int maxRetry, String kubernetesApiEndpoint, String tokenFilePath, 
                                 CloseableHttpClient closeableHttpClient) {
    _maxRetry = maxRetry;
    _kubernetesApiEndpoint = kubernetesApiEndpoint;
    _tokenFilePath = tokenFilePath;
    _closeableHttpClient = Preconditions.checkNotNull(closeableHttpClient,
        "[KubernetesEnvironmentProvider]: Closeable Http Client cannot be null");
  }

  /**
   * Retrieves the availability zone information from Kubernetes node labels.
   * This method queries the Kubernetes API for the node's labels and extracts
   * the zone information from standard Kubernetes zone labels.
   * @return availability zone information or DEFAULT_FAILURE_DOMAIN if not found
   */
  @Override
  public String getFailureDomain() {
    try {
      String nodeName = getNodeName();
      if (StringUtils.isBlank(nodeName)) {
        LOGGER.warn("Could not determine node name from environment variable");
        return org.apache.pinot.spi.utils.CommonConstants.DEFAULT_FAILURE_DOMAIN;
      }

      String nodeApiUrl = String.format("%s/api/v1/nodes/%s", _kubernetesApiEndpoint, nodeName);
      String responsePayload = getKubernetesNodeInfo(nodeApiUrl);
      
      return extractZoneFromNodeLabels(responsePayload);
    } catch (Exception e) {
      LOGGER.warn("Failed to retrieve availability zone information", e);
      return org.apache.pinot.spi.utils.CommonConstants.DEFAULT_FAILURE_DOMAIN;
    }
  }

  /**
   * Extract the zone information from the node labels in the API response
   */
  private String extractZoneFromNodeLabels(String responsePayload) {
    try {
      final JsonNode jsonNode = JsonUtils.stringToJsonNode(responsePayload);
      final JsonNode metadataNode = jsonNode.path("metadata");
      
      if (metadataNode.isMissingNode()) {
        LOGGER.warn("Metadata node is missing in the payload. Cannot retrieve zone information");
        return org.apache.pinot.spi.utils.CommonConstants.DEFAULT_FAILURE_DOMAIN;
      }
      
      final JsonNode labelsNode = metadataNode.path("labels");
      if (labelsNode.isMissingNode()) {
        LOGGER.warn("Labels node is missing in the payload. Cannot retrieve zone information");
        return org.apache.pinot.spi.utils.CommonConstants.DEFAULT_FAILURE_DOMAIN;
      }

      // Try the recommended topology.kubernetes.io/zone label first
      JsonNode zoneNode = labelsNode.path(TOPOLOGY_ZONE_LABEL);
      if (!zoneNode.isMissingNode() && zoneNode.isTextual()) {
        return zoneNode.textValue();
      }
      
      // Fall back to the deprecated failure-domain.beta.kubernetes.io/zone label
      zoneNode = labelsNode.path(FAILURE_DOMAIN_BETA_LABEL);
      if (!zoneNode.isMissingNode() && zoneNode.isTextual()) {
        return zoneNode.textValue();
      }
      
      LOGGER.warn("No zone labels found in node metadata");
      return org.apache.pinot.spi.utils.CommonConstants.DEFAULT_FAILURE_DOMAIN;
    } catch (IOException ex) {
      LOGGER.warn("Error parsing response payload from Kubernetes API", ex);
      return org.apache.pinot.spi.utils.CommonConstants.DEFAULT_FAILURE_DOMAIN;
    }
  }

  /**
   * Get the name of the Kubernetes node the pod is running on
   */
  private String getNodeName() {
    // In Kubernetes, the node name is typically provided via the NODE_NAME environment variable
    String nodeName = System.getenv(NODE_NAME_ENV_VAR);
    if (StringUtils.isBlank(nodeName)) {
      // If not available through env var, try to get hostname
      try {
        nodeName = InetAddress.getLocalHost().getHostName();
      } catch (UnknownHostException e) {
        LOGGER.warn("Could not determine hostname", e);
      }
    }
    return nodeName;
  }

  /**
   * Utility used to construct the HTTP Request and fetch corresponding response entity.
   */
  @VisibleForTesting
  private String getKubernetesNodeInfo(String apiUrl) {
    HttpGet httpGet = new HttpGet(apiUrl);
    
    try {
      // Add the authorization token
      String token = new String(Files.readAllBytes(Paths.get(_tokenFilePath))).trim();
      httpGet.setHeader("Authorization", "Bearer " + token);
      
      // Set the Accept header for API version
      httpGet.setHeader("Accept", "application/json");
      
      final CloseableHttpResponse closeableHttpResponse = _closeableHttpClient.execute(httpGet);
      if (closeableHttpResponse == null) {
        throw new RuntimeException("[KubernetesEnvironmentProvider]: Response is null. Please verify the Kubernetes API endpoint");
      }
      
      final int statusCode = closeableHttpResponse.getCode();
      if (statusCode != HttpStatus.SC_OK) {
        final String errorMsg = String.format(
            "[KubernetesEnvironmentProvider]: Failed to retrieve Kubernetes node information. Response Status code: %s",
            statusCode);
        throw new RuntimeException(errorMsg);
      }
      
      return EntityUtils.toString(closeableHttpResponse.getEntity());
    } catch (RuntimeException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new RuntimeException(String.format(
          "[KubernetesEnvironmentProvider]: Failed to retrieve information from Kubernetes API %s",
          apiUrl), ex);
    }
  }
}