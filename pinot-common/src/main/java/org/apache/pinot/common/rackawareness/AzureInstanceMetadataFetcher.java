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
package org.apache.pinot.common.rackawareness;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.net.UnknownHostException;
import java.util.Properties;
import javax.net.ssl.SSLException;
import javax.ws.rs.WebApplicationException;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Azure implementation for interface InstanceMetadataFetchable.
 */
public class AzureInstanceMetadataFetcher implements InstanceMetadataFetchable {
  private static final Logger LOGGER = LoggerFactory.getLogger(AzureInstanceMetadataFetcher.class);
  private static final String AZURE_CLOUD_PROPERTIES_FILE_NAME = "azure-cloud.properties";
  private static final String IMDS_ENDPOINT_KEY = "imds_endpoint";
  private static final String METADATA = "Metadata";
  private static final String TRUE = "true";
  private static final String COMPUTE = "compute";
  private static final String PLATFORM_FAULT_DOMAIN = "platformFaultDomain";

  private final String _imdsEndpoint;
  private final CloseableHttpClient _closeableHttpClient;


  public AzureInstanceMetadataFetcher(InstanceMetadataFetcherProperties properties) {
    InstanceMetadataFetcherProperties instanceMetadataFetcherProperties =
        new InstanceMetadataFetcherProperties(properties);
    Preconditions.checkNotNull(instanceMetadataFetcherProperties,
        "instanceMetadataFetcherProperties cannot be null.");
    _imdsEndpoint = Preconditions.checkNotNull(getImdsEndpointValue(),
        "imdsEndpoint cannot be null.");

    final RequestConfig requestConfig = RequestConfig.custom()
        .setConnectTimeout(instanceMetadataFetcherProperties.getConnectionTimeOut())
        .setConnectionRequestTimeout(instanceMetadataFetcherProperties.getRequestTimeOut())
        .build();

    final int maxRetry = instanceMetadataFetcherProperties.getMaxRetry();

    final HttpRequestRetryHandler httpRequestRetryHandler = (iOException, executionCount, httpContext) -> {
      LOGGER.debug("Counting number for retry: {}", executionCount);
      return !(executionCount >= maxRetry
          || iOException instanceof InterruptedIOException
          || iOException instanceof UnknownHostException
          || iOException instanceof SSLException
          || HttpClientContext.adapt(httpContext).getRequest() instanceof HttpEntityEnclosingRequest);
    };

    _closeableHttpClient = HttpClients
        .custom()
        .setDefaultRequestConfig(requestConfig)
        .setRetryHandler(httpRequestRetryHandler)
        .build();
  }

  /**
   * This constructor is for unit test only
   */
  AzureInstanceMetadataFetcher(String imdsEndpoint, CloseableHttpClient closeableHttpClient) {
    _imdsEndpoint = imdsEndpoint;
    _closeableHttpClient = closeableHttpClient;
  }

  /**
   * Fetches Azure cloud based server instance metadata
   * @return Azure cloud based server instance metadata
   */
  @Override
  public InstanceMetadata fetch() {
    final String responsePayloadString = getResponsePayload();

    // For a sample response payload,
    // check https://docs.microsoft.com/en-us/azure/virtual-machines/windows/instance-metadata-service?tabs=linux
    final ObjectMapper objectMapper = new ObjectMapper();
    try {
      final JsonNode jsonNode = objectMapper.readTree(responsePayloadString);
      final JsonNode computeNode = jsonNode.path(COMPUTE);

      if (computeNode.isMissingNode()) {
        throw new WebApplicationException("Compute node is missing in the payload.");
      }

      final JsonNode platformFaultDomainNode = computeNode.path(PLATFORM_FAULT_DOMAIN);
      if (platformFaultDomainNode.isMissingNode() || !platformFaultDomainNode.isTextual()) {
        throw new WebApplicationException("Json node platformFaultDomain is not valid.");
      }
      final String platformFaultDomain = platformFaultDomainNode.textValue();

      return new InstanceMetadata(platformFaultDomain);

    } catch (IOException e) {
      throw new WebApplicationException(
          String.format("Errors when parsing response payload from Azure Instance Metadata Service: %s",
              responsePayloadString), e);
    }
  }

  private String getResponsePayload() {
    String url = _imdsEndpoint;
    HttpGet httpGet = new HttpGet(url);
    httpGet.setHeader(METADATA, TRUE);

    try {
      final CloseableHttpResponse closeableHttpResponse = _closeableHttpClient.execute(httpGet);
      if (closeableHttpResponse == null) {
        throw new WebApplicationException("Response is null");
      }
      final StatusLine statusLine = closeableHttpResponse.getStatusLine();
      final int statusCode = statusLine.getStatusCode();
      if (statusCode != HttpStatus.SC_OK) {
        final String errorMsg = String.format("Failed to get metadata. Status code: %s. Response: %s",
            statusCode, statusLine.getReasonPhrase());
        throw new WebApplicationException(errorMsg);
      }
      return EntityUtils.toString(closeableHttpResponse.getEntity());

    } catch (IOException e) {
      throw new WebApplicationException(
          String.format("Failed to get metadata from Azure Instance Metadata Service %s", url), e);
    }
  }

  private String getImdsEndpointValue() {
    String imdsEndpointValue = null;
    try (InputStream input = getClass().getClassLoader().getResourceAsStream(AZURE_CLOUD_PROPERTIES_FILE_NAME)) {
      Properties properties = new Properties();

      if (input == null) {
        throw new IllegalArgumentException("Resource cannot be found: " + AZURE_CLOUD_PROPERTIES_FILE_NAME);
      }

      properties.load(input);
      imdsEndpointValue = properties.getProperty(IMDS_ENDPOINT_KEY);
    } catch (IOException e) {
      e.printStackTrace();
    }

    return imdsEndpointValue;
  }
}
