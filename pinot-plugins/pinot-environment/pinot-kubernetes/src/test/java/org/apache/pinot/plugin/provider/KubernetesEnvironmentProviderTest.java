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

import static org.apache.pinot.spi.utils.CommonConstants.DEFAULT_FAILURE_DOMAIN;

import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class KubernetesEnvironmentProviderTest {

  private CloseableHttpClient _mockHttpClient;
  private CloseableHttpResponse _mockResponse;
  private HttpEntity _mockEntity;
  private KubernetesEnvironmentProvider _kubernetesEnvironmentProvider;
  private File _tempTokenFile;

  @BeforeMethod
  public void setUp() throws IOException {
    // Mock HTTP client and response
    _mockHttpClient = Mockito.mock(CloseableHttpClient.class);
    _mockResponse = Mockito.mock(CloseableHttpResponse.class);
    _mockEntity = Mockito.mock(HttpEntity.class);

    // Setup response
    Mockito.when(_mockResponse.getCode()).thenReturn(HttpStatus.SC_OK);
    Mockito.when(_mockResponse.getEntity()).thenReturn(_mockEntity);
    Mockito.when(_mockHttpClient.execute(Mockito.any(HttpGet.class))).thenReturn(_mockResponse);

    // Create temporary token file
    _tempTokenFile = File.createTempFile("k8s-token", ".tmp");
    Files.writeString(Path.of(_tempTokenFile.getAbsolutePath()), "test-token");
  }

  @AfterMethod
  public void tearDown() {
    if (_tempTokenFile != null && _tempTokenFile.exists()) {
      _tempTokenFile.delete();
    }
  }

  @Test
  public void testInitialization() {
    Map<String, Object> properties = new HashMap<>();
    properties.put("maxRetry", 5);
    properties.put("kubernetesApiEndpoint", "https://custom-k8s-api.example.com");
    properties.put("tokenFilePath", _tempTokenFile.getAbsolutePath());
    properties.put("connectionTimeoutMillis", 3000);
    properties.put("requestTimeoutMillis", 3000);

    PinotConfiguration config = new PinotConfiguration(properties);

    KubernetesEnvironmentProvider provider = new KubernetesEnvironmentProvider();
    provider.init(config);
    
    // No assertions needed for initialization test - we're just ensuring it doesn't throw exceptions
  }

  @Test
  public void testGetFailureDomainWithTopologyZoneLabel() throws Exception {
    // Create test provider with mocked components
    _kubernetesEnvironmentProvider = new KubernetesEnvironmentProvider(
        3, "https://kubernetes.default.svc", _tempTokenFile.getAbsolutePath(), _mockHttpClient);

    // Prepare mock response with topology.kubernetes.io/zone label
    String mockResponseJson = "{\"metadata\":{\"name\":\"test-node\",\"labels\":{\"topology.kubernetes.io/zone\":\"us-west-2a\"}}}";
    Mockito.when(_mockEntity.getContent()).thenReturn(toInputStream(mockResponseJson));

    // Test
    String failureDomain = _kubernetesEnvironmentProvider.getFailureDomain();
    
    // Verify
    Assert.assertEquals(failureDomain, "us-west-2a");
    
    // Verify the API URL was correctly constructed
    ArgumentCaptor<HttpGet> httpGetCaptor = ArgumentCaptor.forClass(HttpGet.class);
    Mockito.verify(_mockHttpClient).execute(httpGetCaptor.capture());
    Assert.assertTrue(httpGetCaptor.getValue().getUri().toString()
        .contains("/api/v1/nodes/"));
  }

  @Test
  public void testGetFailureDomainWithBetaLabel() throws Exception {
    // Create test provider with mocked components
    _kubernetesEnvironmentProvider = new KubernetesEnvironmentProvider(
        3, "https://kubernetes.default.svc", _tempTokenFile.getAbsolutePath(), _mockHttpClient);

    // Prepare mock response with failure-domain.beta.kubernetes.io/zone label
    String mockResponseJson = "{\"metadata\":{\"name\":\"test-node\",\"labels\":{\"failure-domain.beta.kubernetes.io/zone\":\"us-east-1b\"}}}";
    Mockito.when(_mockEntity.getContent()).thenReturn(toInputStream(mockResponseJson));

    // Test
    String failureDomain = _kubernetesEnvironmentProvider.getFailureDomain();
    
    // Verify
    Assert.assertEquals(failureDomain, "us-east-1b");
  }

  @Test
  public void testGetFailureDomainWhenNoLabelsExist() throws Exception {
    // Create test provider with mocked components
    _kubernetesEnvironmentProvider = new KubernetesEnvironmentProvider(
        3, "https://kubernetes.default.svc", _tempTokenFile.getAbsolutePath(), _mockHttpClient);

    // Prepare mock response with no zone labels
    String mockResponseJson = "{\"metadata\":{\"name\":\"test-node\",\"labels\":{}}}";
    Mockito.when(_mockEntity.getContent()).thenReturn(toInputStream(mockResponseJson));

    // Test
    String failureDomain = _kubernetesEnvironmentProvider.getFailureDomain();
    
    // Verify default is returned
    Assert.assertEquals(failureDomain, DEFAULT_FAILURE_DOMAIN);
  }

  @Test
  public void testGetFailureDomainWhenApiCallFails() throws Exception {
    // Create test provider with mocked components
    _kubernetesEnvironmentProvider = new KubernetesEnvironmentProvider(
        3, "https://kubernetes.default.svc", _tempTokenFile.getAbsolutePath(), _mockHttpClient);

    // Mock HTTP failure
    Mockito.when(_mockResponse.getCode()).thenReturn(HttpStatus.SC_UNAUTHORIZED);

    // Test
    String failureDomain = _kubernetesEnvironmentProvider.getFailureDomain();
    
    // Verify default is returned
    Assert.assertEquals(failureDomain, DEFAULT_FAILURE_DOMAIN);
  }

  @Test
  public void testGetFailureDomainWhenJsonParsingFails() throws Exception {
    // Create test provider with mocked components
    _kubernetesEnvironmentProvider = new KubernetesEnvironmentProvider(
        3, "https://kubernetes.default.svc", _tempTokenFile.getAbsolutePath(), _mockHttpClient);

    // Prepare mock response with invalid JSON
    String mockResponseJson = "invalid json{";
    Mockito.when(_mockEntity.getContent()).thenReturn(toInputStream(mockResponseJson));

    // Test
    String failureDomain = _kubernetesEnvironmentProvider.getFailureDomain();
    
    // Verify default is returned
    Assert.assertEquals(failureDomain, DEFAULT_FAILURE_DOMAIN);
  }

  private InputStream toInputStream(String content) {
    return new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
  }
}