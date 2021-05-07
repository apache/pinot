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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.WebApplicationException;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.mockito.Mock;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.http.HttpStatus.*;
import static org.apache.pinot.plugin.provider.AzureEnvironmentProvider.*;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.*;

/**
 * Unit test for {@link AzureEnvironmentProviderTest}
 */
public class AzureEnvironmentProviderTest {
  private final static String IMDS_RESPONSE_FILE = "mock-imds-response.json";
  private final static String IMDS_RESPONSE_WITHOUT_COMPUTE_INFO = "mock-imds-response-without-computenode.json";
  private final static String IMDS_RESPONSE_WITHOUT_FAULT_DOMAIN_INFO = "mock-imds-response-without-faultDomain.json";
  private final static String IMDS_ENDPOINT_VALUE = "http://169.254.169.254/metadata/instance?api-version=2020-09-01";

  @Mock
  private CloseableHttpClient _mockHttpClient;
  @Mock
  private CloseableHttpResponse _mockHttpResponse;
  @Mock
  private StatusLine _mockStatusLine;
  @Mock
  private HttpEntity _mockHttpEntity;

  private AzureEnvironmentProvider _azureEnvironmentProvider;

  private AzureEnvironmentProvider _azureEnvironmentProviderWithParams;

  PinotConfiguration _pinotConfiguration;

  @BeforeMethod
  public void init() {
    initMocks(this);
    _pinotConfiguration = new PinotConfiguration(new HashMap<>());
    _azureEnvironmentProvider = new AzureEnvironmentProvider();
    _azureEnvironmentProviderWithParams = new AzureEnvironmentProvider(3, IMDS_ENDPOINT_VALUE, _mockHttpClient);
  }

  @Test
  public void testFailureDomainRetrieval() throws IOException {
    mockUtil();
    when(_mockHttpEntity.getContent()).thenReturn(getClass().getClassLoader().getResourceAsStream(IMDS_RESPONSE_FILE));
    String failureDomain = _azureEnvironmentProviderWithParams.getFailureDomain();
    Assert.assertEquals(failureDomain, "36");
    verify(_mockHttpClient, times(1)).execute(any(HttpGet.class));
    verify(_mockHttpResponse, times(1)).getStatusLine();
    verify(_mockStatusLine, times(1)).getStatusCode();
    verify(_mockHttpResponse, times(1)).getEntity();
    verifyNoMoreInteractions(_mockHttpClient, _mockHttpResponse, _mockStatusLine);
  }

  @Test(expectedExceptions = IllegalArgumentException.class,
        expectedExceptionsMessageRegExp = "imdsEndpoint should not be null or empty")
  public void testInvalidIMDSEndpoint() throws IllegalArgumentException {
    Map<String, Object> map = _pinotConfiguration.toMap();
    map.put(MAX_RETRY, "3");
    map.put(IMDS_ENDPOINT, "");
    PinotConfiguration pinotConfiguration = new PinotConfiguration(map);
    _azureEnvironmentProvider.init(pinotConfiguration);
  }

  @Test(expectedExceptions = IllegalArgumentException.class,
        expectedExceptionsMessageRegExp = "maxRetry cannot be less than or equal to 0")
  public void testInvalidRetryCount() throws IllegalArgumentException {
    Map<String, Object> map = _pinotConfiguration.toMap();
    map.put(MAX_RETRY, "0");
    PinotConfiguration pinotConfiguration = new PinotConfiguration(map);
    _azureEnvironmentProvider.init(pinotConfiguration);
  }

  @Test(expectedExceptions = NullPointerException.class,
      expectedExceptionsMessageRegExp = "Closeable Http Client cannot be null")
  public void testInvalidHttpClient() throws IllegalArgumentException {
    new AzureEnvironmentProvider(
        3, IMDS_ENDPOINT_VALUE, null);
  }

  @Test(expectedExceptions = WebApplicationException.class,
        expectedExceptionsMessageRegExp = "Compute node is missing in the payload. Cannot retrieve Failure Domain Information")
  public void testMissingComputeNodeResponse() throws WebApplicationException, IOException {
    mockUtil();
    when(_mockHttpEntity.getContent())
        .thenReturn(getClass().getClassLoader().getResourceAsStream(IMDS_RESPONSE_WITHOUT_COMPUTE_INFO));
    _azureEnvironmentProviderWithParams.getFailureDomain();
  }

  @Test(expectedExceptions = WebApplicationException.class,
      expectedExceptionsMessageRegExp = "Json node platformFaultDomain is missing or is invalid.")
  public void testMissingFaultDomainResponse() throws WebApplicationException, IOException {
    mockUtil();
    when(_mockHttpEntity.getContent())
        .thenReturn(getClass().getClassLoader().getResourceAsStream(IMDS_RESPONSE_WITHOUT_FAULT_DOMAIN_INFO));
    _azureEnvironmentProviderWithParams.getFailureDomain();
  }

  @Test(expectedExceptions = WebApplicationException.class,
      expectedExceptionsMessageRegExp = "Failed to retrieve azure instance metadata. Response Status code: " + SC_NOT_FOUND)
  public void testIMDSCallFailure() throws WebApplicationException, IOException {
    mockUtil();
    when(_mockStatusLine.getStatusCode()).thenReturn(SC_NOT_FOUND);
    _azureEnvironmentProviderWithParams.getFailureDomain();
  }

  // Mock Response utility method
  private void mockUtil() throws IOException {
    when(_mockHttpClient.execute(any(HttpGet.class))).thenReturn(_mockHttpResponse);
    when(_mockHttpResponse.getStatusLine()).thenReturn(_mockStatusLine);
    when(_mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
    when(_mockHttpResponse.getEntity()).thenReturn(_mockHttpEntity);
  }
}
