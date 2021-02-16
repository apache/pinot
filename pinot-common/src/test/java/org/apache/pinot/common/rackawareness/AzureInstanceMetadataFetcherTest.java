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

import java.io.IOException;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Unit test for {@link AzureInstanceMetadataFetcher}
 */
public class AzureInstanceMetadataFetcherTest {
  private final static String IMDS_RESPONSE_FILE = "imds-response.json";

  @Mock
  private CloseableHttpClient _mockHttpClient;
  @Mock
  private CloseableHttpResponse _mockHttpResponse;
  @Mock
  private StatusLine _mockStatusLine;
  @Mock
  private HttpEntity _mockHttpEntity;

  private AzureInstanceMetadataFetcher _underTest;

  @BeforeMethod
  public void init() {
    MockitoAnnotations.initMocks(this);
    _underTest = new AzureInstanceMetadataFetcher(
        "http://169.254.169.254/metadata/instance?api-version=2020-09-01", _mockHttpClient);
  }

  @AfterMethod
  public void after() {
    Mockito.verifyNoMoreInteractions(_mockHttpClient, _mockHttpResponse, _mockStatusLine);
  }

  @Test
  public void testFetch() throws IOException {
    Mockito.when(_mockHttpClient.execute(Mockito.any(HttpGet.class))).thenReturn(_mockHttpResponse);
    Mockito.when(_mockHttpResponse.getStatusLine()).thenReturn(_mockStatusLine);
    Mockito.when(_mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
    Mockito.when(_mockHttpResponse.getEntity()).thenReturn(_mockHttpEntity);
    Mockito.when(_mockHttpEntity.getContent()).thenReturn(getClass().getClassLoader().getResourceAsStream(IMDS_RESPONSE_FILE));

    final InstanceMetadata instanceMetadata = _underTest.fetch();

    Assert.assertEquals(instanceMetadata.getFaultDomain(), "36");
    Mockito.verify(_mockHttpClient).execute(Mockito.any(HttpGet.class));
    Mockito.verify(_mockHttpResponse).getStatusLine();
    Mockito.verify(_mockStatusLine).getStatusCode();
    Mockito.verify(_mockHttpResponse).getEntity();
  }

}
