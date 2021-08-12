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
package org.apache.pinot.client;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.io.IOUtils;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;

public class ExternalViewReaderTest {

  @Mock
  private ZkClient mockZkClient;

  private ExternalViewReader externalViewReaderUnderTest;

  @BeforeMethod
  public void setUp() throws Exception {
    initMocks(this);
    InputStream mockInputStream = IOUtils.toInputStream(
        "{\"mapFields\":{\"field1\":{\"Broker_12.34.56.78_1234\":\"ONLINE\"}}}", "UTF-8");
    externalViewReaderUnderTest = Mockito.spy(new ExternalViewReader(mockZkClient) {
      @Override
      protected ByteArrayInputStream getInputStream(byte[] brokerResourceNodeData) {
        return (ByteArrayInputStream) mockInputStream;
      }
    });
  }

  @Test
  public void testGetLiveBrokers() throws IOException {
    // Setup
    final List<String> expectedResult = Arrays.asList("12.34.56.78:1234");
    when(mockZkClient.readData(Mockito.anyString(), Mockito.anyBoolean())).thenReturn(
        "json".getBytes());

    // Run the test
    final List<String> result = externalViewReaderUnderTest.getLiveBrokers();

    // Verify the results
    assertEquals(expectedResult, result);
  }

  @Test
  public void testGetLiveBrokersExceptionState() throws IOException {
    // Setup
    final List<String> expectedResult = Arrays.asList();
    when(mockZkClient.readData(Mockito.anyString(), Mockito.anyBoolean())).thenThrow(
        RuntimeException.class);

    // Run the test
    final List<String> result = externalViewReaderUnderTest.getLiveBrokers();

    // Verify the results
    assertEquals(expectedResult, result);
  }

  @Test
  public void testGetTableToBrokersMap() {
    // Setup
    final Map<String, List<String>> expectedResult = new HashMap<>();
    expectedResult.put("field1", Arrays.asList("12.34.56.78:1234"));
    when(mockZkClient.readData(Mockito.anyString(), Mockito.anyBoolean())).thenReturn(
        "json".getBytes());

    // Run the test
    final Map<String, List<String>> result = externalViewReaderUnderTest.getTableToBrokersMap();

    // Verify the results
    assertEquals(expectedResult, result);
  }

  @Test
  public void testGetTableToBrokersMapExceptionState() {
    // Setup
    final Map<String, List<String>> expectedResult = new HashMap<>();
    when(mockZkClient.readData(Mockito.anyString(), Mockito.anyBoolean())).thenThrow(
        RuntimeException.class);

    // Run the test
    final Map<String, List<String>> result = externalViewReaderUnderTest.getTableToBrokersMap();

    // Verify the results
    assertEquals(expectedResult, result);
  }
}
