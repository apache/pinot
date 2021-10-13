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
package org.apache.pinot.controller.helix;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.io.IOUtils;
import org.apache.pinot.controller.helix.core.util.BrokerResourceExternalViewReader;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;


public class BrokerResourceExternalViewReaderTest {
  @Mock
  private ZkClient _mockZkClient;

  private BrokerResourceExternalViewReader _externalViewReaderUnderTest;

  @BeforeMethod
  public void setUp()
      throws Exception {
    initMocks(this);
    InputStream mockInputStream =
        IOUtils.toInputStream("{\"mapFields\":{\"field1\":{\"Broker_12.34.56.78_1234\":\"ONLINE\"}}}", "UTF-8");
    _externalViewReaderUnderTest = Mockito.spy(new BrokerResourceExternalViewReader(_mockZkClient) {
      @Override
      protected ByteArrayInputStream getInputStream(byte[] brokerResourceNodeData) {
        return (ByteArrayInputStream) mockInputStream;
      }
    });
  }

  @Test
  public void testGetTableWithToBrokersInTenantMap() {
    // Setup
    final Map<String, List<String>> expectedResult = new HashMap<>();
    expectedResult.put("field1", Arrays.asList("12.34.56.78:1234"));
    when(_mockZkClient.readData(Mockito.anyString(), Mockito.anyBoolean())).thenReturn("json".getBytes());

    // Run the test
    final Map<String, List<String>> result = _externalViewReaderUnderTest.getTableToBrokersMap();

    // Verify the results
    assertEquals(expectedResult, result);
  }

  @Test
  public void testGetTableToLiveBrokersMap() {
    // Setup
    final Map<String, List<String>> expectedResult = new HashMap<>();
    expectedResult.put("field1", Arrays.asList("Broker_12.34.56.78_1234"));
    when(_mockZkClient.readData(Mockito.anyString(), Mockito.anyBoolean())).thenReturn("json".getBytes());

    // Run the test
    final Map<String, List<String>> result = _externalViewReaderUnderTest.getTableWithTypeToRawBrokerInstanceIdsMap();

    // Verify the results
    assertEquals(expectedResult, result);
  }

  @Test
  public void testGetTableToBrokersMapExceptionState() {
    // Setup
    final Map<String, List<String>> expectedResult = new HashMap<>();
    when(_mockZkClient.readData(Mockito.anyString(), Mockito.anyBoolean())).thenThrow(RuntimeException.class);

    // Run the test
    final Map<String, List<String>> result = _externalViewReaderUnderTest.getTableToBrokersMap();

    // Verify the results
    assertEquals(expectedResult, result);
  }
}

