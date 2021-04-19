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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.I0Itec.zkclient.ZkClient;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;

public class DynamicBrokerSelectorTest {

  @Mock
  private ExternalViewReader mockExternalViewReader;

  @Mock
  private ZkClient mockZkClient;

  private DynamicBrokerSelector dynamicBrokerSelectorUnderTest;

  @BeforeMethod
  public void setUp() throws Exception {
    initMocks(this);
    Map<String, List<String>> tableToBrokerListMap = new HashMap<>();
    tableToBrokerListMap.put("table1", Arrays.asList("broker1"));
    when(mockExternalViewReader.getTableToBrokersMap()).thenReturn(tableToBrokerListMap);
    dynamicBrokerSelectorUnderTest = Mockito.spy(new DynamicBrokerSelector("zkServers") {
      @Override
      protected ExternalViewReader getEvReader(ZkClient zkClient) {
        return mockExternalViewReader;
      }

      @Override
      protected ZkClient getZkClient(String zkServers) {
        return mockZkClient;
      }
    });
  }

  @Test(priority = 0)
  public void testHandleDataChange() throws Exception {
    // Run the test
    dynamicBrokerSelectorUnderTest.handleDataChange("dataPath", "data");

    // Verify the results
    verify(mockExternalViewReader, times(2)).getTableToBrokersMap();
  }

  @Test(priority = 0)
  public void testHandleDataDeleted() throws Exception {
    // Run the test
    dynamicBrokerSelectorUnderTest.handleDataDeleted("dataPath");

    // Verify the results
    verify(mockExternalViewReader, times(2)).getTableToBrokersMap();
  }

  @Test(priority = 1)
  public void testSelectBroker() throws Exception {
    // Setup
    dynamicBrokerSelectorUnderTest.handleDataChange("dataPath", "data");

    // Run the test
    final String result = dynamicBrokerSelectorUnderTest.selectBroker("table1");

    // Verify the results
    assertEquals("broker1", result);
  }

  @Test(priority = 1)
  public void testSelectBrokerForNullTable() throws Exception {
    // Setup
    dynamicBrokerSelectorUnderTest.handleDataChange("dataPath", "data");

    // Run the test
    final String result = dynamicBrokerSelectorUnderTest.selectBroker(null);

    // Verify the results
    assertEquals("broker1", result);
  }

}
