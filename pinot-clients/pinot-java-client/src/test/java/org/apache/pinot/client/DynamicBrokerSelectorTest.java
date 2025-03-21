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

import com.google.common.collect.ImmutableList;
import java.util.Collections;
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
import static org.mockito.MockitoAnnotations.openMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


public class DynamicBrokerSelectorTest {

  @Mock
  private ExternalViewReader _mockExternalViewReader;

  @Mock
  private ZkClient _mockZkClient;

  private DynamicBrokerSelector _dynamicBrokerSelectorUnderTest;

  private static final String ZK_SERVER = "zkServers";

  @BeforeMethod
  public void setUp()
      throws Exception {
    openMocks(this);
    Map<String, List<String>> tableToBrokerListMap = new HashMap<>();
    tableToBrokerListMap.put("table1", Collections.singletonList("broker1"));
    when(_mockExternalViewReader.getTableToBrokersMap()).thenReturn(tableToBrokerListMap);
    _dynamicBrokerSelectorUnderTest = Mockito.spy(new DynamicBrokerSelector(ZK_SERVER) {
      @Override
      protected ExternalViewReader getEvReader(ZkClient zkClient) {
        return _mockExternalViewReader;
      }

      @Override
      protected ExternalViewReader getEvReader(ZkClient zkClient, boolean preferTlsPort) {
        return _mockExternalViewReader;
      }

      @Override
      protected ExternalViewReader getEvReader(ZkClient zkClient, boolean preferTlsPort, boolean useGrpcPort) {
        return _mockExternalViewReader;
      }

      @Override
      protected ZkClient getZkClient(String zkServers) {
        return _mockZkClient;
      }
    });
  }

  @Test
  public void testHandleDataChange() {
    _dynamicBrokerSelectorUnderTest.handleDataChange("dataPath", "data");

    verify(_mockExternalViewReader, times(2)).getTableToBrokersMap();
  }

  @Test
  public void testHandleDataDeleted() {
    _dynamicBrokerSelectorUnderTest.handleDataDeleted("dataPath");

    verify(_mockExternalViewReader, times(2)).getTableToBrokersMap();
  }

  @Test
  public void testSelectBrokerWithTableName() {
    _dynamicBrokerSelectorUnderTest.handleDataChange("dataPath", "data");

    String result = _dynamicBrokerSelectorUnderTest.selectBroker("table1");

    assertEquals("broker1", result);
  }

  @Test
  public void testSelectBrokerWithDBAndTableName() {
    _dynamicBrokerSelectorUnderTest.handleDataChange("dataPath", "data");
    String result = _dynamicBrokerSelectorUnderTest.selectBroker("db1.table1");
    assertEquals("broker1", result);

    Map<String, List<String>> tableToBrokerListMap = new HashMap<>();
    tableToBrokerListMap.put("table1", Collections.singletonList("broker1"));
    tableToBrokerListMap.put("db1.table1", Collections.singletonList("broker2"));
    when(_mockExternalViewReader.getTableToBrokersMap()).thenReturn(tableToBrokerListMap);
    _dynamicBrokerSelectorUnderTest.handleDataChange("dataPath", "data");
    result = _dynamicBrokerSelectorUnderTest.selectBroker("db1.table1");
    assertEquals("broker2", result);
  }

  @Test
  public void testSelectBrokerForNullTable() {
    _dynamicBrokerSelectorUnderTest.handleDataChange("dataPath", "data");

    String result = _dynamicBrokerSelectorUnderTest.selectBroker(null);

    assertEquals("broker1", result);
  }

  @Test
  public void testSelectBrokerForNullTableAndEmptyBrokerListRef() {
    when(_mockExternalViewReader.getTableToBrokersMap()).thenReturn(Collections.emptyMap());
    _dynamicBrokerSelectorUnderTest.handleDataChange("dummy-data-path", "dummy-date");

    String result = _dynamicBrokerSelectorUnderTest.selectBroker(null);

    assertNull(result);
  }

  @Test
  public void testSelectBrokerForNonNullTableAndEmptyBrokerListRef() {
    when(_mockExternalViewReader.getTableToBrokersMap()).thenReturn(Collections.emptyMap());
    _dynamicBrokerSelectorUnderTest.handleDataChange("dummy-data-path", "dummy-date");

    String result = _dynamicBrokerSelectorUnderTest.selectBroker("dummyTableName");

    assertNull(result);
  }

  @Test
  public void testGetBrokers() {
    assertEquals(_dynamicBrokerSelectorUnderTest.getBrokers(), ImmutableList.of("broker1"));
  }

  @Test
  public void testCloseZkClient() {
    _dynamicBrokerSelectorUnderTest.close();

    Mockito.verify(_mockZkClient, times(1)).close();
  }

  @Test
  public void testSelectBrokerWithInvalidTable() {
    Map<String, List<String>> tableToBrokerListMap = new HashMap<>();
    tableToBrokerListMap.put("table1", Collections.singletonList("broker1"));
    when(_mockExternalViewReader.getTableToBrokersMap()).thenReturn(tableToBrokerListMap);
    _dynamicBrokerSelectorUnderTest.handleDataChange("dataPath", "data");
    String result = _dynamicBrokerSelectorUnderTest.selectBroker("invalidTable");
    assertEquals(result, "broker1");
  }

  @Test
  public void testSelectBrokerWithTwoTablesOneInvalid() {
    Map<String, List<String>> tableToBrokerListMap = new HashMap<>();
    tableToBrokerListMap.put("table1", Collections.singletonList("broker1"));
    when(_mockExternalViewReader.getTableToBrokersMap()).thenReturn(tableToBrokerListMap);
    _dynamicBrokerSelectorUnderTest.handleDataChange("dataPath", "data");
    String result = _dynamicBrokerSelectorUnderTest.selectBroker("table1", "invalidTable");
    assertEquals(result, "broker1");
  }
}
