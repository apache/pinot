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
package org.apache.pinot.controller.util;

import com.google.common.collect.BiMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.core.routing.TimeBoundaryInfo;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


public class BrokerServiceHelperTest {
  @Test
  public void testGetBrokerEndpointsForInstance() {
    // setup
    InstanceConfig instanceConfig = new InstanceConfig("Broker_localhost_1234");
    instanceConfig.setHostName("localhost");
    instanceConfig.setPort("8000");

    ControllerConf controllerConf = new ControllerConf();
    controllerConf.setControllerBrokerProtocol("http");
    PinotHelixResourceManager mockResourceManager = mock(PinotHelixResourceManager.class);
    BrokerServiceHelper brokerServiceHelper =
        new BrokerServiceHelper(mockResourceManager, controllerConf, null, null);

    // test
    BiMap<String, String> brokerEndpoints =
        brokerServiceHelper.getBrokerEndpointsForInstance(Collections.singletonList(instanceConfig));

    // verify
    assertEquals(brokerEndpoints.size(), 1);
    assertEquals(brokerEndpoints.get("http://localhost:8000"), "Broker_localhost_1234");
  }

  @Test
  public void testGetTimeBoundaryInfo() {
    // setup
    InstanceConfig instanceConfig = new InstanceConfig("Broker_localhost_1234");
    instanceConfig.setHostName("localhost");
    instanceConfig.setPort("8000");

    ControllerConf controllerConf = new ControllerConf();

    String offlineTableName = "myTable_OFFLINE";
    TableConfig offlineTableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(offlineTableName).build();

    PinotHelixResourceManager mockResourceManager = mock(PinotHelixResourceManager.class);
    when(mockResourceManager.getBrokerInstancesConfigsFor(offlineTableConfig.getTableName()))
        .thenReturn(Collections.singletonList(instanceConfig));

    CompletionServiceHelper mockServiceHelper = mock(CompletionServiceHelper.class);

    // Mock responses
    Map<String, String> responseMap = new HashMap<>();
    responseMap.put("http://localhost:8000/debug/timeBoundary/" + offlineTableName,
        "{ \"timeColumn\": \"daysSinceEpoch\", \"timeValue\": 16000}");

    CompletionServiceHelper.CompletionServiceResponse serviceResponse =
        new CompletionServiceHelper.CompletionServiceResponse();
    serviceResponse._httpResponses = responseMap;

    when(mockServiceHelper.doMultiGetRequest(anyList(), anyString(), anyBoolean(), anyMap(), anyInt(), anyString()))
        .thenReturn(serviceResponse);

    // test
    BrokerServiceHelper brokerServiceHelper =
        new BrokerServiceHelper(mockResourceManager, controllerConf, null, null);
    brokerServiceHelper.setCompletionServiceHelper(mockServiceHelper);
    TimeBoundaryInfo timeBoundaryInfo = brokerServiceHelper.getTimeBoundaryInfo(offlineTableConfig);

    // verify
    assertNotNull(timeBoundaryInfo);
    assertNotNull(timeBoundaryInfo.getTimeColumn());
    assertNotNull(timeBoundaryInfo.getTimeValue());
  }
}
