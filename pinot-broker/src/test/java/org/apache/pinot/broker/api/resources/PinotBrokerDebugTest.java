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
package org.apache.pinot.broker.api.resources;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import org.apache.pinot.broker.broker.MultiClusterRoutingContextProvider;
import org.apache.pinot.broker.routing.manager.BrokerRoutingManager;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.core.routing.MultiClusterRoutingContext;
import org.apache.pinot.core.routing.RoutingTable;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.apache.pinot.spi.data.PhysicalTableConfig;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class PinotBrokerDebugTest {

  private PinotBrokerDebug createBrokerDebug(BrokerRoutingManager routingManager)
      throws Exception {
    PinotBrokerDebug brokerDebug = new PinotBrokerDebug();
    setField(brokerDebug, "_routingManager", routingManager);
    setField(brokerDebug, "_multiClusterRoutingContextProvider", new MultiClusterRoutingContextProvider(null));
    setField(brokerDebug, "_tableCache", mock(TableCache.class));
    return brokerDebug;
  }

  private static void setField(Object target, String fieldName, Object value)
      throws Exception {
    Field field = target.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(target, value);
  }

  @Test
  public void testGetRoutingTableUsesSameRequestIdForOfflineAndRealtime()
      throws Exception {
    BrokerRoutingManager routingManager = mock(BrokerRoutingManager.class);
    when(routingManager.getRoutingTable(any(BrokerRequest.class), anyLong()))
        .thenReturn(new RoutingTable(Collections.emptyMap(), Collections.emptyList(), 0));

    PinotBrokerDebug brokerDebug = createBrokerDebug(routingManager);

    brokerDebug.getRoutingTable("testTable", false, (HttpHeaders) null);

    ArgumentCaptor<Long> requestIdCaptor = ArgumentCaptor.forClass(Long.class);
    verify(routingManager, times(2)).getRoutingTable(any(BrokerRequest.class), requestIdCaptor.capture());
    List<Long> requestIds = requestIdCaptor.getAllValues();
    assertEquals(requestIds.size(), 2);
    assertEquals(requestIds.get(0), requestIds.get(1));
  }

  @Test
  public void testGetRoutingTableForRealtimeOnlyRawTableDoesNotSkewRequestId()
      throws Exception {
    BrokerRoutingManager routingManager = mock(BrokerRoutingManager.class);
    when(routingManager.getRoutingTable(any(BrokerRequest.class), anyLong())).thenAnswer(invocation -> {
      BrokerRequest brokerRequest = invocation.getArgument(0);
      String tableNameWithType = brokerRequest.getQuerySource().getTableName();
      if (tableNameWithType.endsWith("_REALTIME")) {
        return new RoutingTable(Collections.emptyMap(), Collections.emptyList(), 0);
      }
      return null;
    });

    PinotBrokerDebug brokerDebug = createBrokerDebug(routingManager);

    brokerDebug.getRoutingTable("testTable", false, (HttpHeaders) null);
    brokerDebug.getRoutingTable("testTable", false, (HttpHeaders) null);

    ArgumentCaptor<BrokerRequest> brokerRequestCaptor = ArgumentCaptor.forClass(BrokerRequest.class);
    ArgumentCaptor<Long> requestIdCaptor = ArgumentCaptor.forClass(Long.class);
    verify(routingManager, times(4)).getRoutingTable(brokerRequestCaptor.capture(), requestIdCaptor.capture());
    List<BrokerRequest> brokerRequests = brokerRequestCaptor.getAllValues();
    List<Long> requestIds = requestIdCaptor.getAllValues();

    assertEquals(brokerRequests.size(), 4);
    assertEquals(requestIds.size(), 4);

    Long firstRealtimeRequestId = null;
    Long secondRealtimeRequestId = null;
    for (int i = 0; i < brokerRequests.size(); i++) {
      if (brokerRequests.get(i).getQuerySource().getTableName().endsWith("_REALTIME")) {
        if (firstRealtimeRequestId == null) {
          firstRealtimeRequestId = requestIds.get(i);
        } else {
          secondRealtimeRequestId = requestIds.get(i);
        }
      }
    }

    assertTrue(firstRealtimeRequestId != null);
    assertTrue(secondRealtimeRequestId != null);
    assertEquals((long) secondRealtimeRequestId, firstRealtimeRequestId + 1);
  }

  @Test
  public void testMultiClusterRoutingRejectsWhenNotConfigured()
      throws Exception {
    BrokerRoutingManager routingManager = mock(BrokerRoutingManager.class);
    PinotBrokerDebug brokerDebug = createBrokerDebug(routingManager);

    WebApplicationException ex = expectThrows(WebApplicationException.class,
        () -> brokerDebug.getRoutingTable("testTable", true, (HttpHeaders) null));
    assertEquals(ex.getResponse().getStatus(), 400);
    assertTrue(ex.getMessage().contains("Multi-cluster routing is not configured"));
  }

  @Test
  public void testMultiClusterRoutingRejectsNonLogicalTable()
      throws Exception {
    BrokerRoutingManager routingManager = mock(BrokerRoutingManager.class);
    BrokerRoutingManager multiClusterManager = mock(BrokerRoutingManager.class);
    MultiClusterRoutingContext context =
        new MultiClusterRoutingContext(Collections.emptyMap(), routingManager, multiClusterManager,
            Collections.emptySet());

    TableCache tableCache = mock(TableCache.class);
    when(tableCache.isLogicalTable("physicalTable")).thenReturn(false);

    PinotBrokerDebug brokerDebug = new PinotBrokerDebug();
    setField(brokerDebug, "_routingManager", routingManager);
    setField(brokerDebug, "_multiClusterRoutingContextProvider", new MultiClusterRoutingContextProvider(context));
    setField(brokerDebug, "_tableCache", tableCache);

    WebApplicationException ex = expectThrows(WebApplicationException.class,
        () -> brokerDebug.getRoutingTable("physicalTable", true, (HttpHeaders) null));
    assertEquals(ex.getResponse().getStatus(), 400);
    assertTrue(ex.getMessage().contains("not a logical table"));
  }

  @Test
  public void testMultiClusterRoutingSucceedsForLogicalTable()
      throws Exception {
    BrokerRoutingManager routingManager = mock(BrokerRoutingManager.class);
    BrokerRoutingManager multiClusterManager = mock(BrokerRoutingManager.class);
    when(multiClusterManager.getRoutingTable(any(BrokerRequest.class), anyLong()))
        .thenReturn(new RoutingTable(Collections.emptyMap(), Collections.emptyList(), 0));

    MultiClusterRoutingContext context =
        new MultiClusterRoutingContext(Collections.emptyMap(), routingManager, multiClusterManager,
            Collections.emptySet());

    // Logical table with two physical tables (one OFFLINE each) in the config map
    LogicalTableConfig logicalTableConfig = new LogicalTableConfig();
    logicalTableConfig.setTableName("logicalTable");
    logicalTableConfig.setPhysicalTableConfigMap(Map.of(
        "physicalTable1_OFFLINE", new PhysicalTableConfig(false),
        "physicalTable2_OFFLINE", new PhysicalTableConfig(true)));

    TableCache tableCache = mock(TableCache.class);
    when(tableCache.isLogicalTable("logicalTable")).thenReturn(true);
    when(tableCache.getLogicalTableConfig("logicalTable")).thenReturn(logicalTableConfig);

    PinotBrokerDebug brokerDebug = new PinotBrokerDebug();
    setField(brokerDebug, "_routingManager", routingManager);
    setField(brokerDebug, "_multiClusterRoutingContextProvider", new MultiClusterRoutingContextProvider(context));
    setField(brokerDebug, "_tableCache", tableCache);

    brokerDebug.getRoutingTable("logicalTable", true, (HttpHeaders) null);

    // One call per physical table in the config; the local routingManager is never touched
    verify(multiClusterManager, times(2)).getRoutingTable(any(BrokerRequest.class), anyLong());
    verify(routingManager, times(0)).getRoutingTable(any(BrokerRequest.class), anyLong());
  }
}
