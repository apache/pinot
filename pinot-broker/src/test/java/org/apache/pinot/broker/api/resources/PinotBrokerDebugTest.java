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
import org.apache.pinot.broker.api.AccessControl;
import org.apache.pinot.broker.broker.AccessControlFactory;
import org.apache.pinot.broker.broker.MultiClusterRoutingContextProvider;
import org.apache.pinot.broker.routing.manager.BrokerRoutingManager;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.core.routing.MultiClusterRoutingContext;
import org.apache.pinot.core.routing.RoutingTable;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.apache.pinot.spi.data.PhysicalTableConfig;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/**
 * Unit tests for {@link PinotBrokerDebug} covering request-ID consistency, multi-cluster routing dispatch,
 * and the SQL-based routing endpoints (single-table and logical-table expansion).
 */
public class PinotBrokerDebugTest {

  private PinotBrokerDebug createBrokerDebug(BrokerRoutingManager routingManager)
      throws Exception {
    return createBrokerDebug(routingManager, mock(TableCache.class), null);
  }

  private PinotBrokerDebug createBrokerDebug(BrokerRoutingManager routingManager, TableCache tableCache)
      throws Exception {
    return createBrokerDebug(routingManager, tableCache, null);
  }

  private PinotBrokerDebug createBrokerDebug(BrokerRoutingManager routingManager, TableCache tableCache,
      MultiClusterRoutingContext context)
      throws Exception {
    PinotBrokerDebug brokerDebug = new PinotBrokerDebug();
    setField(brokerDebug, "_routingManager", routingManager);
    setField(brokerDebug, "_multiClusterRoutingContextProvider", new MultiClusterRoutingContextProvider(context));
    setField(brokerDebug, "_tableCache", tableCache);
    return brokerDebug;
  }

  /** Returns a mock {@link TableCache} that reports {@code rawName} as a logical table with two physical tables. */
  private static TableCache logicalTableCache(String rawName) {
    LogicalTableConfig cfg = new LogicalTableConfig();
    cfg.setTableName(rawName);
    cfg.setPhysicalTableConfigMap(Map.of(
        "physicalTable1_OFFLINE", new PhysicalTableConfig(false),
        "physicalTable2_OFFLINE", new PhysicalTableConfig(true)));
    TableCache tc = mock(TableCache.class);
    when(tc.isLogicalTable(rawName)).thenReturn(true);
    when(tc.getLogicalTableConfig(rawName)).thenReturn(cfg);
    return tc;
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

    brokerDebug.getRoutingTable("testTable", (HttpHeaders) null);

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

    brokerDebug.getRoutingTable("testTable", (HttpHeaders) null);
    brokerDebug.getRoutingTable("testTable", (HttpHeaders) null);

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

    assertNotNull(firstRealtimeRequestId);
    assertNotNull(secondRealtimeRequestId);
    assertEquals((long) secondRealtimeRequestId, firstRealtimeRequestId + 1);
  }

  /** Creates a PinotBrokerDebug with a permissive access control factory for SQL endpoint tests. */
  private PinotBrokerDebug createBrokerDebugWithAccessControl(BrokerRoutingManager routingManager,
      MultiClusterRoutingContext multiClusterContext, TableCache tableCache)
      throws Exception {
    AccessControl accessControl = mock(AccessControl.class);
    when(accessControl.hasAccess(any(), any(), any(), any())).thenReturn(true);
    AccessControlFactory accessControlFactory = mock(AccessControlFactory.class);
    when(accessControlFactory.create()).thenReturn(accessControl);

    PinotBrokerDebug brokerDebug = createBrokerDebug(routingManager, tableCache, multiClusterContext);
    setField(brokerDebug, "_accessControlFactory", accessControlFactory);
    return brokerDebug;
  }

  // ---- /debug/routingTable/logical/{tableName} and /debug/routingTable/logical/sql tests ----

  @Test
  public void testLogicalRoutingTableRejectsNonLogicalTable()
      throws Exception {
    // Default mock returns false for isLogicalTable
    PinotBrokerDebug brokerDebug = createBrokerDebug(mock(BrokerRoutingManager.class));
    WebApplicationException ex = expectThrows(WebApplicationException.class,
        () -> brokerDebug.getLogicalRoutingTable("physicalTable", false, (HttpHeaders) null));
    assertEquals(ex.getResponse().getStatus(), 400);
    assertTrue(ex.getMessage().contains("not a logical table"));
  }

  @Test
  public void testLogicalRoutingTableWithLocalRoutingExpandsPhysicalTables()
      throws Exception {
    BrokerRoutingManager routingManager = mock(BrokerRoutingManager.class);
    when(routingManager.getRoutingTable(any(BrokerRequest.class), anyString(), anyLong()))
        .thenReturn(new RoutingTable(Collections.emptyMap(), Collections.emptyList(), 0));

    Map<String, Map<ServerInstance, List<String>>> result =
        createBrokerDebug(routingManager, logicalTableCache("logicalTable"))
            .getLogicalRoutingTable("logicalTable", false, (HttpHeaders) null);

    assertEquals(result.size(), 2);
    assertTrue(result.containsKey("physicalTable1_OFFLINE"));
    assertTrue(result.containsKey("physicalTable2_OFFLINE"));
    verify(routingManager, times(2)).getRoutingTable(any(BrokerRequest.class), anyString(), anyLong());
  }

  @Test
  public void testLogicalRoutingTableWithMultiClusterRoutingUsesMultiClusterManager()
      throws Exception {
    BrokerRoutingManager routingManager = mock(BrokerRoutingManager.class);
    BrokerRoutingManager multiClusterManager = mock(BrokerRoutingManager.class);
    when(multiClusterManager.getRoutingTable(any(BrokerRequest.class), anyString(), anyLong()))
        .thenReturn(new RoutingTable(Collections.emptyMap(), Collections.emptyList(), 0));
    MultiClusterRoutingContext context =
        new MultiClusterRoutingContext(Collections.emptyMap(), routingManager, multiClusterManager,
            Collections.emptySet());

    createBrokerDebug(routingManager, logicalTableCache("logicalTable"), context)
        .getLogicalRoutingTable("logicalTable", true, (HttpHeaders) null);

    verify(multiClusterManager, times(2)).getRoutingTable(any(BrokerRequest.class), anyString(), anyLong());
    verify(routingManager, times(0)).getRoutingTable(any(BrokerRequest.class), anyString(), anyLong());
  }

  @Test
  public void testLogicalRoutingTableMultiClusterRejectsWhenNotConfigured()
      throws Exception {
    // Provider wraps null context → multi-cluster routing not configured
    PinotBrokerDebug brokerDebug =
        createBrokerDebug(mock(BrokerRoutingManager.class), logicalTableCache("logicalTable"));
    WebApplicationException ex = expectThrows(WebApplicationException.class,
        () -> brokerDebug.getLogicalRoutingTable("logicalTable", true, (HttpHeaders) null));
    assertEquals(ex.getResponse().getStatus(), 400);
    assertTrue(ex.getMessage().contains("Multi-cluster routing is not configured"));
  }

  @Test
  public void testLogicalSqlRoutingTableRejectsNonLogicalTable()
      throws Exception {
    // Default mock returns false for isLogicalTable
    PinotBrokerDebug brokerDebug = createBrokerDebugWithAccessControl(
        mock(BrokerRoutingManager.class), null, mock(TableCache.class));
    WebApplicationException ex = expectThrows(WebApplicationException.class,
        () -> brokerDebug.getLogicalRoutingTableForQuery("SELECT * FROM myTable_OFFLINE", false, (HttpHeaders) null));
    assertEquals(ex.getResponse().getStatus(), 400);
    assertTrue(ex.getMessage().contains("not a logical table"));
  }

  @Test
  public void testLogicalSqlRoutingTableWithLocalRoutingExpandsPhysicalTables()
      throws Exception {
    BrokerRoutingManager routingManager = mock(BrokerRoutingManager.class);
    when(routingManager.getRoutingTable(any(BrokerRequest.class), anyString(), anyLong()))
        .thenReturn(new RoutingTable(Collections.emptyMap(), Collections.emptyList(), 0));

    Map<String, Map<ServerInstance, List<String>>> result =
        createBrokerDebugWithAccessControl(routingManager, null, logicalTableCache("logicalTable"))
            .getLogicalRoutingTableForQuery("SELECT * FROM logicalTable_OFFLINE", false, (HttpHeaders) null);

    assertEquals(result.size(), 2);
    assertTrue(result.containsKey("physicalTable1_OFFLINE"));
    assertTrue(result.containsKey("physicalTable2_OFFLINE"));
    verify(routingManager, times(2)).getRoutingTable(any(BrokerRequest.class), anyString(), anyLong());
  }

  @Test
  public void testLogicalSqlRoutingTableWithMultiClusterRoutingUsesMultiClusterManager()
      throws Exception {
    BrokerRoutingManager routingManager = mock(BrokerRoutingManager.class);
    BrokerRoutingManager multiClusterManager = mock(BrokerRoutingManager.class);
    when(multiClusterManager.getRoutingTable(any(BrokerRequest.class), anyString(), anyLong()))
        .thenReturn(new RoutingTable(Collections.emptyMap(), Collections.emptyList(), 0));
    MultiClusterRoutingContext context =
        new MultiClusterRoutingContext(Collections.emptyMap(), routingManager, multiClusterManager,
            Collections.emptySet());

    Map<String, Map<ServerInstance, List<String>>> result =
        createBrokerDebugWithAccessControl(routingManager, context, logicalTableCache("logicalTable"))
            .getLogicalRoutingTableForQuery("SELECT * FROM logicalTable_OFFLINE", true, (HttpHeaders) null);

    assertEquals(result.size(), 2);
    assertTrue(result.containsKey("physicalTable1_OFFLINE"));
    assertTrue(result.containsKey("physicalTable2_OFFLINE"));
    verify(routingManager, times(0)).getRoutingTable(any(BrokerRequest.class), anyString(), anyLong());
    verify(multiClusterManager, times(2)).getRoutingTable(any(BrokerRequest.class), anyString(), anyLong());
  }
}
