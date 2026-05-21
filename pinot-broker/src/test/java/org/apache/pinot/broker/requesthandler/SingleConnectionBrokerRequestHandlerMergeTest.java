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
package org.apache.pinot.broker.requesthandler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.core.transport.ServerResponse;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.spi.config.table.TableType;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertTrue;


/**
 * Regression test for the MV-split DataTable merge contract in
 * {@link SingleConnectionBrokerRequestHandler#mergeDataTablesByIdentity}.
 *
 * <p>Pins the invariant that the same physical server (matching hostname/port/tableType) appearing
 * on BOTH the base sub-query and the MV sub-query contributes two distinct entries to the merged
 * map.  Using a regular HashMap would collapse the two entries (since
 * {@link ServerRoutingInstance#equals} keys on hostname/port/tableType), silently undercounting
 * results during the broker reduce.
 */
public class SingleConnectionBrokerRequestHandlerMergeTest {

  @Test
  public void testSameServerOnBothBranchesProducesTwoEntries() {
    // Two distinct ServerRoutingInstance objects with identical hostname/port/tableType — i.e.
    // hash/equals identical, references different.  This mirrors the production case where the
    // base sub-query and MV sub-query each hit an OFFLINE server hosted on the same host:port.
    ServerRoutingInstance baseInstance = new ServerRoutingInstance("server-1", 9000, TableType.OFFLINE);
    ServerRoutingInstance viewInstance = new ServerRoutingInstance("server-1", 9000, TableType.OFFLINE);
    assertEquals(baseInstance, viewInstance, "Test fixture: instances must be equal()");
    assertEquals(baseInstance.hashCode(), viewInstance.hashCode(),
        "Test fixture: instances must hash equally");
    assertNotSame(baseInstance, viewInstance, "Test fixture: instances must be distinct references");

    DataTable baseDataTable = mock(DataTable.class);
    DataTable viewDataTable = mock(DataTable.class);

    ServerResponse baseResponse = mock(ServerResponse.class);
    when(baseResponse.getDataTable()).thenReturn(baseDataTable);
    when(baseResponse.getResponseSize()).thenReturn(100);
    ServerResponse viewResponse = mock(ServerResponse.class);
    when(viewResponse.getDataTable()).thenReturn(viewDataTable);
    when(viewResponse.getResponseSize()).thenReturn(50);

    Map<ServerRoutingInstance, ServerResponse> baseResponses = new HashMap<>();
    baseResponses.put(baseInstance, baseResponse);
    Map<ServerRoutingInstance, ServerResponse> viewResponses = new HashMap<>();
    viewResponses.put(viewInstance, viewResponse);

    List<ServerRoutingInstance> serversNotResponded = new ArrayList<>();
    long[] totalResponseSizeHolder = {0L};
    Map<ServerRoutingInstance, DataTable> merged = SingleConnectionBrokerRequestHandler
        .mergeDataTablesByIdentity(baseResponses, viewResponses, serversNotResponded,
            totalResponseSizeHolder);

    assertEquals(merged.size(), 2,
        "IdentityHashMap must hold both DataTables when the two sub-queries hit the same physical server. "
            + "A regular HashMap collapses them via ServerRoutingInstance.equals(), silently dropping one. "
            + "Got: " + merged);
    assertTrue(merged.values().contains(baseDataTable), "Base DataTable must be retained");
    assertTrue(merged.values().contains(viewDataTable), "View DataTable must be retained");
    assertEquals(totalResponseSizeHolder[0], 150L);
    assertEquals(serversNotResponded.size(), 0);
  }

  /// Pins the production guard logic in `processMaterializedViewSplitBrokerRequest`:
  ///
  ///   if (!viewFinalResponses.isEmpty() && countSuccessfulDataTables(viewFinalResponses) == 0) throw …
  ///
  /// The guard catches the case where every server on one branch of the MV split returned no
  /// DataTable.  Without this regression test, flipping the comparison (e.g. `!= 0`) or removing
  /// the call would silently undercount the historical half of every MV-split query.
  @Test
  public void testCountSuccessfulDataTablesAllNullReturnsZero() {
    ServerRoutingInstance s1 = new ServerRoutingInstance("server-1", 9000, TableType.OFFLINE);
    ServerRoutingInstance s2 = new ServerRoutingInstance("server-2", 9000, TableType.OFFLINE);
    ServerResponse r1 = mock(ServerResponse.class);
    when(r1.getDataTable()).thenReturn(null);
    ServerResponse r2 = mock(ServerResponse.class);
    when(r2.getDataTable()).thenReturn(null);
    Map<ServerRoutingInstance, ServerResponse> responses = new HashMap<>();
    responses.put(s1, r1);
    responses.put(s2, r2);

    int count = SingleConnectionBrokerRequestHandler.countSuccessfulDataTables(responses);
    assertEquals(count, 0,
        "All-null DataTable responses must yield zero — guard depends on this to detect total split-side failure");
  }

  @Test
  public void testCountSuccessfulDataTablesMixedReturnsCorrectCount() {
    ServerRoutingInstance s1 = new ServerRoutingInstance("server-1", 9000, TableType.OFFLINE);
    ServerRoutingInstance s2 = new ServerRoutingInstance("server-2", 9000, TableType.OFFLINE);
    ServerRoutingInstance s3 = new ServerRoutingInstance("server-3", 9000, TableType.OFFLINE);
    ServerResponse ok1 = mock(ServerResponse.class);
    when(ok1.getDataTable()).thenReturn(mock(DataTable.class));
    ServerResponse failed = mock(ServerResponse.class);
    when(failed.getDataTable()).thenReturn(null);
    ServerResponse ok2 = mock(ServerResponse.class);
    when(ok2.getDataTable()).thenReturn(mock(DataTable.class));
    Map<ServerRoutingInstance, ServerResponse> responses = new HashMap<>();
    responses.put(s1, ok1);
    responses.put(s2, failed);
    responses.put(s3, ok2);

    int count = SingleConnectionBrokerRequestHandler.countSuccessfulDataTables(responses);
    assertEquals(count, 2,
        "Two of three responses carry a DataTable — guard must NOT trip (count > 0)");
  }

  @Test
  public void testCountSuccessfulDataTablesEmptyMapReturnsZero() {
    // Empty map is the "this side was not dispatched" case — the production guard skips it with
    // `!viewFinalResponses.isEmpty() && ...`.  Confirming the count is zero anchors the
    // documented short-circuit so future refactors don't accidentally split the two checks.
    assertEquals(SingleConnectionBrokerRequestHandler.countSuccessfulDataTables(new HashMap<>()), 0);
  }

  @Test
  public void testNullDataTableRecordedAsServerNotResponded() {
    ServerRoutingInstance instance = new ServerRoutingInstance("server-1", 9000, TableType.OFFLINE);
    ServerResponse response = mock(ServerResponse.class);
    when(response.getDataTable()).thenReturn(null);
    when(response.getResponseSize()).thenReturn(0);

    Map<ServerRoutingInstance, ServerResponse> baseResponses = new HashMap<>();
    baseResponses.put(instance, response);
    List<ServerRoutingInstance> serversNotResponded = new ArrayList<>();
    long[] totalResponseSizeHolder = {0L};
    Map<ServerRoutingInstance, DataTable> merged = SingleConnectionBrokerRequestHandler
        .mergeDataTablesByIdentity(baseResponses, new HashMap<>(), serversNotResponded,
            totalResponseSizeHolder);

    assertEquals(merged.size(), 0);
    assertEquals(serversNotResponded.size(), 1);
    assertEquals(serversNotResponded.get(0), instance);
  }
}
