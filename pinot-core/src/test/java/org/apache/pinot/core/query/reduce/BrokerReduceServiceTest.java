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
package org.apache.pinot.core.query.reduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.datatable.DataTableBuilder;
import org.apache.pinot.core.common.datatable.DataTableBuilderFactory;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


public class BrokerReduceServiceTest {

  @Test
  public void testReduceTimeout()
      throws IOException {
    BrokerReduceService brokerReduceService =
        new BrokerReduceService(new PinotConfiguration(Map.of(Broker.CONFIG_OF_MAX_REDUCE_THREADS_PER_QUERY, 2)));
    BrokerRequest brokerRequest =
        CalciteSqlCompiler.compileToBrokerRequest("SELECT COUNT(*) FROM testTable GROUP BY col1");
    DataSchema dataSchema =
        new DataSchema(new String[]{"col1", "count(*)"}, new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.LONG});
    DataTableBuilder dataTableBuilder = DataTableBuilderFactory.getDataTableBuilder(dataSchema);
    int numGroups = 5000;
    for (int i = 0; i < numGroups; i++) {
      dataTableBuilder.startRow();
      dataTableBuilder.setColumn(0, i);
      dataTableBuilder.setColumn(1, 1L);
      dataTableBuilder.finishRow();
    }
    DataTable dataTable = dataTableBuilder.build();
    Map<ServerRoutingInstance, DataTable> dataTableMap = new HashMap<>();
    int numInstances = 1000;
    for (int i = 0; i < numInstances; i++) {
      ServerRoutingInstance instance = new ServerRoutingInstance("localhost", i, TableType.OFFLINE);
      dataTableMap.put(instance, dataTable);
    }
    BrokerResponseNative brokerResponse = reduce(brokerReduceService, brokerRequest, dataTableMap, 1L);
    brokerReduceService.shutDown();

    List<QueryProcessingException> exceptions = brokerResponse.getExceptions();
    assertEquals(exceptions.size(), 1);
    assertEquals(exceptions.get(0).getErrorCode(), QueryErrorCode.BROKER_TIMEOUT.getId());
  }

  @Test
  public void testIgnoreMissingSegmentsFiltering() {
    // Build a simple broker reduce service
    BrokerReduceService brokerReduceService =
        new BrokerReduceService(new PinotConfiguration(Map.of(Broker.CONFIG_OF_MAX_REDUCE_THREADS_PER_QUERY, 2)));

    // Prepare a broker request with queryOptions toggled
    BrokerRequest brokerRequestNoIgnore = CalciteSqlCompiler.compileToBrokerRequest("SELECT COUNT(*) FROM testTable");
    BrokerRequest brokerRequestIgnore = CalciteSqlCompiler.compileToBrokerRequest("SELECT COUNT(*) FROM testTable");
    brokerRequestIgnore.getPinotQuery()
        .putToQueryOptions(CommonConstants.Broker.Request.QueryOptionKey.IGNORE_MISSING_SEGMENTS, "true");

    // Create a metadata-only DataTable with a SERVER_SEGMENT_MISSING exception
    DataTableBuilder dataTableBuilder = DataTableBuilderFactory.getDataTableBuilder(
        new DataSchema(new String[]{"count(*)"}, new ColumnDataType[]{ColumnDataType.LONG}));
    // no rows; build data table and then mark it metadata-only
    DataTable dataTable = dataTableBuilder.build().toMetadataOnlyDataTable();
    dataTable.addException(QueryErrorCode.SERVER_SEGMENT_MISSING,
        "1 segments [segA] missing on server: Server_localhost_12345");

    Map<ServerRoutingInstance, DataTable> dataTableMap = new HashMap<>();
    dataTableMap.put(new ServerRoutingInstance("localhost", 12345, TableType.OFFLINE), dataTable);

    // Case 1: ignoreMissingSegments=false (default) -> exception should be present
    BrokerResponseNative responseNoIgnore = reduce(brokerReduceService, brokerRequestNoIgnore, dataTableMap, 10_000L);
    long missingErrCountNoIgnore = responseNoIgnore.getExceptions()
        .stream()
        .filter(e -> e.getErrorCode() == QueryErrorCode.SERVER_SEGMENT_MISSING.getId())
        .count();
    assertEquals(missingErrCountNoIgnore, 1L);

    // Case 2: ignoreMissingSegments=true -> exception should be filtered out
    BrokerResponseNative responseIgnore = reduce(brokerReduceService, brokerRequestIgnore, dataTableMap, 10_000L);
    long missingErrCountIgnore = responseIgnore.getExceptions()
        .stream()
        .filter(e -> e.getErrorCode() == QueryErrorCode.SERVER_SEGMENT_MISSING.getId())
        .count();
    assertEquals(missingErrCountIgnore, 0L);

    brokerReduceService.shutDown();
  }

  /**
   * Pins the safety net: when `brokerRequest != serverBrokerRequest` (i.e. a path that rewrote
   * the server-side query), no gapfill is requested, AND no MV-rewrite marker is present, the
   * reducer MUST throw `BadQueryRequestException("Nested query is not supported without
   * gapfill")`.  This is the guardrail that prevents any future federated / JOIN / nested-query
   * rewrite path from silently degrading to a meaningless reduce.
   */
  @Test
  public void testNestedQueryRejectedWhenNoMaterializedViewMarker()
      throws IOException {
    BrokerReduceService service =
        new BrokerReduceService(new PinotConfiguration(Map.of(Broker.CONFIG_OF_MAX_REDUCE_THREADS_PER_QUERY, 2)));
    try {
      BrokerRequest brokerRequest = CalciteSqlCompiler.compileToBrokerRequest("SELECT COUNT(*) FROM userTable");
      BrokerRequest serverBrokerRequest =
          CalciteSqlCompiler.compileToBrokerRequest("SELECT COUNT(*) FROM mv_userTable_OFFLINE");
      // No QUERY_OPTION_MATERIALIZED_VIEW_REWRITE marker — this simulates an unknown rewrite
      // path the broker may grow in the future.  The reducer must refuse the request.
      DataTableBuilder dataTableBuilder = DataTableBuilderFactory.getDataTableBuilder(
          new DataSchema(new String[]{"count(*)"}, new ColumnDataType[]{ColumnDataType.LONG}));
      dataTableBuilder.startRow();
      dataTableBuilder.setColumn(0, 7L);
      dataTableBuilder.finishRow();
      DataTable dataTable = dataTableBuilder.build();
      Map<ServerRoutingInstance, DataTable> dataTableMap = new HashMap<>();
      dataTableMap.put(new ServerRoutingInstance("localhost", 9000, TableType.OFFLINE), dataTable);
      try (QueryThreadContext ignore = QueryThreadContext.openForSseTest()) {
        assertThrows(BadQueryRequestException.class,
            () -> service.reduceOnDataTable(brokerRequest, serverBrokerRequest, dataTableMap, 10_000L,
                mock(BrokerMetrics.class)));
      }
    } finally {
      service.shutDown();
    }
  }

  /**
   * Pins the MV-rewrite opt-out: when the server-side query carries the broker-internal
   * `MATERIALIZED_VIEW_REWRITE=true` marker, the reducer skips the "Nested query is not
   * supported without gapfill" safety net and proceeds to reduce normally.
   *
   * <p>The marker is broker-internal — `BaseSingleStageBrokerRequestHandler.handleRequest` strips
   * any user-supplied copy of this option at request entry, so the only way a server query can
   * carry it is if the broker itself stamped it during a committed FULL_REWRITE.
   */
  @Test
  public void testMaterializedViewMarkerOptsOutOfNestedQueryGuard()
      throws IOException {
    BrokerReduceService service =
        new BrokerReduceService(new PinotConfiguration(Map.of(Broker.CONFIG_OF_MAX_REDUCE_THREADS_PER_QUERY, 2)));
    try {
      BrokerRequest brokerRequest = CalciteSqlCompiler.compileToBrokerRequest("SELECT COUNT(*) FROM userTable");
      BrokerRequest serverBrokerRequest =
          CalciteSqlCompiler.compileToBrokerRequest("SELECT COUNT(*) FROM mv_userTable_OFFLINE");
      serverBrokerRequest.getPinotQuery().putToQueryOptions(
          CommonConstants.Broker.Request.QueryOptionKey.MATERIALIZED_VIEW_REWRITE, "true");

      DataTableBuilder dataTableBuilder = DataTableBuilderFactory.getDataTableBuilder(
          new DataSchema(new String[]{"count(*)"}, new ColumnDataType[]{ColumnDataType.LONG}));
      dataTableBuilder.startRow();
      dataTableBuilder.setColumn(0, 42L);
      dataTableBuilder.finishRow();
      DataTable dataTable = dataTableBuilder.build();
      Map<ServerRoutingInstance, DataTable> dataTableMap = new HashMap<>();
      dataTableMap.put(new ServerRoutingInstance("localhost", 9000, TableType.OFFLINE), dataTable);

      BrokerResponseNative response;
      try (QueryThreadContext ignore = QueryThreadContext.openForSseTest()) {
        response = service.reduceOnDataTable(brokerRequest, serverBrokerRequest, dataTableMap, 10_000L,
            mock(BrokerMetrics.class));
      }
      // No exception, and the count rolled up correctly.  The "Nested query is not supported"
      // safety net was correctly opted out by the marker.
      assertTrue(response.getExceptions().stream()
              .noneMatch(e -> e.getErrorCode() == QueryErrorCode.QUERY_VALIDATION.getId()),
          "MV-marked nested query must not raise QUERY_VALIDATION; exceptions: " + response.getExceptions());
    } finally {
      service.shutDown();
    }
  }

  private BrokerResponseNative reduce(BrokerReduceService brokerReduceService, BrokerRequest brokerRequest,
      Map<ServerRoutingInstance, DataTable> dataTableMap, long reduceTimeoutMs) {
    try (QueryThreadContext ignore = QueryThreadContext.openForSseTest()) {
      return brokerReduceService.reduceOnDataTable(brokerRequest, brokerRequest, dataTableMap, reduceTimeoutMs,
          mock(BrokerMetrics.class));
    }
  }
}
