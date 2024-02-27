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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import javax.annotation.Nullable;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedHashMap;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.broker.broker.AllowAllAccessControlFactory;
import org.apache.pinot.broker.queryquota.QueryQuotaManager;
import org.apache.pinot.broker.routing.BrokerRoutingManager;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.core.routing.RoutingTable;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TenantConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.trace.Tracing;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.util.TestUtils;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class BaseBrokerRequestHandlerTest {

  @Test
  public void testUpdateColumnNames() {
    String query = "SELECT database.my_table.column_name_1st, column_name_2nd from database.my_table";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    Map<String, String> columnNameMap =
        ImmutableMap.of("column_name_1st", "column_name_1st", "column_name_2nd", "column_name_2nd");
    BaseBrokerRequestHandler.updateColumnNames("database.my_table", pinotQuery, false, columnNameMap);
    Assert.assertEquals(pinotQuery.getSelectList().size(), 2);
    for (Expression expression : pinotQuery.getSelectList()) {
      String columnName = expression.getIdentifier().getName();
      if (columnName.endsWith("column_name_1st")) {
        Assert.assertEquals(columnName, "column_name_1st");
      } else if (columnName.endsWith("column_name_2nd")) {
        Assert.assertEquals(columnName, "column_name_2nd");
      } else {
        Assert.fail("rewritten column name should be column_name_1st or column_name_1st, but is " + columnName);
      }
    }
  }

  @Test
  public void testGetActualColumnNameCaseSensitive() {
    Map<String, String> columnNameMap = new HashMap<>();
    columnNameMap.put("student_name", "student_name");
    String actualColumnName =
        BaseBrokerRequestHandler.getActualColumnName("mytable", "mytable.student_name", columnNameMap, false);
    Assert.assertEquals(actualColumnName, "student_name");
    Assert.assertEquals(
        BaseBrokerRequestHandler.getActualColumnName("mytable", "default.mytable.student_name", columnNameMap, false),
        "student_name");
    Assert.assertEquals(
        BaseBrokerRequestHandler.getActualColumnName("db1.mytable", "db1.mytable.student_name", columnNameMap, false),
        "student_name");
    Assert.assertEquals(
        BaseBrokerRequestHandler.getActualColumnName("db1.mytable", "mytable.student_name", columnNameMap, false),
        "student_name");
    boolean exceptionThrown = false;
    try {
      BaseBrokerRequestHandler.getActualColumnName("mytable", "mytable2.student_name", columnNameMap, false);
      Assert.fail("should throw exception if column is not known");
    } catch (BadQueryRequestException ex) {
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown, "should throw exception if column is not known");
    exceptionThrown = false;
    try {
      BaseBrokerRequestHandler.getActualColumnName("mytable", "MYTABLE.student_name", columnNameMap, false);
      Assert.fail("should throw exception if case sensitive and table name different");
    } catch (BadQueryRequestException ex) {
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown, "should throw exception if column is not known");
    columnNameMap.put("mytable_student_name", "mytable_student_name");
    String wrongColumnName2 =
        BaseBrokerRequestHandler.getActualColumnName("mytable", "mytable_student_name", columnNameMap, false);
    Assert.assertEquals(wrongColumnName2, "mytable_student_name");

    columnNameMap.put("mytable", "mytable");
    String wrongColumnName3 =
        BaseBrokerRequestHandler.getActualColumnName("mytable", "mytable", columnNameMap, false);
    Assert.assertEquals(wrongColumnName3, "mytable");
  }

  @Test
  public void testGetActualColumnNameCaseInSensitive() {
    Map<String, String> columnNameMap = new HashMap<>();
    columnNameMap.put("student_name", "student_name");
    String actualColumnName =
        BaseBrokerRequestHandler.getActualColumnName("mytable", "MYTABLE.student_name", columnNameMap, true);
    Assert.assertEquals(actualColumnName, "student_name");
    Assert.assertEquals(
        BaseBrokerRequestHandler.getActualColumnName("MYTABLE", "DEFAULT.mytable.student_name", columnNameMap, true),
        "student_name");
    Assert.assertEquals(
        BaseBrokerRequestHandler.getActualColumnName("db1.MYTABLE", "DB1.mytable.student_name", columnNameMap, true),
        "student_name");
    Assert.assertEquals(
        BaseBrokerRequestHandler.getActualColumnName("db1.mytable", "MYTABLE.student_name", columnNameMap, true),
        "student_name");
    boolean exceptionThrown = false;
    try {
      BaseBrokerRequestHandler.getActualColumnName("student", "MYTABLE2.student_name", columnNameMap, true);
      Assert.fail("should throw exception if column is not known");
    } catch (BadQueryRequestException ex) {
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown, "should throw exception if column is not known");
    columnNameMap.put("mytable_student_name", "mytable_student_name");
    String wrongColumnName2 =
        BaseBrokerRequestHandler.getActualColumnName("mytable", "MYTABLE_student_name", columnNameMap, true);
    Assert.assertEquals(wrongColumnName2, "mytable_student_name");

    columnNameMap.put("mytable", "mytable");
    String wrongColumnName3 =
        BaseBrokerRequestHandler.getActualColumnName("MYTABLE", "mytable", columnNameMap, true);
    Assert.assertEquals(wrongColumnName3, "mytable");
  }

  @Test
  public void testCancelQuery()
      throws Exception {
    String tableName = "myTable_OFFLINE";
    // Mock pretty much everything until the query can be submitted.
    TableCache tableCache = mock(TableCache.class);
    TableConfig tableCfg = mock(TableConfig.class);
    when(tableCache.getActualTableName(anyString())).thenReturn(tableName);
    HttpHeaders headers = mock(HttpHeaders.class);
    when(headers.getRequestHeaders()).thenReturn(new MultivaluedHashMap<>());
    when(headers.getHeaderString(anyString())).thenReturn(null);
    TenantConfig tenant = new TenantConfig("tier_BROKER", "tier_SERVER", null);
    when(tableCfg.getTenantConfig()).thenReturn(tenant);
    when(tableCache.getTableConfig(anyString())).thenReturn(tableCfg);
    BrokerRoutingManager routingManager = mock(BrokerRoutingManager.class);
    when(routingManager.routingExists(anyString())).thenReturn(true);
    RoutingTable rt = mock(RoutingTable.class);
    when(rt.getServerInstanceToSegmentsMap()).thenReturn(
        Collections.singletonMap(new ServerInstance(new InstanceConfig("server01_9000")),
            Pair.of(Collections.singletonList("segment01"), Collections.emptyList())));
    when(routingManager.getRoutingTable(any(), Mockito.anyLong())).thenReturn(rt);
    QueryQuotaManager queryQuotaManager = mock(QueryQuotaManager.class);
    when(queryQuotaManager.acquire(anyString())).thenReturn(true);
    CountDownLatch latch = new CountDownLatch(1);
    final long[] testRequestId = {-1};
    PinotConfiguration config =
        new PinotConfiguration(Collections.singletonMap("pinot.broker.enable.query.cancellation", "true"));
    BaseBrokerRequestHandler requestHandler =
        new BaseBrokerRequestHandler(config, "testBrokerId", routingManager, new AllowAllAccessControlFactory(),
            queryQuotaManager, tableCache,
            new BrokerMetrics("", PinotMetricUtils.getPinotMetricsRegistry(), true,
                Collections.emptySet()), null) {
          @Override
          public void start() {
          }

          @Override
          public void shutDown() {
          }

          @Override
          protected BrokerResponseNative processBrokerRequest(long requestId, BrokerRequest originalBrokerRequest,
              BrokerRequest serverBrokerRequest, @Nullable BrokerRequest offlineBrokerRequest,
              @Nullable Map<ServerInstance, Pair<List<String>, List<String>>> offlineRoutingTable,
              @Nullable BrokerRequest realtimeBrokerRequest,
              @Nullable Map<ServerInstance, Pair<List<String>, List<String>>> realtimeRoutingTable, long timeoutMs,
              ServerStats serverStats, RequestContext requestContext)
              throws Exception {
            testRequestId[0] = requestId;
            latch.await();
            return null;
          }
        };
    CompletableFuture.runAsync(() -> {
      try {
        JsonNode request = JsonUtils.stringToJsonNode(
            String.format("{\"sql\":\"select * from %s limit 10\",\"queryOptions\":\"timeoutMs=10000\"}", tableName));
        RequestContext requestStats = Tracing.getTracer().createRequestScope();
        requestHandler.handleRequest(request, null, requestStats, headers);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    TestUtils.waitForCondition((aVoid) -> requestHandler.getRunningServers(testRequestId[0]).size() == 1, 500, 5000,
        "Failed to submit query");
    Map.Entry<Long, String> entry = requestHandler.getRunningQueries().entrySet().iterator().next();
    Assert.assertEquals(entry.getKey().longValue(), testRequestId[0]);
    Assert.assertTrue(entry.getValue().contains("select * from myTable_OFFLINE limit 10"));
    Set<ServerInstance> servers = requestHandler.getRunningServers(testRequestId[0]);
    Assert.assertEquals(servers.size(), 1);
    Assert.assertEquals(servers.iterator().next().getHostname(), "server01");
    Assert.assertEquals(servers.iterator().next().getPort(), 9000);
    Assert.assertEquals(servers.iterator().next().getInstanceId(), "server01_9000");
    Assert.assertEquals(servers.iterator().next().getAdminEndpoint(), "http://server01:8097");
    latch.countDown();
  }
}
