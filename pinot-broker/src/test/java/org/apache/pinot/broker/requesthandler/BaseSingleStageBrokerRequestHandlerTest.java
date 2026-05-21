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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.broker.api.AccessControl;
import org.apache.pinot.broker.broker.AccessControlFactory;
import org.apache.pinot.broker.broker.AllowAllAccessControlFactory;
import org.apache.pinot.broker.queryquota.QueryQuotaManager;
import org.apache.pinot.broker.routing.manager.BrokerRoutingManager;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.core.routing.RoutingTable;
import org.apache.pinot.core.routing.SegmentsToQuery;
import org.apache.pinot.core.routing.TableRouteInfo;
import org.apache.pinot.core.routing.timeboundary.TimeBoundaryInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.materializedview.handler.DefaultMaterializedViewHandler;
import org.apache.pinot.materializedview.handler.MaterializedViewHandler;
import org.apache.pinot.materializedview.rewrite.ExecutionMode;
import org.apache.pinot.materializedview.rewrite.MatchType;
import org.apache.pinot.materializedview.rewrite.MaterializedViewQueryRewriteEngine;
import org.apache.pinot.materializedview.rewrite.MaterializedViewRewritePlan;
import org.apache.pinot.materializedview.rewrite.MaterializedViewRewriteResult;
import org.apache.pinot.spi.accounting.ThreadAccountantUtils;
import org.apache.pinot.spi.auth.TableAuthorizationResult;
import org.apache.pinot.spi.auth.TableRowColAccessResultImpl;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TenantConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.eventlistener.query.BrokerQueryEventListenerFactory;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.spi.trace.LoggerConstants;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.apache.pinot.spi.utils.CommonConstants.Query.Range;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.FilterKind;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.util.TestUtils;
import org.mockito.Mockito;
import org.slf4j.MDC;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class BaseSingleStageBrokerRequestHandlerTest {

  @AfterMethod
  public void cleanupMdc() {
    MDC.clear();
  }

  @Test
  public void testUpdateColumnNames() {
    String query = "SELECT database.my_table.column_name_1st, column_name_2nd from database.my_table";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    Map<String, String> columnNameMap =
        Map.of("column_name_1st", "column_name_1st", "column_name_2nd", "column_name_2nd");
    BaseSingleStageBrokerRequestHandler.updateColumnNames("database.my_table", pinotQuery, false, columnNameMap);
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
  public void testOnQueryCompletionHookReceivesBrokerResponse() {
    // Verify that the overridable onQueryCompletion(RequestContext, BrokerResponse) hook is invoked
    // and receives the BrokerResponse that handleRequest() produced.
    AtomicReference<BrokerResponse> capturedResponse = new AtomicReference<>();

    PinotConfiguration config = new PinotConfiguration();
    BrokerQueryEventListenerFactory.init(config);
    BrokerMetrics.register(mock(BrokerMetrics.class));
    QueryQuotaManager queryQuotaManager = mock(QueryQuotaManager.class);
    when(queryQuotaManager.acquire(anyString())).thenReturn(true);
    when(queryQuotaManager.acquireDatabase(anyString())).thenReturn(true);
    when(queryQuotaManager.acquireApplication(anyString())).thenReturn(true);
    TableCache tableCache = mock(TableCache.class);

    BaseSingleStageBrokerRequestHandler handler =
        new BaseSingleStageBrokerRequestHandler(config, "testBrokerId", new BrokerRequestIdGenerator(),
            mock(org.apache.pinot.core.routing.RoutingManager.class), new AllowAllAccessControlFactory(),
            queryQuotaManager, tableCache, ThreadAccountantUtils.getNoOpAccountant(), null, null) {
          @Override
          public void start() {
          }

          @Override
          public void shutDown() {
          }

          @Override
          protected BrokerResponseNative processBrokerRequest(long requestId, BrokerRequest originalBrokerRequest,
              BrokerRequest serverBrokerRequest, TableRouteInfo route, long timeoutMs, ServerStats serverStats,
              RequestContext requestContext) {
            return new BrokerResponseNative();
          }

          @Override
          protected BrokerResponseNative processMaterializedViewSplitBrokerRequest(long requestId,
              long materializedViewRequestId, BrokerRequest originalBrokerRequest, TableRouteInfo baseRoute,
              TableRouteInfo materializedViewRoute, long timeoutMs, ServerStats serverStats,
              RequestContext requestContext) {
            return new BrokerResponseNative();
          }

          @Override
          protected void onQueryCompletion(RequestContext requestContext, BrokerResponse brokerResponse) {
            capturedResponse.set(brokerResponse);
          }
        };

    try {
      handler.handleRequest("SELECT 1");
    } catch (Exception ignored) {
      // routing may fail — we only care that the hook was called with a non-null response
    }
    Assert.assertNotNull(capturedResponse.get(),
        "onQueryCompletion hook must be called with the BrokerResponse from handleRequest");
  }

  @Test
  public void testLegacyConstructorIsAvailableForCustomSubclasses() {
    PinotConfiguration config = new PinotConfiguration();
    QueryQuotaManager queryQuotaManager = mock(QueryQuotaManager.class);
    TableCache tableCache = mock(TableCache.class);

    BaseSingleStageBrokerRequestHandler handler =
        new BaseSingleStageBrokerRequestHandler(config, "testBrokerId", new BrokerRequestIdGenerator(),
            mock(org.apache.pinot.core.routing.RoutingManager.class), new AllowAllAccessControlFactory(),
            queryQuotaManager, tableCache, ThreadAccountantUtils.getNoOpAccountant(), null, null) {
          @Override
          public void start() {
          }

          @Override
          public void shutDown() {
          }

          @Override
          protected BrokerResponseNative processBrokerRequest(long requestId, BrokerRequest originalBrokerRequest,
              BrokerRequest serverBrokerRequest, TableRouteInfo route, long timeoutMs, ServerStats serverStats,
              RequestContext requestContext) {
            return new BrokerResponseNative();
          }

          @Override
          protected BrokerResponseNative processMaterializedViewSplitBrokerRequest(long requestId,
              long materializedViewRequestId, BrokerRequest originalBrokerRequest, TableRouteInfo baseRoute,
              TableRouteInfo materializedViewRoute, long timeoutMs, ServerStats serverStats,
              RequestContext requestContext) {
            return new BrokerResponseNative();
          }
        };

    Assert.assertNotNull(handler);
  }

  @Test
  public void testGetActualColumnNameCaseSensitive() {
    Map<String, String> columnNameMap = new HashMap<>();
    columnNameMap.put("student_name", "student_name");
    String actualColumnName =
        BaseSingleStageBrokerRequestHandler.getActualColumnName("mytable", "mytable.student_name", columnNameMap,
            false);
    Assert.assertEquals(actualColumnName, "student_name");
    Assert.assertEquals(
        BaseSingleStageBrokerRequestHandler.getActualColumnName("db1.mytable", "db1.mytable.student_name",
            columnNameMap, false), "student_name");
    Assert.assertEquals(
        BaseSingleStageBrokerRequestHandler.getActualColumnName("db1.mytable", "mytable.student_name", columnNameMap,
            false), "student_name");
    boolean exceptionThrown = false;
    try {
      BaseSingleStageBrokerRequestHandler.getActualColumnName("mytable", "mytable2.student_name", columnNameMap, false);
      Assert.fail("should throw exception if column is not known");
    } catch (BadQueryRequestException ex) {
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown, "should throw exception if column is not known");
    exceptionThrown = false;
    try {
      BaseSingleStageBrokerRequestHandler.getActualColumnName("mytable", "MYTABLE.student_name", columnNameMap, false);
      Assert.fail("should throw exception if case sensitive and table name different");
    } catch (BadQueryRequestException ex) {
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown, "should throw exception if column is not known");
    columnNameMap.put("mytable_student_name", "mytable_student_name");
    String wrongColumnName2 =
        BaseSingleStageBrokerRequestHandler.getActualColumnName("mytable", "mytable_student_name", columnNameMap,
            false);
    Assert.assertEquals(wrongColumnName2, "mytable_student_name");

    columnNameMap.put("mytable", "mytable");
    String wrongColumnName3 =
        BaseSingleStageBrokerRequestHandler.getActualColumnName("mytable", "mytable", columnNameMap, false);
    Assert.assertEquals(wrongColumnName3, "mytable");
  }

  @Test
  public void testGetActualColumnNameCaseInSensitive() {
    Map<String, String> columnNameMap = new HashMap<>();
    columnNameMap.put("student_name", "student_name");
    String actualColumnName =
        BaseSingleStageBrokerRequestHandler.getActualColumnName("mytable", "MYTABLE.student_name", columnNameMap, true);
    Assert.assertEquals(actualColumnName, "student_name");
    Assert.assertEquals(
        BaseSingleStageBrokerRequestHandler.getActualColumnName("db1.MYTABLE", "DB1.mytable.student_name",
            columnNameMap, true), "student_name");
    Assert.assertEquals(
        BaseSingleStageBrokerRequestHandler.getActualColumnName("db1.mytable", "MYTABLE.student_name", columnNameMap,
            true), "student_name");
    boolean exceptionThrown = false;
    try {
      BaseSingleStageBrokerRequestHandler.getActualColumnName("student", "MYTABLE2.student_name", columnNameMap, true);
      Assert.fail("should throw exception if column is not known");
    } catch (BadQueryRequestException ex) {
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown, "should throw exception if column is not known");
    columnNameMap.put("mytable_student_name", "mytable_student_name");
    String wrongColumnName2 =
        BaseSingleStageBrokerRequestHandler.getActualColumnName("mytable", "MYTABLE_student_name", columnNameMap, true);
    Assert.assertEquals(wrongColumnName2, "mytable_student_name");

    columnNameMap.put("mytable", "mytable");
    String wrongColumnName3 =
        BaseSingleStageBrokerRequestHandler.getActualColumnName("MYTABLE", "mytable", columnNameMap, true);
    Assert.assertEquals(wrongColumnName3, "mytable");
  }

  @Test
  public void testCancelQuery() {
    String tableName = "myTable_OFFLINE";
    // Mock pretty much everything until the query can be submitted.
    TableCache tableCache = mock(TableCache.class);
    TableConfig tableCfg = mock(TableConfig.class);
    when(tableCache.getActualTableName(anyString())).thenReturn(tableName);
    TenantConfig tenant = new TenantConfig("tier_BROKER", "tier_SERVER", null);
    when(tableCfg.getTenantConfig()).thenReturn(tenant);
    when(tableCache.getTableConfig(tableName)).thenReturn(tableCfg);
    BrokerRoutingManager routingManager = mock(BrokerRoutingManager.class);
    when(routingManager.routingExists(tableName)).thenReturn(true);
    when(routingManager.getQueryTimeoutMs(tableName)).thenReturn(10000L);
    RoutingTable rt = mock(RoutingTable.class);
    when(rt.getServerInstanceToSegmentsMap()).thenReturn(Map.of(new ServerInstance(new InstanceConfig("server01_9000")),
        new SegmentsToQuery(List.of("segment01"), List.of())));
    when(routingManager.getRoutingTable(any(), Mockito.anyLong())).thenReturn(rt);
    QueryQuotaManager queryQuotaManager = mock(QueryQuotaManager.class);
    when(queryQuotaManager.acquire(anyString())).thenReturn(true);
    when(queryQuotaManager.acquireDatabase(anyString())).thenReturn(true);
    when(queryQuotaManager.acquireApplication(anyString())).thenReturn(true);
    CountDownLatch latch = new CountDownLatch(1);
    long[] testRequestId = {-1};
    BrokerMetrics.register(mock(BrokerMetrics.class));
    PinotConfiguration config = new PinotConfiguration();
    BrokerQueryEventListenerFactory.init(config);
    BaseSingleStageBrokerRequestHandler requestHandler =
        new BaseSingleStageBrokerRequestHandler(config, "testBrokerId", new BrokerRequestIdGenerator(), routingManager,
            new AllowAllAccessControlFactory(), queryQuotaManager, tableCache,
            ThreadAccountantUtils.getNoOpAccountant(), null, null) {
          @Override
          public void start() {
          }

          @Override
          public void shutDown() {
          }

          @Override
          protected BrokerResponseNative processBrokerRequest(long requestId, BrokerRequest originalBrokerRequest,
              BrokerRequest serverBrokerRequest, TableRouteInfo route, long timeoutMs, ServerStats serverStats,
              RequestContext requestContext)
              throws Exception {
            testRequestId[0] = requestId;
            latch.await();
            return null;
          }

          @Override
          protected BrokerResponseNative processMaterializedViewSplitBrokerRequest(long requestId,
              long materializedViewRequestId, BrokerRequest originalBrokerRequest, TableRouteInfo baseRoute,
              TableRouteInfo materializedViewRoute, long timeoutMs, ServerStats serverStats,
              RequestContext requestContext)
              throws Exception {
            throw new UnsupportedOperationException("Not implemented in test");
          }
        };
    CompletableFuture.runAsync(() -> {
      try {
        requestHandler.handleRequest(String.format("select * from %s limit 10", tableName));
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

  @Test
  public void testAddRoutingPolicyInErrMsg() {
    Assert.assertEquals(BaseSingleStageBrokerRequestHandler.addRoutingPolicyInErrMsg("error1", null, null), "error1");
    Assert.assertEquals(BaseSingleStageBrokerRequestHandler.addRoutingPolicyInErrMsg("error1", "rt_rp", null),
        "error1, with routing policy: rt_rp [realtime]");
    Assert.assertEquals(BaseSingleStageBrokerRequestHandler.addRoutingPolicyInErrMsg("error1", null, "off_rp"),
        "error1, with routing policy: off_rp [offline]");
    Assert.assertEquals(BaseSingleStageBrokerRequestHandler.addRoutingPolicyInErrMsg("error1", "rt_rp", "off_rp"),
        "error1, with routing policy: rt_rp [realtime], off_rp [offline]");
  }

  @Test
  public void testQueryHashRegisteredInMdc() {
    String queryHash = "test_hash_abc123";
    LoggerConstants.QUERY_HASH_KEY.registerInMdc(queryHash);
    String mdcValue = MDC.get(LoggerConstants.QUERY_HASH_KEY.getKey());
    Assert.assertNotNull(mdcValue, "QueryHash should be registered in MDC");
    Assert.assertEquals(mdcValue, queryHash, "MDC should contain the correct queryHash");
  }

  @Test
  public void testQueryHashNotRegisteredWhenNull() {
    String mdcValue = MDC.get(LoggerConstants.QUERY_HASH_KEY.getKey());
    Assert.assertNull(mdcValue, "Null queryHash should not be registered in MDC");
  }

  @Test
  public void testQueryHashAddedToQueryOptions() {
    String query = "SELECT * FROM myTable WHERE col = 100";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    String queryHash = "generated_hash_xyz";
    pinotQuery.putToQueryOptions(
        CommonConstants.Broker.Request.QueryOptionKey.QUERY_HASH,
        queryHash);
    Assert.assertTrue(pinotQuery.getQueryOptions().containsKey(
        CommonConstants.Broker.Request.QueryOptionKey.QUERY_HASH),
        "QueryHash should be added to queryOptions");
    Assert.assertEquals(
        pinotQuery.getQueryOptions().get(CommonConstants.Broker.Request.QueryOptionKey.QUERY_HASH),
        queryHash,
        "QueryOptions should contain the correct queryHash value");
  }

  private BaseSingleStageBrokerRequestHandler createHybridHandlerWithTimeBoundary(
      AtomicReference<TableRouteInfo> capturedRouteInfo) {
    String offlineTableName = "myTable_OFFLINE";
    String realtimeTableName = "myTable_REALTIME";

    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("myTable")
        .addDateTime("created_15min", DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();

    TableCache tableCache = mock(TableCache.class);
    when(tableCache.getActualTableName("myTable")).thenReturn("myTable");
    when(tableCache.getSchema("myTable")).thenReturn(schema);
    when(tableCache.getColumnNameMap("myTable")).thenReturn(Map.of("created_15min", "created_15min"));

    TableConfig offlineTableCfg = mock(TableConfig.class);
    TableConfig realtimeTableCfg = mock(TableConfig.class);
    TenantConfig tenant = new TenantConfig("tier_BROKER", "tier_SERVER", null);
    when(offlineTableCfg.getTenantConfig()).thenReturn(tenant);
    when(realtimeTableCfg.getTenantConfig()).thenReturn(tenant);
    when(tableCache.getTableConfig(offlineTableName)).thenReturn(offlineTableCfg);
    when(tableCache.getTableConfig(realtimeTableName)).thenReturn(realtimeTableCfg);

    BrokerRoutingManager routingManager = mock(BrokerRoutingManager.class);
    when(routingManager.routingExists(offlineTableName)).thenReturn(true);
    when(routingManager.routingExists(realtimeTableName)).thenReturn(true);
    when(routingManager.getQueryTimeoutMs(anyString())).thenReturn(10000L);
    TimeBoundaryInfo timeBoundaryInfo = new TimeBoundaryInfo("created_15min", "1772109900000");
    when(routingManager.getTimeBoundaryInfo(offlineTableName)).thenReturn(timeBoundaryInfo);

    RoutingTable rt = mock(RoutingTable.class);
    when(rt.getServerInstanceToSegmentsMap()).thenReturn(
        Map.of(new ServerInstance(new InstanceConfig("server01_9000")),
            new SegmentsToQuery(List.of("segment01"), List.of())));
    when(routingManager.getRoutingTable(any(), Mockito.anyLong())).thenReturn(rt);

    QueryQuotaManager queryQuotaManager = mock(QueryQuotaManager.class);
    when(queryQuotaManager.acquire(anyString())).thenReturn(true);
    when(queryQuotaManager.acquireDatabase(anyString())).thenReturn(true);
    when(queryQuotaManager.acquireApplication(anyString())).thenReturn(true);

    BrokerMetrics.register(mock(BrokerMetrics.class));
    PinotConfiguration config = new PinotConfiguration();
    BrokerQueryEventListenerFactory.init(config);

    return new BaseSingleStageBrokerRequestHandler(config, "testBrokerId", new BrokerRequestIdGenerator(),
        routingManager, new AllowAllAccessControlFactory(), queryQuotaManager, tableCache,
        ThreadAccountantUtils.getNoOpAccountant(), null, null) {
      @Override
      public void start() {
      }

      @Override
      public void shutDown() {
      }

      @Override
      protected BrokerResponseNative processBrokerRequest(long requestId, BrokerRequest originalBrokerRequest,
          BrokerRequest serverBrokerRequest, TableRouteInfo route, long timeoutMs, ServerStats serverStats,
          RequestContext requestContext) {
        capturedRouteInfo.set(route);
        return BrokerResponseNative.empty();
      }

      @Override
      protected BrokerResponseNative processMaterializedViewSplitBrokerRequest(long requestId,
          long materializedViewRequestId, BrokerRequest originalBrokerRequest, TableRouteInfo baseRoute,
          TableRouteInfo materializedViewRoute, long timeoutMs, ServerStats serverStats, RequestContext requestContext)
          throws Exception {
        throw new UnsupportedOperationException("Not implemented in test");
      }
    };
  }

  private static void assertRangeFilter(BrokerRequest brokerRequest, String column, String expectedRange,
      String label) {
    Assert.assertNotNull(brokerRequest, label + ": broker request should exist");
    Expression filter = brokerRequest.getPinotQuery().getFilterExpression();
    // Walk past any AND nodes wrapping non-time-column predicates to find the RANGE on our column
    Function filterFunc = filter.getFunctionCall();
    if (FilterKind.AND.name().equals(filterFunc.getOperator())) {
      Expression rangeExpr = null;
      for (Expression operand : filterFunc.getOperands()) {
        Function fn = operand.getFunctionCall();
        if (FilterKind.RANGE.name().equals(fn.getOperator())
            && column.equals(fn.getOperands().get(0).getIdentifier().getName())) {
          rangeExpr = operand;
          break;
        }
      }
      Assert.assertNotNull(rangeExpr, label + ": expected a RANGE filter on " + column + " within AND");
      filterFunc = rangeExpr.getFunctionCall();
    }
    Assert.assertEquals(filterFunc.getOperator(), FilterKind.RANGE.name(),
        label + ": filter should be a RANGE");
    List<Expression> operands = filterFunc.getOperands();
    Assert.assertEquals(operands.get(0).getIdentifier().getName(), column);
    Assert.assertEquals(operands.get(1).getLiteral().getStringValue(), expectedRange);
  }

  @Test
  public void testTimeBoundaryMergesWithBetween()
      throws Exception {
    AtomicReference<TableRouteInfo> capturedRouteInfo = new AtomicReference<>();
    BaseSingleStageBrokerRequestHandler handler = createHybridHandlerWithTimeBoundary(capturedRouteInfo);

    handler.handleRequest("SELECT * FROM myTable "
        + "WHERE created_15min BETWEEN 1772106300000 AND 1772113500000 LIMIT 10");

    TableRouteInfo routeInfo = capturedRouteInfo.get();
    Assert.assertNotNull(routeInfo, "processBrokerRequest should have been called");
    assertRangeFilter(routeInfo.getOfflineBrokerRequest(), "created_15min",
        "[1772106300000" + Range.DELIMITER + "1772109900000]", "Offline BETWEEN");
    assertRangeFilter(routeInfo.getRealtimeBrokerRequest(), "created_15min",
        "(1772109900000" + Range.DELIMITER + "1772113500000]", "Realtime BETWEEN");
  }

  @Test
  public void testTimeBoundaryMergesWithExplicitRange()
      throws Exception {
    AtomicReference<TableRouteInfo> capturedRouteInfo = new AtomicReference<>();
    BaseSingleStageBrokerRequestHandler handler = createHybridHandlerWithTimeBoundary(capturedRouteInfo);

    handler.handleRequest("SELECT * FROM myTable "
        + "WHERE created_15min > 1772106300000 AND created_15min < 1772113500000 LIMIT 10");

    TableRouteInfo routeInfo = capturedRouteInfo.get();
    Assert.assertNotNull(routeInfo, "processBrokerRequest should have been called");
    assertRangeFilter(routeInfo.getOfflineBrokerRequest(), "created_15min",
        "(1772106300000" + Range.DELIMITER + "1772109900000]", "Offline explicit range");
    assertRangeFilter(routeInfo.getRealtimeBrokerRequest(), "created_15min",
        "(1772109900000" + Range.DELIMITER + "1772113500000)", "Realtime explicit range");
  }

  @Test
  public void testTimeBoundaryMergesWithOneSidedRange()
      throws Exception {
    AtomicReference<TableRouteInfo> capturedRouteInfo = new AtomicReference<>();
    BaseSingleStageBrokerRequestHandler handler = createHybridHandlerWithTimeBoundary(capturedRouteInfo);

    // Query has only a lower bound — time boundary supplies the complementary bound for each side
    handler.handleRequest("SELECT * FROM myTable "
        + "WHERE created_15min > 1772106300000 LIMIT 10");

    TableRouteInfo routeInfo = capturedRouteInfo.get();
    Assert.assertNotNull(routeInfo, "processBrokerRequest should have been called");
    // Offline: query's > 1772106300000 merged with time boundary's <= 1772109900000
    assertRangeFilter(routeInfo.getOfflineBrokerRequest(), "created_15min",
        "(1772106300000" + Range.DELIMITER + "1772109900000]", "Offline one-sided");
    // Realtime: query's > 1772106300000 merged with time boundary's > 1772109900000
    // Tighter bound wins: > 1772109900000 with no upper bound
    assertRangeFilter(routeInfo.getRealtimeBrokerRequest(), "created_15min",
        "(1772109900000" + Range.DELIMITER + "*)", "Realtime one-sided");
  }

  @Test
  public void testTimeBoundaryMergesWithMixedFilters()
      throws Exception {
    AtomicReference<TableRouteInfo> capturedRouteInfo = new AtomicReference<>();
    BaseSingleStageBrokerRequestHandler handler = createHybridHandlerWithTimeBoundary(capturedRouteInfo);

    handler.handleRequest("SELECT * FROM myTable "
        + "WHERE created_15min BETWEEN 1772106300000 AND 1772113500000 "
        + "AND created_15min > 1772109900000 LIMIT 10");

    TableRouteInfo routeInfo = capturedRouteInfo.get();
    Assert.assertNotNull(routeInfo, "processBrokerRequest should have been called");
    // Offline merges to (1772109900000, 1772109900000] — an empty range (exclusive lower = inclusive upper),
    // but the handler doesn't prune it as always-false; the server handles that at execution time.
    assertRangeFilter(routeInfo.getOfflineBrokerRequest(), "created_15min",
        "(1772109900000" + Range.DELIMITER + "1772109900000]", "Offline mixed");
    // Realtime merges to (1772109900000, 1772113500000].
    assertRangeFilter(routeInfo.getRealtimeBrokerRequest(), "created_15min",
        "(1772109900000" + Range.DELIMITER + "1772113500000]", "Realtime mixed");
  }

  /**
   * Bug: FULL_REWRITE overwrites tableName to the materialized view table name, so
   * _queryQuotaManager.acquire(tableName) charges quota against the MV
   * instead of the base table. A throttled base table is effectively
   * bypassed when its quota allows no traffic but the MV has no quota entry.
   *
   * <p>Before fix: acquire("baseTable_OFFLINE") is never called; the MV
   * table passes because the mock only denies the base table name.
   * <p>After fix: the base table name is used for quota accounting and the
   * request is correctly rate-limited.
   */
  @Test
  public void testMaterializedViewFullRewriteQuotaAccountedAgainstBaseTable()
      throws Exception {
    String baseOfflineTable = "baseTable_OFFLINE";
    String materializedViewOfflineTable = "mv_baseTable_OFFLINE";
    String baseRawTable = "baseTable";
    String materializedViewRawTable = "mv_baseTable";

    // materialized view query: exact same columns but issued against the MV
    String userSql = "SELECT ts, SUM(revenue) FROM baseTable GROUP BY ts LIMIT 100 "
        + "";
    PinotQuery materializedViewQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT ts, SUM(revenue) FROM mv_baseTable_OFFLINE GROUP BY ts LIMIT 100");

    MaterializedViewRewritePlan plan = new MaterializedViewRewritePlan(
        materializedViewOfflineTable, MatchType.EXACT, ExecutionMode.FULL_REWRITE, materializedViewQuery, 1.0);
    MaterializedViewRewriteResult viewResult =
        new MaterializedViewRewriteResult(List.of(materializedViewOfflineTable), plan);

    AtomicReference<PinotQuery> querySeenByMaterializedViewRewrite = new AtomicReference<>();
    MaterializedViewQueryRewriteEngine materializedViewEngine = mock(MaterializedViewQueryRewriteEngine.class);
    when(materializedViewEngine.tryRewrite(any(PinotQuery.class), anyString())).thenAnswer(invocation -> {
      querySeenByMaterializedViewRewrite.set(((PinotQuery) invocation.getArgument(0)).deepCopy());
      return viewResult;
    });
    MaterializedViewHandler materializedViewHandler = new DefaultMaterializedViewHandler(materializedViewEngine);

    Schema baseSchema = new Schema.SchemaBuilder()
        .setSchemaName(baseRawTable)
        .addSingleValueDimension("ts", DataType.STRING)
        .addMetric("revenue", DataType.DOUBLE)
        .build();
    Schema materializedViewSchema = new Schema.SchemaBuilder()
        .setSchemaName(materializedViewRawTable)
        .addSingleValueDimension("ts", DataType.STRING)
        .addMetric("revenue", DataType.DOUBLE)
        .build();

    TableCache tableCache = mock(TableCache.class);
    when(tableCache.getActualTableName(baseRawTable)).thenReturn(baseRawTable);
    when(tableCache.getSchema(baseRawTable)).thenReturn(baseSchema);
    when(tableCache.getSchema(materializedViewRawTable)).thenReturn(materializedViewSchema);
    when(tableCache.getColumnNameMap(anyString())).thenReturn(Map.of("ts", "ts", "revenue", "revenue"));
    TableConfig tableCfg = mock(TableConfig.class);
    TenantConfig tenant = new TenantConfig("t_BROKER", "t_SERVER", null);
    when(tableCfg.getTenantConfig()).thenReturn(tenant);
    when(tableCache.getTableConfig(baseOfflineTable)).thenReturn(tableCfg);
    when(tableCache.getTableConfig(materializedViewOfflineTable)).thenReturn(tableCfg);

    BrokerRoutingManager routingManager = mock(BrokerRoutingManager.class);
    when(routingManager.routingExists(baseOfflineTable)).thenReturn(true);
    when(routingManager.routingExists(materializedViewOfflineTable)).thenReturn(true);
    when(routingManager.getQueryTimeoutMs(anyString())).thenReturn(10000L);
    RoutingTable rt = mock(RoutingTable.class);
    when(rt.getServerInstanceToSegmentsMap()).thenReturn(
        Map.of(new ServerInstance(new InstanceConfig("server01_9000")),
            new SegmentsToQuery(List.of("seg01"), List.of())));
    when(routingManager.getRoutingTable(any(), Mockito.anyLong())).thenReturn(rt);

    // Only deny the base table; the MV has no quota entry (returns true).
    QueryQuotaManager quotaManager = mock(QueryQuotaManager.class);
    when(quotaManager.acquireDatabase(anyString())).thenReturn(true);
    when(quotaManager.acquireApplication(anyString())).thenReturn(true);
    // Base table is over quota; MV is not throttled.
    when(quotaManager.acquire(baseOfflineTable)).thenReturn(false);
    when(quotaManager.acquire(materializedViewOfflineTable)).thenReturn(true);

    BrokerMetrics.register(mock(BrokerMetrics.class));
    PinotConfiguration config = new PinotConfiguration();
    BrokerQueryEventListenerFactory.init(config);

    BaseSingleStageBrokerRequestHandler handler =
        new BaseSingleStageBrokerRequestHandler(config, "broker1", new BrokerRequestIdGenerator(),
            routingManager, new AllowAllAccessControlFactory(), quotaManager, tableCache,
            ThreadAccountantUtils.getNoOpAccountant(), null, materializedViewHandler) {
          @Override
          public void start() {
          }

          @Override
          public void shutDown() {
          }

          @Override
          protected BrokerResponseNative processBrokerRequest(long requestId,
              BrokerRequest originalBrokerRequest, BrokerRequest serverBrokerRequest,
              TableRouteInfo route, long timeoutMs, ServerStats serverStats,
              RequestContext requestContext) {
            // Should not reach here — quota must reject before routing
            Assert.fail("processBrokerRequest should not be called when base table is over quota");
            return null;
          }

          @Override
          protected BrokerResponseNative processMaterializedViewSplitBrokerRequest(long requestId,
              long materializedViewRequestId, BrokerRequest originalBrokerRequest, TableRouteInfo baseRoute,
              TableRouteInfo materializedViewRoute, long timeoutMs, ServerStats serverStats,
              RequestContext requestContext) {
            Assert.fail("processMaterializedViewSplitBrokerRequest should not be called when base table is over quota");
            return null;
          }
        };

    BrokerResponseNative response = (BrokerResponseNative) handler.handleRequest(userSql);
    Assert.assertNotNull(response);
    // The request must be rejected with TOO_MANY_REQUESTS because the base table is over quota
    Assert.assertEquals(response.getExceptionsSize(), 1,
        "Expected quota rejection exception but got: " + response.getExceptions());
    Assert.assertEquals(response.getExceptions().get(0).getErrorCode(),
        org.apache.pinot.spi.exception.QueryErrorCode.TOO_MANY_REQUESTS.getId(),
        "Expected TOO_MANY_REQUESTS error code");
  }

  /**
   * Bug: FULL_REWRITE overwrites tableName to the materialized view table name, so
   * accessControl.getRowColFilters(requesterIdentity, tableName) fetches RLS
   * policy for the materialized view table instead of the base table.
   *
   * <p>Before fix: getRowColFilters is called with the materialized view table name
   * "mv_baseTable_OFFLINE".
   * <p>After fix: getRowColFilters is called with the original base table
   * name "baseTable_OFFLINE".
   */
  @Test
  public void testMaterializedViewFullRewriteRlsLookupUsesBaseTable()
      throws Exception {
    String baseOfflineTable = "baseTable_OFFLINE";
    String materializedViewOfflineTable = "mv_baseTable_OFFLINE";
    String baseRawTable = "baseTable";
    String materializedViewRawTable = "mv_baseTable";

    String userSql = "SELECT ts, SUM(revenue) FROM baseTable GROUP BY ts LIMIT 100 "
        + "";
    PinotQuery materializedViewQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT ts, SUM(revenue) FROM mv_baseTable_OFFLINE GROUP BY ts LIMIT 100");

    MaterializedViewRewritePlan plan = new MaterializedViewRewritePlan(
        materializedViewOfflineTable, MatchType.EXACT, ExecutionMode.FULL_REWRITE, materializedViewQuery, 1.0);
    MaterializedViewRewriteResult viewResult =
        new MaterializedViewRewriteResult(List.of(materializedViewOfflineTable), plan);

    AtomicReference<PinotQuery> querySeenByMaterializedViewRewrite = new AtomicReference<>();
    MaterializedViewQueryRewriteEngine materializedViewEngine = mock(MaterializedViewQueryRewriteEngine.class);
    when(materializedViewEngine.tryRewrite(any(PinotQuery.class), anyString())).thenAnswer(invocation -> {
      querySeenByMaterializedViewRewrite.set(((PinotQuery) invocation.getArgument(0)).deepCopy());
      return viewResult;
    });
    MaterializedViewHandler materializedViewHandler = new DefaultMaterializedViewHandler(materializedViewEngine);

    Schema baseSchema = new Schema.SchemaBuilder()
        .setSchemaName(baseRawTable)
        .addSingleValueDimension("ts", DataType.STRING)
        .addMetric("revenue", DataType.DOUBLE)
        .build();
    Schema materializedViewSchema = new Schema.SchemaBuilder()
        .setSchemaName(materializedViewRawTable)
        .addSingleValueDimension("ts", DataType.STRING)
        .addMetric("revenue", DataType.DOUBLE)
        .build();

    TableCache tableCache = mock(TableCache.class);
    when(tableCache.getActualTableName(baseRawTable)).thenReturn(baseRawTable);
    when(tableCache.getSchema(baseRawTable)).thenReturn(baseSchema);
    when(tableCache.getSchema(materializedViewRawTable)).thenReturn(materializedViewSchema);
    when(tableCache.getColumnNameMap(anyString())).thenReturn(Map.of("ts", "ts", "revenue", "revenue"));
    TableConfig tableCfg = mock(TableConfig.class);
    TenantConfig tenant = new TenantConfig("t_BROKER", "t_SERVER", null);
    when(tableCfg.getTenantConfig()).thenReturn(tenant);
    when(tableCache.getTableConfig(baseOfflineTable)).thenReturn(tableCfg);
    when(tableCache.getTableConfig(materializedViewOfflineTable)).thenReturn(tableCfg);

    BrokerRoutingManager routingManager = mock(BrokerRoutingManager.class);
    when(routingManager.routingExists(baseOfflineTable)).thenReturn(true);
    when(routingManager.routingExists(materializedViewOfflineTable)).thenReturn(true);
    when(routingManager.getQueryTimeoutMs(anyString())).thenReturn(10000L);
    RoutingTable rt = mock(RoutingTable.class);
    when(rt.getServerInstanceToSegmentsMap()).thenReturn(
        Map.of(new ServerInstance(new InstanceConfig("server01_9000")),
            new SegmentsToQuery(List.of("seg01"), List.of())));
    when(routingManager.getRoutingTable(any(), Mockito.anyLong())).thenReturn(rt);

    QueryQuotaManager quotaManager = mock(QueryQuotaManager.class);
    when(quotaManager.acquire(anyString())).thenReturn(true);
    when(quotaManager.acquireDatabase(anyString())).thenReturn(true);
    when(quotaManager.acquireApplication(anyString())).thenReturn(true);

    // Use a concrete AccessControl that allows everything but records the table passed to getRowColFilters.
    // A Mockito mock of an interface cannot reliably stub default interface methods, so we use an
    // anonymous implementation to avoid NPEs from un-stubbed default-method paths.
    List<String> capturedRlsTables = new java.util.ArrayList<>();
    AccessControl accessControl = new AccessControl() {
      @Override
      public org.apache.pinot.spi.auth.AuthorizationResult authorize(
          org.apache.pinot.spi.auth.broker.RequesterIdentity identity, BrokerRequest request) {
        return TableAuthorizationResult.success();
      }

      @Override
      public TableAuthorizationResult authorize(
          org.apache.pinot.spi.auth.broker.RequesterIdentity identity, Set<String> tables) {
        return TableAuthorizationResult.success();
      }

      @Override
      public org.apache.pinot.spi.auth.TableRowColAccessResult getRowColFilters(
          org.apache.pinot.spi.auth.broker.RequesterIdentity identity, String tableWithType) {
        capturedRlsTables.add(tableWithType);
        return new TableRowColAccessResultImpl(List.of("ts = 'allowed'"));
      }
    };

    AccessControlFactory accessControlFactory = mock(AccessControlFactory.class);
    when(accessControlFactory.create()).thenReturn(accessControl);

    BrokerMetrics.register(mock(BrokerMetrics.class));
    // Enable row/column-level auth so the RLS path is exercised
    PinotConfiguration config = new PinotConfiguration(
        Map.of(Broker.CONFIG_OF_BROKER_ENABLE_ROW_COLUMN_LEVEL_AUTH, "true"));
    BrokerQueryEventListenerFactory.init(config);

    BaseSingleStageBrokerRequestHandler handler =
        new BaseSingleStageBrokerRequestHandler(config, "broker1", new BrokerRequestIdGenerator(),
            routingManager, accessControlFactory, quotaManager, tableCache,
            ThreadAccountantUtils.getNoOpAccountant(), null, materializedViewHandler) {
          @Override
          public void start() {
          }

          @Override
          public void shutDown() {
          }

          @Override
          protected BrokerResponseNative processBrokerRequest(long requestId,
              BrokerRequest originalBrokerRequest, BrokerRequest serverBrokerRequest,
              TableRouteInfo route, long timeoutMs, ServerStats serverStats,
              RequestContext requestContext) {
            return BrokerResponseNative.empty();
          }

          @Override
          protected BrokerResponseNative processMaterializedViewSplitBrokerRequest(long requestId,
              long materializedViewRequestId, BrokerRequest originalBrokerRequest, TableRouteInfo baseRoute,
              TableRouteInfo materializedViewRoute, long timeoutMs, ServerStats serverStats,
              RequestContext requestContext) {
            return BrokerResponseNative.empty();
          }
        };

    BrokerResponseNative response = (BrokerResponseNative) handler.handleRequest(userSql);

    // getRowColFilters must have been called with the base table name, not the materialized view table
    Assert.assertFalse(capturedRlsTables.isEmpty(),
        "getRowColFilters should have been called");
    String rlsTable = capturedRlsTables.get(0);
    Assert.assertNotEquals(rlsTable, materializedViewOfflineTable,
        "RLS filter lookup must NOT use materialized view table name but got: " + rlsTable);
    // The RLS lookup must use the original base table identity, not the materialized view table.
    // The table name stored in preRewriteTableName is the raw name from compileSingleStageBrokerRequest
    // (before type resolution), so we assert on baseRawTable, not baseOfflineTable.
    Assert.assertEquals(rlsTable, baseRawTable,
        "RLS filter lookup must use base table '" + baseRawTable
            + "' not materialized view table, but got: " + rlsTable);
    Assert.assertTrue(response.getRLSFiltersApplied(), "response should report that RLS filters were applied");
    PinotQuery rewriteInput = querySeenByMaterializedViewRewrite.get();
    Assert.assertNotNull(rewriteInput, "MV rewrite should see the server query after RLS rewrite");
    Assert.assertNotNull(rewriteInput.getFilterExpression(), "MV rewrite input must carry the base-table RLS filter");
    Assert.assertTrue(rewriteInput.getFilterExpression().toString().contains("allowed"),
        "MV rewrite input must include the RLS predicate, got: " + rewriteInput.getFilterExpression());
  }

  // The split-mode time-boundary filter attach helpers moved to
  // org.apache.pinot.materializedview.handler.DefaultMaterializedViewHandler#attachFilter; their
  // regression coverage lives in DefaultMaterializedViewHandlerTest in pinot-materialized-view.

  /**
   * Pins the security-style defense that a user-supplied `materializedViewRewrite=true` query
   * option (e.g. via `SET materializedViewRewrite='true'`) is stripped at the broker entry
   * before any compile work. Without the strip, a hostile client could stamp the
   * broker-internal marker themselves and bypass `BrokerReduceService`'s "Nested query is not
   * supported without gapfill" safety net on any path where `brokerRequest != serverBrokerRequest`.
   */
  @Test
  public void testMaterializedViewMarkerStrippedFromUserSuppliedOptions()
      throws Exception {
    String baseOfflineTable = "baseTable_OFFLINE";
    String baseRawTable = "baseTable";

    // User attempts to set the internal MV-rewrite marker via the SQL SET-options syntax.
    // The strip in handleRequest must remove it before the marker can reach the server query.
    String userSql = "SET materializedViewRewrite='true';"
        + "SELECT ts, SUM(revenue) FROM baseTable GROUP BY ts LIMIT 100";

    Schema baseSchema = new Schema.SchemaBuilder()
        .setSchemaName(baseRawTable)
        .addSingleValueDimension("ts", DataType.STRING)
        .addMetric("revenue", DataType.DOUBLE)
        .build();

    TableCache tableCache = mock(TableCache.class);
    when(tableCache.getActualTableName(baseRawTable)).thenReturn(baseRawTable);
    when(tableCache.getSchema(baseRawTable)).thenReturn(baseSchema);
    when(tableCache.getColumnNameMap(anyString())).thenReturn(Map.of("ts", "ts", "revenue", "revenue"));
    TableConfig tableCfg = mock(TableConfig.class);
    TenantConfig tenant = new TenantConfig("t_BROKER", "t_SERVER", null);
    when(tableCfg.getTenantConfig()).thenReturn(tenant);
    when(tableCache.getTableConfig(baseOfflineTable)).thenReturn(tableCfg);

    BrokerRoutingManager routingManager = mock(BrokerRoutingManager.class);
    when(routingManager.routingExists(baseOfflineTable)).thenReturn(true);
    when(routingManager.getQueryTimeoutMs(anyString())).thenReturn(10000L);
    RoutingTable rt = mock(RoutingTable.class);
    when(rt.getServerInstanceToSegmentsMap()).thenReturn(
        Map.of(new ServerInstance(new InstanceConfig("server01_9000")),
            new SegmentsToQuery(List.of("seg01"), List.of())));
    when(routingManager.getRoutingTable(any(), Mockito.anyLong())).thenReturn(rt);

    QueryQuotaManager quotaManager = mock(QueryQuotaManager.class);
    when(quotaManager.acquireDatabase(anyString())).thenReturn(true);
    when(quotaManager.acquireApplication(anyString())).thenReturn(true);
    when(quotaManager.acquire(anyString())).thenReturn(true);

    BrokerMetrics.register(mock(BrokerMetrics.class));
    PinotConfiguration config = new PinotConfiguration();
    BrokerQueryEventListenerFactory.init(config);

    AtomicReference<Map<String, String>> capturedServerOptions = new AtomicReference<>();
    BaseSingleStageBrokerRequestHandler handler =
        new BaseSingleStageBrokerRequestHandler(config, "broker1", new BrokerRequestIdGenerator(),
            routingManager, new AllowAllAccessControlFactory(), quotaManager, tableCache,
            ThreadAccountantUtils.getNoOpAccountant(), null, /*materializedViewHandler*/ null) {
          @Override
          public void start() {
          }

          @Override
          public void shutDown() {
          }

          @Override
          protected BrokerResponseNative processBrokerRequest(long requestId,
              BrokerRequest originalBrokerRequest, BrokerRequest serverBrokerRequest,
              TableRouteInfo route, long timeoutMs, ServerStats serverStats,
              RequestContext requestContext) {
            Map<String, String> options = serverBrokerRequest.getPinotQuery().getQueryOptions();
            capturedServerOptions.set(options == null ? Map.of() : Map.copyOf(options));
            return BrokerResponseNative.empty();
          }

          @Override
          protected BrokerResponseNative processMaterializedViewSplitBrokerRequest(long requestId,
              long materializedViewRequestId, BrokerRequest originalBrokerRequest, TableRouteInfo baseRoute,
              TableRouteInfo materializedViewRoute, long timeoutMs, ServerStats serverStats,
              RequestContext requestContext) {
            return BrokerResponseNative.empty();
          }
        };

    handler.handleRequest(userSql);
    Map<String, String> serverOptions = capturedServerOptions.get();
    Assert.assertNotNull(serverOptions, "processBrokerRequest should have been reached and captured server options");
    Assert.assertFalse(serverOptions.containsKey(
        CommonConstants.Broker.Request.QueryOptionKey.MATERIALIZED_VIEW_REWRITE),
        "User-supplied materializedViewRewrite option must be stripped before compile but options were: "
            + serverOptions);
  }

  /**
   * Pins the C1 fix: FULL_REWRITE is skipped at the broker layer when the base table has a
   * REALTIME sibling, because a batch MV cannot cover newly-streamed rows. Without this guard
   * the MV swap would silently drop all rows ingested via the realtime stream since the MV last
   * refreshed — an invisible data-loss path.
   */
  @Test
  public void testMaterializedViewFullRewriteSkippedForHybridBaseTable()
      throws Exception {
    String baseOfflineTable = "baseTable_OFFLINE";
    String baseRealtimeTable = "baseTable_REALTIME";
    String materializedViewOfflineTable = "mv_baseTable_OFFLINE";
    String baseRawTable = "baseTable";
    String materializedViewRawTable = "mv_baseTable";

    String userSql = "SELECT ts, SUM(revenue) FROM baseTable GROUP BY ts LIMIT 100";
    PinotQuery materializedViewQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT ts, SUM(revenue) FROM mv_baseTable_OFFLINE GROUP BY ts LIMIT 100");

    MaterializedViewRewritePlan plan = new MaterializedViewRewritePlan(
        materializedViewOfflineTable, MatchType.EXACT, ExecutionMode.FULL_REWRITE, materializedViewQuery, 1.0);
    MaterializedViewRewriteResult viewResult =
        new MaterializedViewRewriteResult(List.of(materializedViewOfflineTable), plan);

    MaterializedViewQueryRewriteEngine materializedViewEngine = mock(MaterializedViewQueryRewriteEngine.class);
    when(materializedViewEngine.tryRewrite(any(PinotQuery.class), anyString())).thenReturn(viewResult);
    MaterializedViewHandler materializedViewHandler = new DefaultMaterializedViewHandler(materializedViewEngine);

    Schema baseSchema = new Schema.SchemaBuilder()
        .setSchemaName(baseRawTable)
        .addSingleValueDimension("ts", DataType.STRING)
        .addMetric("revenue", DataType.DOUBLE)
        .build();
    Schema materializedViewSchema = new Schema.SchemaBuilder()
        .setSchemaName(materializedViewRawTable)
        .addSingleValueDimension("ts", DataType.STRING)
        .addMetric("revenue", DataType.DOUBLE)
        .build();

    TableCache tableCache = mock(TableCache.class);
    when(tableCache.getActualTableName(baseRawTable)).thenReturn(baseRawTable);
    when(tableCache.getSchema(baseRawTable)).thenReturn(baseSchema);
    when(tableCache.getSchema(materializedViewRawTable)).thenReturn(materializedViewSchema);
    when(tableCache.getColumnNameMap(anyString())).thenReturn(Map.of("ts", "ts", "revenue", "revenue"));
    TableConfig tableCfg = mock(TableConfig.class);
    TenantConfig tenant = new TenantConfig("t_BROKER", "t_SERVER", null);
    when(tableCfg.getTenantConfig()).thenReturn(tenant);
    when(tableCache.getTableConfig(baseOfflineTable)).thenReturn(tableCfg);
    when(tableCache.getTableConfig(materializedViewOfflineTable)).thenReturn(tableCfg);
    // Critical to this test: the base table is HYBRID. The realtime sibling must exist in the cache
    // so the FULL_REWRITE hybrid-guard at BaseSingleStageBrokerRequestHandler trips.
    when(tableCache.getTableConfig(baseRealtimeTable)).thenReturn(tableCfg);

    BrokerRoutingManager routingManager = mock(BrokerRoutingManager.class);
    when(routingManager.routingExists(baseOfflineTable)).thenReturn(true);
    when(routingManager.routingExists(baseRealtimeTable)).thenReturn(true);
    when(routingManager.getQueryTimeoutMs(anyString())).thenReturn(10000L);
    RoutingTable rt = mock(RoutingTable.class);
    when(rt.getServerInstanceToSegmentsMap()).thenReturn(
        Map.of(new ServerInstance(new InstanceConfig("server01_9000")),
            new SegmentsToQuery(List.of("seg01"), List.of())));
    when(routingManager.getRoutingTable(any(), Mockito.anyLong())).thenReturn(rt);

    QueryQuotaManager quotaManager = mock(QueryQuotaManager.class);
    when(quotaManager.acquireDatabase(anyString())).thenReturn(true);
    when(quotaManager.acquireApplication(anyString())).thenReturn(true);
    when(quotaManager.acquire(anyString())).thenReturn(true);

    BrokerMetrics.register(mock(BrokerMetrics.class));
    PinotConfiguration config = new PinotConfiguration();
    BrokerQueryEventListenerFactory.init(config);

    AtomicReference<BrokerRequest> capturedServerBrokerRequest = new AtomicReference<>();
    BaseSingleStageBrokerRequestHandler handler =
        new BaseSingleStageBrokerRequestHandler(config, "broker1", new BrokerRequestIdGenerator(),
            routingManager, new AllowAllAccessControlFactory(), quotaManager, tableCache,
            ThreadAccountantUtils.getNoOpAccountant(), null, materializedViewHandler) {
          @Override
          public void start() {
          }

          @Override
          public void shutDown() {
          }

          @Override
          protected BrokerResponseNative processBrokerRequest(long requestId,
              BrokerRequest originalBrokerRequest, BrokerRequest serverBrokerRequest,
              TableRouteInfo route, long timeoutMs, ServerStats serverStats,
              RequestContext requestContext) {
            capturedServerBrokerRequest.set(serverBrokerRequest);
            return BrokerResponseNative.empty();
          }

          @Override
          protected BrokerResponseNative processMaterializedViewSplitBrokerRequest(long requestId,
              long materializedViewRequestId, BrokerRequest originalBrokerRequest, TableRouteInfo baseRoute,
              TableRouteInfo materializedViewRoute, long timeoutMs, ServerStats serverStats,
              RequestContext requestContext) {
            Assert.fail("Split path must not be entered when FULL_REWRITE is skipped");
            return null;
          }
        };

    BrokerResponseNative response = (BrokerResponseNative) handler.handleRequest(userSql);
    BrokerRequest serverBrokerRequest = capturedServerBrokerRequest.get();
    Assert.assertNotNull(serverBrokerRequest,
        "processBrokerRequest must have been reached after the FULL_REWRITE skip; exceptions: "
            + response.getExceptions());
    // Server-side query MUST target a base-table variant (OFFLINE or REALTIME) — never the MV
    // table — because the hybrid guard rejected the FULL_REWRITE swap. Hybrid tables dispatch
    // to both offline + realtime broker requests, so the test accepts either base-side name.
    String serverTableName = serverBrokerRequest.getPinotQuery().getDataSource().getTableName();
    Assert.assertTrue(
        serverTableName.equals(baseOfflineTable) || serverTableName.equals(baseRealtimeTable),
        "FULL_REWRITE must be skipped on hybrid base tables to avoid silently dropping realtime data; "
            + "expected server query against a base-table variant but got: " + serverTableName);
    Assert.assertNotEquals(serverTableName, materializedViewOfflineTable,
        "Server query must not target the MV table when the FULL_REWRITE was skipped");
    // And the MV-rewrite marker must NOT have been stamped on the server query.
    Map<String, String> serverOptions = serverBrokerRequest.getPinotQuery().getQueryOptions();
    Assert.assertTrue(serverOptions == null
            || !serverOptions.containsKey(CommonConstants.Broker.Request.QueryOptionKey.MATERIALIZED_VIEW_REWRITE),
        "FULL_REWRITE marker must not be stamped when rewrite is skipped; options: " + serverOptions);
  }

  /**
   * Pins the F1 fix: when the rewrite engine returns a FULL_REWRITE plan and no skip condition
   * trips (base table is OFFLINE-only, schema present, etc.), the server-side query MUST actually
   * be swapped to target the MV — not the base table.  The earlier `watermarkMs <= 0` guard in
   * `DefaultMaterializedViewHandler.compile` was over-broad and silently dropped every
   * FULL_REWRITE attempt while `annotateResponse` continued to stamp `materializedViewQueried`,
   * producing a false-positive on every operator-facing metric.  This test asserts the actual
   * dataSource table name on the server query equals the MV name AND the response's
   * `materializedViewQueried` matches.
   */
  @Test
  public void testMaterializedViewFullRewriteActuallySwapsServerQuery()
      throws Exception {
    String baseOfflineTable = "baseTable_OFFLINE";
    String materializedViewOfflineTable = "mv_baseTable_OFFLINE";
    String baseRawTable = "baseTable";
    String materializedViewRawTable = "mv_baseTable";

    String userSql = "SELECT ts, SUM(revenue) FROM baseTable GROUP BY ts LIMIT 100";
    PinotQuery materializedViewQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT ts, SUM(revenue) FROM mv_baseTable_OFFLINE GROUP BY ts LIMIT 100");

    // Engine returns FULL_REWRITE with watermarkMs=0 (the actual production code path for batch
    // MVs).  Pre-F1, this was silently downgraded to "no rewrite" by the handler's `<= 0` guard
    // while the response still claimed the MV was used.
    MaterializedViewRewritePlan plan = new MaterializedViewRewritePlan(
        materializedViewOfflineTable, MatchType.EXACT, ExecutionMode.FULL_REWRITE, materializedViewQuery, 1.0);
    MaterializedViewRewriteResult viewResult =
        new MaterializedViewRewriteResult(List.of(materializedViewOfflineTable), plan);

    MaterializedViewQueryRewriteEngine materializedViewEngine = mock(MaterializedViewQueryRewriteEngine.class);
    when(materializedViewEngine.tryRewrite(any(PinotQuery.class), anyString())).thenReturn(viewResult);
    MaterializedViewHandler materializedViewHandler = new DefaultMaterializedViewHandler(materializedViewEngine);

    Schema baseSchema = new Schema.SchemaBuilder()
        .setSchemaName(baseRawTable)
        .addSingleValueDimension("ts", DataType.STRING)
        .addMetric("revenue", DataType.DOUBLE)
        .build();
    Schema materializedViewSchema = new Schema.SchemaBuilder()
        .setSchemaName(materializedViewRawTable)
        .addSingleValueDimension("ts", DataType.STRING)
        .addMetric("revenue", DataType.DOUBLE)
        .build();

    TableCache tableCache = mock(TableCache.class);
    when(tableCache.getActualTableName(baseRawTable)).thenReturn(baseRawTable);
    when(tableCache.getSchema(baseRawTable)).thenReturn(baseSchema);
    when(tableCache.getSchema(materializedViewRawTable)).thenReturn(materializedViewSchema);
    when(tableCache.getColumnNameMap(anyString())).thenReturn(Map.of("ts", "ts", "revenue", "revenue"));
    TableConfig tableCfg = mock(TableConfig.class);
    TenantConfig tenant = new TenantConfig("t_BROKER", "t_SERVER", null);
    when(tableCfg.getTenantConfig()).thenReturn(tenant);
    when(tableCache.getTableConfig(baseOfflineTable)).thenReturn(tableCfg);
    when(tableCache.getTableConfig(materializedViewOfflineTable)).thenReturn(tableCfg);
    // OFFLINE-only base table — no realtime sibling, so the hybrid-guard does NOT trip.
    when(tableCache.getTableConfig(TableNameBuilder.REALTIME.tableNameWithType(baseRawTable))).thenReturn(null);

    BrokerRoutingManager routingManager = mock(BrokerRoutingManager.class);
    when(routingManager.routingExists(baseOfflineTable)).thenReturn(true);
    when(routingManager.routingExists(materializedViewOfflineTable)).thenReturn(true);
    when(routingManager.getQueryTimeoutMs(anyString())).thenReturn(10000L);
    RoutingTable rt = mock(RoutingTable.class);
    when(rt.getServerInstanceToSegmentsMap()).thenReturn(
        Map.of(new ServerInstance(new InstanceConfig("server01_9000")),
            new SegmentsToQuery(List.of("seg01"), List.of())));
    when(routingManager.getRoutingTable(any(), Mockito.anyLong())).thenReturn(rt);

    QueryQuotaManager quotaManager = mock(QueryQuotaManager.class);
    when(quotaManager.acquireDatabase(anyString())).thenReturn(true);
    when(quotaManager.acquireApplication(anyString())).thenReturn(true);
    when(quotaManager.acquire(anyString())).thenReturn(true);

    BrokerMetrics.register(mock(BrokerMetrics.class));
    PinotConfiguration config = new PinotConfiguration();
    BrokerQueryEventListenerFactory.init(config);

    AtomicReference<BrokerRequest> capturedServerBrokerRequest = new AtomicReference<>();
    BaseSingleStageBrokerRequestHandler handler =
        new BaseSingleStageBrokerRequestHandler(config, "broker1", new BrokerRequestIdGenerator(),
            routingManager, new AllowAllAccessControlFactory(), quotaManager, tableCache,
            ThreadAccountantUtils.getNoOpAccountant(), null, materializedViewHandler) {
          @Override
          public void start() {
          }

          @Override
          public void shutDown() {
          }

          @Override
          protected BrokerResponseNative processBrokerRequest(long requestId,
              BrokerRequest originalBrokerRequest, BrokerRequest serverBrokerRequest,
              TableRouteInfo route, long timeoutMs, ServerStats serverStats,
              RequestContext requestContext) {
            capturedServerBrokerRequest.set(serverBrokerRequest);
            return BrokerResponseNative.empty();
          }

          @Override
          protected BrokerResponseNative processMaterializedViewSplitBrokerRequest(long requestId,
              long materializedViewRequestId, BrokerRequest originalBrokerRequest, TableRouteInfo baseRoute,
              TableRouteInfo materializedViewRoute, long timeoutMs, ServerStats serverStats,
              RequestContext requestContext) {
            return BrokerResponseNative.empty();
          }
        };

    BrokerResponseNative response = (BrokerResponseNative) handler.handleRequest(userSql);
    BrokerRequest serverBrokerRequest = capturedServerBrokerRequest.get();
    Assert.assertNotNull(serverBrokerRequest,
        "processBrokerRequest must have been reached after the FULL_REWRITE swap; exceptions: "
            + response.getExceptions());
    // The actual swap MUST have happened — server-side query targets the MV, not the base table.
    String serverTableName = serverBrokerRequest.getPinotQuery().getDataSource().getTableName();
    Assert.assertEquals(serverTableName, materializedViewOfflineTable,
        "FULL_REWRITE must swap the server query to target the MV table but got: " + serverTableName);
    // The MV-rewrite marker MUST be stamped on the server query.
    Map<String, String> serverOptions = serverBrokerRequest.getPinotQuery().getQueryOptions();
    Assert.assertNotNull(serverOptions, "Server query options must contain the MV-rewrite marker");
    Assert.assertEquals(
        serverOptions.get(CommonConstants.Broker.Request.QueryOptionKey.MATERIALIZED_VIEW_REWRITE), "true",
        "FULL_REWRITE marker must be stamped on the committed swap; options: " + serverOptions);
    // And the response must report the MV name (no false positive — the swap really did happen).
    Assert.assertEquals(response.getMaterializedViewQueried(), materializedViewOfflineTable,
        "Response must report the MV name when the swap was committed");
  }
}
