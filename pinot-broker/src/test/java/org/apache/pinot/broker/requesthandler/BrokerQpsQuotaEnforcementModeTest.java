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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.broker.broker.AllowAllAccessControlFactory;
import org.apache.pinot.broker.queryquota.QueryQuotaManager;
import org.apache.pinot.broker.routing.manager.BrokerRoutingManager;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.core.routing.RoutingTable;
import org.apache.pinot.core.routing.SegmentsToQuery;
import org.apache.pinot.core.routing.TableRouteInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.spi.accounting.ThreadAccountantUtils;
import org.apache.pinot.spi.config.table.QuotaEnforcementMode;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TenantConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.eventlistener.query.BrokerQueryEventListenerFactory;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.trace.RequestContext;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


///  Verifies how the table QPS quota enforcement mode (DISABLED / SHADOW / ENFORCED) is applied by the broker request
///  handlers, for both the single-stage and the multi-stage code paths.
public class BrokerQpsQuotaEnforcementModeTest {
  private static final String RAW_TABLE_NAME = "myTable";
  private static final String TABLE_NAME = "myTable_OFFLINE";
  private static final String LOGICAL_TABLE_NAME = "myLogicalTable";

  private BrokerMetrics _brokerMetrics;
  private MockedStatic<BrokerMetrics> _brokerMetricsStatic;
  private QueryQuotaManager _quotaManager;

  @BeforeMethod
  public void setUp() {
    // BrokerMetrics.register() keeps the first registered instance for the whole JVM, so scope our own mock instead
    // of depending on which test class runs first
    _brokerMetrics = mock(BrokerMetrics.class);
    _brokerMetricsStatic = Mockito.mockStatic(BrokerMetrics.class);
    _brokerMetricsStatic.when(BrokerMetrics::get).thenReturn(_brokerMetrics);
    _quotaManager = mock(QueryQuotaManager.class);
    when(_quotaManager.acquireDatabase(anyString())).thenReturn(true);
    when(_quotaManager.acquireApplication(anyString())).thenReturn(true);
  }

  @AfterMethod(alwaysRun = true)
  public void tearDown() {
    _brokerMetricsStatic.close();
  }

  private BaseSingleStageBrokerRequestHandler newHandler(BrokerRoutingManager routingManager, TableCache tableCache,
      AtomicBoolean queryExecuted) {
    PinotConfiguration config = new PinotConfiguration();
    BrokerQueryEventListenerFactory.init(config);
    return new BaseSingleStageBrokerRequestHandler(config, "testBrokerId", new BrokerRequestIdGenerator(),
        routingManager, new AllowAllAccessControlFactory(), _quotaManager, tableCache,
        ThreadAccountantUtils.getNoOpAccountant(), null) {
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
        queryExecuted.set(true);
        return new BrokerResponseNative();
      }
    };
  }

  private BaseSingleStageBrokerRequestHandler newHandler() {
    return newHandler(mock(BrokerRoutingManager.class), mock(TableCache.class), new AtomicBoolean());
  }

  @DataProvider(name = "modes")
  public Object[][] modes() {
    // mode, acquire() result, expected reject, expected breach meters, expected acquire() calls
    return new Object[][]{
        {QuotaEnforcementMode.ENFORCED, false, true, 1, 1},
        {QuotaEnforcementMode.ENFORCED, true, false, 0, 1},
        {QuotaEnforcementMode.SHADOW, false, false, 1, 1},
        {QuotaEnforcementMode.SHADOW, true, false, 0, 1},
        // DISABLED neither rejects nor reports a breach
        {QuotaEnforcementMode.DISABLED, false, false, 0, 1},
        {QuotaEnforcementMode.DISABLED, true, false, 0, 1}
    };
  }

  @Test(dataProvider = "modes")
  public void testTableQpsQuotaBreach(QuotaEnforcementMode mode, boolean acquireResult, boolean expectedReject,
      int expectedBreachMeters, int expectedAcquires) {
    when(_quotaManager.getQueryQuotaEnforcementMode(eq(TABLE_NAME))).thenReturn(mode);
    when(_quotaManager.acquire(TABLE_NAME)).thenReturn(acquireResult);

    Assert.assertEquals(newHandler().isTableQpsQuotaBreachEnforced(TABLE_NAME), expectedReject);

    verify(_quotaManager, times(expectedAcquires)).acquire(TABLE_NAME);
    verify(_brokerMetrics, times(expectedBreachMeters)).addMeteredTableValue(RAW_TABLE_NAME,
        BrokerMeter.QUERY_QUOTA_EXCEEDED, 1L);
  }

  @Test(dataProvider = "modes")
  public void testLogicalTableQpsQuotaBreach(QuotaEnforcementMode mode, boolean acquireResult, boolean expectedReject,
      int expectedBreachMeters, int expectedAcquires) {
    when(_quotaManager.getQueryQuotaEnforcementMode(eq(LOGICAL_TABLE_NAME))).thenReturn(mode);
    when(_quotaManager.acquireLogicalTable(LOGICAL_TABLE_NAME)).thenReturn(acquireResult);

    Assert.assertEquals(newHandler().isLogicalTableQpsQuotaBreachEnforced(LOGICAL_TABLE_NAME), expectedReject);

    verify(_quotaManager, times(expectedAcquires)).acquireLogicalTable(LOGICAL_TABLE_NAME);
    verify(_brokerMetrics, times(expectedBreachMeters)).addMeteredTableValue(LOGICAL_TABLE_NAME,
        BrokerMeter.QUERY_QUOTA_EXCEEDED, 1L);
  }

  @Test
  public void testMultiStagePathHonorsEnforcementMode() {
    RequestContext requestContext = mock(RequestContext.class);
    when(_quotaManager.acquire(TABLE_NAME)).thenReturn(false);

    when(_quotaManager.getQueryQuotaEnforcementMode(eq(TABLE_NAME))).thenReturn(QuotaEnforcementMode.SHADOW);
    Assert.assertFalse(newHandler().hasExceededQPSQuota(null, Set.of(TABLE_NAME), requestContext));
    verify(requestContext, never()).setErrorCode(any());

    when(_quotaManager.getQueryQuotaEnforcementMode(eq(TABLE_NAME))).thenReturn(QuotaEnforcementMode.ENFORCED);
    Assert.assertTrue(newHandler().hasExceededQPSQuota(null, Set.of(TABLE_NAME), requestContext));
    verify(requestContext).setErrorCode(QueryErrorCode.TOO_MANY_REQUESTS);
  }

  @DataProvider(name = "fullPathModes")
  public Object[][] fullPathModes() {
    return new Object[][]{
        {QuotaEnforcementMode.ENFORCED, false},
        {QuotaEnforcementMode.SHADOW, true},
        {QuotaEnforcementMode.DISABLED, true}
    };
  }

  ///  End-to-end through the default (single-stage) `handleRequest` path with a table whose quota is breached.
  @Test(dataProvider = "fullPathModes")
  public void testSingleStageHandleRequestHonorsEnforcementMode(QuotaEnforcementMode mode, boolean expectQueryExecuted)
      throws Exception {
    TableCache tableCache = mock(TableCache.class);
    TableConfig tableConfig = mock(TableConfig.class);
    when(tableCache.getActualTableName(anyString())).thenReturn(TABLE_NAME);
    when(tableConfig.getTenantConfig()).thenReturn(new TenantConfig("tier_BROKER", "tier_SERVER", null));
    when(tableCache.getTableConfig(TABLE_NAME)).thenReturn(tableConfig);
    BrokerRoutingManager routingManager = mock(BrokerRoutingManager.class);
    when(routingManager.routingExists(TABLE_NAME)).thenReturn(true);
    when(routingManager.getQueryTimeoutMs(TABLE_NAME)).thenReturn(10000L);
    RoutingTable routingTable = mock(RoutingTable.class);
    when(routingTable.getServerInstanceToSegmentsMap()).thenReturn(
        Map.of(new ServerInstance(new InstanceConfig("server01_9000")),
            new SegmentsToQuery(List.of("segment01"), List.of())));
    when(routingManager.getRoutingTable(any(), Mockito.anyLong())).thenReturn(routingTable);

    when(_quotaManager.acquire(anyString())).thenReturn(false);
    when(_quotaManager.getQueryQuotaEnforcementMode(anyString())).thenReturn(mode);

    AtomicBoolean queryExecuted = new AtomicBoolean();
    BrokerResponse response =
        newHandler(routingManager, tableCache, queryExecuted).handleRequest("select * from myTable limit 10");

    boolean rejected = response.getExceptions().stream()
        .anyMatch(e -> e.getErrorCode() == QueryErrorCode.TOO_MANY_REQUESTS.getId());
    Assert.assertEquals(queryExecuted.get(), expectQueryExecuted, "query executed in mode " + mode);
    Assert.assertEquals(rejected, !expectQueryExecuted, "query rejected in mode " + mode);
    // A breach is metered in both ENFORCED and SHADOW, but not when the quota is DISABLED
    verify(_brokerMetrics, times(mode == QuotaEnforcementMode.DISABLED ? 0 : 1)).addMeteredTableValue(RAW_TABLE_NAME,
        BrokerMeter.QUERY_QUOTA_EXCEEDED, 1L);
  }
}
