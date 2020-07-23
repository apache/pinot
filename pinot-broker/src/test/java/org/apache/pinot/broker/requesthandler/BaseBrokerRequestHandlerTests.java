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

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.yammer.metrics.core.MetricsRegistry;
import java.time.LocalDate;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.broker.api.RequestStatistics;
import org.apache.pinot.broker.broker.AccessControlFactory;
import org.apache.pinot.broker.broker.AllowAllAccessControlFactory;
import org.apache.pinot.broker.queryquota.QueryQuotaManager;
import org.apache.pinot.broker.routing.RoutingManager;
import org.apache.pinot.broker.routing.RoutingTable;
import org.apache.pinot.broker.routing.timeboundary.TimeBoundaryInfo;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.utils.helix.TableCache;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.spi.config.table.QueryConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.common.utils.CommonConstants.Broker.Request.*;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.*;
import static org.testng.Assert.*;


public class BaseBrokerRequestHandlerTests {

  private static final String TABLE = "tableName";
  private static final String TIME_COLUMN = "daysSinceEpoch";

  private PinotConfiguration _config;
  @Mock private RoutingManager _routingManager;
  private final AccessControlFactory _accessControlFactory = new AllowAllAccessControlFactory();
  @Mock private QueryQuotaManager _queryQuotaManager;
  private final BrokerMetrics _brokerMetrics = new BrokerMetrics(new MetricsRegistry());
  @Mock private TableCache _tableCache;
  private StubBrokerRequestHandler _handler;

  @BeforeMethod
  public void setUp() {
    initMocks(this);
    _config = new PinotConfiguration();
    when(_routingManager.routingExists(any())).thenReturn(true);
    when(_routingManager.getRoutingTable(any())).thenReturn(new RoutingTable(ImmutableMap.of(new ServerInstance(new InstanceConfig("Server_localhost_0000")), Collections.emptyList()), Collections.emptyList()));
    when(_routingManager.getQueryTimeoutMs(any())).thenReturn(Long.MAX_VALUE);
    when(_queryQuotaManager.acquire(any())).thenReturn(true);
    when(_tableCache.getTableConfig(TableNameBuilder.OFFLINE.tableNameWithType(TABLE))).thenReturn(new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE).setQueryConfig(new QueryConfig(100L, false)).build());
  }

  @Test
  public void testAttachesTimeBoundary() throws Exception {
    String query = String.format("select * from %s", TABLE);
    ObjectNode request = JsonUtils.newObjectNode();
    request.put(PQL, query);

    long daysSinceEpoch = LocalDate.of(2000, 1, 1).toEpochDay();
    when(_routingManager.getTimeBoundaryInfo(TableNameBuilder.OFFLINE.tableNameWithType(TABLE))).thenReturn(new TimeBoundaryInfo(TIME_COLUMN, String.valueOf(daysSinceEpoch)));

    _handler = new StubBrokerRequestHandler(_config, _routingManager, _accessControlFactory, _queryQuotaManager, _brokerMetrics, _tableCache);
    _handler.handleRequest(request, null, new RequestStatistics());

    BrokerRequest offlineBrokerRequest = _handler.getOfflineBrokerRequest();
    assertEquals(offlineBrokerRequest.getFilterQuery().getColumn(), TIME_COLUMN);
    assertEquals(offlineBrokerRequest.getFilterQuery().getValue().size(), 1);
    assertEquals(offlineBrokerRequest.getFilterQuery().getValue().get(0), String.format("(*\t\t%d]", daysSinceEpoch));

    BrokerRequest realtimeBrokerRequest = _handler.getRealtimeBrokerRequest();
    assertEquals(realtimeBrokerRequest.getFilterQuery().getColumn(), TIME_COLUMN);
    assertEquals(realtimeBrokerRequest.getFilterQuery().getValue().size(), 1);
    assertEquals(realtimeBrokerRequest.getFilterQuery().getValue().get(0), String.format("(%s\t\t*)", daysSinceEpoch));
  }

  @Test
  public void testAttachesTimeBoundaryWithPreexistingTimeFilter() throws Exception {
    long daysSinceEpoch = LocalDate.of(2000, 1, 1).toEpochDay();
    long yesterday = daysSinceEpoch - 1;
    long threeDaysAgo = daysSinceEpoch - 3;
    String query = String.format("select * from %s where %s >= %d", TABLE, TIME_COLUMN, threeDaysAgo);
    ObjectNode request = JsonUtils.newObjectNode();
    request.put(PQL, query);

    when(_routingManager.getTimeBoundaryInfo(TableNameBuilder.OFFLINE.tableNameWithType(TABLE))).thenReturn(new TimeBoundaryInfo(TIME_COLUMN, String.valueOf(yesterday)));

    _handler = new StubBrokerRequestHandler(_config, _routingManager, _accessControlFactory, _queryQuotaManager, _brokerMetrics, _tableCache);
    _handler.handleRequest(request, null, new RequestStatistics());

    BrokerRequest offlineBrokerRequest = _handler.getOfflineBrokerRequest();
    assertEquals(offlineBrokerRequest.getFilterQuery().getColumn(), TIME_COLUMN);
    assertEquals(offlineBrokerRequest.getFilterQuery().getValue().size(), 1);
    assertEquals(offlineBrokerRequest.getFilterQuery().getValue().get(0), String.format("[%d\t\t%d]", threeDaysAgo, yesterday));

    BrokerRequest realtimeBrokerRequest = _handler.getRealtimeBrokerRequest();
    assertEquals(realtimeBrokerRequest.getFilterQuery().getColumn(), TIME_COLUMN);
    assertEquals(realtimeBrokerRequest.getFilterQuery().getValue().size(), 1);
    assertEquals(realtimeBrokerRequest.getFilterQuery().getValue().get(0), String.format("(%d\t\t*)", yesterday));
  }

  @Test
  public void testAttachesTimeBoundaryWithConfigToServeOfflineImmediately() throws Exception {
    when(_tableCache.getTableConfig(TableNameBuilder.OFFLINE.tableNameWithType(TABLE))).thenReturn(new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE).setQueryConfig(new QueryConfig(100L, true)).build());

    String query = String.format("select * from %s", TABLE);
    ObjectNode request = JsonUtils.newObjectNode();
    request.put(PQL, query);

    long daysSinceEpoch = LocalDate.of(2000, 1, 1).toEpochDay();
    when(_routingManager.getTimeBoundaryInfo(TableNameBuilder.OFFLINE.tableNameWithType(TABLE))).thenReturn(new TimeBoundaryInfo(TIME_COLUMN, String.valueOf(daysSinceEpoch)));

    _handler = new StubBrokerRequestHandler(_config, _routingManager, _accessControlFactory, _queryQuotaManager, _brokerMetrics, _tableCache);
    _handler.handleRequest(request, null, new RequestStatistics());

    BrokerRequest offlineBrokerRequest = _handler.getOfflineBrokerRequest();
    assertEquals(offlineBrokerRequest.getFilterQuery().getColumn(), TIME_COLUMN);
    assertEquals(offlineBrokerRequest.getFilterQuery().getValue().size(), 1);
    assertEquals(offlineBrokerRequest.getFilterQuery().getValue().get(0), String.format("(*\t\t%d)", daysSinceEpoch));

    BrokerRequest realtimeBrokerRequest = _handler.getRealtimeBrokerRequest();
    assertEquals(realtimeBrokerRequest.getFilterQuery().getColumn(), TIME_COLUMN);
    assertEquals(realtimeBrokerRequest.getFilterQuery().getValue().size(), 1);
    assertEquals(realtimeBrokerRequest.getFilterQuery().getValue().get(0), String.format("[%s\t\t*)", daysSinceEpoch));
  }

  @Test
  public void testAttachesTimeBoundaryWithPreexistingTimeFilterAndConfigToServeOfflineImmediately() throws Exception {
    when(_tableCache.getTableConfig(TableNameBuilder.OFFLINE.tableNameWithType(TABLE))).thenReturn(new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE).setQueryConfig(new QueryConfig(100L, true)).build());

    long daysSinceEpoch = LocalDate.of(2000, 1, 1).toEpochDay();
    long yesterday = daysSinceEpoch - 1;
    long threeDaysAgo = daysSinceEpoch - 3;
    String query = String.format("select * from %s where %s >= %d", TABLE, TIME_COLUMN, threeDaysAgo);
    ObjectNode request = JsonUtils.newObjectNode();
    request.put(PQL, query);

    when(_routingManager.getTimeBoundaryInfo(TableNameBuilder.OFFLINE.tableNameWithType(TABLE))).thenReturn(new TimeBoundaryInfo(TIME_COLUMN, String.valueOf(yesterday)));

    _handler = new StubBrokerRequestHandler(_config, _routingManager, _accessControlFactory, _queryQuotaManager, _brokerMetrics, _tableCache);
    _handler.handleRequest(request, null, new RequestStatistics());

    BrokerRequest offlineBrokerRequest = _handler.getOfflineBrokerRequest();
    assertEquals(offlineBrokerRequest.getFilterQuery().getColumn(), TIME_COLUMN);
    assertEquals(offlineBrokerRequest.getFilterQuery().getValue().size(), 1);
    assertEquals(offlineBrokerRequest.getFilterQuery().getValue().get(0), String.format("[%d\t\t%d)", threeDaysAgo, yesterday));

    BrokerRequest realtimeBrokerRequest = _handler.getRealtimeBrokerRequest();
    assertEquals(realtimeBrokerRequest.getFilterQuery().getColumn(), TIME_COLUMN);
    assertEquals(realtimeBrokerRequest.getFilterQuery().getValue().size(), 1);
    assertEquals(realtimeBrokerRequest.getFilterQuery().getValue().get(0), String.format("[%d\t\t*)", yesterday));
  }

  private static class StubBrokerRequestHandler extends BaseBrokerRequestHandler {

    private BrokerRequest _offlineBrokerRequest;
    private BrokerRequest _realtimeBrokerRequest;

    public StubBrokerRequestHandler(PinotConfiguration config, RoutingManager routingManager,
        AccessControlFactory accessControlFactory, QueryQuotaManager queryQuotaManager, BrokerMetrics brokerMetrics,
        TableCache tableCache) {
      super(config, routingManager, accessControlFactory, queryQuotaManager, brokerMetrics, tableCache);
    }

    @Override
    protected BrokerResponse processBrokerRequest(long requestId, BrokerRequest originalBrokerRequest,
        @Nullable BrokerRequest offlineBrokerRequest, @Nullable Map<ServerInstance, List<String>> offlineRoutingTable,
        @Nullable BrokerRequest realtimeBrokerRequest, @Nullable Map<ServerInstance, List<String>> realtimeRoutingTable,
        long timeoutMs, ServerStats serverStats, RequestStatistics requestStatistics) {
      _offlineBrokerRequest = offlineBrokerRequest;
      _realtimeBrokerRequest = realtimeBrokerRequest;
      return new BrokerResponseNative();
    }

    @Override
    public void start() {}

    @Override
    public void shutDown() {}

    public BrokerRequest getOfflineBrokerRequest() {
      return _offlineBrokerRequest;
    }

    public BrokerRequest getRealtimeBrokerRequest() {
      return _realtimeBrokerRequest;
    }
  }
}
