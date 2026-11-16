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

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.broker.api.AccessControl;
import org.apache.pinot.broker.broker.AccessControlFactory;
import org.apache.pinot.broker.queryquota.QueryQuotaManager;
import org.apache.pinot.broker.routing.manager.BrokerRoutingManager;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.operator.blocks.InstanceResponseBlock;
import org.apache.pinot.core.operator.blocks.results.AggregationResultsBlock;
import org.apache.pinot.core.query.executor.ServerQueryExecutorV1Impl;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.routing.RoutingTable;
import org.apache.pinot.core.routing.SegmentsToQuery;
import org.apache.pinot.core.routing.TableRouteInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.accounting.ThreadAccountantUtils;
import org.apache.pinot.spi.auth.AuthorizationResult;
import org.apache.pinot.spi.auth.TableAuthorizationResult;
import org.apache.pinot.spi.auth.TableRowColAccessResult;
import org.apache.pinot.spi.auth.TableRowColAccessResultImpl;
import org.apache.pinot.spi.auth.broker.RequesterIdentity;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.QueryConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.eventlistener.query.BrokerQueryEventListenerFactory;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.rewriter.RlsUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// Exercises broker expression overrides and server RLS enforcement together, without starting a cluster.
/// The shared immutable segment is read only; the tests run sequentially because broker metrics are process global.
@Test(singleThreaded = true)
public class RowLevelSecurityExpressionOverrideTest {
  private static final String RAW_TABLE_NAME = "rlsExpressionOverride";
  private static final String OFFLINE_TABLE_NAME = RAW_TABLE_NAME + "_OFFLINE";
  private static final String SEGMENT_NAME = "tenants";
  private static final String POLICY = "lower(rawTenant) = 'a'";
  private static final int NUM_ROWS = 8;

  private File _indexDir;
  private ImmutableSegment _segment;
  private Schema _schema;
  private TableConfig _tableConfig;
  private ExecutorService _queryExecutorService;
  private ServerQueryExecutorV1Impl _server;
  private ServerMetrics _serverMetrics;

  @BeforeClass
  public void setUp()
      throws Exception {
    _indexDir = Files.createTempDirectory("RowLevelSecurityExpressionOverrideTest").toFile();
    _schema = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
        .addSingleValueDimension("rawTenant", DataType.STRING)
        .addSingleValueDimension("tenantLower", DataType.STRING)
        .addSingleValueDimension("payload", DataType.STRING)
        .build();
    _tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setInvertedIndexColumns(List.of("rawTenant", "tenantLower"))
        .setFieldConfigList(List.of(new FieldConfig("rawTenant", FieldConfig.EncodingType.DICTIONARY, List.of(), null,
            Map.of(FieldConfig.FORWARD_INDEX_DISABLED, "true"))))
        .setQueryConfig(new QueryConfig(null, null, null, Map.of("lower(rawTenant)", "tenantLower"), null, null))
        .build();
    List<GenericRow> rows = new ArrayList<>();
    for (int i = 0; i < NUM_ROWS; i++) {
      boolean visible = i % 2 == 0;
      GenericRow row = new GenericRow();
      row.putValue("rawTenant", visible ? "A" : "B");
      row.putValue("tenantLower", visible ? "a" : "b");
      row.putValue("payload", visible ? Integer.toString(i + 1) : "hidden-not-a-number-" + i);
      rows.add(row);
    }
    SegmentGeneratorConfig generatorConfig = new SegmentGeneratorConfig(_tableConfig, _schema);
    generatorConfig.setTableName(RAW_TABLE_NAME);
    generatorConfig.setSegmentName(SEGMENT_NAME);
    generatorConfig.setOutDir(_indexDir.getPath());
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(generatorConfig, new GenericRowRecordReader(rows));
    driver.build();
    _segment = ImmutableSegmentLoader.load(new File(_indexDir, SEGMENT_NAME),
        new IndexLoadingConfig(_tableConfig, _schema));
    assertNull(_segment.getForwardIndex("rawTenant"), "The RLS expression must require its derived-column override");
    assertNotNull(_segment.getInvertedIndex("tenantLower"));

    // Mock segment ownership only. The server builds and executes the actual filter and aggregation plan.
    SegmentDataManager segmentDataManager = mock(SegmentDataManager.class);
    when(segmentDataManager.getSegment()).thenReturn(_segment);
    when(segmentDataManager.getSegmentName()).thenReturn(SEGMENT_NAME);
    TableDataManager tableDataManager = mock(TableDataManager.class);
    when(tableDataManager.getCachedTableConfigAndSchema()).thenReturn(Pair.of(_tableConfig, _schema));
    when(tableDataManager.acquireSegments(anyList(), any(), anyList()))
        .thenAnswer(_ -> new ArrayList<>(List.of(segmentDataManager)));
    when(tableDataManager.getSegmentContexts(anyList(), anyMap())).thenAnswer(invocation -> {
      List<IndexSegment> selectedSegments = invocation.getArgument(0);
      List<SegmentContext> contexts = new ArrayList<>(selectedSegments.size());
      for (IndexSegment segment : selectedSegments) {
        contexts.add(new SegmentContext(segment));
      }
      return contexts;
    });
    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);
    when(instanceDataManager.getTableDataManager(OFFLINE_TABLE_NAME)).thenReturn(tableDataManager);
    _serverMetrics = mock(ServerMetrics.class);
    ServerMetrics.register(_serverMetrics);
    _server = new ServerQueryExecutorV1Impl();
    _server.init(new PinotConfiguration(), instanceDataManager, _serverMetrics);
    _queryExecutorService = QueryThreadContext.contextAwareExecutorService(Executors.newSingleThreadExecutor());
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() {
    if (_queryExecutorService != null) {
      _queryExecutorService.shutdownNow();
    }
    if (_segment != null) {
      _segment.destroy();
    }
    FileUtils.deleteQuietly(_indexDir);
  }

  @Test
  public void testBrokerOverrideAlsoAppliesToServerRowFilter()
      throws Exception {
    BrokerRequest request = captureBrokerRequest();
    // Keep the existing wire contract: the policy option is unmodified even though the WHERE copy is overridden.
    assertEquals(request.getPinotQuery().getQueryOptions().get(RlsUtils.buildRlsFilterKey(RAW_TABLE_NAME)),
        "( " + POLICY + " )");
    assertTrue(request.getPinotQuery().getFilterExpression().toString().contains("tenantLower"));
    assertVisibleCount(request);
  }

  @Test
  public void testServerAcceptsOriginalPolicyFromOlderBroker() {
    // An older broker already overrides the WHERE clause, but sends the original policy in its query option.
    BrokerRequest request = CalciteSqlCompiler.compileToBrokerRequest("SELECT COUNT(*) FROM " + OFFLINE_TABLE_NAME
        + " WHERE tenantLower = 'a' AND CAST(payload AS INT) > 0");
    request.getPinotQuery().setQueryOptions(Map.of(RlsUtils.buildRlsFilterKey(RAW_TABLE_NAME), POLICY));
    assertVisibleCount(request);
  }

  private void assertVisibleCount(BrokerRequest request) {
    InstanceRequest instanceRequest = new InstanceRequest(0L, request);
    instanceRequest.setSearchSegments(List.of(SEGMENT_NAME));
    ServerQueryRequest serverRequest =
        new ServerQueryRequest(instanceRequest, _serverMetrics, System.currentTimeMillis());
    InstanceResponseBlock response;
    try (QueryThreadContext ignored = QueryThreadContext.openForSseTest()) {
      response = _server.execute(serverRequest, _queryExecutorService);
    }
    assertTrue(response.getExceptions().isEmpty(), response.getExceptions().toString());
    assertTrue(response.getResultsBlock() instanceof AggregationResultsBlock);
    assertEquals(((AggregationResultsBlock) response.getResultsBlock()).getResults().get(0), (long) NUM_ROWS / 2);
  }

  private BrokerRequest captureBrokerRequest()
      throws Exception {
    TableCache tableCache = mock(TableCache.class);
    when(tableCache.getActualTableName(RAW_TABLE_NAME)).thenReturn(RAW_TABLE_NAME);
    when(tableCache.getSchema(RAW_TABLE_NAME)).thenReturn(_schema);
    when(tableCache.getTableConfig(OFFLINE_TABLE_NAME)).thenReturn(_tableConfig);
    when(tableCache.getColumnNameMap(anyString())).thenReturn(
        Map.of("rawTenant", "rawTenant", "tenantLower", "tenantLower", "payload", "payload"));
    Map<Expression, Expression> overrides = Map.of(CalciteSqlParser.compileToExpression("lower(rawTenant)"),
        CalciteSqlParser.compileToExpression("tenantLower"));
    when(tableCache.getExpressionOverrideMap(OFFLINE_TABLE_NAME)).thenReturn(overrides);
    BrokerRoutingManager routingManager = mock(BrokerRoutingManager.class);
    when(routingManager.routingExists(OFFLINE_TABLE_NAME)).thenReturn(true);
    when(routingManager.getQueryTimeoutMs(anyString())).thenReturn(10000L);
    RoutingTable routingTable = mock(RoutingTable.class);
    when(routingTable.getServerInstanceToSegmentsMap()).thenReturn(
        Map.of(new ServerInstance(new InstanceConfig("server01_9000")),
            new SegmentsToQuery(List.of(SEGMENT_NAME), List.of())));
    when(routingManager.getRoutingTable(any(), anyLong())).thenReturn(routingTable);
    QueryQuotaManager quotaManager = mock(QueryQuotaManager.class);
    when(quotaManager.acquire(anyString())).thenReturn(true);
    when(quotaManager.acquireDatabase(anyString())).thenReturn(true);
    when(quotaManager.acquireApplication(anyString())).thenReturn(true);
    AccessControl accessControl = new AccessControl() {
      @Override
      public AuthorizationResult authorize(RequesterIdentity identity, BrokerRequest request) {
        return TableAuthorizationResult.success();
      }

      @Override
      public TableAuthorizationResult authorize(RequesterIdentity identity, Set<String> tables) {
        return TableAuthorizationResult.success();
      }

      @Override
      public TableRowColAccessResult getRowColFilters(RequesterIdentity identity, String tableWithType) {
        return new TableRowColAccessResultImpl(List.of(POLICY));
      }
    };
    AccessControlFactory accessControlFactory = mock(AccessControlFactory.class);
    when(accessControlFactory.create()).thenReturn(accessControl);
    BrokerMetrics.register(mock(BrokerMetrics.class));
    PinotConfiguration config = new PinotConfiguration(
        Map.of(Broker.CONFIG_OF_BROKER_ENABLE_ROW_COLUMN_LEVEL_AUTH, "true"));
    BrokerQueryEventListenerFactory.init(config);
    AtomicReference<BrokerRequest> captured = new AtomicReference<>();
    BaseSingleStageBrokerRequestHandler handler =
        new BaseSingleStageBrokerRequestHandler(config, "broker1", new BrokerRequestIdGenerator(), routingManager,
            accessControlFactory, quotaManager, tableCache, ThreadAccountantUtils.getNoOpAccountant(), null) {
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
            captured.set(new BrokerRequest(serverBrokerRequest));
            return BrokerResponseNative.empty();
          }
        };
    BrokerResponseNative response = (BrokerResponseNative) handler.handleRequest(
        "SELECT COUNT(*) FROM " + RAW_TABLE_NAME + " WHERE CAST(payload AS INT) > 0");
    assertTrue(response.getExceptions().isEmpty(), response.getExceptions().toString());
    assertTrue(response.getRLSFiltersApplied());
    assertNotNull(captured.get(), "The real broker must route the query to the server");
    return captured.get();
  }
}
