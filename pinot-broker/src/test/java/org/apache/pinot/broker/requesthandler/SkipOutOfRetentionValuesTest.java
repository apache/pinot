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
import java.util.concurrent.atomic.AtomicReference;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.broker.broker.AllowAllAccessControlFactory;
import org.apache.pinot.broker.queryquota.QueryQuotaManager;
import org.apache.pinot.broker.routing.manager.BrokerRoutingManager;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.core.routing.RoutingTable;
import org.apache.pinot.core.routing.SegmentsToQuery;
import org.apache.pinot.core.routing.TableRouteInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.spi.accounting.ThreadAccountantUtils;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TenantConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.eventlistener.query.BrokerQueryEventListenerFactory;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.utils.CommonConstants.Query.Range;
import org.apache.pinot.sql.FilterKind;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Tests for the {@code skipOutOfRetentionValues} query option in {@link BaseBrokerRequestHandler}.
 *
 * <p>When the option is set the broker injects a lower-bound time filter derived from the table's
 * retention config before the query reaches any server. Tests verify the injected filter's
 * structure and value, and the skip conditions (option absent, no retention config, missing time
 * column in schema, ORDER BY wrapper, existing WHERE clause).
 */
public class SkipOutOfRetentionValuesTest {

  private static final String RAW_TABLE = "myTable";
  private static final String OFFLINE_TABLE = "myTable_OFFLINE";
  private static final String TIME_COLUMN = "eventTime";
  private static final int RETENTION_DAYS = 30;
  private static final long RETENTION_MS = RETENTION_DAYS * 24L * 3600 * 1000;

  /** Allowed delta between expected and actual cutoff to absorb test execution time. */
  private static final long CUTOFF_TOLERANCE_MS = 5_000L;

  @Test
  public void testOptionNotSetNoFilterInjected()
      throws Exception {
    AtomicReference<BrokerRequest> captured = new AtomicReference<>();
    BaseSingleStageBrokerRequestHandler handler = createHandler(buildTableCache(true, true), captured);

    handler.handleRequest("SELECT * FROM " + RAW_TABLE);

    BrokerRequest req = captured.get();
    Assert.assertNotNull(req);
    Assert.assertNull(req.getPinotQuery().getFilterExpression(),
        "No filter should be injected when skipOutOfRetentionValues is not set");
  }

  @Test
  public void testOptionSetNumericEpochColumnInjectsLowerBoundFilter()
      throws Exception {
    AtomicReference<BrokerRequest> captured = new AtomicReference<>();
    BaseSingleStageBrokerRequestHandler handler = createHandler(buildTableCache(true, true), captured);

    long beforeMs = System.currentTimeMillis();
    handler.handleRequest("SET skipOutOfRetentionValues='true'; SELECT * FROM " + RAW_TABLE);
    long afterMs = System.currentTimeMillis();

    BrokerRequest req = captured.get();
    Assert.assertNotNull(req);
    Expression filter = req.getPinotQuery().getFilterExpression();
    Assert.assertNotNull(filter, "Filter should be injected when option is set");
    Assert.assertTrue(filterContainsColumn(filter, TIME_COLUMN),
        "Injected filter must reference the time column");

    long cutoff = extractLowerBound(filter, TIME_COLUMN);
    long expectedMin = beforeMs - RETENTION_MS - CUTOFF_TOLERANCE_MS;
    long expectedMax = afterMs - RETENTION_MS + CUTOFF_TOLERANCE_MS;
    Assert.assertTrue(cutoff >= expectedMin && cutoff <= expectedMax,
        "Cutoff must be approximately now - " + RETENTION_DAYS + " days; got " + cutoff
            + " expected in [" + expectedMin + ", " + expectedMax + "]");
  }

  @Test
  public void testOptionSetExistingWhereClauseFilterAndedIn()
      throws Exception {
    AtomicReference<BrokerRequest> captured = new AtomicReference<>();
    BaseSingleStageBrokerRequestHandler handler = createHandler(buildTableCache(true, true), captured);

    handler.handleRequest(
        "SET skipOutOfRetentionValues='true'; SELECT * FROM " + RAW_TABLE + " WHERE col = 'foo'");

    BrokerRequest req = captured.get();
    Assert.assertNotNull(req);
    Expression filter = req.getPinotQuery().getFilterExpression();
    Assert.assertNotNull(filter, "Filter should exist");

    Function func = filter.getFunctionCall();
    Assert.assertNotNull(func, "Filter must be a function call");
    Assert.assertEquals(func.getOperator(), FilterKind.AND.name(),
        "Existing WHERE and the injected retention filter must be combined under AND");
    Assert.assertEquals(func.getOperands().size(), 2,
        "AND must have exactly two operands");
    Assert.assertTrue(filterContainsColumn(filter, TIME_COLUMN),
        "Combined filter must include the time-column retention predicate");
  }

  @Test
  public void testOptionSetOrderByQueryFilterInjected()
      throws Exception {
    AtomicReference<BrokerRequest> captured = new AtomicReference<>();
    BaseSingleStageBrokerRequestHandler handler = createHandler(buildTableCache(true, true), captured);

    // ORDER BY causes the Calcite AST root to be SqlOrderBy, not SqlSelect.
    // The broker must unwrap it before injecting the filter.
    handler.handleRequest("SET skipOutOfRetentionValues='true'; SELECT * FROM " + RAW_TABLE
        + " ORDER BY " + TIME_COLUMN + " DESC LIMIT 10");

    BrokerRequest req = captured.get();
    Assert.assertNotNull(req);
    Expression filter = req.getPinotQuery().getFilterExpression();
    Assert.assertNotNull(filter, "Filter must be injected even for ORDER BY queries");
    Assert.assertTrue(filterContainsColumn(filter, TIME_COLUMN),
        "Injected filter must reference the time column");
  }

  @Test
  public void testOptionSetNoRetentionConfiguredNoFilterInjected()
      throws Exception {
    AtomicReference<BrokerRequest> captured = new AtomicReference<>();
    // Table config has no retention values set
    BaseSingleStageBrokerRequestHandler handler = createHandler(buildTableCache(false, true), captured);

    handler.handleRequest("SET skipOutOfRetentionValues='true'; SELECT * FROM " + RAW_TABLE);

    BrokerRequest req = captured.get();
    Assert.assertNotNull(req);
    Assert.assertNull(req.getPinotQuery().getFilterExpression(),
        "No filter should be injected when table has no retention configuration");
  }

  @Test
  public void testOptionSetTimeColumnMissingFromSchemaNoFilterInjected()
      throws Exception {
    AtomicReference<BrokerRequest> captured = new AtomicReference<>();
    // Table config declares a time column and retention, but the schema has no such column
    BaseSingleStageBrokerRequestHandler handler = createHandler(buildTableCache(true, false), captured);

    handler.handleRequest("SET skipOutOfRetentionValues='true'; SELECT * FROM " + RAW_TABLE);

    BrokerRequest req = captured.get();
    Assert.assertNotNull(req);
    Assert.assertNull(req.getPinotQuery().getFilterExpression(),
        "No filter should be injected when the time column is absent from the schema");
  }

  // ---------------------------------------------------------------------------
  // Infrastructure helpers
  // ---------------------------------------------------------------------------

  /**
   * Builds a mock {@link TableCache} for {@value RAW_TABLE}.
   *
   * @param withRetention when {@code true} the validation config carries a 30-day retention on
   *     {@value TIME_COLUMN}; when {@code false} the retention fields are left empty
   * @param withTimeColumnInSchema when {@code true} the schema includes the {@value TIME_COLUMN}
   *     DateTime column; when {@code false} only {@code col} is present
   */
  private static TableCache buildTableCache(boolean withRetention, boolean withTimeColumnInSchema) {
    Schema.SchemaBuilder schemaBuilder = new Schema.SchemaBuilder()
        .setSchemaName(RAW_TABLE)
        .addSingleValueDimension("col", DataType.STRING);
    if (withTimeColumnInSchema) {
      schemaBuilder.addDateTime(TIME_COLUMN, DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS");
    }
    Schema schema = schemaBuilder.build();

    Map<String, String> columnMap = withTimeColumnInSchema
        ? Map.of(TIME_COLUMN, TIME_COLUMN, "col", "col")
        : Map.of("col", "col");

    SegmentsValidationAndRetentionConfig validationConfig = new SegmentsValidationAndRetentionConfig();
    if (withRetention) {
      validationConfig.setTimeColumnName(TIME_COLUMN);
      validationConfig.setRetentionTimeUnit("DAYS");
      validationConfig.setRetentionTimeValue(String.valueOf(RETENTION_DAYS));
    }

    TableConfig tableConfig = mock(TableConfig.class);
    when(tableConfig.getTenantConfig()).thenReturn(new TenantConfig("tier_BROKER", "tier_SERVER", null));
    when(tableConfig.getValidationConfig()).thenReturn(validationConfig);

    TableCache tableCache = mock(TableCache.class);
    when(tableCache.getActualTableName(RAW_TABLE)).thenReturn(RAW_TABLE);
    when(tableCache.getSchema(RAW_TABLE)).thenReturn(schema);
    when(tableCache.getColumnNameMap(anyString())).thenReturn(columnMap);
    // retention lookup uses raw name; routing lookup uses type-suffixed name
    when(tableCache.getTableConfig(RAW_TABLE)).thenReturn(tableConfig);
    when(tableCache.getTableConfig(OFFLINE_TABLE)).thenReturn(tableConfig);
    return tableCache;
  }

  private static BaseSingleStageBrokerRequestHandler createHandler(TableCache tableCache,
      AtomicReference<BrokerRequest> capturedServerRequest) {
    BrokerRoutingManager routingManager = mock(BrokerRoutingManager.class);
    when(routingManager.routingExists(OFFLINE_TABLE)).thenReturn(true);
    when(routingManager.getQueryTimeoutMs(anyString())).thenReturn(10_000L);
    RoutingTable rt = mock(RoutingTable.class);
    when(rt.getServerInstanceToSegmentsMap()).thenReturn(
        Map.of(new ServerInstance(new InstanceConfig("server01_9000")),
            new SegmentsToQuery(List.of("seg01"), List.of())));
    when(routingManager.getRoutingTable(any(), Mockito.anyLong())).thenReturn(rt);

    QueryQuotaManager quotaManager = mock(QueryQuotaManager.class);
    when(quotaManager.acquire(anyString())).thenReturn(true);
    when(quotaManager.acquireDatabase(anyString())).thenReturn(true);
    when(quotaManager.acquireApplication(anyString())).thenReturn(true);

    BrokerMetrics.register(mock(BrokerMetrics.class));
    PinotConfiguration config = new PinotConfiguration();
    BrokerQueryEventListenerFactory.init(config);

    return new BaseSingleStageBrokerRequestHandler(config, "testBroker", new BrokerRequestIdGenerator(),
        routingManager, new AllowAllAccessControlFactory(), quotaManager, tableCache,
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
        capturedServerRequest.set(serverBrokerRequest);
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
  }

  /**
   * Returns true if {@code expr} or any of its descendants references {@code columnName}.
   */
  private static boolean filterContainsColumn(Expression expr, String columnName) {
    if (expr == null) {
      return false;
    }
    if (expr.getIdentifier() != null && columnName.equals(expr.getIdentifier().getName())) {
      return true;
    }
    if (expr.getFunctionCall() != null) {
      for (Expression operand : expr.getFunctionCall().getOperands()) {
        if (filterContainsColumn(operand, columnName)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Finds the retention lower-bound predicate on {@code timeColumn} inside {@code filter} and
   * returns the cutoff as an epoch-ms long.
   *
   * <p>A standalone {@code >=} injected by the broker stays as {@code GREATER_THAN_OR_EQUAL} in
   * the PinotQuery; Pinot only rewrites to a {@code RANGE} when merging two-sided bounds (e.g.
   * {@code BETWEEN}, a time-boundary merge). Both forms are handled here.
   */
  private static long extractLowerBound(Expression filter, String timeColumn) {
    Assert.assertNotNull(filter, "filter must not be null");
    Function func = filter.getFunctionCall();
    Assert.assertNotNull(func, "filter must be a function call");

    if (FilterKind.AND.name().equals(func.getOperator())) {
      for (Expression operand : func.getOperands()) {
        Function fn = operand.getFunctionCall();
        if (fn != null && !fn.getOperands().isEmpty()
            && timeColumn.equals(fn.getOperands().get(0).getIdentifier().getName())) {
          if (FilterKind.RANGE.name().equals(fn.getOperator())) {
            return parseLowerBoundFromRange(fn.getOperands().get(1).getLiteral().getStringValue());
          }
          if (FilterKind.GREATER_THAN_OR_EQUAL.name().equals(fn.getOperator())) {
            return extractLongLiteral(fn.getOperands().get(1));
          }
        }
      }
      throw new AssertionError(
          "No RANGE or GREATER_THAN_OR_EQUAL predicate found on " + timeColumn + " inside AND");
    }

    Assert.assertEquals(func.getOperands().get(0).getIdentifier().getName(), timeColumn,
        "Predicate must be on " + timeColumn);
    if (FilterKind.RANGE.name().equals(func.getOperator())) {
      return parseLowerBoundFromRange(func.getOperands().get(1).getLiteral().getStringValue());
    }
    Assert.assertEquals(func.getOperator(), FilterKind.GREATER_THAN_OR_EQUAL.name(),
        "Expected GREATER_THAN_OR_EQUAL or RANGE but found: " + func.getOperator());
    return extractLongLiteral(func.getOperands().get(1));
  }

  /**
   * Parses the lower bound from a RANGE string of the form {@code '[value\0*)'},
   * using {@link Range#DELIMITER} as the separator between lower and upper bound.
   */
  private static long parseLowerBoundFromRange(String rangeStr) {
    int delimIdx = rangeStr.indexOf(Range.DELIMITER);
    Assert.assertTrue(delimIdx > 1, "Range string missing delimiter: " + rangeStr);
    return Long.parseLong(rangeStr.substring(1, delimIdx).trim());
  }

  /** Extracts a long value from a numeric literal expression. */
  private static long extractLongLiteral(Expression expr) {
    Assert.assertNotNull(expr.getLiteral(), "Expected a literal expression");
    if (expr.getLiteral().isSetLongValue()) {
      return expr.getLiteral().getLongValue();
    }
    // Numeric literals compiled from string (e.g. createExactNumeric) may land as stringValue
    return Long.parseLong(expr.getLiteral().getStringValue());
  }
}
