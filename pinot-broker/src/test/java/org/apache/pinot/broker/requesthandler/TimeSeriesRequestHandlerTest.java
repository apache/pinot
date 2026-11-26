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

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import javax.ws.rs.core.HttpHeaders;
import org.apache.pinot.broker.api.AccessControl;
import org.apache.pinot.broker.broker.AccessControlFactory;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.spi.auth.BasicAuthorizationResultImpl;
import org.apache.pinot.spi.auth.TableAuthorizationResult;
import org.apache.pinot.spi.auth.TableRowColAccessResultImpl;
import org.apache.pinot.spi.auth.broker.RequesterIdentity;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.eventlistener.query.BrokerQueryEventListenerFactory;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryException;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.tsdb.planner.TimeSeriesQueryEnvironment;
import org.apache.pinot.tsdb.spi.PinotTimeSeriesConfiguration;
import org.apache.pinot.tsdb.spi.TimeBuckets;
import org.apache.pinot.tsdb.spi.TimeSeriesLogicalPlanResult;
import org.apache.pinot.tsdb.spi.plan.BaseTimeSeriesPlanNode;
import org.apache.pinot.tsdb.spi.plan.LeafTimeSeriesPlanNode;
import org.apache.pinot.tsdb.spi.series.SimpleTimeSeriesBuilderFactory;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;


public class TimeSeriesRequestHandlerTest {

  private AutoCloseable _mocks;

  @Mock
  private LeafTimeSeriesPlanNode _leafNode;
  @Mock
  private Schema _tableSchema;

  @BeforeMethod
  public void setUp() {
    _mocks = MockitoAnnotations.openMocks(this);

    // Set up default mock behaviors
    lenient().when(_leafNode.getTableName()).thenReturn("metrics_REALTIME");
    lenient().when(_tableSchema.hasColumn("ts")).thenReturn(true);
    lenient().when(_tableSchema.hasColumn("colA")).thenReturn(true);
    lenient().when(_tableSchema.hasColumn("colB")).thenReturn(true);
    lenient().when(_tableSchema.hasColumn("colC")).thenReturn(true);
  }

  @DataProvider
  public Object[][] timeSeriesRequests() {
    return new Object[][]{{2, false}, {0, false}, {2, true}};
  }

  @Test(dataProvider = "timeSeriesRequests")
  public void testAccessDenialsKeepTheirErrorCode(int numBuckets, boolean explain) {
    PinotConfiguration config = new PinotConfiguration(Map.of(
        CommonConstants.Broker.CONFIG_OF_BROKER_ENABLE_ROW_COLUMN_LEVEL_AUTH, true,
        PinotTimeSeriesConfiguration.getEnabledLanguagesConfigKey(), "test",
        PinotTimeSeriesConfiguration.getSeriesBuilderFactoryConfigKey("test"),
        SimpleTimeSeriesBuilderFactory.class.getName()));
    BrokerQueryEventListenerFactory.init(config);
    AccessControl access = mock(AccessControl.class);
    AccessControlFactory factory = mock(AccessControlFactory.class);
    when(factory.create()).thenReturn(access);
    RequesterIdentity identity = mock(RequesterIdentity.class);
    HttpHeaders headers = mock(HttpHeaders.class);
    when(access.authorize(identity)).thenReturn(new BasicAuthorizationResultImpl(false, "broker denial"),
        BasicAuthorizationResultImpl.success());
    when(access.authorize(identity, Set.of("metrics_REALTIME", "other")))
        .thenReturn(new TableAuthorizationResult(Set.of("other")), TableAuthorizationResult.success());
    when(access.authorize(identity, Set.of("metrics"))).thenReturn(new TableAuthorizationResult(Set.of("metrics")));
    when(access.authorize(identity, Set.of("other_OFFLINE"))).thenReturn(TableAuthorizationResult.success());
    when(access.authorize(headers, TargetType.TABLE, "metrics_REALTIME", Actions.Table.QUERY))
        .thenReturn(BasicAuthorizationResultImpl.success());
    when(access.authorize(headers, TargetType.TABLE, "other_OFFLINE", Actions.Table.QUERY))
        .thenReturn(new BasicAuthorizationResultImpl(false, "table denial"), BasicAuthorizationResultImpl.success());
    when(access.getRowColFilters(eq(identity), anyString())).thenReturn(TableRowColAccessResultImpl.unrestricted());
    when(access.getRowColFilters(identity, "other_OFFLINE"))
        .thenReturn(new TableRowColAccessResultImpl(List.of("region = 'US'")));

    try (MockedConstruction<TimeSeriesQueryEnvironment> environments =
        mockConstruction(TimeSeriesQueryEnvironment.class)) {
      TimeSeriesRequestHandler handler = new TimeSeriesRequestHandler(config, "broker", new BrokerRequestIdGenerator(),
          null, factory, null, null, null, null);
      TimeSeriesQueryEnvironment planner = environments.constructed().get(0);
      LeafTimeSeriesPlanNode otherLeaf = mock(LeafTimeSeriesPlanNode.class);
      when(otherLeaf.getTableName()).thenReturn("other");
      BaseTimeSeriesPlanNode root = mock(BaseTimeSeriesPlanNode.class);
      when(root.getInputs()).thenReturn(List.of(_leafNode, otherLeaf));
      when(planner.buildLogicalPlan(any())).thenReturn(new TimeSeriesLogicalPlanResult(root,
          TimeBuckets.ofSeconds(0L, Duration.ofSeconds(1), numBuckets)));
      when(planner.resolveAndAuthorizeTables(any(), any(), any())).thenAnswer(invocation -> {
        invocation.<Consumer<List<String>>>getArgument(2).accept(List.of("metrics_REALTIME", "other_OFFLINE"));
        return invocation.getArgument(0);
      });
      when(planner.buildPhysicalPlan(any(), any(), any(), any())).thenAnswer(invocation -> {
        planner.resolveAndAuthorizeTables(invocation.getArgument(2), invocation.getArgument(1),
            invocation.getArgument(3));
        throw new AssertionError("Routing reached after table denial");
      });

      Map<String, String> params = Map.of("query", "metric", "start", "0", "end", "2");
      for (String reason : List.of("broker denial", "Authorization Failed for tables: [other]", "table denial",
          "row filters")) {
        QueryException error = expectThrows(QueryException.class, () -> {
          if (explain) {
            handler.handleExplainTimeSeriesRequest("test", "", params, identity, headers);
          } else {
            handler.handleTimeSeriesRequest("test", "", params, mock(RequestContext.class), identity, headers);
          }
        });
        assertEquals(error.getErrorCode(), QueryErrorCode.ACCESS_DENIED);
        assertTrue(error.getMessage().contains(reason), error.getMessage());
      }
    }
  }

  @Test
  void testValidateColumnNamesSuccess() {
    when(_leafNode.getTimeColumn()).thenReturn("ts");
    when(_leafNode.getValueExpression()).thenReturn("colA / colB");
    when(_leafNode.getGroupByExpressions()).thenReturn(List.of(
        "colB",
        "DIMENSION(colC)",
        "case when colA > 0 then 'positive' else 'non-negative' end",
        "lower(colB)"
        ));
    when(_leafNode.getFilterExpression()).thenReturn("colB > 10 AND colA < 5 AND ts > 12345");

    try {
      TimeSeriesRequestHandler.validateColumnNames(_leafNode, _tableSchema);
    } catch (Exception e) {
      fail("Should not throw exception when all columns are valid: " + e.getMessage());
    }
  }

  @Test
  void testValidateColumnNamesSuccessWithTimeColumnExpression() {
    when(_leafNode.getTimeColumn()).thenReturn("ts / 1000");
    when(_leafNode.getValueExpression()).thenReturn("colA + 100");
    when(_leafNode.getGroupByExpressions()).thenReturn(List.of(
        "colB",
        "DIMENSION(colC)",
        "case when colA > 0 then 'positive' else 'non-negative' end",
        "lower(colB)"
    ));
    when(_leafNode.getFilterExpression()).thenReturn("colB > 10 AND colA < 5 AND ts > 12345");

    try {
      TimeSeriesRequestHandler.validateColumnNames(_leafNode, _tableSchema);
    } catch (Exception e) {
      fail("Should not throw exception when all columns are valid: " + e.getMessage());
    }
  }

  @Test
  public void testValidateColumnNamesWhenInvalidTimeColumn() {
    when(_leafNode.getTimeColumn()).thenReturn("invalid_ts_column");
    when(_tableSchema.hasColumn("invalid_ts_column")).thenReturn(false);

    try {
      TimeSeriesRequestHandler.validateColumnNames(_leafNode, _tableSchema);
      fail("Expected a QueryException to be thrown");
    } catch (QueryException e) {
      assertEquals(e.getErrorCode(), QueryErrorCode.UNKNOWN_COLUMN);
      assertTrue(e.getMessage().contains("Column 'invalid_ts_column' in expression 'invalid_ts_column' not found"));
    } catch (Exception e) {
      fail("Expected QueryException, but got " + e.getClass().getName(), e);
    }
  }

  @Test
  public void testValidateColumnNamesWhenInvalidValueExpressionColumn() {
    when(_leafNode.getTimeColumn()).thenReturn("ts");
    when(_leafNode.getValueExpression()).thenReturn("SUM(invalid_value_col)");
    when(_tableSchema.hasColumn("invalid_value_col")).thenReturn(false);

    try {
      TimeSeriesRequestHandler.validateColumnNames(_leafNode, _tableSchema);
      fail("Expected a QueryException to be thrown");
    } catch (QueryException e) {
      assertEquals(e.getErrorCode(), QueryErrorCode.UNKNOWN_COLUMN);
      assertTrue(e.getMessage().contains("Column 'invalid_value_col' in expression 'SUM(invalid_value_col)'"));
    } catch (Exception e) {
      fail("Expected QueryException, but got " + e.getClass().getName(), e);
    }
  }

  @Test
  public void testValidateColumnNamesWhenInvalidGroupByColumn() {
    when(_leafNode.getTimeColumn()).thenReturn("ts");
    when(_leafNode.getValueExpression()).thenReturn("SUM(colA)");
    when(_leafNode.getGroupByExpressions()).thenReturn(List.of("colB", "invalid_group_col"));
    when(_tableSchema.hasColumn("invalid_group_col")).thenReturn(false);

    try {
      TimeSeriesRequestHandler.validateColumnNames(_leafNode, _tableSchema);
      fail("Expected a QueryException to be thrown");
    } catch (QueryException e) {
      assertEquals(e.getErrorCode(), QueryErrorCode.UNKNOWN_COLUMN);
      assertTrue(e.getMessage().contains("Column 'invalid_group_col' in expression 'invalid_group_col'"));
    } catch (Exception e) {
      fail("Expected QueryException, but got " + e.getClass().getName(), e);
    }
  }

  @Test
  public void testValidateColumnNamesWhenInvalidFilterColumn() {
    when(_leafNode.getTimeColumn()).thenReturn("ts");
    when(_leafNode.getValueExpression()).thenReturn("SUM(colA)");
    when(_leafNode.getGroupByExpressions()).thenReturn(List.of("colB"));
    when(_leafNode.getFilterExpression()).thenReturn("colB > 10 AND invalid_filter_col = 'foo'");
    when(_tableSchema.hasColumn("invalid_filter_col")).thenReturn(false);

    try {
      TimeSeriesRequestHandler.validateColumnNames(_leafNode, _tableSchema);
      fail("Expected a QueryException to be thrown");
    } catch (QueryException e) {
      assertEquals(e.getErrorCode(), QueryErrorCode.UNKNOWN_COLUMN);
      assertTrue(e.getMessage().contains("Column 'invalid_filter_col' in filter expression"));
    } catch (Exception e) {
      fail("Expected QueryException, but got " + e.getClass().getName(), e);
    }
  }

  @Test
  public void testValidateColumnSuccessWhenEmptyFilterColumn() {
    when(_leafNode.getTimeColumn()).thenReturn("ts");
    when(_leafNode.getValueExpression()).thenReturn("SUM(colA)");
    when(_leafNode.getGroupByExpressions()).thenReturn(List.of("colB"));
    when(_leafNode.getFilterExpression()).thenReturn("");
    when(_tableSchema.hasColumn("invalid_filter_col")).thenReturn(false);

    try {
      TimeSeriesRequestHandler.validateColumnNames(_leafNode, _tableSchema);
    } catch (Exception e) {
      fail("Did not expect exception, but got " + e.getClass().getName(), e);
    }
  }

  @Test
  public void testValidateColumnNamesWhenComplexExpressionsAllValid() {
    when(_leafNode.getTimeColumn()).thenReturn("ts + 10");
    when(_leafNode.getValueExpression()).thenReturn("colA + colB/100");
    when(_leafNode.getGroupByExpressions()).thenReturn(List.of("DATETRUNC('day', ts)", "CONCAT(colC, '-')"));
    when(_leafNode.getFilterExpression()).thenReturn("ts > 100 AND colA != 50");

    try {
      TimeSeriesRequestHandler.validateColumnNames(_leafNode, _tableSchema);
    } catch (Exception e) {
      fail("Should pass with complex expressions if all columns are valid: " + e.getMessage());
    }
  }
}
