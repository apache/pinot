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
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryException;
import org.apache.pinot.tsdb.spi.plan.LeafTimeSeriesPlanNode;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.lenient;
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
