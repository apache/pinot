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

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.common.request.AggregationFunctionBinding;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.query.aggregation.AggregationFunctionBinder;
import org.apache.pinot.core.query.aggregation.GapfillAggregationFunctionBinder;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.core.util.GapfillUtils;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;


/// Covers type propagation through gapfill aliases and the formatted-result/storage-value boundary.
public class PolymorphicGapfillTest {
  private static final Schema TABLE_SCHEMA = new Schema.SchemaBuilder()
      .addSingleValueDimension("time_col", DataType.LONG)
      .addSingleValueDimension("entity", DataType.STRING)
      .addSingleValueDimension("status", DataType.BOOLEAN)
      .addSingleValueDimension("event_time", DataType.TIMESTAMP)
      .build();

  @Test
  public void testInnerBindingAndOuterAliasTypes() {
    PinotQuery query = CalciteSqlParser.compileToPinotQuery("SELECT bucket, MODE(latest) FROM (SELECT "
        + "GAPFILL(bucket, '1:MILLISECONDS:EPOCH', '0', '2000', '1000:MILLISECONDS', "
        + "FILL(latest, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(entity)) AS bucket, entity, latest FROM ("
        + "SELECT time_col AS bucket, entity, LAST_WITH_TIME(status, event_time) AS latest FROM testTable "
        + "GROUP BY bucket, entity)) GROUP BY bucket");
    PinotQuery serverQuery = GapfillUtils.stripGapfill(query);
    PinotQuery inner = query.getDataSource().getSubquery().getDataSource().getSubquery();
    assertNull(inner.getSelectList().get(2).getFunctionCall().getOperands().get(0).getFunctionCall()
        .getAggregationBinding());
    AggregationFunctionBinder.bind(serverQuery, TABLE_SCHEMA);
    DataSchema resultSchema = new DataSchema(new String[]{"time_col", "entity", "lastwithtime(status,event_time)"},
        new ColumnDataType[]{ColumnDataType.LONG, ColumnDataType.STRING, ColumnDataType.BOOLEAN});
    GapfillAggregationFunctionBinder.bind(query, serverQuery, resultSchema);
    assertNotNull(inner.getSelectList().get(2).getFunctionCall().getOperands().get(0).getFunctionCall()
        .getAggregationBinding());
    QueryContext context = QueryContextConverterUtils.getQueryContext(query);
    assertEquals(context.getAggregationFunctions()[0].getFinalResultColumnType(), ColumnDataType.BOOLEAN);
    assertEquals(context.getSubquery().getSubquery().getAggregationFunctions()[0].getFinalResultColumnType(),
        ColumnDataType.BOOLEAN);
  }

  @Test
  public void testLegacyAggregateReplacement() {
    PinotQuery query = CalciteSqlParser.compileToPinotQuery("SELECT GAPFILL(time_col, '1:MILLISECONDS:EPOCH', "
        + "'0', '2000', '1000:MILLISECONDS', TIMESERIESON(entity)) AS time_col, entity, total FROM ("
        + "SELECT time_col, entity, SUM(value) AS total FROM testTable GROUP BY time_col, entity)");
    PinotQuery serverQuery = GapfillUtils.stripGapfill(query);
    // A configured expression override can replace a legacy aggregate with a materialized column.
    serverQuery.getSelectList().get(2).getFunctionCall().getOperands().set(0,
        CalciteSqlParser.compileToExpression("precomputed_total"));
    DataSchema schema = new DataSchema(new String[]{"time_col", "entity", "total"},
        new ColumnDataType[]{ColumnDataType.LONG, ColumnDataType.STRING, ColumnDataType.DOUBLE});
    GapfillAggregationFunctionBinder.bind(query, serverQuery, schema);
    assertEquals(QueryContextConverterUtils.getQueryContext(query).getSubquery().getAggregationFunctions()[0]
        .getFinalResultColumnType(), ColumnDataType.DOUBLE);
  }

  @Test
  public void testTypedAggregateReplacementPreservesEarlyBinding() {
    PinotQuery query = CalciteSqlParser.compileToPinotQuery("SELECT GAPFILL(time_col, '1:MILLISECONDS:EPOCH', "
        + "'0', '2000', '1000:MILLISECONDS', TIMESERIESON(entity)) AS time_col, entity, earliest, latest FROM ("
        + "SELECT time_col, entity, FIRST_WITH_TIME(status, event_time) AS earliest, "
        + "LAST_WITH_TIME(status, event_time) AS latest "
        + "FROM testTable GROUP BY time_col, entity)");
    PinotQuery serverQuery = GapfillUtils.stripGapfill(query);
    AggregationFunctionBinder.bind(serverQuery, TABLE_SCHEMA);
    GapfillAggregationFunctionBinder.bind(query, serverQuery, null);
    Function original = query.getDataSource().getSubquery().getSelectList().get(2).getFunctionCall()
        .getOperands().get(0).getFunctionCall();
    AggregationFunctionBinding binding = original.getAggregationBinding();
    assertNotNull(binding);
    serverQuery.getSelectList().get(2).getFunctionCall().getOperands().set(0,
        CalciteSqlParser.compileToExpression("precomputed_earliest"));
    DataSchema schema = new DataSchema(new String[]{"time_col", "entity", "earliest", "latest"},
        new ColumnDataType[]{ColumnDataType.LONG, ColumnDataType.STRING, ColumnDataType.BOOLEAN,
            ColumnDataType.BOOLEAN});
    GapfillAggregationFunctionBinder.bind(query, serverQuery, schema);
    assertSame(original.getAggregationBinding(), binding);
    assertEquals(QueryContextConverterUtils.getQueryContext(query).getSubquery().getAggregationFunctions()[0]
        .getFinalResultColumnType(), ColumnDataType.BOOLEAN);
  }

  @Test
  public void testLogicalValuesAndEmptyInput() {
    String sql = "SELECT time_col, MODE(status), MODE(event_time), FIRST_WITH_TIME(status, event_time) FROM (SELECT "
        + "GAPFILL(time_col, '1:MILLISECONDS:EPOCH', '0', '3000', '1000:MILLISECONDS', "
        + "TIMESERIESON(entity)) AS time_col, entity, status, event_time FROM testTable) GROUP BY time_col";
    for (String input : List.of("values", "nulls", "empty", "having")) {
      boolean empty = input.equals("empty");
      boolean nulls = input.equals("nulls");
      boolean having = input.equals("having");
      PinotQuery query = CalciteSqlParser.compileToPinotQuery(sql
          + (having ? " HAVING MODE(event_time) > '" + new Timestamp(1000L) + "'" : "")
          + " LIMIT 10 OPTION(enableNullHandling=true)");
      PinotQuery serverQuery = GapfillUtils.stripGapfill(query);
      DataSchema schema = new DataSchema(new String[]{"time_col", "entity", "status", "event_time"},
          new ColumnDataType[]{ColumnDataType.LONG, ColumnDataType.STRING, ColumnDataType.BOOLEAN,
              ColumnDataType.TIMESTAMP});
      GapfillAggregationFunctionBinder.bind(query, serverQuery, schema);
      QueryContext context = QueryContextConverterUtils.getQueryContext(query);
      List<Object[]> rows = empty ? List.of() : new ArrayList<>(List.of(
          new Object[]{0L, "a", nulls ? null : false, nulls ? null : new Timestamp(100L).toString()},
          new Object[]{2000L, "a", nulls ? null : true, nulls ? null : new Timestamp(2100L).toString()}));
      BrokerResponseNative response = new BrokerResponseNative();
      response.setResultTable(new ResultTable(schema, rows));
      GapfillProcessorFactory.getGapfillProcessor(context, GapfillUtils.getGapfillType(context)).process(response);
      ResultTable result = response.getResultTable();
      assertEquals(result.getDataSchema().getColumnDataTypes(), new ColumnDataType[]{ColumnDataType.LONG,
          ColumnDataType.BOOLEAN, ColumnDataType.TIMESTAMP, ColumnDataType.BOOLEAN});
      if (empty) {
        assertEquals(result.getRows().size(), 0);
      } else if (having) {
        assertEquals(result.getRows().size(), 1);
        assertEquals(result.getRows().get(0), new Object[]{2000L, true, new Timestamp(2100L).toString(), true});
      } else {
        assertEquals(result.getRows().size(), 3);
        assertEquals(result.getRows().get(0), nulls
            ? new Object[]{0L, null, null, null}
            : new Object[]{0L, false, new Timestamp(100L).toString(), false});
        assertEquals(result.getRows().get(1), new Object[]{1000L, false, new Timestamp(0L).toString(), false});
        assertEquals(result.getRows().get(2), nulls
            ? new Object[]{2000L, null, null, null}
            : new Object[]{2000L, true, new Timestamp(2100L).toString(), true});
        // Restoring storage values for aggregation must not mutate the inner-query response.
        assertEquals(rows.get(0)[2], nulls ? null : false);
        assertEquals(rows.get(0)[3], nulls ? null : new Timestamp(100L).toString());
      }
    }
  }
}
