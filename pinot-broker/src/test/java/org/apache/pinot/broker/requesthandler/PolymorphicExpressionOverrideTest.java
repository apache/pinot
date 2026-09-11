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

import java.util.Map;
import java.util.TreeMap;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.query.aggregation.AggregationFunctionBinder;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;


/// Ensures execution bindings preserve expression override matching and logical materialized-column types.
public class PolymorphicExpressionOverrideTest {
  private static final Schema SCHEMA = new Schema.SchemaBuilder()
      .addSingleValueDimension("name", DataType.STRING)
      .addSingleValueDimension("event_time", DataType.TIMESTAMP)
      .addSingleValueDimension("stored_time", DataType.LONG)
      .build();

  @Test
  public void testOverrideIgnoresBindingAndBindsReplacement() {
    for (boolean sorted : new boolean[]{false, true}) {
      PinotQuery query = CalciteSqlParser.compileToPinotQuery("SELECT trim(MODE(name)) FROM testTable");
      AggregationFunctionBinder.bind(query, SCHEMA);
      Expression replacement = CalciteSqlParser.compileToExpression("FIRST_WITH_TIME(name,event_time)");
      Map<Expression, Expression> overrides = Map.of(
          CalciteSqlParser.compileToExpression("trim(MODE(name))"), replacement);
      if (sorted) {
        overrides = new TreeMap<>(overrides);
      }
      BaseSingleStageBrokerRequestHandler.handleExpressionOverride(query, overrides, SCHEMA);
      Expression selected = query.getSelectList().get(0);
      assertEquals(RequestContextUtils.getExpression(selected).toString(), "firstwithtime(name,event_time)");
      assertEquals(selected.getFunctionCall().getAggregationBinding().getResultType(), "STRING");
      assertFalse(replacement.getFunctionCall().isSetAggregationBinding());
    }
  }

  @Test
  public void testMaterializedStorageTypePreservesLogicalBinding() {
    PinotQuery query = CalciteSqlParser.compileToPinotQuery("SELECT MODE(event_time) FROM testTable");
    AggregationFunctionBinder.bind(query, SCHEMA);
    BaseSingleStageBrokerRequestHandler.handleExpressionOverride(query, Map.of(
        CalciteSqlParser.compileToExpression("event_time"), CalciteSqlParser.compileToExpression("stored_time")),
        SCHEMA);
    Expression selected = query.getSelectList().get(0);
    assertEquals(RequestContextUtils.getExpression(selected).toString(), "mode(stored_time)");
    assertEquals(RequestContextUtils.getExpression(selected).getFunction().getAggregationBinding().getResultType(),
        ColumnDataType.TIMESTAMP);
  }
}
