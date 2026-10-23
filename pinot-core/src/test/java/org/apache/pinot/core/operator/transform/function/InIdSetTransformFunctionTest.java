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
package org.apache.pinot.core.operator.transform.function;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.core.query.utils.idset.IdSet;
import org.apache.pinot.core.query.utils.idset.IdSets;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;


/// Tests the IN_ID_SET transform function on a segment; `IdSetQueriesTest` runs it in queries.
public class InIdSetTransformFunctionTest extends BaseTransformFunctionTest {

  @Test
  public void testSharesTheIdSetAcrossTheSegmentsOfAQuery()
      throws IOException {
    IdSet idSet = createIdSet(0, NUM_ROWS / 2);
    ExpressionContext expression = createExpression(idSet);
    QueryContext queryContext = createQueryContext();

    // Each segment of a query builds its own transform function, but they share one deserialized IdSet
    InIdSetTransformFunction segment1Function = createTransformFunction(expression, queryContext);
    InIdSetTransformFunction segment2Function = createTransformFunction(expression, queryContext);
    assertNotSame(segment2Function, segment1Function);
    assertSame(segment2Function.getIdSet(), segment1Function.getIdSet());
    assertResults(segment2Function, idSet);

    // Another IdSet of the same query is deserialized on its own
    IdSet otherIdSet = createIdSet(NUM_ROWS / 2, NUM_ROWS);
    InIdSetTransformFunction otherIdSetFunction = createTransformFunction(createExpression(otherIdSet), queryContext);
    assertNotSame(otherIdSetFunction.getIdSet(), segment1Function.getIdSet());
    assertResults(otherIdSetFunction, otherIdSet);

    // Another query deserializes its own IdSet
    InIdSetTransformFunction otherQueryFunction = createTransformFunction(expression, createQueryContext());
    assertNotSame(otherQueryFunction.getIdSet(), segment1Function.getIdSet());
  }

  private IdSet createIdSet(int fromRow, int toRow) {
    IdSet idSet = IdSets.create(DataType.INT);
    for (int i = fromRow; i < toRow; i++) {
      idSet.add(_intSVValues[i]);
    }
    return idSet;
  }

  private static ExpressionContext createExpression(IdSet idSet)
      throws IOException {
    return RequestContextUtils.getExpression(
        String.format("inIdSet(%s, '%s')", INT_SV_COLUMN, idSet.toBase64String()));
  }

  private static QueryContext createQueryContext() {
    return QueryContextConverterUtils.getQueryContext("SELECT * FROM testTable");
  }

  private InIdSetTransformFunction createTransformFunction(ExpressionContext expression, QueryContext queryContext) {
    Map<String, ColumnContext> columnContextMap = new HashMap<>();
    _dataSourceMap.forEach(
        (column, dataSource) -> columnContextMap.put(column, ColumnContext.fromDataSource(dataSource)));
    return (InIdSetTransformFunction) TransformFunctionFactory.get(expression, columnContextMap, queryContext);
  }

  private void assertResults(InIdSetTransformFunction transformFunction, IdSet idSet) {
    int[] results = transformFunction.transformToIntValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      assertEquals(results[i], idSet.contains(_intSVValues[i]) ? 1 : 0);
    }
  }
}
