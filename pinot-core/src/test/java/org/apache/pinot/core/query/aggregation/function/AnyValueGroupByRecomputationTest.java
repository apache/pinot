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
package org.apache.pinot.core.query.aggregation.function;

import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


/// Counts how many times ANY_VALUE group-by reads the values of a non-dictionary input per block.
///
/// A transform input (for example `jsonExtractScalar`) recomputes the whole block on every `get*ValuesSV()` call,
/// so the number of calls per block multiplies the transform cost.
public class AnyValueGroupByRecomputationTest {
  private static final int NUM_DOCS = 1000;

  @Test
  public void testGroupByReadsNonDictionaryValuesOncePerBlock() {
    ExpressionContext expression = ExpressionContext.forIdentifier("jsonExtractResult");
    AnyValueAggregationFunction function = new AnyValueAggregationFunction(List.of(expression), false);

    String[] values = new String[NUM_DOCS];
    int[] groupKeys = new int[NUM_DOCS];
    for (int i = 0; i < NUM_DOCS; i++) {
      values[i] = "zone-" + (i % 3);
      groupKeys[i] = i;
    }
    BlockValSet valueSet = mock(BlockValSet.class);
    when(valueSet.getValueType()).thenReturn(DataType.STRING);
    when(valueSet.isDictionaryEncoded()).thenReturn(false);
    when(valueSet.isSingleValue()).thenReturn(true);
    when(valueSet.getStringValuesSV()).thenReturn(values);

    GroupByResultHolder holder = function.createGroupByResultHolder(NUM_DOCS, NUM_DOCS);
    function.aggregateGroupBySV(NUM_DOCS, groupKeys, holder, Map.of(expression, valueSet));

    long numCalls = mockingDetails(valueSet).getInvocations().stream()
        .filter(invocation -> invocation.getMethod().getName().equals("getStringValuesSV"))
        .count();
    assertEquals(numCalls, 1, "getStringValuesSV() must be called once per block, not once per row");
    for (int i = 0; i < NUM_DOCS; i++) {
      assertEquals(holder.<String>getResult(i), values[i]);
    }
  }
}
