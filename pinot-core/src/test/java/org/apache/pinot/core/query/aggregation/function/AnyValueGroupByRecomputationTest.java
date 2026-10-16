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

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ByteArray;
import org.testng.annotations.DataProvider;
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

  @DataProvider(name = "storedTypes")
  public Object[][] storedTypes() {
    return new Object[][]{
        {DataType.INT, "getIntValuesSV"},
        {DataType.LONG, "getLongValuesSV"},
        {DataType.FLOAT, "getFloatValuesSV"},
        {DataType.DOUBLE, "getDoubleValuesSV"},
        {DataType.BIG_DECIMAL, "getBigDecimalValuesSV"},
        {DataType.STRING, "getStringValuesSV"},
        {DataType.BYTES, "getBytesValuesSV"}
    };
  }

  @Test(dataProvider = "storedTypes")
  public void testGroupByReadsNonDictionaryValuesOncePerBlock(DataType storedType, String valuesMethod) {
    BlockValSet valueSet = mock(BlockValSet.class);
    when(valueSet.getValueType()).thenReturn(storedType);
    when(valueSet.isDictionaryEncoded()).thenReturn(false);
    IntFunction<Object> expectedValue = stubValues(valueSet, storedType);

    GroupByResultHolder holder = aggregateGroupBy(valueSet);

    long numCalls = mockingDetails(valueSet).getInvocations().stream()
        .filter(invocation -> invocation.getMethod().getName().equals(valuesMethod))
        .count();
    assertEquals(numCalls, 1, valuesMethod + "() must be called once per block, not once per row");
    for (int i = 0; i < NUM_DOCS; i++) {
      assertEquals(holder.<Object>getResult(i), expectedValue.apply(i));
    }
  }

  /// Stubs the values getter for the stored type and returns the value that ANY_VALUE must keep for each group.
  private static IntFunction<Object> stubValues(BlockValSet valueSet, DataType storedType) {
    switch (storedType) {
      case INT: {
        int[] values = new int[NUM_DOCS];
        for (int i = 0; i < NUM_DOCS; i++) {
          values[i] = i;
        }
        when(valueSet.getIntValuesSV()).thenReturn(values);
        return i -> values[i];
      }
      case LONG: {
        long[] values = new long[NUM_DOCS];
        for (int i = 0; i < NUM_DOCS; i++) {
          values[i] = i * 10L;
        }
        when(valueSet.getLongValuesSV()).thenReturn(values);
        return i -> values[i];
      }
      case FLOAT: {
        float[] values = new float[NUM_DOCS];
        for (int i = 0; i < NUM_DOCS; i++) {
          values[i] = i + 0.5f;
        }
        when(valueSet.getFloatValuesSV()).thenReturn(values);
        return i -> values[i];
      }
      case DOUBLE: {
        double[] values = new double[NUM_DOCS];
        for (int i = 0; i < NUM_DOCS; i++) {
          values[i] = i + 0.25;
        }
        when(valueSet.getDoubleValuesSV()).thenReturn(values);
        return i -> values[i];
      }
      case BIG_DECIMAL: {
        BigDecimal[] values = new BigDecimal[NUM_DOCS];
        for (int i = 0; i < NUM_DOCS; i++) {
          values[i] = BigDecimal.valueOf(i, 2);
        }
        when(valueSet.getBigDecimalValuesSV()).thenReturn(values);
        return i -> values[i];
      }
      case STRING: {
        String[] values = new String[NUM_DOCS];
        for (int i = 0; i < NUM_DOCS; i++) {
          values[i] = "zone-" + (i % 3);
        }
        when(valueSet.getStringValuesSV()).thenReturn(values);
        return i -> values[i];
      }
      case BYTES: {
        byte[][] values = new byte[NUM_DOCS][];
        for (int i = 0; i < NUM_DOCS; i++) {
          values[i] = new byte[]{(byte) i, (byte) (i >> 8)};
        }
        when(valueSet.getBytesValuesSV()).thenReturn(values);
        // ANY_VALUE wraps BYTES values in ByteArray.
        return i -> new ByteArray(values[i]);
      }
      default:
        throw new IllegalArgumentException("Unsupported stored type: " + storedType);
    }
  }

  private static GroupByResultHolder aggregateGroupBy(BlockValSet valueSet) {
    ExpressionContext expression = ExpressionContext.forIdentifier("transformResult");
    AnyValueAggregationFunction function = new AnyValueAggregationFunction(List.of(expression), false);
    int[] groupKeys = new int[NUM_DOCS];
    for (int i = 0; i < NUM_DOCS; i++) {
      groupKeys[i] = i;
    }
    GroupByResultHolder holder = function.createGroupByResultHolder(NUM_DOCS, NUM_DOCS);
    function.aggregateGroupBySV(NUM_DOCS, groupKeys, holder, Map.of(expression, valueSet));
    return holder;
  }
}
