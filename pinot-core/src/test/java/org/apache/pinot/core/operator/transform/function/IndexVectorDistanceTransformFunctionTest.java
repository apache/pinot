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

import it.unimi.dsi.fastutil.ints.Int2FloatOpenHashMap;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.common.function.scalar.VectorFunctions;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.VectorIndexReader;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// `VECTOR_DISTANCE` returns the distances the vector index serves and computes the rest like `cosineDistance`.
public class IndexVectorDistanceTransformFunctionTest extends BaseTransformFunctionTest {
  private static final int TOP_K = 10;

  @Test
  public void testComputesDistancesWithoutVectorIndex() {
    float[] query = _vector2Values[0];
    double[] actual = transform(_dataSourceMap, query);
    for (int i = 0; i < NUM_ROWS; i++) {
      assertEquals(actual[i], VectorFunctions.cosineDistance(_vector1Values[i], query), 1e-9);
    }
  }

  @Test
  public void testUsesDistancesServedByVectorIndex() {
    float[] query = _vector2Values[0];
    Int2FloatOpenHashMap served = new Int2FloatOpenHashMap();
    for (int i = 0; i < NUM_ROWS; i++) {
      served.put(i, -i);
    }
    VectorIndexReader reader = mock(VectorIndexReader.class);
    when(reader.getDocDistances(any(float[].class), eq(TOP_K))).thenReturn(served);
    double[] actual = transform(withVectorIndex(reader), query);
    for (int i = 0; i < NUM_ROWS; i++) {
      assertEquals(actual[i], -i, 0.0);
    }
    verify(reader, times(1)).getDocDistances(query, TOP_K);
  }

  @Test
  public void testComputesAllDistancesWhenIndexMissesADocument() {
    float[] query = _vector2Values[0];
    Int2FloatOpenHashMap served = new Int2FloatOpenHashMap();
    for (int i = 1; i < NUM_ROWS; i++) {
      served.put(i, -i);
    }
    VectorIndexReader reader = mock(VectorIndexReader.class);
    when(reader.getDocDistances(any(float[].class), eq(TOP_K))).thenReturn(served);
    double[] actual = transform(withVectorIndex(reader), query);
    for (int i = 0; i < NUM_ROWS; i++) {
      assertEquals(actual[i], VectorFunctions.cosineDistance(_vector1Values[i], query), 1e-9);
    }
  }

  @Test
  public void testRejectsInvalidArguments() {
    String vector = toLiteral(_vector2Values[0]);
    for (String expression : new String[]{
        "vectorDistance(vector1, " + vector + ")",
        "vectorDistance(intSV, " + vector + ", 10)",
        "vectorDistance(vector1, vector2, 10)",
        "vectorDistance(vector1, " + vector + ", 0)"}) {
      expectThrows(Exception.class,
          () -> TransformFunctionFactory.get(RequestContextUtils.getExpression(expression), _dataSourceMap));
    }
  }

  private double[] transform(Map<String, DataSource> dataSourceMap, float[] query) {
    ExpressionContext expression =
        RequestContextUtils.getExpression("vectorDistance(vector1, " + toLiteral(query) + ", " + TOP_K + ")");
    TransformFunction function = TransformFunctionFactory.get(expression, dataSourceMap);
    assertTrue(function instanceof IndexVectorDistanceTransformFunction);
    return function.transformToDoubleValuesSV(_projectionBlock);
  }

  private Map<String, DataSource> withVectorIndex(VectorIndexReader reader) {
    DataSource dataSource = spy(_dataSourceMap.get(VECTOR_1_COLUMN));
    when(dataSource.getVectorIndex()).thenReturn(reader);
    Map<String, DataSource> dataSourceMap = new HashMap<>(_dataSourceMap);
    dataSourceMap.put(VECTOR_1_COLUMN, dataSource);
    return dataSourceMap;
  }

  private static String toLiteral(float[] vector) {
    StringBuilder literal = new StringBuilder("ARRAY[");
    for (int i = 0; i < vector.length; i++) {
      literal.append(i == 0 ? "" : ",").append(vector[i]);
    }
    return literal.append(']').toString();
  }
}
