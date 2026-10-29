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
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.filter.VectorSearchDistances;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig.VectorDistanceFunction;
import org.apache.pinot.segment.spi.index.reader.VectorIndexReader;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// `VECTOR_DISTANCE` returns the distances a `VECTOR_SIMILARITY` search hands over through the query, and otherwise
/// computes them under the column's configured distance function.
public class IndexVectorDistanceTransformFunctionTest extends BaseTransformFunctionTest {

  /// A column without a vector index config ranks by EUCLIDEAN in VECTOR_SIMILARITY, so VECTOR_DISTANCE uses it too.
  @Test
  public void testComputesEuclideanDistancesWithoutVectorIndexConfig() {
    float[] query = _vector2Values[0];
    double[] actual = transform(_dataSourceMap, query, newQueryContext());
    for (int i = 0; i < NUM_ROWS; i++) {
      assertEquals(actual[i], VectorFunctions.euclideanDistance(_vector1Values[i], query), 1e-9);
    }
  }

  /// Distances follow the column's configured distance function, as VECTOR_SIMILARITY does.
  @Test
  public void testComputesDistancesUnderConfiguredFunction() {
    float[] query = _vector2Values[0];
    for (VectorDistanceFunction function : VectorDistanceFunction.values()) {
      double[] actual = transform(withVectorIndex(null, function), query, newQueryContext());
      for (int i = 0; i < NUM_ROWS; i++) {
        double expected;
        switch (function) {
          case COSINE:
            expected = VectorFunctions.cosineDistance(_vector1Values[i], query);
            break;
          case EUCLIDEAN:
          case L2:
            expected = VectorFunctions.euclideanDistance(_vector1Values[i], query);
            break;
          default:
            expected = -VectorFunctions.dotProduct(_vector1Values[i], query);
            break;
        }
        assertEquals(actual[i], expected, 1e-9, function.name());
      }
    }
  }

  /// Distances handed over by the filter's search are used as they are, without searching the index again.
  @Test
  public void testUsesDistancesHandedOverByTheSearch() {
    float[] query = _vector2Values[0];
    VectorIndexReader reader = mock(VectorIndexReader.class);
    QueryContext queryContext = newQueryContext();
    TransformFunction function = create(withVectorIndex(reader, VectorDistanceFunction.COSINE), query, queryContext);
    VectorSearchDistances.Request request = VectorSearchDistances.of(queryContext).getRequest(reader, query.clone());
    assertNotNull(request, "VECTOR_DISTANCE must request the distances of the search while it is planned");
    Int2FloatOpenHashMap served = new Int2FloatOpenHashMap();
    for (int i = 0; i < NUM_ROWS; i++) {
      served.put(i, -i);
    }
    request.setDistances(served);
    double[] actual = function.transformToDoubleValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      assertEquals(actual[i], -i, 0.0);
    }
    verify(reader, never()).getDocDistances(any(float[].class), anyInt());
    verify(reader, never()).getDocIds(any(float[].class), anyInt());
  }

  /// Without a search to take distances from, they are computed rather than searched for.
  @Test
  public void testComputesDistancesWhenNoSearchHandsThemOver() {
    float[] query = _vector2Values[0];
    VectorIndexReader reader = mock(VectorIndexReader.class);
    double[] actual = transform(withVectorIndex(reader, VectorDistanceFunction.COSINE), query, newQueryContext());
    for (int i = 0; i < NUM_ROWS; i++) {
      assertEquals(actual[i], VectorFunctions.cosineDistance(_vector1Values[i], query), 1e-9);
    }
    verify(reader, never()).getDocDistances(any(float[].class), anyInt());
  }

  /// Distances are matched by query vector: a search for another vector hands nothing over.
  @Test
  public void testIgnoresSearchForAnotherVector() {
    float[] query = _vector2Values[0];
    VectorIndexReader reader = mock(VectorIndexReader.class);
    QueryContext queryContext = newQueryContext();
    create(withVectorIndex(reader, VectorDistanceFunction.COSINE), query, queryContext);
    assertNull(VectorSearchDistances.of(queryContext).getRequest(reader, _vector2Values[1]));
  }

  @Test
  public void testComputesAllDistancesWhenSearchMissesADocument() {
    float[] query = _vector2Values[0];
    VectorIndexReader reader = mock(VectorIndexReader.class);
    QueryContext queryContext = newQueryContext();
    TransformFunction function = create(withVectorIndex(reader, VectorDistanceFunction.COSINE), query, queryContext);
    Int2FloatOpenHashMap served = new Int2FloatOpenHashMap();
    for (int i = 1; i < NUM_ROWS; i++) {
      served.put(i, -i);
    }
    VectorSearchDistances.of(queryContext).getRequest(reader, query).setDistances(served);
    double[] actual = function.transformToDoubleValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      assertEquals(actual[i], VectorFunctions.cosineDistance(_vector1Values[i], query), 1e-9);
    }
  }

  /// An integer vector literal (`ARRAY[1, 0, ...]`) is accepted, as VECTOR_SIMILARITY accepts it.
  @Test
  public void testAcceptsIntegerVectorLiteral() {
    StringBuilder literal = new StringBuilder("ARRAY[");
    float[] query = new float[VECTOR_DIM_SIZE];
    for (int i = 0; i < VECTOR_DIM_SIZE; i++) {
      query[i] = i % 3;
      literal.append(i == 0 ? "" : ",").append(i % 3);
    }
    ExpressionContext expression = RequestContextUtils.getExpression(
        "vectorDistance(vector1, " + literal.append(']') + ")");
    double[] actual =
        TransformFunctionFactory.get(expression, _dataSourceMap).transformToDoubleValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      assertEquals(actual[i], VectorFunctions.euclideanDistance(_vector1Values[i], query), 1e-9);
    }
  }

  @Test
  public void testRejectsInvalidArguments() {
    String vector = toLiteral(_vector2Values[0]);
    for (String expression : new String[]{
        "vectorDistance(vector1)",
        "vectorDistance(vector1, " + vector + ", 10)",
        "vectorDistance(intSV, " + vector + ")",
        "vectorDistance(vector1, vector2)"}) {
      expectThrows(Exception.class,
          () -> TransformFunctionFactory.get(RequestContextUtils.getExpression(expression), _dataSourceMap));
    }
  }

  private double[] transform(Map<String, DataSource> dataSourceMap, float[] query, QueryContext queryContext) {
    return create(dataSourceMap, query, queryContext).transformToDoubleValuesSV(_projectionBlock);
  }

  private static TransformFunction create(Map<String, DataSource> dataSourceMap, float[] query,
      QueryContext queryContext) {
    ExpressionContext expression =
        RequestContextUtils.getExpression("vectorDistance(vector1, " + toLiteral(query) + ")");
    Map<String, ColumnContext> columnContextMap = new HashMap<>();
    dataSourceMap.forEach(
        (column, dataSource) -> columnContextMap.put(column, ColumnContext.fromDataSource(dataSource)));
    TransformFunction function = TransformFunctionFactory.get(expression, columnContextMap, queryContext);
    assertTrue(function instanceof IndexVectorDistanceTransformFunction);
    return function;
  }

  private static QueryContext newQueryContext() {
    return QueryContextConverterUtils.getQueryContext(CalciteSqlParser.compileToPinotQuery("SELECT * FROM testTable"));
  }

  private Map<String, DataSource> withVectorIndex(VectorIndexReader reader, VectorDistanceFunction function) {
    VectorIndexConfig config = mock(VectorIndexConfig.class);
    when(config.getVectorDistanceFunction()).thenReturn(function);
    DataSource dataSource = spy(_dataSourceMap.get(VECTOR_1_COLUMN));
    when(dataSource.getVectorIndex()).thenReturn(reader);
    when(dataSource.getVectorIndexConfig()).thenReturn(config);
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
