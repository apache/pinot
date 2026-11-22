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
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.function.scalar.VectorFunctions;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.filter.VectorSearchDistances;
import org.apache.pinot.core.operator.transform.function.VectorTransformFunctions.CosineDistanceTransformFunction;
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


/// `cosineDistance` on a column with a COSINE vector index returns the distances a `VECTOR_SIMILARITY` search hands
/// over through the query, and otherwise computes them from the forward index.
public class CosineDistanceIndexTransformFunctionTest extends BaseTransformFunctionTest {

  @Test
  public void testComputesDistancesWithoutVectorIndex() {
    float[] query = _vector2Values[0];
    QueryContext queryContext = approximateQueryContext();
    double[] actual = transform(_dataSourceMap, "cosineDistance(vector1, " + toLiteral(query) + ")", queryContext);
    for (int i = 0; i < NUM_ROWS; i++) {
      assertEquals(actual[i], VectorFunctions.cosineDistance(_vector1Values[i], query), 1e-9);
    }
  }

  /// Distances handed over by the filter's search are used as they are, without searching the index again, with the
  /// column on either side and with a default value.
  @Test
  public void testUsesDistancesHandedOverByTheSearch() {
    float[] query = _vector2Values[0];
    String literal = toLiteral(query);
    for (String expression : new String[]{
        "cosineDistance(vector1, " + literal + ")",
        "cosineDistance(" + literal + ", vector1)",
        "cosineDistance(vector1, " + literal + ", 0.5)"}) {
      VectorIndexReader reader = mock(VectorIndexReader.class);
      QueryContext queryContext = approximateQueryContext();
      TransformFunction function =
          create(withVectorIndex(reader, VectorDistanceFunction.COSINE), expression, queryContext);
      VectorSearchDistances.Request request = VectorSearchDistances.of(queryContext).getRequest(reader, query.clone());
      assertNotNull(request, expression + " must request the distances of the search while it is planned");
      Int2FloatOpenHashMap served = new Int2FloatOpenHashMap();
      for (int i = 0; i < NUM_ROWS; i++) {
        served.put(i, -i);
      }
      request.setDistances(served);
      double[] actual = function.transformToDoubleValuesSV(_projectionBlock);
      for (int i = 0; i < NUM_ROWS; i++) {
        assertEquals(actual[i], -i, 0.0, expression);
      }
      verify(reader, never()).getCosineDistances(any(float[].class), anyInt());
      verify(reader, never()).getDocIds(any(float[].class), anyInt());
    }
  }

  /// Only a COSINE index serves cosine distances: other distance functions request nothing.
  @Test
  public void testRequestsNothingFromNonCosineIndex() {
    float[] query = _vector2Values[0];
    for (VectorDistanceFunction distanceFunction : VectorDistanceFunction.values()) {
      if (distanceFunction == VectorDistanceFunction.COSINE) {
        continue;
      }
      VectorIndexReader reader = mock(VectorIndexReader.class);
      QueryContext queryContext = approximateQueryContext();
      double[] actual = transform(withVectorIndex(reader, distanceFunction),
          "cosineDistance(vector1, " + toLiteral(query) + ")", queryContext);
      assertNull(VectorSearchDistances.of(queryContext).getRequest(reader, query), distanceFunction.name());
      for (int i = 0; i < NUM_ROWS; i++) {
        assertEquals(actual[i], VectorFunctions.cosineDistance(_vector1Values[i], query), 1e-9);
      }
    }
  }

  /// Without a search to take distances from, they are computed rather than searched for.
  @Test
  public void testComputesDistancesWhenNoSearchHandsThemOver() {
    float[] query = _vector2Values[0];
    VectorIndexReader reader = mock(VectorIndexReader.class);
    double[] actual = transform(withVectorIndex(reader, VectorDistanceFunction.COSINE),
        "cosineDistance(vector1, " + toLiteral(query) + ")", approximateQueryContext());
    for (int i = 0; i < NUM_ROWS; i++) {
      assertEquals(actual[i], VectorFunctions.cosineDistance(_vector1Values[i], query), 1e-9);
    }
    verify(reader, never()).getCosineDistances(any(float[].class), anyInt());
  }

  @Test
  public void testExactDistanceByDefaultAndWithRowFilter() {
    float[] query = _vector2Values[0];
    VectorIndexReader reader = mock(VectorIndexReader.class);
    QueryContext defaultContext = QueryContextConverterUtils.getQueryContext(
        CalciteSqlParser.compileToPinotQuery("SELECT * FROM testTable"));
    QueryContext policyContext = approximateQueryContext();
    policyContext.setRowFilter(RequestContextUtils.getFilter(CalciteSqlParser.compileToExpression("intSV = 1")));
    for (QueryContext queryContext : List.of(defaultContext, policyContext)) {
      double[] actual = transform(withVectorIndex(reader, VectorDistanceFunction.COSINE),
          "cosineDistance(vector1, " + toLiteral(query) + ")", queryContext);
      assertNull(VectorSearchDistances.of(queryContext).getRequest(reader, query));
      for (int i = 0; i < NUM_ROWS; i++) {
        assertEquals(actual[i], VectorFunctions.cosineDistance(_vector1Values[i], query), 0.0);
      }
    }
  }

  /// Distances are matched by query vector: a search for another vector hands nothing over.
  @Test
  public void testIgnoresSearchForAnotherVector() {
    float[] query = _vector2Values[0];
    VectorIndexReader reader = mock(VectorIndexReader.class);
    QueryContext queryContext = approximateQueryContext();
    create(withVectorIndex(reader, VectorDistanceFunction.COSINE), "cosineDistance(vector1, " + toLiteral(query) + ")",
        queryContext);
    assertNull(VectorSearchDistances.of(queryContext).getRequest(reader, _vector2Values[1]));
  }

  @Test
  public void testComputesOnlyMissingDocuments() {
    float[] query = _vector2Values[0];
    VectorIndexReader reader = mock(VectorIndexReader.class);
    QueryContext queryContext = approximateQueryContext();
    TransformFunction function = create(withVectorIndex(reader, VectorDistanceFunction.COSINE),
        "cosineDistance(vector1, " + toLiteral(query) + ")", queryContext);
    Int2FloatOpenHashMap served = new Int2FloatOpenHashMap();
    int[] docIds = _projectionBlock.getDocIds();
    served.put(docIds[1], 0.25f);
    VectorSearchDistances.of(queryContext).getRequest(reader, query).setDistances(served);
    double[] actual = function.transformToDoubleValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      assertEquals(actual[i], i == 1 ? 0.25 : VectorFunctions.cosineDistance(_vector1Values[docIds[i]], query), 0.0);
    }
  }

  /// A non-finite distance from the search (a zero vector, or a norm that underflows in float) is computed exactly
  /// instead, which applies the default value where one is given.
  @Test
  public void testComputesDistancesWhenSearchReturnsNonFinite() {
    for (float[] query : new float[][]{new float[VECTOR_DIM_SIZE], _vector2Values[0]}) {
      for (String defaultValue : new String[]{"", ", 7"}) {
        VectorIndexReader reader = mock(VectorIndexReader.class);
        QueryContext queryContext = approximateQueryContext();
        TransformFunction function = create(withVectorIndex(reader, VectorDistanceFunction.COSINE),
            "cosineDistance(vector1, " + toLiteral(query) + defaultValue + ")", queryContext);
        Int2FloatOpenHashMap served = new Int2FloatOpenHashMap();
        int[] docIds = _projectionBlock.getDocIds();
        served.put(docIds[0], Float.NaN);
        served.put(docIds[1], Float.POSITIVE_INFINITY);
        for (int i = 2; i < NUM_ROWS; i++) {
          served.put(docIds[i], 0.25f);
        }
        VectorSearchDistances.of(queryContext).getRequest(reader, query).setDistances(served);
        double[] actual = function.transformToDoubleValuesSV(_projectionBlock);
        for (int i = 0; i < NUM_ROWS; i++) {
          double expected = 0.25;
          if (i < 2) {
            float[] row = _vector1Values[docIds[i]];
            expected = defaultValue.isEmpty() ? VectorFunctions.cosineDistance(row, query)
                : VectorFunctions.cosineDistance(row, query, 7);
          }
          assertEquals(actual[i], expected, 1e-9);
        }
      }
    }
  }

  @Test
  public void testVariableWidthColumnKeepsScalarDimensionCheck() {
    VectorIndexReader reader = mock(VectorIndexReader.class);
    QueryContext queryContext = approximateQueryContext();
    Map<String, DataSource> dataSources = withVectorIndex(FLOAT_MV_COLUMN, reader, VectorDistanceFunction.COSINE);
    // The literal matches the longest row. Shorter rows in this column would be padded by HNSW ingestion.
    int maxWidth = dataSources.get(FLOAT_MV_COLUMN).getDataSourceMetadata().getMaxNumValuesPerMVEntry();
    float[] query = new float[maxWidth];
    TransformFunction function = create(dataSources, "cosineDistance(" + FLOAT_MV_COLUMN + ", " + toLiteral(query)
        + ")", queryContext);
    assertNull(VectorSearchDistances.of(queryContext).getRequest(reader, query));
    IllegalArgumentException exception =
        expectThrows(IllegalArgumentException.class, () -> function.transformToDoubleValuesSV(_projectionBlock));
    assertEquals(exception.getMessage(), "Vector lengths do not match");
  }

  /// An integer projection literal matches a search for the equivalent float vector.
  @Test
  public void testIntegerVectorLiteralMatchesTheSearch() {
    StringBuilder literal = new StringBuilder("ARRAY[");
    float[] query = new float[VECTOR_DIM_SIZE];
    for (int i = 0; i < VECTOR_DIM_SIZE; i++) {
      query[i] = i % 3 + 1;
      literal.append(i == 0 ? "" : ",").append(i % 3 + 1);
    }
    VectorIndexReader reader = mock(VectorIndexReader.class);
    QueryContext queryContext = approximateQueryContext();
    create(withVectorIndex(reader, VectorDistanceFunction.COSINE), "cosineDistance(vector1, " + literal + "])",
        queryContext);
    assertNotNull(VectorSearchDistances.of(queryContext).getRequest(reader, query));
  }

  private double[] transform(Map<String, DataSource> dataSourceMap, String expression, QueryContext queryContext) {
    return create(dataSourceMap, expression, queryContext).transformToDoubleValuesSV(_projectionBlock);
  }

  private static TransformFunction create(Map<String, DataSource> dataSourceMap, String expression,
      QueryContext queryContext) {
    Map<String, ColumnContext> columnContextMap = new HashMap<>();
    dataSourceMap.forEach(
        (column, dataSource) -> columnContextMap.put(column, ColumnContext.fromDataSource(dataSource)));
    TransformFunction function =
        TransformFunctionFactory.get(RequestContextUtils.getExpression(expression), columnContextMap, queryContext);
    assertTrue(function instanceof CosineDistanceTransformFunction);
    return function;
  }

  private static QueryContext approximateQueryContext() {
    return QueryContextConverterUtils.getQueryContext(
        CalciteSqlParser.compileToPinotQuery("SET vectorApproximateDistance = true; SELECT * FROM testTable"));
  }

  private Map<String, DataSource> withVectorIndex(VectorIndexReader reader, VectorDistanceFunction function) {
    return withVectorIndex(VECTOR_1_COLUMN, reader, function);
  }

  private Map<String, DataSource> withVectorIndex(String column, VectorIndexReader reader,
      VectorDistanceFunction function) {
    VectorIndexConfig config = mock(VectorIndexConfig.class);
    when(config.getVectorDistanceFunction()).thenReturn(function);
    DataSource dataSource = spy(_dataSourceMap.get(column));
    when(dataSource.getVectorIndex()).thenReturn(reader);
    when(dataSource.getVectorIndexConfig()).thenReturn(config);
    Map<String, DataSource> dataSourceMap = new HashMap<>(_dataSourceMap);
    dataSourceMap.put(column, dataSource);
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
