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

import it.unimi.dsi.fastutil.ints.Int2FloatMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.filter.VectorDistanceUtils;
import org.apache.pinot.core.operator.filter.VectorSearchDistances;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig.VectorDistanceFunction;
import org.apache.pinot.segment.spi.index.reader.VectorIndexReader;


/// Implements `VECTOR_DISTANCE(column, ARRAY[...], k)`: the distance of each document from the query vector under the
/// column's configured vector distance function, i.e. the distance `VECTOR_SIMILARITY` ranks by and
/// `vectorDistanceThreshold` applies to:
/// - `COSINE`: `1 - cosine similarity`, as `cosineDistance(column, ARRAY[...])`
/// - `EUCLIDEAN` / `L2`: Euclidean distance, as `euclideanDistance(column, ARRAY[...])`
/// - `INNER_PRODUCT` / `DOT_PRODUCT`: the negated dot product, as `-dotProduct(column, ARRAY[...])`
///
/// A column without a vector index config uses EUCLIDEAN, like `VECTOR_SIMILARITY`.
///
/// Paired with `VECTOR_SIMILARITY(column, ARRAY[...], k)` on the same column and vector, it takes the distances from
/// the index search the filter runs (up to the index's single-precision arithmetic) instead of reading the vectors of
/// the matched documents from the forward index. The distances are handed over through the query's
/// [VectorSearchDistances], so nothing outlives the query. When the index returns no distances (e.g. it does not
/// support the column's distance function) or misses a document, the distances are computed from the forward index.
///
/// Not thread-safe: like other transform functions, an instance serves one segment on one thread.
public class IndexVectorDistanceTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "vectorDistance";

  private TransformFunction _columnTransformFunction;
  @Nullable
  private VectorIndexReader _vectorIndexReader;
  private VectorDistanceFunction _distanceFunction;
  private float[] _queryVector;
  @Nullable
  private VectorSearchDistances.Request _distancesRequest;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
    super.init(arguments, columnContextMap);
    if (arguments.size() != 3) {
      throw new IllegalArgumentException(
          "Expected 3 arguments for VECTOR_DISTANCE: VECTOR_DISTANCE(column, ARRAY[...], topK)");
    }
    _columnTransformFunction = arguments.get(0);
    if (!(_columnTransformFunction instanceof IdentifierTransformFunction)
        || _columnTransformFunction.getResultMetadata().isSingleValue()) {
      throw new IllegalArgumentException("The first argument of VECTOR_DISTANCE must be a multi-valued column");
    }
    String columnName = ((IdentifierTransformFunction) _columnTransformFunction).getColumnName();
    DataSource dataSource = columnContextMap.get(columnName).getDataSource();
    _vectorIndexReader = dataSource != null ? dataSource.getVectorIndex() : null;
    _distanceFunction =
        VectorDistanceUtils.resolveDistanceFunction(dataSource != null ? dataSource.getVectorIndexConfig() : null);

    TransformFunction vectorArgument = arguments.get(1);
    if (!(vectorArgument instanceof ArrayLiteralTransformFunction)) {
      throw new IllegalArgumentException("The second argument of VECTOR_DISTANCE must be an array literal");
    }
    _queryVector = toFloatArray((ArrayLiteralTransformFunction) vectorArgument);

    TransformFunction topKArgument = arguments.get(2);
    if (!(topKArgument instanceof LiteralTransformFunction)) {
      throw new IllegalArgumentException("The third argument of VECTOR_DISTANCE must be an integer literal");
    }
    // `k` mirrors the paired VECTOR_SIMILARITY; the distances come from that filter's search, whatever its `k`.
    int topK = ((LiteralTransformFunction) topKArgument).getIntLiteral();
    if (topK <= 0) {
      throw new IllegalArgumentException("The third argument of VECTOR_DISTANCE must be positive, got: " + topK);
    }
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap,
      QueryContext queryContext) {
    super.init(arguments, columnContextMap, queryContext);
    if (_vectorIndexReader != null) {
      // Ask a VECTOR_SIMILARITY filter on this column and vector to hand over the distances of its search.
      _distancesRequest = VectorSearchDistances.of(queryContext).request(_vectorIndexReader, _queryVector);
    }
  }

  /// Float array literals are rare in SQL (`ARRAY[0.1, ...]` parses as DOUBLE, `ARRAY[1, 0]` as INT); narrow every
  /// numeric array like VECTOR_SIMILARITY does, so the same vector literal works in both.
  private static float[] toFloatArray(ArrayLiteralTransformFunction literal) {
    float[] floats = literal.getFloatArrayLiteral();
    if (floats != null) {
      return floats;
    }
    double[] doubles = literal.getDoubleArrayLiteral();
    int[] ints = literal.getIntArrayLiteral();
    long[] longs = literal.getLongArrayLiteral();
    int length = doubles != null ? doubles.length : ints != null ? ints.length : longs != null ? longs.length : -1;
    if (length < 0) {
      throw new IllegalArgumentException("The second argument of VECTOR_DISTANCE must be a numeric array literal");
    }
    floats = new float[length];
    for (int i = 0; i < length; i++) {
      floats[i] = doubles != null ? (float) doubles[i] : ints != null ? ints[i] : (float) longs[i];
    }
    return floats;
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return DOUBLE_SV_NO_DICTIONARY_METADATA;
  }

  @Override
  public double[] transformToDoubleValuesSV(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    initDoubleValuesSV(length);
    Int2FloatMap distances = _distancesRequest != null ? _distancesRequest.getDistances() : null;
    int[] docIds = valueBlock.getDocIds();
    if (distances != null && docIds != null) {
      boolean allServed = true;
      for (int i = 0; i < length; i++) {
        if (!distances.containsKey(docIds[i])) {
          allServed = false;
          break;
        }
        _doubleValuesSV[i] = distances.get(docIds[i]);
      }
      if (allServed) {
        return _doubleValuesSV;
      }
    }
    float[][] vectors = _columnTransformFunction.transformToFloatValuesMV(valueBlock);
    for (int i = 0; i < length; i++) {
      _doubleValuesSV[i] = VectorDistanceUtils.computeDoubleDistance(vectors[i], _queryVector, _distanceFunction);
    }
    return _doubleValuesSV;
  }
}
