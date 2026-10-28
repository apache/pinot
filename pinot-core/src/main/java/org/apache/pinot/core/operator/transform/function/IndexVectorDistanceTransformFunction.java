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
import org.apache.pinot.common.function.scalar.VectorFunctions;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.VectorIndexReader;


/// Implements `VECTOR_DISTANCE(column, ARRAY[...], k)`: the cosine distance of each document from the query vector,
/// served by the column's vector index for the `k` documents closest to the query vector.
///
/// Paired with `VECTOR_SIMILARITY(column, ARRAY[...], k)` on the same vector and `k`, it returns the values of
/// `cosineDistance(column, ARRAY[...])` (up to the index's single-precision arithmetic) without reading the vectors
/// of the matched documents from the forward index: the distances come from the index search the filter already ran.
/// A document the index search did not return (e.g. when the column's index does not serve distances) falls back to
/// computing the distance from the forward index.
///
/// Not thread-safe: like other transform functions, an instance serves one segment on one thread.
public class IndexVectorDistanceTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "vectorDistance";

  private TransformFunction _columnTransformFunction;
  @Nullable
  private VectorIndexReader _vectorIndexReader;
  private float[] _queryVector;
  private int _topK;
  @Nullable
  private Int2FloatMap _distances;
  private boolean _distancesLoaded;

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

    TransformFunction vectorArgument = arguments.get(1);
    if (!(vectorArgument instanceof ArrayLiteralTransformFunction)) {
      throw new IllegalArgumentException("The second argument of VECTOR_DISTANCE must be an array literal");
    }
    _queryVector = toFloatArray((ArrayLiteralTransformFunction) vectorArgument);

    TransformFunction topKArgument = arguments.get(2);
    if (!(topKArgument instanceof LiteralTransformFunction)) {
      throw new IllegalArgumentException("The third argument of VECTOR_DISTANCE must be an integer literal");
    }
    _topK = ((LiteralTransformFunction) topKArgument).getIntLiteral();
    if (_topK <= 0) {
      throw new IllegalArgumentException("The third argument of VECTOR_DISTANCE must be positive, got: " + _topK);
    }
  }

  /// Float array literals are rare in SQL (`ARRAY[0.1, ...]` parses as DOUBLE); narrow like VECTOR_SIMILARITY does.
  private static float[] toFloatArray(ArrayLiteralTransformFunction literal) {
    float[] floats = literal.getFloatArrayLiteral();
    if (floats != null) {
      return floats;
    }
    double[] doubles = literal.getDoubleArrayLiteral();
    if (doubles == null) {
      throw new IllegalArgumentException("The second argument of VECTOR_DISTANCE must be a numeric array literal");
    }
    floats = new float[doubles.length];
    for (int i = 0; i < doubles.length; i++) {
      floats[i] = (float) doubles[i];
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
    Int2FloatMap distances = getDistances();
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
      _doubleValuesSV[i] = VectorFunctions.cosineDistance(vectors[i], _queryVector);
    }
    return _doubleValuesSV;
  }

  @Nullable
  private Int2FloatMap getDistances() {
    if (!_distancesLoaded) {
      _distances = _vectorIndexReader != null ? _vectorIndexReader.getDocDistances(_queryVector, _topK) : null;
      _distancesLoaded = true;
    }
    return _distances;
  }
}
