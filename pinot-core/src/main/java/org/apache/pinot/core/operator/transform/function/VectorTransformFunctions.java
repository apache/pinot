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

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.ints.Int2FloatMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.function.scalar.VectorFunctions;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.filter.VectorSearchDistances;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.segment.spi.index.reader.VectorIndexReader;


public class VectorTransformFunctions {
  /// Implements `cosineDistance(left, right[, defaultValue])`.
  ///
  /// When one argument is a column with a COSINE vector index and the other an array literal, and the query filters
  /// with `VECTOR_SIMILARITY` on the same column and vector, the distances of the matched documents are taken from the
  /// index search the filter runs instead of reading their vectors from the forward index. They are handed over
  /// through the query's [VectorSearchDistances], so nothing outlives the query. Index distances carry the index's
  /// single-precision arithmetic (within about 1e-6 of the exact value). When the index returns no distances or misses
  /// a document, or the `vectorExactDistance` query option is set, the distances are computed from the forward index.
  public static class CosineDistanceTransformFunction extends VectorDistanceTransformFunction {
    public static final String FUNCTION_NAME = "cosineDistance";
    private Double _defaultValue = null;
    @Nullable
    private VectorSearchDistances.Request _distancesRequest;

    @Override
    protected void checkArgumentSize(List<TransformFunction> arguments) {
      // Check that there are 2 or 3 arguments
      if (arguments.size() < 2 || arguments.size() > 3) {
        throw new IllegalArgumentException("2 or 3 arguments are required for CosineDistance function");
      }
    }

    @Override
    public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
      super.init(arguments, columnContextMap);
      if (arguments.size() == 3) {
        _defaultValue = ((LiteralTransformFunction) arguments.get(2)).getDoubleLiteral();
      }
    }

    @Override
    public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap,
        QueryContext queryContext) {
      super.init(arguments, columnContextMap, queryContext);
      if (_leftTransformFunction instanceof IdentifierTransformFunction
          && _rightTransformFunction instanceof ArrayLiteralTransformFunction) {
        _distancesRequest = requestIndexDistances((IdentifierTransformFunction) _leftTransformFunction,
            (ArrayLiteralTransformFunction) _rightTransformFunction, columnContextMap, queryContext);
      } else if (_rightTransformFunction instanceof IdentifierTransformFunction
          && _leftTransformFunction instanceof ArrayLiteralTransformFunction) {
        _distancesRequest = requestIndexDistances((IdentifierTransformFunction) _rightTransformFunction,
            (ArrayLiteralTransformFunction) _leftTransformFunction, columnContextMap, queryContext);
      }
    }

    /// Asks a `VECTOR_SIMILARITY` filter on the column and vector to hand over the distances of its search, when the
    /// column has a COSINE vector index (whose distances are cosine distances).
    @Nullable
    private static VectorSearchDistances.Request requestIndexDistances(IdentifierTransformFunction column,
        ArrayLiteralTransformFunction vector, Map<String, ColumnContext> columnContextMap, QueryContext queryContext) {
      if (QueryOptionsUtils.isVectorExactDistance(queryContext.getQueryOptions())) {
        return null;
      }
      ColumnContext columnContext = columnContextMap.get(column.getColumnName());
      DataSource dataSource = columnContext != null ? columnContext.getDataSource() : null;
      if (dataSource == null) {
        return null;
      }
      VectorIndexReader vectorIndexReader = dataSource.getVectorIndex();
      VectorIndexConfig vectorIndexConfig = dataSource.getVectorIndexConfig();
      if (vectorIndexReader == null || vectorIndexConfig == null
          || vectorIndexConfig.getVectorDistanceFunction() != VectorIndexConfig.VectorDistanceFunction.COSINE) {
        return null;
      }
      float[] queryVector = toFloatArray(vector);
      return queryVector != null ? VectorSearchDistances.of(queryContext).request(vectorIndexReader, queryVector)
          : null;
    }

    /// Float array literals are rare in SQL (`ARRAY[0.1, ...]` parses as DOUBLE, `ARRAY[1, 0]` as INT); narrow every
    /// numeric array like VECTOR_SIMILARITY does, so the same vector literal matches the filter's search.
    @Nullable
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
        return null;
      }
      floats = new float[length];
      for (int i = 0; i < length; i++) {
        floats[i] = doubles != null ? (float) doubles[i] : ints != null ? ints[i] : (float) longs[i];
      }
      return floats;
    }

    @Override
    public String getName() {
      return FUNCTION_NAME;
    }

    @Override
    public double[] transformToDoubleValuesSV(ValueBlock valueBlock) {
      Int2FloatMap distances = _distancesRequest != null ? _distancesRequest.getDistances() : null;
      int[] docIds = valueBlock.getDocIds();
      if (distances != null && docIds != null) {
        int length = valueBlock.getNumDocs();
        initDoubleValuesSV(length);
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
      return super.transformToDoubleValuesSV(valueBlock);
    }

    @Override
    protected double computeDistance(float[] vector1, float[] vector2) {
      if (_defaultValue != null) {
        return VectorFunctions.cosineDistance(vector1, vector2, _defaultValue);
      }
      return VectorFunctions.cosineDistance(vector1, vector2);
    }
  }

  public static class InnerProductTransformFunction extends VectorDistanceTransformFunction {
    public static final String FUNCTION_NAME = "innerProduct";

    @Override
    public String getName() {
      return FUNCTION_NAME;
    }

    @Override
    protected double computeDistance(float[] vector1, float[] vector2) {
      return VectorFunctions.innerProduct(vector1, vector2);
    }
  }

  public static class L1DistanceTransformFunction extends VectorDistanceTransformFunction {
    public static final String FUNCTION_NAME = "l1Distance";

    @Override
    public String getName() {
      return FUNCTION_NAME;
    }

    @Override
    protected double computeDistance(float[] vector1, float[] vector2) {
      return VectorFunctions.l1Distance(vector1, vector2);
    }
  }

  public static class L2DistanceTransformFunction extends VectorDistanceTransformFunction {
    public static final String FUNCTION_NAME = "l2Distance";

    @Override
    public String getName() {
      return FUNCTION_NAME;
    }

    @Override
    protected double computeDistance(float[] vector1, float[] vector2) {
      return VectorFunctions.l2Distance(vector1, vector2);
    }
  }

  public static abstract class VectorDistanceTransformFunction extends BaseTransformFunction {

    protected TransformFunction _leftTransformFunction;
    protected TransformFunction _rightTransformFunction;

    @Override
    public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
      super.init(arguments, columnContextMap);
      checkArgumentSize(arguments);
      _leftTransformFunction = arguments.get(0);
      _rightTransformFunction = arguments.get(1);
      Preconditions.checkArgument(
          !_leftTransformFunction.getResultMetadata().isSingleValue()
              && !_rightTransformFunction.getResultMetadata().isSingleValue(),
          "Argument must be multi-valued float vector for vector distance transform function: %s", getName());
    }

    protected void checkArgumentSize(List<TransformFunction> arguments) {
      // Check that there are 2 arguments
      if (arguments.size() != 2) {
        throw new IllegalArgumentException("Exactly 2 arguments are required for Vector transform function");
      }
    }

    @Override
    public TransformResultMetadata getResultMetadata() {
      return DOUBLE_SV_NO_DICTIONARY_METADATA;
    }

    @Override
    public double[] transformToDoubleValuesSV(ValueBlock valueBlock) {
      int length = valueBlock.getNumDocs();
      initDoubleValuesSV(length);
      float[][] leftValues = _leftTransformFunction.transformToFloatValuesMV(valueBlock);
      float[][] rightValues = _rightTransformFunction.transformToFloatValuesMV(valueBlock);
      for (int i = 0; i < length; i++) {
        _doubleValuesSV[i] = computeDistance(leftValues[i], rightValues[i]);
      }
      return _doubleValuesSV;
    }

    protected abstract double computeDistance(float[] vector1, float[] vector2);
  }

  public static class VectorDimsTransformFunction extends BaseTransformFunction {
    public static final String FUNCTION_NAME = "vectorDims";

    private TransformFunction _transformFunction;

    @Override
    public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
      super.init(arguments, columnContextMap);
      // Check that there is exact 1 argument
      if (arguments.size() != 1) {
        throw new IllegalArgumentException("Exactly 1 argument is required for Vector transform function");
      }
      _transformFunction = arguments.get(0);
      Preconditions.checkArgument(!_transformFunction.getResultMetadata().isSingleValue(),
          "Argument must be multi-valued float vector for vector distance transform function: %s", getName());
    }

    @Override
    public String getName() {
      return FUNCTION_NAME;
    }

    @Override
    public TransformResultMetadata getResultMetadata() {
      return INT_SV_NO_DICTIONARY_METADATA;
    }

    @Override
    public int[] transformToIntValuesSV(ValueBlock valueBlock) {
      int length = valueBlock.getNumDocs();
      initIntValuesSV(length);
      float[][] values = _transformFunction.transformToFloatValuesMV(valueBlock);
      for (int i = 0; i < length; i++) {
        _intValuesSV[i] = VectorFunctions.vectorDims(values[i]);
      }
      return _intValuesSV;
    }
  }

  public static class VectorNormTransformFunction extends BaseTransformFunction {
    public static final String FUNCTION_NAME = "vectorNorm";

    private TransformFunction _transformFunction;

    @Override
    public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
      super.init(arguments, columnContextMap);
      // Check that there is exact 1 argument
      if (arguments.size() != 1) {
        throw new IllegalArgumentException("Exactly 1 argument is required for Vector transform function");
      }

      _transformFunction = arguments.get(0);
      Preconditions.checkArgument(!_transformFunction.getResultMetadata().isSingleValue(),
          "Argument must be multi-valued float vector for vector distance transform function: %s", getName());
    }

    @Override
    public String getName() {
      return FUNCTION_NAME;
    }

    @Override
    public TransformResultMetadata getResultMetadata() {
      return DOUBLE_SV_NO_DICTIONARY_METADATA;
    }

    @Override
    public double[] transformToDoubleValuesSV(ValueBlock valueBlock) {
      int length = valueBlock.getNumDocs();
      initDoubleValuesSV(length);
      float[][] values = _transformFunction.transformToFloatValuesMV(valueBlock);
      for (int i = 0; i < length; i++) {
        _doubleValuesSV[i] = VectorFunctions.vectorNorm(values[i]);
      }
      return _doubleValuesSV;
    }
  }
}
