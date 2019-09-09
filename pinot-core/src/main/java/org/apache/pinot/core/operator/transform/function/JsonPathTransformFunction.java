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

import com.jayway.jsonpath.JsonPath;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;


/**
 * json_path(jsonFieldName, 'json_path', 'results_type')
 */
public class JsonPathTransformFunction extends BaseTransformFunction {

  public static final String FUNCTION_NAME = "json_path";

  private TransformFunction _jsonFieldTransformFunction;
  private String _jsonPath;
  private String _resultsType;
  private TransformResultMetadata _resultMetadata;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(@Nonnull List<TransformFunction> arguments, @Nonnull Map<String, DataSource> dataSourceMap) {
    // Check that there are exactly 2 arguments
    if (arguments.size() != 3) {
      throw new IllegalArgumentException(
          "Exactly 3 arguments are required for transform function: JSON_PATH(jsonFieldName, 'json_path', 'results_type')");
    }

    TransformFunction firstArgument = arguments.get(0);
    if (firstArgument instanceof LiteralTransformFunction || !firstArgument.getResultMetadata().isSingleValue()) {
      throw new IllegalArgumentException(
          "The first argument of JSON_PATH transform function must be a single-valued column or a transform function");
    }
    _jsonFieldTransformFunction = firstArgument;
    _jsonPath = ((LiteralTransformFunction) arguments.get(1)).getLiteral();
    _resultsType = ((LiteralTransformFunction) arguments.get(2)).getLiteral().toUpperCase();
    boolean isSingleValue = !_resultsType.toUpperCase().endsWith("_ARRAY");
    try {
      FieldSpec.DataType fieldType = FieldSpec.DataType.valueOf(_resultsType.split("_ARRAY")[0]);
      _resultMetadata = new TransformResultMetadata(fieldType, isSingleValue, false);
    } catch (Exception e) {
      throw new UnsupportedOperationException(String.format(
          "Unsupported results type: %s for 'JSON_PATH' Udf. Supported types are: INT/LONG/FLOAT/DOUBLE/STRING/INT_ARRAY/LONG/FLOAT_ARRAY/DOUBLE_ARRAY/STRING_ARRAY",
          _resultsType));
    }
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return _resultMetadata;
  }

  @Override
  public int[] transformToIntValuesSV(@Nonnull ProjectionBlock projectionBlock) {
    final String[] stringValuesSV = _jsonFieldTransformFunction.transformToStringValuesSV(projectionBlock);
    final int[] results = new int[projectionBlock.getNumDocs()];
    for (int i = 0; i < results.length; i++) {
      results[i] = JsonPath.read(stringValuesSV[i], _jsonPath);
    }
    return results;
  }

  @Override
  public long[] transformToLongValuesSV(@Nonnull ProjectionBlock projectionBlock) {
    final String[] stringValuesSV = _jsonFieldTransformFunction.transformToStringValuesSV(projectionBlock);
    final long[] results = new long[projectionBlock.getNumDocs()];
    for (int i = 0; i < projectionBlock.getNumDocs(); i++) {
      results[i] = JsonPath.read(stringValuesSV[i], _jsonPath);
    }
    return results;
  }

  @Override
  public float[] transformToFloatValuesSV(@Nonnull ProjectionBlock projectionBlock) {
    final String[] stringValuesSV = _jsonFieldTransformFunction.transformToStringValuesSV(projectionBlock);
    final float[] results = new float[projectionBlock.getNumDocs()];
    for (int i = 0; i < projectionBlock.getNumDocs(); i++) {
      double doubleValue = JsonPath.read(stringValuesSV[i], _jsonPath);
      results[i] = (float) doubleValue;
    }
    return results;
  }

  @Override
  public double[] transformToDoubleValuesSV(@Nonnull ProjectionBlock projectionBlock) {
    final String[] stringValuesSV = _jsonFieldTransformFunction.transformToStringValuesSV(projectionBlock);
    final double[] results = new double[projectionBlock.getNumDocs()];
    for (int i = 0; i < projectionBlock.getNumDocs(); i++) {
      Object doubleValue = JsonPath.read(stringValuesSV[i], _jsonPath);
      if (doubleValue instanceof BigDecimal) {
        results[i] = ((BigDecimal) doubleValue).doubleValue();
      } else {
        results[i] = (double) doubleValue;
      }
    }
    return results;
  }

  @Override
  public String[] transformToStringValuesSV(@Nonnull ProjectionBlock projectionBlock) {
    final String[] stringValuesSV = _jsonFieldTransformFunction.transformToStringValuesSV(projectionBlock);
    final String[] results = new String[projectionBlock.getNumDocs()];
    for (int i = 0; i < projectionBlock.getNumDocs(); i++) {
      results[i] = JsonPath.read(stringValuesSV[i], _jsonPath).toString();
    }
    return results;
  }

  @Override
  public int[][] transformToIntValuesMV(@Nonnull ProjectionBlock projectionBlock) {
    final String[] stringValuesMV = _jsonFieldTransformFunction.transformToStringValuesSV(projectionBlock);
    final int[][] results = new int[projectionBlock.getNumDocs()][];
    for (int i = 0; i < projectionBlock.getNumDocs(); i++) {
      final List<Integer> intVals = JsonPath.read(stringValuesMV[i], _jsonPath);
      results[i] = new int[intVals.size()];
      for (int j = 0; j < intVals.size(); j++) {
        results[i][j] = intVals.get(j);
      }
    }
    return results;
  }

  @Override
  public long[][] transformToLongValuesMV(@Nonnull ProjectionBlock projectionBlock) {
    final String[] stringValuesMV = _jsonFieldTransformFunction.transformToStringValuesSV(projectionBlock);
    final long[][] results = new long[projectionBlock.getNumDocs()][];
    for (int i = 0; i < projectionBlock.getNumDocs(); i++) {
      final List<Long> intVals = JsonPath.read(stringValuesMV[i], _jsonPath);
      results[i] = new long[intVals.size()];
      for (int j = 0; j < intVals.size(); j++) {
        results[i][j] = intVals.get(j);
      }
    }
    return results;
  }

  @Override
  public float[][] transformToFloatValuesMV(@Nonnull ProjectionBlock projectionBlock) {
    final String[] stringValuesMV = _jsonFieldTransformFunction.transformToStringValuesSV(projectionBlock);
    final float[][] results = new float[projectionBlock.getNumDocs()][];
    for (int i = 0; i < projectionBlock.getNumDocs(); i++) {
      final List<Float> intVals = JsonPath.read(stringValuesMV[i], _jsonPath);
      results[i] = new float[intVals.size()];
      for (int j = 0; j < intVals.size(); j++) {
        results[i][j] = intVals.get(j);
      }
    }
    return results;
  }

  @Override
  public double[][] transformToDoubleValuesMV(@Nonnull ProjectionBlock projectionBlock) {
    final String[] stringValuesMV = _jsonFieldTransformFunction.transformToStringValuesSV(projectionBlock);
    final double[][] results = new double[projectionBlock.getNumDocs()][];
    for (int i = 0; i < projectionBlock.getNumDocs(); i++) {
      final List<Double> intVals = JsonPath.read(stringValuesMV[i], _jsonPath);
      results[i] = new double[intVals.size()];
      for (int j = 0; j < intVals.size(); j++) {
        results[i][j] = intVals.get(j);
      }
    }
    return results;
  }

  @Override
  public String[][] transformToStringValuesMV(@Nonnull ProjectionBlock projectionBlock) {
    final String[] stringValuesMV = _jsonFieldTransformFunction.transformToStringValuesSV(projectionBlock);
    final String[][] results = new String[projectionBlock.getNumDocs()][];
    for (int i = 0; i < projectionBlock.getNumDocs(); i++) {
      final List<String> intVals = JsonPath.read(stringValuesMV[i], _jsonPath);
      results[i] = new String[intVals.size()];
      for (int j = 0; j < intVals.size(); j++) {
        results[i][j] = intVals.get(j);
      }
    }
    return results;
  }
}
