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

import com.google.common.collect.ImmutableSet;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.json.JsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import com.jayway.jsonpath.spi.mapper.MappingProvider;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * The <code>JsonExtractScalarTransformFunction</code> class implements the json path transformation based on
 * <a href="https://goessner.net/articles/JsonPath/">Stefan Goessner JsonPath implementation.</a>.
 *
 * Please note, currently this method only works with String field. The values in this field should be Json String.
 *
 * Usage:
 * jsonExtractScalar(jsonFieldName, 'jsonPath', 'resultsType')
 * <code>jsonFieldName</code> is the Json String field/expression.
 * <code>jsonPath</code> is a JsonPath expression which used to read from JSON document
 * <code>results_type</code> refers to the results data type, could be INT, LONG, FLOAT, DOUBLE, STRING, INT_ARRAY,
 * LONG_ARRAY, FLOAT_ARRAY, DOUBLE_ARRAY, STRING_ARRAY.
 *
 */
public class JsonExtractScalarTransformFunction extends BaseTransformFunction {

  public static final String FUNCTION_NAME = "jsonExtractScalar";
  private static final Configuration JSON_PARSER_CONFIG =
      Configuration.defaultConfiguration().addOptions(Option.SUPPRESS_EXCEPTIONS);
  private TransformFunction _jsonFieldTransformFunction;
  private String _jsonPath;
  private String _resultsType;
  private Object _defaultValue = null;
  private TransformResultMetadata _resultMetadata;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(@Nonnull List<TransformFunction> arguments, @Nonnull Map<String, DataSource> dataSourceMap) {
    // Check that there are exactly 3 or 4 arguments
    if (arguments.size() < 3 || arguments.size() > 4) {
      throw new IllegalArgumentException(
          "Expected 3/4 arguments for transform function: jsonExtractScalar(jsonFieldName, 'jsonPath', 'resultsType', ['defaultValue'])");
    }

    TransformFunction firstArgument = arguments.get(0);
    if (firstArgument instanceof LiteralTransformFunction || !firstArgument.getResultMetadata().isSingleValue()) {
      throw new IllegalArgumentException(
          "The first argument of jsonExtractScalar transform function must be a single-valued column or a transform function");
    }
    _jsonFieldTransformFunction = firstArgument;
    _jsonPath = ((LiteralTransformFunction) arguments.get(1)).getLiteral();
    _resultsType = ((LiteralTransformFunction) arguments.get(2)).getLiteral().toUpperCase();
    boolean isSingleValue = !_resultsType.toUpperCase().endsWith("_ARRAY");
    try {
      FieldSpec.DataType fieldType = FieldSpec.DataType.valueOf(_resultsType.split("_ARRAY")[0]);

      if (arguments.size() == 4) {
        String defaultValue = ((LiteralTransformFunction) arguments.get(3)).getLiteral();
        switch (fieldType) {
          case INT:
            _defaultValue = Double.valueOf(defaultValue).intValue();
            break;
          case LONG:
            _defaultValue = Double.valueOf(defaultValue).longValue();
            break;
          case FLOAT:
            _defaultValue = Double.valueOf(defaultValue).floatValue();
            break;
          case DOUBLE:
            _defaultValue = Double.valueOf(defaultValue);
            break;
          case BOOLEAN:
          case STRING:
            _defaultValue = defaultValue;
            break;
          case BYTES:
            throw new UnsupportedOperationException(String.format(
                "Unsupported results type: BYTES for 'jsonExtractScalar' Udf. Supported types are: INT/LONG/FLOAT/DOUBLE/STRING/INT_ARRAY/LONG/FLOAT_ARRAY/DOUBLE_ARRAY/STRING_ARRAY",
                _resultsType));
        }
      }
      _resultMetadata = new TransformResultMetadata(fieldType, isSingleValue, false);
    } catch (Exception e) {
      throw new UnsupportedOperationException(String.format(
          "Unsupported results type: %s for 'jsonExtractScalar' Udf. Supported types are: INT/LONG/FLOAT/DOUBLE/STRING/INT_ARRAY/LONG/FLOAT_ARRAY/DOUBLE_ARRAY/STRING_ARRAY",
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
      Object read = JsonPath.using(JSON_PARSER_CONFIG).parse(stringValuesSV[i]).read(_jsonPath);
      if (read == null) {
        if (_defaultValue != null) {
          results[i] = (int) _defaultValue;
          continue;
        }
        throw new RuntimeException(
            String.format("Illegal Json Path: [%s], when reading [%s]", _jsonPath, stringValuesSV[i]));
      }
      if (read instanceof Number) {
        results[i] = ((Number) read).intValue();
      } else {
        results[i] = Integer.parseInt(read.toString());
      }
    }
    return results;
  }

  @Override
  public long[] transformToLongValuesSV(@Nonnull ProjectionBlock projectionBlock) {
    final String[] stringValuesSV = _jsonFieldTransformFunction.transformToStringValuesSV(projectionBlock);
    final long[] results = new long[projectionBlock.getNumDocs()];
    for (int i = 0; i < projectionBlock.getNumDocs(); i++) {
      Object read = JsonPath.using(JSON_PARSER_CONFIG).parse(stringValuesSV[i]).read(_jsonPath);
      if (read == null) {
        if (_defaultValue != null) {
          results[i] = (long) _defaultValue;
          continue;
        }
        throw new RuntimeException(
            String.format("Illegal Json Path: [%s], when reading [%s]", _jsonPath, stringValuesSV[i]));
      }
      if (read instanceof Number) {
        results[i] = ((Number) read).longValue();
      } else {
        // Handle scientific notation
        results[i] = Double.valueOf(read.toString()).longValue();
      }
    }
    return results;
  }

  @Override
  public float[] transformToFloatValuesSV(@Nonnull ProjectionBlock projectionBlock) {
    final String[] stringValuesSV = _jsonFieldTransformFunction.transformToStringValuesSV(projectionBlock);
    final float[] results = new float[projectionBlock.getNumDocs()];
    for (int i = 0; i < projectionBlock.getNumDocs(); i++) {
      Object read = JsonPath.using(JSON_PARSER_CONFIG).parse(stringValuesSV[i]).read(_jsonPath);
      if (read == null) {
        if (_defaultValue != null) {
          results[i] = (float) _defaultValue;
          continue;
        }
        throw new RuntimeException(
            String.format("Illegal Json Path: [%s], when reading [%s]", _jsonPath, stringValuesSV[i]));
      }
      if (read instanceof Number) {
        results[i] = ((Number) read).floatValue();
      } else {
        results[i] = Double.valueOf(read.toString()).floatValue();
      }
    }
    return results;
  }

  @Override
  public double[] transformToDoubleValuesSV(@Nonnull ProjectionBlock projectionBlock) {
    final String[] stringValuesSV = _jsonFieldTransformFunction.transformToStringValuesSV(projectionBlock);
    final double[] results = new double[projectionBlock.getNumDocs()];
    for (int i = 0; i < projectionBlock.getNumDocs(); i++) {
      Object read = JsonPath.using(JSON_PARSER_CONFIG).parse(stringValuesSV[i]).read(_jsonPath);
      if (read == null) {
        if (_defaultValue != null) {
          results[i] = (double) _defaultValue;
          continue;
        }
        throw new RuntimeException(
            String.format("Illegal Json Path: [%s], when reading [%s]", _jsonPath, stringValuesSV[i]));
      }
      if (read instanceof Number) {
        results[i] = ((Number) read).doubleValue();
      } else if (read instanceof BigDecimal) {
        results[i] = ((BigDecimal) read).doubleValue();
      } else {
        results[i] = Double.valueOf(read.toString()).doubleValue();
      }
    }
    return results;
  }

  @Override
  public String[] transformToStringValuesSV(@Nonnull ProjectionBlock projectionBlock) {
    final String[] stringValuesSV = _jsonFieldTransformFunction.transformToStringValuesSV(projectionBlock);
    final String[] results = new String[projectionBlock.getNumDocs()];
    for (int i = 0; i < projectionBlock.getNumDocs(); i++) {
      Object read = JsonPath.using(JSON_PARSER_CONFIG).parse(stringValuesSV[i]).read(_jsonPath);
      if (read == null) {
        if (_defaultValue != null) {
          results[i] = (String) _defaultValue;
          continue;
        }
        throw new RuntimeException(
            String.format("Illegal Json Path: [%s], when reading [%s]", _jsonPath, stringValuesSV[i]));
      }
      if (read instanceof String) {
        results[i] = read.toString();
      } else {
        results[i] = JsonUtils.objectToJsonNode(read).toString();
      }
    }
    return results;
  }

  @Override
  public int[][] transformToIntValuesMV(@Nonnull ProjectionBlock projectionBlock) {
    final String[] stringValuesMV = _jsonFieldTransformFunction.transformToStringValuesSV(projectionBlock);
    final int[][] results = new int[projectionBlock.getNumDocs()][];
    for (int i = 0; i < projectionBlock.getNumDocs(); i++) {
      final List<Integer> intVals = JsonPath.using(JSON_PARSER_CONFIG).parse(stringValuesMV[i]).read(_jsonPath);
      if (intVals == null) {
        results[i] = new int[0];
        continue;
      }
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
      final List<Long> longVals = JsonPath.using(JSON_PARSER_CONFIG).parse(stringValuesMV[i]).read(_jsonPath);
      if (longVals == null) {
        results[i] = new long[0];
        continue;
      }
      results[i] = new long[longVals.size()];
      for (int j = 0; j < longVals.size(); j++) {
        results[i][j] = longVals.get(j);
      }
    }
    return results;
  }

  @Override
  public float[][] transformToFloatValuesMV(@Nonnull ProjectionBlock projectionBlock) {
    final String[] stringValuesMV = _jsonFieldTransformFunction.transformToStringValuesSV(projectionBlock);
    final float[][] results = new float[projectionBlock.getNumDocs()][];
    for (int i = 0; i < projectionBlock.getNumDocs(); i++) {
      final List<Float> floatVals = JsonPath.using(JSON_PARSER_CONFIG).parse(stringValuesMV[i]).read(_jsonPath);
      if (floatVals == null) {
        results[i] = new float[0];
        continue;
      }
      results[i] = new float[floatVals.size()];
      for (int j = 0; j < floatVals.size(); j++) {
        results[i][j] = floatVals.get(j);
      }
    }
    return results;
  }

  @Override
  public double[][] transformToDoubleValuesMV(@Nonnull ProjectionBlock projectionBlock) {
    final String[] stringValuesMV = _jsonFieldTransformFunction.transformToStringValuesSV(projectionBlock);
    final double[][] results = new double[projectionBlock.getNumDocs()][];
    for (int i = 0; i < projectionBlock.getNumDocs(); i++) {
      final List<Double> doubleVals = JsonPath.using(JSON_PARSER_CONFIG).parse(stringValuesMV[i]).read(_jsonPath);
      if (doubleVals == null) {
        results[i] = new double[0];
        continue;
      }
      results[i] = new double[doubleVals.size()];
      for (int j = 0; j < doubleVals.size(); j++) {
        results[i][j] = doubleVals.get(j);
      }
    }
    return results;
  }

  @Override
  public String[][] transformToStringValuesMV(@Nonnull ProjectionBlock projectionBlock) {
    final String[] stringValuesMV = _jsonFieldTransformFunction.transformToStringValuesSV(projectionBlock);
    final String[][] results = new String[projectionBlock.getNumDocs()][];
    for (int i = 0; i < projectionBlock.getNumDocs(); i++) {
      final List<String> stringVals = JsonPath.using(JSON_PARSER_CONFIG).parse(stringValuesMV[i]).read(_jsonPath);
      if (stringVals == null) {
        results[i] = new String[0];
        continue;
      }
      results[i] = new String[stringVals.size()];
      for (int j = 0; j < stringVals.size(); j++) {
        results[i][j] = stringVals.get(j);
      }
    }
    return results;
  }

  static {
    Configuration.setDefaults(new Configuration.Defaults() {

      private final JsonProvider jsonProvider = new JacksonJsonProvider();
      private final MappingProvider mappingProvider = new JacksonMappingProvider();

      @Override
      public JsonProvider jsonProvider() {
        return jsonProvider;
      }

      @Override
      public MappingProvider mappingProvider() {
        return mappingProvider;
      }

      @Override
      public Set<Option> options() {
        return ImmutableSet.of(Option.SUPPRESS_EXCEPTIONS);
      }
    });
  }
}
