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

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ParseContext;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.spi.data.FieldSpec.DataType;
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

  private static final ParseContext JSON_PARSER_CONTEXT = JsonPath.using(
      new Configuration.ConfigurationBuilder().jsonProvider(new JacksonJsonProvider())
          .mappingProvider(new JacksonMappingProvider()).options(Option.SUPPRESS_EXCEPTIONS).build());

  private TransformFunction _jsonFieldTransformFunction;
  private String _jsonPathString;
  private JsonPath _jsonPath;
  private Object _defaultValue = null;
  private TransformResultMetadata _resultMetadata;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
    // Check that there are exactly 3 or 4 arguments
    if (arguments.size() < 3 || arguments.size() > 4) {
      throw new IllegalArgumentException(
          "Expected 3/4 arguments for transform function: jsonExtractScalar(jsonFieldName, 'jsonPath', 'resultsType',"
              + " ['defaultValue'])");
    }

    TransformFunction firstArgument = arguments.get(0);
    if (firstArgument instanceof LiteralTransformFunction || !firstArgument.getResultMetadata().isSingleValue()) {
      throw new IllegalArgumentException(
          "The first argument of jsonExtractScalar transform function must be a single-valued column or a transform "
              + "function");
    }
    _jsonFieldTransformFunction = firstArgument;
    _jsonPathString = ((LiteralTransformFunction) arguments.get(1)).getLiteral();
    _jsonPath = JsonPath.compile(_jsonPathString);
    String resultsType = ((LiteralTransformFunction) arguments.get(2)).getLiteral().toUpperCase();
    boolean isSingleValue = !resultsType.endsWith("_ARRAY");
    try {
      DataType dataType =
          DataType.valueOf(isSingleValue ? resultsType : resultsType.substring(0, resultsType.length() - 6));
      if (arguments.size() == 4) {
        _defaultValue = dataType.convert(((LiteralTransformFunction) arguments.get(3)).getLiteral());
      }
      _resultMetadata = new TransformResultMetadata(dataType, isSingleValue, false);
    } catch (Exception e) {
      throw new IllegalStateException(String.format(
          "Unsupported results type: %s for jsonExtractScalar function. Supported types are: "
              + "INT/LONG/FLOAT/DOUBLE/BOOLEAN/TIMESTAMP/STRING/INT_ARRAY/LONG_ARRAY/FLOAT_ARRAY/DOUBLE_ARRAY"
              + "/STRING_ARRAY", resultsType));
    }
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return _resultMetadata;
  }

  @Override
  public int[] transformToIntValuesSV(ProjectionBlock projectionBlock) {
    if (_intValuesSV == null) {
      _intValuesSV = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }

    String[] jsonStrings = _jsonFieldTransformFunction.transformToStringValuesSV(projectionBlock);
    int numDocs = projectionBlock.getNumDocs();
    for (int i = 0; i < numDocs; i++) {
      Object result = null;
      try {
        result = JSON_PARSER_CONTEXT.parse(jsonStrings[i]).read(_jsonPath);
      } catch (Exception ignored) {
      }
      if (result == null) {
        if (_defaultValue != null) {
          _intValuesSV[i] = (int) _defaultValue;
          continue;
        }
        throw new RuntimeException(
            String.format("Illegal Json Path: [%s], when reading [%s]", _jsonPathString, jsonStrings[i]));
      }
      if (result instanceof Number) {
        _intValuesSV[i] = ((Number) result).intValue();
      } else {
        _intValuesSV[i] = Integer.parseInt(result.toString());
      }
    }
    return _intValuesSV;
  }

  @Override
  public long[] transformToLongValuesSV(ProjectionBlock projectionBlock) {
    if (_longValuesSV == null) {
      _longValuesSV = new long[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }

    String[] jsonStrings = _jsonFieldTransformFunction.transformToStringValuesSV(projectionBlock);
    int numDocs = projectionBlock.getNumDocs();
    for (int i = 0; i < numDocs; i++) {
      Object result = null;
      try {
        result = JSON_PARSER_CONTEXT.parse(jsonStrings[i]).read(_jsonPath);
      } catch (Exception ignored) {
      }
      if (result == null) {
        if (_defaultValue != null) {
          _longValuesSV[i] = (long) _defaultValue;
          continue;
        }
        throw new RuntimeException(
            String.format("Illegal Json Path: [%s], when reading [%s]", _jsonPathString, jsonStrings[i]));
      }
      if (result instanceof Number) {
        _longValuesSV[i] = ((Number) result).longValue();
      } else {
        // Handle scientific notation
        _longValuesSV[i] = (long) Double.parseDouble(result.toString());
      }
    }
    return _longValuesSV;
  }

  @Override
  public float[] transformToFloatValuesSV(ProjectionBlock projectionBlock) {
    if (_floatValuesSV == null) {
      _floatValuesSV = new float[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }

    String[] jsonStrings = _jsonFieldTransformFunction.transformToStringValuesSV(projectionBlock);
    int numDocs = projectionBlock.getNumDocs();
    for (int i = 0; i < numDocs; i++) {
      Object result = null;
      try {
        result = JSON_PARSER_CONTEXT.parse(jsonStrings[i]).read(_jsonPath);
      } catch (Exception ignored) {
      }
      if (result == null) {
        if (_defaultValue != null) {
          _floatValuesSV[i] = (float) _defaultValue;
          continue;
        }
        throw new RuntimeException(
            String.format("Illegal Json Path: [%s], when reading [%s]", _jsonPathString, jsonStrings[i]));
      }
      if (result instanceof Number) {
        _floatValuesSV[i] = ((Number) result).floatValue();
      } else {
        _floatValuesSV[i] = Float.parseFloat(result.toString());
      }
    }
    return _floatValuesSV;
  }

  @Override
  public double[] transformToDoubleValuesSV(ProjectionBlock projectionBlock) {
    if (_doubleValuesSV == null) {
      _doubleValuesSV = new double[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }

    String[] jsonStrings = _jsonFieldTransformFunction.transformToStringValuesSV(projectionBlock);
    int numDocs = projectionBlock.getNumDocs();
    for (int i = 0; i < numDocs; i++) {
      Object result = null;
      try {
        result = JSON_PARSER_CONTEXT.parse(jsonStrings[i]).read(_jsonPath);
      } catch (Exception ignored) {
      }
      if (result == null) {
        if (_defaultValue != null) {
          _doubleValuesSV[i] = (double) _defaultValue;
          continue;
        }
        throw new RuntimeException(
            String.format("Illegal Json Path: [%s], when reading [%s]", _jsonPathString, jsonStrings[i]));
      }
      if (result instanceof Number) {
        _doubleValuesSV[i] = ((Number) result).doubleValue();
      } else {
        _doubleValuesSV[i] = Double.parseDouble(result.toString());
      }
    }
    return _doubleValuesSV;
  }

  @Override
  public String[] transformToStringValuesSV(ProjectionBlock projectionBlock) {
    if (_stringValuesSV == null) {
      _stringValuesSV = new String[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }

    String[] jsonStrings = _jsonFieldTransformFunction.transformToStringValuesSV(projectionBlock);
    int numDocs = projectionBlock.getNumDocs();
    for (int i = 0; i < numDocs; i++) {
      Object result = null;
      try {
        result = JSON_PARSER_CONTEXT.parse(jsonStrings[i]).read(_jsonPath);
      } catch (Exception ignored) {
      }
      if (result == null) {
        if (_defaultValue != null) {
          _stringValuesSV[i] = (String) _defaultValue;
          continue;
        }
        throw new RuntimeException(
            String.format("Illegal Json Path: [%s], when reading [%s]", _jsonPathString, jsonStrings[i]));
      }
      if (result instanceof String) {
        _stringValuesSV[i] = (String) result;
      } else {
        _stringValuesSV[i] = JsonUtils.objectToJsonNode(result).toString();
      }
    }
    return _stringValuesSV;
  }

  @Override
  public int[][] transformToIntValuesMV(ProjectionBlock projectionBlock) {
    if (_intValuesMV == null) {
      _intValuesMV = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL][];
    }

    String[] jsonStrings = _jsonFieldTransformFunction.transformToStringValuesSV(projectionBlock);
    int numDocs = projectionBlock.getNumDocs();
    for (int i = 0; i < numDocs; i++) {
      List<Integer> result = null;
      try {
        result = JSON_PARSER_CONTEXT.parse(jsonStrings[i]).read(_jsonPath);
      } catch (Exception ignored) {
      }
      if (result == null) {
        _intValuesMV[i] = new int[0];
        continue;
      }
      int numValues = result.size();
      int[] values = new int[numValues];
      for (int j = 0; j < numValues; j++) {
        values[j] = result.get(j);
      }
      _intValuesMV[i] = values;
    }
    return _intValuesMV;
  }

  @Override
  public long[][] transformToLongValuesMV(ProjectionBlock projectionBlock) {
    if (_longValuesMV == null) {
      _longValuesMV = new long[DocIdSetPlanNode.MAX_DOC_PER_CALL][];
    }

    String[] jsonStrings = _jsonFieldTransformFunction.transformToStringValuesSV(projectionBlock);
    int length = projectionBlock.getNumDocs();
    for (int i = 0; i < length; i++) {
      List<Long> result = null;
      try {
        result = JSON_PARSER_CONTEXT.parse(jsonStrings[i]).read(_jsonPath);
      } catch (Exception ignored) {
      }
      if (result == null) {
        _longValuesMV[i] = new long[0];
        continue;
      }
      int numValues = result.size();
      long[] values = new long[numValues];
      for (int j = 0; j < numValues; j++) {
        values[j] = result.get(j);
      }
      _longValuesMV[i] = values;
    }
    return _longValuesMV;
  }

  @Override
  public float[][] transformToFloatValuesMV(ProjectionBlock projectionBlock) {
    if (_floatValuesMV == null) {
      _floatValuesMV = new float[DocIdSetPlanNode.MAX_DOC_PER_CALL][];
    }

    String[] jsonStrings = _jsonFieldTransformFunction.transformToStringValuesSV(projectionBlock);
    int length = projectionBlock.getNumDocs();
    for (int i = 0; i < length; i++) {
      List<Float> result = null;
      try {
        result = JSON_PARSER_CONTEXT.parse(jsonStrings[i]).read(_jsonPath);
      } catch (Exception ignored) {
      }
      if (result == null) {
        _floatValuesMV[i] = new float[0];
        continue;
      }
      int numValues = result.size();
      float[] values = new float[numValues];
      for (int j = 0; j < numValues; j++) {
        values[j] = result.get(j);
      }
      _floatValuesMV[i] = values;
    }
    return _floatValuesMV;
  }

  @Override
  public double[][] transformToDoubleValuesMV(ProjectionBlock projectionBlock) {
    if (_doubleValuesMV == null) {
      _doubleValuesMV = new double[DocIdSetPlanNode.MAX_DOC_PER_CALL][];
    }

    String[] jsonStrings = _jsonFieldTransformFunction.transformToStringValuesSV(projectionBlock);
    int length = projectionBlock.getNumDocs();
    for (int i = 0; i < length; i++) {
      List<Double> result = null;
      try {
        result = JSON_PARSER_CONTEXT.parse(jsonStrings[i]).read(_jsonPath);
      } catch (Exception ignored) {
      }
      if (result == null) {
        _doubleValuesMV[i] = new double[0];
        continue;
      }
      int numValues = result.size();
      double[] values = new double[numValues];
      for (int j = 0; j < numValues; j++) {
        values[j] = result.get(j);
      }
      _doubleValuesMV[i] = values;
    }
    return _doubleValuesMV;
  }

  @Override
  public String[][] transformToStringValuesMV(ProjectionBlock projectionBlock) {
    if (_stringValuesMV == null) {
      _stringValuesMV = new String[DocIdSetPlanNode.MAX_DOC_PER_CALL][];
    }

    String[] jsonStrings = _jsonFieldTransformFunction.transformToStringValuesSV(projectionBlock);
    int length = projectionBlock.getNumDocs();
    for (int i = 0; i < length; i++) {
      List<String> result = null;
      try {
        result = JSON_PARSER_CONTEXT.parse(jsonStrings[i]).read(_jsonPath);
      } catch (Exception ignored) {
      }
      if (result == null) {
        _stringValuesMV[i] = new String[0];
        continue;
      }
      int numValues = result.size();
      String[] values = new String[numValues];
      for (int j = 0; j < numValues; j++) {
        values[j] = result.get(j);
      }
      _stringValuesMV[i] = values;
    }
    return _stringValuesMV;
  }
}
