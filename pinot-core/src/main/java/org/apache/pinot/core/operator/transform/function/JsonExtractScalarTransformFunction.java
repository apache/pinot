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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ParseContext;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.function.JsonPathCache;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.segment.spi.evaluator.json.JsonPathEvaluator;
import org.apache.pinot.segment.spi.evaluator.json.JsonPathEvaluators;
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
 * <code>results_type</code> refers to the results data type, could be INT, LONG, FLOAT, DOUBLE, BIG_DECIMAL, STRING,
 * INT_ARRAY, LONG_ARRAY, FLOAT_ARRAY, DOUBLE_ARRAY, STRING_ARRAY.
 *
 */
public class JsonExtractScalarTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "jsonExtractScalar";

  // This ObjectMapper requires special configurations, hence we can't use pinot JsonUtils here.
  private static final ObjectMapper OBJECT_MAPPER_WITH_BIG_DECIMAL =
      new ObjectMapper().configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true);

  private static final ParseContext JSON_PARSER_CONTEXT_WITH_BIG_DECIMAL = JsonPath.using(
      new Configuration.ConfigurationBuilder().jsonProvider(new JacksonJsonProvider(OBJECT_MAPPER_WITH_BIG_DECIMAL))
          .mappingProvider(new JacksonMappingProvider()).options(Option.SUPPRESS_EXCEPTIONS).build());

  private static final ParseContext JSON_PARSER_CONTEXT = JsonPath.using(
      new Configuration.ConfigurationBuilder().jsonProvider(new JacksonJsonProvider())
          .mappingProvider(new JacksonMappingProvider()).options(Option.SUPPRESS_EXCEPTIONS).build());

  private TransformFunction _jsonFieldTransformFunction;
  private String _jsonPathString;
  private JsonPath _jsonPath;
  private Object _defaultValue;
  private JsonPathEvaluator _jsonPathEvaluator;
  private TransformResultMetadata _resultMetadata;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
    super.init(arguments, columnContextMap);
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
    _jsonPathString = ((LiteralTransformFunction) arguments.get(1)).getStringLiteral();
    String resultsType = ((LiteralTransformFunction) arguments.get(2)).getStringLiteral().toUpperCase();
    boolean isSingleValue = !resultsType.endsWith("_ARRAY");
    DataType dataType;
    try {
      dataType = DataType.valueOf(isSingleValue ? resultsType : resultsType.substring(0, resultsType.length() - 6));
    } catch (Exception e) {
      throw new IllegalArgumentException(String.format(
          "Unsupported results type: %s for jsonExtractScalar function. Supported types are: "
              + "INT/LONG/FLOAT/DOUBLE/BOOLEAN/BIG_DECIMAL/TIMESTAMP/STRING/INT_ARRAY/LONG_ARRAY/FLOAT_ARRAY"
              + "/DOUBLE_ARRAY/STRING_ARRAY", resultsType));
    }
    if (arguments.size() == 4) {
      _defaultValue = dataType.convert(((LiteralTransformFunction) arguments.get(3)).getStringLiteral());
    }
    _resultMetadata = new TransformResultMetadata(dataType, isSingleValue, false);
    try {
      _jsonPathEvaluator = JsonPathEvaluators.create(_jsonPathString, _defaultValue);
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid json path: " + _jsonPathString, e);
    }
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return _resultMetadata;
  }

  @Override
  public int[] transformToIntValuesSV(ValueBlock valueBlock) {
    initIntValuesSV(valueBlock.getNumDocs());
    if (_jsonFieldTransformFunction instanceof PushDownTransformFunction && valueBlock instanceof ProjectionBlock) {
      ((PushDownTransformFunction) _jsonFieldTransformFunction).transformToIntValuesSV((ProjectionBlock) valueBlock,
          _jsonPathEvaluator, _intValuesSV);
      return _intValuesSV;
    }
    // operating on the output of another transform so can't pass the evaluation down to the storage
    return transformTransformedValuesToIntValuesSV(valueBlock);
  }

  private int[] transformTransformedValuesToIntValuesSV(ValueBlock valueBlock) {
    // operating on the output of another transform so can't pass the evaluation down to the storage
    ensureJsonPathCompiled();
    String[] jsonStrings = _jsonFieldTransformFunction.transformToStringValuesSV(valueBlock);
    int numDocs = valueBlock.getNumDocs();
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
  public long[] transformToLongValuesSV(ValueBlock valueBlock) {
    initLongValuesSV(valueBlock.getNumDocs());
    if (_jsonFieldTransformFunction instanceof PushDownTransformFunction && valueBlock instanceof ProjectionBlock) {
      ((PushDownTransformFunction) _jsonFieldTransformFunction).transformToLongValuesSV((ProjectionBlock) valueBlock,
          _jsonPathEvaluator, _longValuesSV);
      return _longValuesSV;
    }
    return transformTransformedValuesToLongValuesSV(valueBlock);
  }

  private long[] transformTransformedValuesToLongValuesSV(ValueBlock valueBlock) {
    // operating on the output of another transform so can't pass the evaluation down to the storage
    ensureJsonPathCompiled();
    String[] jsonStrings = _jsonFieldTransformFunction.transformToStringValuesSV(valueBlock);
    int numDocs = valueBlock.getNumDocs();
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
  public float[] transformToFloatValuesSV(ValueBlock valueBlock) {
    initFloatValuesSV(valueBlock.getNumDocs());
    if (_jsonFieldTransformFunction instanceof PushDownTransformFunction && valueBlock instanceof ProjectionBlock) {
      ((PushDownTransformFunction) _jsonFieldTransformFunction).transformToFloatValuesSV((ProjectionBlock) valueBlock,
          _jsonPathEvaluator, _floatValuesSV);
      return _floatValuesSV;
    }
    return transformTransformedValuesToFloatValuesSV(valueBlock);
  }

  private float[] transformTransformedValuesToFloatValuesSV(ValueBlock valueBlock) {
    // operating on the output of another transform so can't pass the evaluation down to the storage
    ensureJsonPathCompiled();
    String[] jsonStrings = _jsonFieldTransformFunction.transformToStringValuesSV(valueBlock);
    int numDocs = valueBlock.getNumDocs();
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
  public double[] transformToDoubleValuesSV(ValueBlock valueBlock) {
    initDoubleValuesSV(valueBlock.getNumDocs());
    if (_jsonFieldTransformFunction instanceof PushDownTransformFunction && valueBlock instanceof ProjectionBlock) {
      ((PushDownTransformFunction) _jsonFieldTransformFunction).transformToDoubleValuesSV((ProjectionBlock) valueBlock,
          _jsonPathEvaluator, _doubleValuesSV);
      return _doubleValuesSV;
    }
    return transformTransformedValuesToDoubleValuesSV(valueBlock);
  }

  private double[] transformTransformedValuesToDoubleValuesSV(ValueBlock valueBlock) {
    // operating on the output of another transform so can't pass the evaluation down to the storage
    ensureJsonPathCompiled();
    String[] jsonStrings = _jsonFieldTransformFunction.transformToStringValuesSV(valueBlock);
    int numDocs = valueBlock.getNumDocs();
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
  public BigDecimal[] transformToBigDecimalValuesSV(ValueBlock valueBlock) {
    initBigDecimalValuesSV(valueBlock.getNumDocs());
    if (_jsonFieldTransformFunction instanceof PushDownTransformFunction && valueBlock instanceof ProjectionBlock) {
      ((PushDownTransformFunction) _jsonFieldTransformFunction).transformToBigDecimalValuesSV(
          (ProjectionBlock) valueBlock, _jsonPathEvaluator, _bigDecimalValuesSV);
      return _bigDecimalValuesSV;
    }
    return transformTransformedValuesToBigDecimalValuesSV(valueBlock);
  }

  private BigDecimal[] transformTransformedValuesToBigDecimalValuesSV(ValueBlock valueBlock) {
    // operating on the output of another transform so can't pass the evaluation down to the storage
    ensureJsonPathCompiled();
    String[] jsonStrings = _jsonFieldTransformFunction.transformToStringValuesSV(valueBlock);
    int numDocs = valueBlock.getNumDocs();
    for (int i = 0; i < numDocs; i++) {
      Object result = null;
      try {
        result = JSON_PARSER_CONTEXT_WITH_BIG_DECIMAL.parse(jsonStrings[i]).read(_jsonPath);
      } catch (Exception ignored) {
      }
      if (result == null) {
        if (_defaultValue != null) {
          _bigDecimalValuesSV[i] = (BigDecimal) _defaultValue;
          continue;
        }
        throw new RuntimeException(
            String.format("Illegal Json Path: [%s], when reading [%s]", _jsonPathString, jsonStrings[i]));
      }
      if (result instanceof Number) {
        _bigDecimalValuesSV[i] = (BigDecimal) result;
      } else {
        _bigDecimalValuesSV[i] = new BigDecimal(result.toString());
      }
    }
    return _bigDecimalValuesSV;
  }

  @Override
  public String[] transformToStringValuesSV(ValueBlock valueBlock) {
    initStringValuesSV(valueBlock.getNumDocs());
    if (_jsonFieldTransformFunction instanceof PushDownTransformFunction && valueBlock instanceof ProjectionBlock) {
      ((PushDownTransformFunction) _jsonFieldTransformFunction).transformToStringValuesSV((ProjectionBlock) valueBlock,
          _jsonPathEvaluator, _stringValuesSV);
      return _stringValuesSV;
    }
    return transformTransformedValuesToStringValuesSV(valueBlock);
  }

  private String[] transformTransformedValuesToStringValuesSV(ValueBlock valueBlock) {
    // operating on the output of another transform so can't pass the evaluation down to the storage
    ensureJsonPathCompiled();
    String[] jsonStrings = _jsonFieldTransformFunction.transformToStringValuesSV(valueBlock);
    int numDocs = valueBlock.getNumDocs();
    for (int i = 0; i < numDocs; i++) {
      Object result = null;
      try {
        result = JSON_PARSER_CONTEXT_WITH_BIG_DECIMAL.parse(jsonStrings[i]).read(_jsonPath);
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
  public int[][] transformToIntValuesMV(ValueBlock valueBlock) {
    initIntValuesMV(valueBlock.getNumDocs());
    if (_jsonFieldTransformFunction instanceof PushDownTransformFunction && valueBlock instanceof ProjectionBlock) {
      ((PushDownTransformFunction) _jsonFieldTransformFunction).transformToIntValuesMV((ProjectionBlock) valueBlock,
          _jsonPathEvaluator, _intValuesMV);
      return _intValuesMV;
    }
    return transformTransformedValuesToIntValuesMV(valueBlock);
  }

  private int[][] transformTransformedValuesToIntValuesMV(ValueBlock valueBlock) {
    // operating on the output of another transform so can't pass the evaluation down to the storage
    ensureJsonPathCompiled();
    String[] jsonStrings = _jsonFieldTransformFunction.transformToStringValuesSV(valueBlock);
    int numDocs = valueBlock.getNumDocs();
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
  public long[][] transformToLongValuesMV(ValueBlock valueBlock) {
    initLongValuesMV(valueBlock.getNumDocs());
    if (_jsonFieldTransformFunction instanceof PushDownTransformFunction && valueBlock instanceof ProjectionBlock) {
      ((PushDownTransformFunction) _jsonFieldTransformFunction).transformToLongValuesMV((ProjectionBlock) valueBlock,
          _jsonPathEvaluator, _longValuesMV);
      return _longValuesMV;
    }
    return transformTransformedValuesToLongValuesMV(valueBlock);
  }

  private long[][] transformTransformedValuesToLongValuesMV(ValueBlock valueBlock) {
    // operating on the output of another transform so can't pass the evaluation down to the storage
    ensureJsonPathCompiled();
    String[] jsonStrings = _jsonFieldTransformFunction.transformToStringValuesSV(valueBlock);
    int length = valueBlock.getNumDocs();
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
  public float[][] transformToFloatValuesMV(ValueBlock valueBlock) {
    initFloatValuesMV(valueBlock.getNumDocs());
    if (_jsonFieldTransformFunction instanceof PushDownTransformFunction && valueBlock instanceof ProjectionBlock) {
      ((PushDownTransformFunction) _jsonFieldTransformFunction).transformToFloatValuesMV((ProjectionBlock) valueBlock,
          _jsonPathEvaluator, _floatValuesMV);
      return _floatValuesMV;
    }
    return transformTransformedValuesToFloatValuesMV(valueBlock);
  }

  private float[][] transformTransformedValuesToFloatValuesMV(ValueBlock valueBlock) {
    // operating on the output of another transform so can't pass the evaluation down to the storage
    ensureJsonPathCompiled();
    String[] jsonStrings = _jsonFieldTransformFunction.transformToStringValuesSV(valueBlock);
    int length = valueBlock.getNumDocs();
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
  public double[][] transformToDoubleValuesMV(ValueBlock valueBlock) {
    initDoubleValuesMV(valueBlock.getNumDocs());
    if (_jsonFieldTransformFunction instanceof PushDownTransformFunction && valueBlock instanceof ProjectionBlock) {
      ((PushDownTransformFunction) _jsonFieldTransformFunction).transformToDoubleValuesMV((ProjectionBlock) valueBlock,
          _jsonPathEvaluator, _doubleValuesMV);
      return _doubleValuesMV;
    }
    return transformTransformedToDoubleValuesMV(valueBlock);
  }

  private double[][] transformTransformedToDoubleValuesMV(ValueBlock valueBlock) {
    // operating on the output of another transform so can't pass the evaluation down to the storage
    ensureJsonPathCompiled();
    String[] jsonStrings = _jsonFieldTransformFunction.transformToStringValuesSV(valueBlock);
    int length = valueBlock.getNumDocs();
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
  public String[][] transformToStringValuesMV(ValueBlock valueBlock) {
    initStringValuesMV(valueBlock.getNumDocs());
    if (_jsonFieldTransformFunction instanceof PushDownTransformFunction && valueBlock instanceof ProjectionBlock) {
      ((PushDownTransformFunction) _jsonFieldTransformFunction).transformToStringValuesMV((ProjectionBlock) valueBlock,
          _jsonPathEvaluator, _stringValuesMV);
      return _stringValuesMV;
    }
    return transformTransformedValuesToStringValuesMV(valueBlock);
  }

  private String[][] transformTransformedValuesToStringValuesMV(ValueBlock valueBlock) {
    // operating on the output of another transform so can't pass the evaluation down to the storage
    ensureJsonPathCompiled();
    String[] jsonStrings = _jsonFieldTransformFunction.transformToStringValuesSV(valueBlock);
    int length = valueBlock.getNumDocs();
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

  private void ensureJsonPathCompiled() {
    if (_jsonPath == null) {
      _jsonPath = JsonPathCache.INSTANCE.getOrCompute(_jsonPathString);
    }
  }
}
