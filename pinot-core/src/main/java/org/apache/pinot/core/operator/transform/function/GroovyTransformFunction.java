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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.EnumUtils;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.segment.local.function.GroovyFunctionEvaluator;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * The GroovyTransformFunction executes groovy expressions
 * 1st argument - json string containing returnType and isSingleValue e.g. '{"returnType":"LONG",
 * "isSingleValue":false}'
 * 2nd argument - groovy script (string) using arg0, arg1, arg2... as arguments e.g. 'arg0 + " " + arg1', 'arg0 +
 * arg1.toList().max() + arg2' etc
 * rest of the arguments - identifiers/functions to the groovy script
 *
 * Sample queries:
 * SELECT GROOVY('{"returnType":"LONG", "isSingleValue":false}', 'arg0.findIndexValues{it==1}', products) FROM myTable
 * SELECT GROOVY('{"returnType":"INT", "isSingleValue":true}', 'arg0 * arg1 * 10', arraylength(units), columnB ) FROM
 * bob
 */
public class GroovyTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "groovy";

  private static final String RETURN_TYPE_KEY = "returnType";
  private static final String IS_SINGLE_VALUE_KEY = "isSingleValue";
  private static final String ARGUMENT_PREFIX = "arg";
  private static final String GROOVY_TEMPLATE_WITH_ARGS = "Groovy({%s}, %s)";
  private static final String GROOVY_TEMPLATE_WITHOUT_ARGS = "Groovy({%s})";
  private static final String GROOVY_ARG_DELIMITER = ",";

  private TransformResultMetadata _resultMetadata;
  private GroovyFunctionEvaluator _groovyFunctionEvaluator;
  private int _numGroovyArgs;
  private TransformFunction[] _groovyArguments;
  private boolean[] _isSourceSingleValue;
  private DataType[] _sourceStoredTypes;
  private BiFunction<TransformFunction, ValueBlock, Object>[] _transformToValuesFunctions;
  private BiFunction<Object, Integer, Object>[] _fetchElementFunctions;
  private Object[] _sourceArrays;
  private Object[] _bindingValues;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
    super.init(arguments, columnContextMap);
    int numArgs = arguments.size();
    if (numArgs < 2) {
      throw new IllegalArgumentException("GROOVY transform function requires at least 2 arguments");
    }

    // 1st argument is a json string
    TransformFunction returnValueMetadata = arguments.get(0);
    Preconditions.checkState(returnValueMetadata instanceof LiteralTransformFunction,
        "First argument of GROOVY transform function must be a literal, representing a json string");
    String returnValueMetadataStr = ((LiteralTransformFunction) returnValueMetadata).getStringLiteral();
    try {
      JsonNode returnValueMetadataJson = JsonUtils.stringToJsonNode(returnValueMetadataStr);
      Preconditions.checkState(returnValueMetadataJson.hasNonNull(RETURN_TYPE_KEY),
          "The json string in the first argument of GROOVY transform function must have non-null 'returnType'");
      Preconditions.checkState(returnValueMetadataJson.hasNonNull(IS_SINGLE_VALUE_KEY),
          "The json string in the first argument of GROOVY transform function must have non-null 'isSingleValue'");
      String returnTypeStr = returnValueMetadataJson.get(RETURN_TYPE_KEY).asText();
      Preconditions.checkState(EnumUtils.isValidEnum(DataType.class, returnTypeStr),
          "The 'returnType' in the json string which is the first argument of GROOVY transform function must be a "
              + "valid FieldSpec.DataType enum value");
      _resultMetadata = new TransformResultMetadata(DataType.valueOf(returnTypeStr),
          returnValueMetadataJson.get(IS_SINGLE_VALUE_KEY).asBoolean(true), false);
    } catch (IOException e) {
      throw new IllegalStateException(
          "Caught exception when converting json string '" + returnValueMetadataStr + "' to JsonNode", e);
    }

    // 2nd argument is groovy expression string
    TransformFunction groovyTransformFunction = arguments.get(1);
    Preconditions.checkState(groovyTransformFunction instanceof LiteralTransformFunction,
        "Second argument of GROOVY transform function must be a literal string, representing the groovy expression");

    // 3rd argument onwards, all are arguments to the groovy function
    _numGroovyArgs = numArgs - 2;
    if (_numGroovyArgs > 0) {
      _groovyArguments = new TransformFunction[_numGroovyArgs];
      _isSourceSingleValue = new boolean[_numGroovyArgs];
      _sourceStoredTypes = new DataType[_numGroovyArgs];
      int idx = 0;
      for (int i = 2; i < numArgs; i++) {
        TransformFunction argument = arguments.get(i);
        Preconditions.checkState(!(argument instanceof LiteralTransformFunction),
            "Third argument onwards, all arguments must be a column or other transform function");
        _groovyArguments[idx] = argument;
        TransformResultMetadata resultMetadata = argument.getResultMetadata();
        _isSourceSingleValue[idx] = resultMetadata.isSingleValue();
        _sourceStoredTypes[idx++] = resultMetadata.getDataType().getStoredType();
      }
      // construct arguments string for GroovyFunctionEvaluator
      String argumentsStr = IntStream.range(0, _numGroovyArgs).mapToObj(i -> ARGUMENT_PREFIX + i)
          .collect(Collectors.joining(GROOVY_ARG_DELIMITER));
      _groovyFunctionEvaluator = new GroovyFunctionEvaluator(String.format(GROOVY_TEMPLATE_WITH_ARGS,
          ((LiteralTransformFunction) groovyTransformFunction).getStringLiteral(),
              argumentsStr));

      _transformToValuesFunctions = new BiFunction[_numGroovyArgs];
      _fetchElementFunctions = new BiFunction[_numGroovyArgs];
      initFunctions();
    } else {
      _groovyFunctionEvaluator = new GroovyFunctionEvaluator(String.format(GROOVY_TEMPLATE_WITHOUT_ARGS,
          ((LiteralTransformFunction) groovyTransformFunction).getStringLiteral()));
    }
    _sourceArrays = new Object[_numGroovyArgs];
    _bindingValues = new Object[_numGroovyArgs];
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return _resultMetadata;
  }

  private void initFunctions() {
    for (int i = 0; i < _numGroovyArgs; i++) {
      BiFunction<Object, Integer, Object> getElementFunction;
      BiFunction<TransformFunction, ValueBlock, Object> transformToValuesFunction;
      if (_isSourceSingleValue[i]) {
        switch (_sourceStoredTypes[i]) {
          case INT:
            transformToValuesFunction = TransformFunction::transformToIntValuesSV;
            getElementFunction = (sourceArray, position) -> ((int[]) sourceArray)[position];
            break;
          case LONG:
            transformToValuesFunction = TransformFunction::transformToLongValuesSV;
            getElementFunction = (sourceArray, position) -> ((long[]) sourceArray)[position];
            break;
          case FLOAT:
            transformToValuesFunction = TransformFunction::transformToFloatValuesSV;
            getElementFunction = (sourceArray, position) -> ((float[]) sourceArray)[position];
            break;
          case DOUBLE:
            transformToValuesFunction = TransformFunction::transformToDoubleValuesSV;
            getElementFunction = (sourceArray, position) -> ((double[]) sourceArray)[position];
            break;
          case BIG_DECIMAL:
            transformToValuesFunction = TransformFunction::transformToBigDecimalValuesSV;
            getElementFunction = (sourceArray, position) -> ((BigDecimal[]) sourceArray)[position];
            break;
          case STRING:
            transformToValuesFunction = TransformFunction::transformToStringValuesSV;
            getElementFunction = (sourceArray, position) -> ((String[]) sourceArray)[position];
            break;
          default:
            throw new IllegalStateException(
                "Unsupported data type '" + _sourceStoredTypes[i] + "' for GROOVY transform function");
        }
      } else {
        switch (_sourceStoredTypes[i]) {
          case INT:
            transformToValuesFunction = TransformFunction::transformToIntValuesMV;
            getElementFunction = (sourceArray, position) -> ((int[][]) sourceArray)[position];
            break;
          case LONG:
            transformToValuesFunction = TransformFunction::transformToLongValuesMV;
            getElementFunction = (sourceArray, position) -> ((long[][]) sourceArray)[position];
            break;
          case FLOAT:
            transformToValuesFunction = TransformFunction::transformToFloatValuesMV;
            getElementFunction = (sourceArray, position) -> ((float[][]) sourceArray)[position];
            break;
          case DOUBLE:
            transformToValuesFunction = TransformFunction::transformToDoubleValuesMV;
            getElementFunction = (sourceArray, position) -> ((double[][]) sourceArray)[position];
            break;
          case STRING:
            transformToValuesFunction = TransformFunction::transformToStringValuesMV;
            getElementFunction = (sourceArray, position) -> ((String[][]) sourceArray)[position];
            break;
          default:
            throw new IllegalStateException(
                "Unsupported data type '" + _sourceStoredTypes[i] + "' for GROOVY transform function");
        }
      }
      _transformToValuesFunctions[i] = transformToValuesFunction;
      _fetchElementFunctions[i] = getElementFunction;
    }
  }

  @Override
  public int[] transformToIntValuesSV(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    initIntValuesSV(length);
    for (int i = 0; i < _numGroovyArgs; i++) {
      _sourceArrays[i] = _transformToValuesFunctions[i].apply(_groovyArguments[i], valueBlock);
    }
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numGroovyArgs; j++) {
        _bindingValues[j] = _fetchElementFunctions[j].apply(_sourceArrays[j], i);
      }
      _intValuesSV[i] = (int) _groovyFunctionEvaluator.evaluate(_bindingValues);
    }
    return _intValuesSV;
  }

  @Override
  public long[] transformToLongValuesSV(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    initLongValuesSV(length);
    for (int i = 0; i < _numGroovyArgs; i++) {
      _sourceArrays[i] = _transformToValuesFunctions[i].apply(_groovyArguments[i], valueBlock);
    }
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numGroovyArgs; j++) {
        _bindingValues[j] = _fetchElementFunctions[j].apply(_sourceArrays[j], i);
      }
      _longValuesSV[i] = (long) _groovyFunctionEvaluator.evaluate(_bindingValues);
    }
    return _longValuesSV;
  }

  @Override
  public float[] transformToFloatValuesSV(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    initFloatValuesSV(length);
    for (int i = 0; i < _numGroovyArgs; i++) {
      _sourceArrays[i] = _transformToValuesFunctions[i].apply(_groovyArguments[i], valueBlock);
    }
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numGroovyArgs; j++) {
        _bindingValues[j] = _fetchElementFunctions[j].apply(_sourceArrays[j], i);
      }
      _floatValuesSV[i] = (float) _groovyFunctionEvaluator.evaluate(_bindingValues);
    }
    return _floatValuesSV;
  }

  @Override
  public double[] transformToDoubleValuesSV(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    initDoubleValuesSV(length);
    for (int i = 0; i < _numGroovyArgs; i++) {
      _sourceArrays[i] = _transformToValuesFunctions[i].apply(_groovyArguments[i], valueBlock);
    }
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numGroovyArgs; j++) {
        _bindingValues[j] = _fetchElementFunctions[j].apply(_sourceArrays[j], i);
      }
      _doubleValuesSV[i] = (double) _groovyFunctionEvaluator.evaluate(_bindingValues);
    }
    return _doubleValuesSV;
  }

  @Override
  public BigDecimal[] transformToBigDecimalValuesSV(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    initBigDecimalValuesSV(length);
    for (int i = 0; i < _numGroovyArgs; i++) {
      _sourceArrays[i] = _transformToValuesFunctions[i].apply(_groovyArguments[i], valueBlock);
    }
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numGroovyArgs; j++) {
        _bindingValues[j] = _fetchElementFunctions[j].apply(_sourceArrays[j], i);
      }
      _bigDecimalValuesSV[i] = (BigDecimal) _groovyFunctionEvaluator.evaluate(_bindingValues);
    }
    return _bigDecimalValuesSV;
  }

  @Override
  public String[] transformToStringValuesSV(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    initStringValuesSV(length);
    for (int i = 0; i < _numGroovyArgs; i++) {
      _sourceArrays[i] = _transformToValuesFunctions[i].apply(_groovyArguments[i], valueBlock);
    }
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numGroovyArgs; j++) {
        _bindingValues[j] = _fetchElementFunctions[j].apply(_sourceArrays[j], i);
      }
      _stringValuesSV[i] = (String) _groovyFunctionEvaluator.evaluate(_bindingValues);
    }
    return _stringValuesSV;
  }

  @Override
  public int[][] transformToIntValuesMV(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    initIntValuesMV(length);
    for (int i = 0; i < _numGroovyArgs; i++) {
      _sourceArrays[i] = _transformToValuesFunctions[i].apply(_groovyArguments[i], valueBlock);
    }
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numGroovyArgs; j++) {
        _bindingValues[j] = _fetchElementFunctions[j].apply(_sourceArrays[j], i);
      }
      Object result = _groovyFunctionEvaluator.evaluate(_bindingValues);
      if (result instanceof List) {
        _intValuesMV[i] = new IntArrayList((List<Integer>) result).toIntArray();
      } else if (result instanceof int[]) {
        _intValuesMV[i] = (int[]) result;
      } else {
        throw new IllegalStateException("Unexpected result type '" + result.getClass() + "' for GROOVY function");
      }
    }
    return _intValuesMV;
  }

  @Override
  public long[][] transformToLongValuesMV(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    initLongValuesMV(length);
    for (int i = 0; i < _numGroovyArgs; i++) {
      _sourceArrays[i] = _transformToValuesFunctions[i].apply(_groovyArguments[i], valueBlock);
    }
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numGroovyArgs; j++) {
        _bindingValues[j] = _fetchElementFunctions[j].apply(_sourceArrays[j], i);
      }
      Object result = _groovyFunctionEvaluator.evaluate(_bindingValues);
      if (result instanceof List) {
        _longValuesMV[i] = new LongArrayList((List<Long>) result).toLongArray();
      } else if (result instanceof long[]) {
        _longValuesMV[i] = (long[]) result;
      } else {
        throw new IllegalStateException("Unexpected result type '" + result.getClass() + "' for GROOVY function");
      }
    }
    return _longValuesMV;
  }

  @Override
  public float[][] transformToFloatValuesMV(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    initFloatValuesMV(length);
    for (int i = 0; i < _numGroovyArgs; i++) {
      _sourceArrays[i] = _transformToValuesFunctions[i].apply(_groovyArguments[i], valueBlock);
    }
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numGroovyArgs; j++) {
        _bindingValues[j] = _fetchElementFunctions[j].apply(_sourceArrays[j], i);
      }
      Object result = _groovyFunctionEvaluator.evaluate(_bindingValues);
      if (result instanceof List) {
        _floatValuesMV[i] = new FloatArrayList((List<Float>) result).toFloatArray();
      } else if (result instanceof float[]) {
        _floatValuesMV[i] = (float[]) result;
      } else {
        throw new IllegalStateException("Unexpected result type '" + result.getClass() + "' for GROOVY function");
      }
    }
    return _floatValuesMV;
  }

  @Override
  public double[][] transformToDoubleValuesMV(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    initDoubleValuesMV(length);
    for (int i = 0; i < _numGroovyArgs; i++) {
      _sourceArrays[i] = _transformToValuesFunctions[i].apply(_groovyArguments[i], valueBlock);
    }
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numGroovyArgs; j++) {
        _bindingValues[j] = _fetchElementFunctions[j].apply(_sourceArrays[j], i);
      }
      Object result = _groovyFunctionEvaluator.evaluate(_bindingValues);
      if (result instanceof List) {
        _doubleValuesMV[i] = new DoubleArrayList((List<Double>) result).toDoubleArray();
      } else if (result instanceof double[]) {
        _doubleValuesMV[i] = (double[]) result;
      } else {
        throw new IllegalStateException("Unexpected result type '" + result.getClass() + "' for GROOVY function");
      }
    }
    return _doubleValuesMV;
  }

  @Override
  public String[][] transformToStringValuesMV(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    initStringValuesMV(length);
    for (int i = 0; i < _numGroovyArgs; i++) {
      _sourceArrays[i] = _transformToValuesFunctions[i].apply(_groovyArguments[i], valueBlock);
    }
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numGroovyArgs; j++) {
        _bindingValues[j] = _fetchElementFunctions[j].apply(_sourceArrays[j], i);
      }
      Object result = _groovyFunctionEvaluator.evaluate(_bindingValues);
      if (result instanceof List) {
        _stringValuesMV[i] = ((List<String>) result).toArray(new String[0]);
      } else if (result instanceof String[]) {
        _stringValuesMV[i] = (String[]) result;
      } else {
        throw new IllegalStateException("Unexpected result type '" + result.getClass() + "' for GROOVY function");
      }
    }
    return _stringValuesMV;
  }
}
