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

import com.fasterxml.jackson.core.JsonProcessingException;
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
import java.util.function.IntFunction;
import javax.annotation.Nullable;
import org.apache.pinot.common.function.JsonPathCache;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.util.NumberUtils;
import org.apache.pinot.core.util.NumericException;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.BooleanUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.TimestampUtils;
import org.roaringbitmap.RoaringBitmap;


/// Implements the `jsonExtractScalar(jsonField, jsonPath, resultsType[, defaultValue])` transform.
/// Reads a JSON document from `jsonField` for each row, resolves the
/// [Stefan Goessner JsonPath](https://goessner.net/articles/JsonPath/) expression against it, and
/// converts the resolved value to `resultsType`.
///
/// **Arguments:**
/// - `jsonField` — single-value `STRING` or `BYTES` column / transform expression containing JSON.
/// - `jsonPath` — JsonPath expression used to read the value.
/// - `resultsType` — Pinot data type for the output. Append `_ARRAY` for multi-value results.
/// - `defaultValue` (optional) — used when the path resolves to `null` or fails. Without it, unresolved
///   SV rows throw `IllegalArgumentException`; MV rows surface as empty arrays, but null elements within
///   a resolved array still throw.
///
/// **Supported `resultsType`:** `INT`, `LONG`, `FLOAT`, `DOUBLE`, `BIG_DECIMAL`, `BOOLEAN`, `TIMESTAMP`,
/// `STRING`, `JSON`, `BYTES`, plus `_ARRAY` variants of `INT` / `LONG` / `FLOAT` / `DOUBLE` /
/// `BIG_DECIMAL` / `STRING`.
///
/// **Per-row coercion** of the JsonPath result to `resultsType`:
/// - `BOOLEAN` (stored as `INT`) follows Pinot's numeric convention — any non-zero `Number` is true;
///   `Boolean` and `"true"` / `"TRUE"` / `"1"` strings (via [BooleanUtils#toInt(String)]) are also true.
/// - `TIMESTAMP` (stored as `LONG`) accepts numeric epoch millis directly; strings go through
///   [TimestampUtils#toMillisSinceEpoch] (ISO-8601 and numeric millis strings).
/// - `STRING` returns `String` values as-is; other JSON values are serialized via
///   [JsonUtils#objectToString].
/// - `BIG_DECIMAL` and `STRING` paths use a BigDecimal-preserving JSON parser
///   (`JSON_PARSER_CONTEXT_WITH_BIG_DECIMAL`) to avoid precision loss on numeric values; other paths use
///   the default parser.
/// - Other types coerce via `Number` cast (preserved as the canonical primitive form) or
///   `parse*(toString())`.
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
  private JsonPath _jsonPath;
  private DataType _dataType;
  private DataType _storedType;
  private Object _defaultValue;
  private boolean _defaultIsNull;
  private TransformResultMetadata _resultMetadata;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap,
      boolean nullHandlingEnabled) {
    super.init(arguments, columnContextMap, nullHandlingEnabled);
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
    String jsonPathString = ((LiteralTransformFunction) arguments.get(1)).getStringLiteral();
    _jsonPath = JsonPathCache.INSTANCE.getOrCompute(jsonPathString);
    String resultsType = ((LiteralTransformFunction) arguments.get(2)).getStringLiteral().toUpperCase();
    boolean isSingleValue = !resultsType.endsWith("_ARRAY");
    try {
      _dataType = DataType.valueOf(isSingleValue ? resultsType : resultsType.substring(0, resultsType.length() - 6));
    } catch (Exception e) {
      throw new IllegalArgumentException(String.format(
          "Unsupported results type: %s for jsonExtractScalar function. Supported types are: "
              + "INT/LONG/FLOAT/DOUBLE/BIG_DECIMAL/BOOLEAN/TIMESTAMP/STRING/JSON/BYTES/"
              + "INT_ARRAY/LONG_ARRAY/FLOAT_ARRAY/DOUBLE_ARRAY/BIG_DECIMAL_ARRAY/STRING_ARRAY", resultsType));
    }
    _storedType = _dataType.getStoredType();
    if (arguments.size() == 4) {
      LiteralTransformFunction literalTransformFun = (LiteralTransformFunction) arguments.get(3);
      _defaultIsNull = literalTransformFun.isNull() && _nullHandlingEnabled;
      switch (_dataType) {
        case INT:
          _defaultValue = literalTransformFun.getIntLiteral();
          break;
        case LONG:
          _defaultValue = literalTransformFun.getLongLiteral();
          break;
        case FLOAT:
          _defaultValue = literalTransformFun.getFloatLiteral();
          break;
        case DOUBLE:
          _defaultValue = literalTransformFun.getDoubleLiteral();
          break;
        case BIG_DECIMAL:
          _defaultValue = literalTransformFun.getBigDecimalLiteral();
          break;
        case BOOLEAN:
          // Stored as Integer 0 / 1 to match BOOLEAN's storedType (INT) so per-row consumers can
          // unbox directly without a Boolean → Integer conversion.
          _defaultValue = literalTransformFun.getBooleanLiteral() ? 1 : 0;
          break;
        case TIMESTAMP:
          // Use long literal so numeric millis stay exact and string timestamps use LiteralContext parsing.
          _defaultValue = literalTransformFun.getLongLiteral();
          break;
        case STRING:
        case JSON:
          _defaultValue = literalTransformFun.getStringLiteral();
          break;
        case BYTES:
          _defaultValue = literalTransformFun.getBytesLiteral();
          break;
        default:
          throw new IllegalArgumentException(
              "Unsupported results type: " + _dataType + " for jsonExtractScalar function. Supported types are: "
                  + "INT/LONG/FLOAT/DOUBLE/BIG_DECIMAL/BOOLEAN/TIMESTAMP/STRING/JSON/BYTES"
          );
      }
    }
    _resultMetadata = new TransformResultMetadata(_dataType, isSingleValue, false);
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return _resultMetadata;
  }

  @Override
  @Nullable
  public RoaringBitmap getNullBitmap(ValueBlock valueBlock) {
    if (!_defaultIsNull) {
      return super.getNullBitmap(valueBlock);
    }
    RoaringBitmap bitmap = new RoaringBitmap();
    for (TransformFunction arg : _arguments.subList(1, _arguments.size() - 1)) {
      RoaringBitmap argBitmap = arg.getNullBitmap(valueBlock);
      if (argBitmap != null) {
        bitmap.or(argBitmap);
      }
    }
    int numDocs = valueBlock.getNumDocs();
    RoaringBitmap nullBitmap = new RoaringBitmap();
    IntFunction<Object> resultExtractor = getResultExtractor(valueBlock);
    for (int i = 0; i < numDocs; i++) {
      Object result = null;
      try {
        result = resultExtractor.apply(i);
      } catch (Exception ignored) {
      }
      if (result == null) {
        nullBitmap.add(i);
      }
    }
    if (!nullBitmap.isEmpty()) {
      bitmap.or(nullBitmap);
    }
    if (bitmap.isEmpty()) {
      return null;
    }
    return bitmap;
  }

  @Override
  public int[] transformToIntValuesSV(ValueBlock valueBlock) {
    if (_storedType != DataType.INT) {
      return super.transformToIntValuesSV(valueBlock);
    }
    initIntValuesSV(valueBlock.getNumDocs());
    IntFunction<Object> resultExtractor = getResultExtractor(valueBlock);
    int defaultValue = _defaultValue != null ? (Integer) _defaultValue : 0;
    boolean isBoolean = _dataType == DataType.BOOLEAN;
    int numDocs = valueBlock.getNumDocs();
    for (int i = 0; i < numDocs; i++) {
      Object result = null;
      try {
        result = resultExtractor.apply(i);
      } catch (Exception ignored) {
      }
      if (result == null) {
        if (_defaultValue != null) {
          _intValuesSV[i] = defaultValue;
          continue;
        }
        throw new IllegalArgumentException(
            "Cannot resolve JSON path on some records. Consider setting a default value.");
      }
      _intValuesSV[i] = toInt(result, isBoolean);
    }
    return _intValuesSV;
  }

  @Override
  public long[] transformToLongValuesSV(ValueBlock valueBlock) {
    if (_storedType != DataType.LONG) {
      return super.transformToLongValuesSV(valueBlock);
    }
    initLongValuesSV(valueBlock.getNumDocs());
    IntFunction<Object> resultExtractor = getResultExtractor(valueBlock);
    long defaultValue = _defaultValue != null ? (Long) _defaultValue : 0L;
    boolean isTimestamp = _dataType == DataType.TIMESTAMP;
    int numDocs = valueBlock.getNumDocs();
    for (int i = 0; i < numDocs; i++) {
      Object result = null;
      try {
        result = resultExtractor.apply(i);
      } catch (Exception ignored) {
      }
      if (result == null) {
        if (_defaultValue != null) {
          _longValuesSV[i] = defaultValue;
          continue;
        }
        throw new IllegalArgumentException(
            "Cannot resolve JSON path on some records. Consider setting a default value.");
      }
      _longValuesSV[i] = toLong(result, isTimestamp);
    }
    return _longValuesSV;
  }

  @Override
  public float[] transformToFloatValuesSV(ValueBlock valueBlock) {
    if (_storedType != DataType.FLOAT) {
      return super.transformToFloatValuesSV(valueBlock);
    }
    initFloatValuesSV(valueBlock.getNumDocs());
    IntFunction<Object> resultExtractor = getResultExtractor(valueBlock);
    float defaultValue = _defaultValue != null ? (Float) _defaultValue : 0f;
    int numDocs = valueBlock.getNumDocs();
    for (int i = 0; i < numDocs; i++) {
      Object result = null;
      try {
        result = resultExtractor.apply(i);
      } catch (Exception ignored) {
      }
      if (result == null) {
        if (_defaultValue != null) {
          _floatValuesSV[i] = defaultValue;
          continue;
        }
        throw new IllegalArgumentException(
            "Cannot resolve JSON path on some records. Consider setting a default value.");
      }
      _floatValuesSV[i] = toFloat(result);
    }
    return _floatValuesSV;
  }

  @Override
  public double[] transformToDoubleValuesSV(ValueBlock valueBlock) {
    if (_storedType != DataType.DOUBLE) {
      return super.transformToDoubleValuesSV(valueBlock);
    }
    initDoubleValuesSV(valueBlock.getNumDocs());
    IntFunction<Object> resultExtractor = getResultExtractor(valueBlock);
    double defaultValue = _defaultValue != null ? (Double) _defaultValue : 0d;
    int numDocs = valueBlock.getNumDocs();
    for (int i = 0; i < numDocs; i++) {
      Object result = null;
      try {
        result = resultExtractor.apply(i);
      } catch (Exception ignored) {
      }
      if (result == null) {
        if (_defaultValue != null) {
          _doubleValuesSV[i] = defaultValue;
          continue;
        }
        throw new IllegalArgumentException(
            "Cannot resolve JSON path on some records. Consider setting a default value.");
      }
      _doubleValuesSV[i] = toDouble(result);
    }
    return _doubleValuesSV;
  }

  @Override
  public BigDecimal[] transformToBigDecimalValuesSV(ValueBlock valueBlock) {
    if (_storedType != DataType.BIG_DECIMAL) {
      return super.transformToBigDecimalValuesSV(valueBlock);
    }
    initBigDecimalValuesSV(valueBlock.getNumDocs());
    IntFunction<Object> resultExtractor = getResultExtractorWithBigDecimal(valueBlock);
    BigDecimal defaultValue = (BigDecimal) _defaultValue;
    int numDocs = valueBlock.getNumDocs();
    for (int i = 0; i < numDocs; i++) {
      Object result = null;
      try {
        result = resultExtractor.apply(i);
      } catch (Exception ignored) {
      }
      if (result == null) {
        if (_defaultValue != null) {
          _bigDecimalValuesSV[i] = defaultValue;
          continue;
        }
        throw new IllegalArgumentException(
            "Cannot resolve JSON path on some records. Consider setting a default value.");
      }
      _bigDecimalValuesSV[i] = toBigDecimal(result);
    }
    return _bigDecimalValuesSV;
  }

  @Override
  public String[] transformToStringValuesSV(ValueBlock valueBlock) {
    if (_storedType != DataType.STRING) {
      return super.transformToStringValuesSV(valueBlock);
    }
    initStringValuesSV(valueBlock.getNumDocs());
    IntFunction<Object> resultExtractor = getResultExtractorWithBigDecimal(valueBlock);
    String defaultValue = (String) _defaultValue;
    int numDocs = valueBlock.getNumDocs();
    for (int i = 0; i < numDocs; i++) {
      Object result = null;
      try {
        result = resultExtractor.apply(i);
      } catch (Exception ignored) {
      }
      if (result == null) {
        if (_defaultValue != null) {
          _stringValuesSV[i] = defaultValue;
          continue;
        }
        throw new IllegalArgumentException(
            "Cannot resolve JSON path on some records. Consider setting a default value.");
      }
      _stringValuesSV[i] = toString(result);
    }
    return _stringValuesSV;
  }

  @Override
  public int[][] transformToIntValuesMV(ValueBlock valueBlock) {
    if (_storedType != DataType.INT) {
      return super.transformToIntValuesMV(valueBlock);
    }
    initIntValuesMV(valueBlock.getNumDocs());
    IntFunction<List<Object>> resultExtractor = getResultExtractor(valueBlock);
    int defaultValue = _defaultValue != null ? (Integer) _defaultValue : 0;
    boolean isBoolean = _dataType == DataType.BOOLEAN;
    int numDocs = valueBlock.getNumDocs();
    for (int i = 0; i < numDocs; i++) {
      List<Object> result = null;
      try {
        result = resultExtractor.apply(i);
      } catch (Exception ignored) {
      }
      if (result == null) {
        _intValuesMV[i] = new int[0];
        continue;
      }
      int numValues = result.size();
      int[] values = new int[numValues];
      for (int j = 0; j < numValues; j++) {
        Object element = result.get(j);
        if (element == null) {
          if (_defaultValue != null) {
            values[j] = defaultValue;
            continue;
          }
          throw new IllegalArgumentException(
              "At least one of the resolved JSON arrays include nulls, which is not supported in Pinot. "
                  + "Consider setting a default value as the fourth argument of json_extract_scalar.");
        }
        values[j] = toInt(element, isBoolean);
      }
      _intValuesMV[i] = values;
    }
    return _intValuesMV;
  }

  @Override
  public long[][] transformToLongValuesMV(ValueBlock valueBlock) {
    if (_storedType != DataType.LONG) {
      return super.transformToLongValuesMV(valueBlock);
    }
    initLongValuesMV(valueBlock.getNumDocs());
    IntFunction<List<Object>> resultExtractor = getResultExtractor(valueBlock);
    long defaultValue = _defaultValue != null ? (Long) _defaultValue : 0L;
    boolean isTimestamp = _dataType == DataType.TIMESTAMP;
    int numDocs = valueBlock.getNumDocs();
    for (int i = 0; i < numDocs; i++) {
      List<Object> result = null;
      try {
        result = resultExtractor.apply(i);
      } catch (Exception ignored) {
      }
      if (result == null) {
        _longValuesMV[i] = new long[0];
        continue;
      }
      int numValues = result.size();
      long[] values = new long[numValues];
      for (int j = 0; j < numValues; j++) {
        Object element = result.get(j);
        if (element == null) {
          if (_defaultValue != null) {
            values[j] = defaultValue;
            continue;
          }
          throw new IllegalArgumentException(
              "At least one of the resolved JSON arrays include nulls, which is not supported in Pinot. "
                  + "Consider setting a default value as the fourth argument of json_extract_scalar.");
        }
        values[j] = toLong(element, isTimestamp);
      }
      _longValuesMV[i] = values;
    }
    return _longValuesMV;
  }

  @Override
  public float[][] transformToFloatValuesMV(ValueBlock valueBlock) {
    if (_storedType != DataType.FLOAT) {
      return super.transformToFloatValuesMV(valueBlock);
    }
    initFloatValuesMV(valueBlock.getNumDocs());
    IntFunction<List<Object>> resultExtractor = getResultExtractor(valueBlock);
    float defaultValue = _defaultValue != null ? (Float) _defaultValue : 0f;
    int numDocs = valueBlock.getNumDocs();
    for (int i = 0; i < numDocs; i++) {
      List<Object> result = null;
      try {
        result = resultExtractor.apply(i);
      } catch (Exception ignored) {
      }
      if (result == null) {
        _floatValuesMV[i] = new float[0];
        continue;
      }
      int numValues = result.size();
      float[] values = new float[numValues];
      for (int j = 0; j < numValues; j++) {
        Object element = result.get(j);
        if (element == null) {
          if (_defaultValue != null) {
            values[j] = defaultValue;
            continue;
          }
          throw new IllegalArgumentException(
              "At least one of the resolved JSON arrays include nulls, which is not supported in Pinot. "
                  + "Consider setting a default value as the fourth argument of json_extract_scalar.");
        }
        values[j] = toFloat(element);
      }
      _floatValuesMV[i] = values;
    }
    return _floatValuesMV;
  }

  @Override
  public double[][] transformToDoubleValuesMV(ValueBlock valueBlock) {
    if (_storedType != DataType.DOUBLE) {
      return super.transformToDoubleValuesMV(valueBlock);
    }
    initDoubleValuesMV(valueBlock.getNumDocs());
    IntFunction<List<Object>> resultExtractor = getResultExtractor(valueBlock);
    double defaultValue = _defaultValue != null ? (Double) _defaultValue : 0d;
    int numDocs = valueBlock.getNumDocs();
    for (int i = 0; i < numDocs; i++) {
      List<Object> result = null;
      try {
        result = resultExtractor.apply(i);
      } catch (Exception ignored) {
      }
      if (result == null) {
        _doubleValuesMV[i] = new double[0];
        continue;
      }
      int numValues = result.size();
      double[] values = new double[numValues];
      for (int j = 0; j < numValues; j++) {
        Object element = result.get(j);
        if (element == null) {
          if (_defaultValue != null) {
            values[j] = defaultValue;
            continue;
          }
          throw new IllegalArgumentException(
              "At least one of the resolved JSON arrays include nulls, which is not supported in Pinot. "
                  + "Consider setting a default value as the fourth argument of json_extract_scalar.");
        }
        values[j] = toDouble(element);
      }
      _doubleValuesMV[i] = values;
    }
    return _doubleValuesMV;
  }

  @Override
  public BigDecimal[][] transformToBigDecimalValuesMV(ValueBlock valueBlock) {
    if (_storedType != DataType.BIG_DECIMAL) {
      return super.transformToBigDecimalValuesMV(valueBlock);
    }
    initBigDecimalValuesMV(valueBlock.getNumDocs());
    IntFunction<List<Object>> resultExtractor = getResultExtractorWithBigDecimal(valueBlock);
    BigDecimal defaultValue = (BigDecimal) _defaultValue;
    int numDocs = valueBlock.getNumDocs();
    for (int i = 0; i < numDocs; i++) {
      List<Object> result = null;
      try {
        result = resultExtractor.apply(i);
      } catch (Exception ignored) {
      }
      if (result == null) {
        _bigDecimalValuesMV[i] = new BigDecimal[0];
        continue;
      }
      int numValues = result.size();
      BigDecimal[] values = new BigDecimal[numValues];
      for (int j = 0; j < numValues; j++) {
        Object element = result.get(j);
        if (element == null) {
          if (_defaultValue != null) {
            values[j] = defaultValue;
            continue;
          }
          throw new IllegalArgumentException(
              "At least one of the resolved JSON arrays include nulls, which is not supported in Pinot. "
                  + "Consider setting a default value as the fourth argument of json_extract_scalar.");
        }
        values[j] = toBigDecimal(element);
      }
      _bigDecimalValuesMV[i] = values;
    }
    return _bigDecimalValuesMV;
  }

  @Override
  public String[][] transformToStringValuesMV(ValueBlock valueBlock) {
    if (_storedType != DataType.STRING) {
      return super.transformToStringValuesMV(valueBlock);
    }
    initStringValuesMV(valueBlock.getNumDocs());
    IntFunction<List<Object>> resultExtractor = getResultExtractorWithBigDecimal(valueBlock);
    String defaultValue = (String) _defaultValue;
    int numDocs = valueBlock.getNumDocs();
    for (int i = 0; i < numDocs; i++) {
      List<Object> result = null;
      try {
        result = resultExtractor.apply(i);
      } catch (Exception ignored) {
      }
      if (result == null) {
        _stringValuesMV[i] = new String[0];
        continue;
      }
      int numValues = result.size();
      String[] values = new String[numValues];
      for (int j = 0; j < numValues; j++) {
        Object element = result.get(j);
        if (element == null) {
          if (_defaultValue != null) {
            values[j] = defaultValue;
            continue;
          }
          throw new IllegalArgumentException(
              "At least one of the resolved JSON arrays include nulls, which is not supported in Pinot. "
                  + "Consider setting a default value as the fourth argument of json_extract_scalar.");
        }
        values[j] = toString(element);
      }
      _stringValuesMV[i] = values;
    }
    return _stringValuesMV;
  }

  private static int toInt(Object value, boolean isBoolean) {
    if (isBoolean) {
      if (value instanceof Boolean) {
        return (Boolean) value ? 1 : 0;
      }
      // For BOOLEAN result, follow PinotDataType numeric convention: non-zero number → true.
      if (value instanceof Number) {
        return ((Number) value).doubleValue() != 0 ? 1 : 0;
      }
      // String fallback: BooleanUtils.toInt accepts "true" / "TRUE" / "1".
      return BooleanUtils.toInt(value.toString());
    }
    if (value instanceof Number) {
      return ((Number) value).intValue();
    }
    if (value instanceof Boolean) {
      return (Boolean) value ? 1 : 0;
    }
    return Integer.parseInt(value.toString());
  }

  private static long toLong(Object value, boolean isTimestamp) {
    if (value instanceof Number) {
      return ((Number) value).longValue();
    }
    if (isTimestamp) {
      return TimestampUtils.toMillisSinceEpoch(value.toString());
    }
    if (value instanceof Boolean) {
      return (Boolean) value ? 1L : 0L;
    }
    try {
      return NumberUtils.parseJsonLong(value.toString());
    } catch (NumericException nfe) {
      throw new NumberFormatException("For input string: \"" + value + "\"");
    }
  }

  private static float toFloat(Object value) {
    if (value instanceof Number) {
      return ((Number) value).floatValue();
    }
    if (value instanceof Boolean) {
      return (Boolean) value ? 1f : 0f;
    }
    return Float.parseFloat(value.toString());
  }

  private static double toDouble(Object value) {
    if (value instanceof Number) {
      return ((Number) value).doubleValue();
    }
    if (value instanceof Boolean) {
      return (Boolean) value ? 1d : 0d;
    }
    return Double.parseDouble(value.toString());
  }

  private static BigDecimal toBigDecimal(Object value) {
    if (value instanceof BigDecimal) {
      return (BigDecimal) value;
    }
    if (value instanceof Boolean) {
      return (Boolean) value ? BigDecimal.ONE : BigDecimal.ZERO;
    }
    return new BigDecimal(value.toString());
  }

  private static String toString(Object value) {
    if (value instanceof String) {
      return (String) value;
    }
    try {
      return JsonUtils.objectToString(value);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Caught exception while serializing JSON value: " + value, e);
    }
  }

  private <T> IntFunction<T> getResultExtractor(ValueBlock valueBlock, ParseContext parseContext) {
    if (_jsonFieldTransformFunction.getResultMetadata().getDataType() == DataType.BYTES) {
      byte[][] jsonBytes = _jsonFieldTransformFunction.transformToBytesValuesSV(valueBlock);
      return i -> parseContext.parseUtf8(jsonBytes[i]).read(_jsonPath);
    } else {
      String[] jsonStrings = _jsonFieldTransformFunction.transformToStringValuesSV(valueBlock);
      return i -> parseContext.parse(jsonStrings[i]).read(_jsonPath);
    }
  }

  private <T> IntFunction<T> getResultExtractor(ValueBlock valueBlock) {
    return getResultExtractor(valueBlock, JSON_PARSER_CONTEXT);
  }

  private <T> IntFunction<T> getResultExtractorWithBigDecimal(ValueBlock valueBlock) {
    return getResultExtractor(valueBlock, JSON_PARSER_CONTEXT_WITH_BIG_DECIMAL);
  }
}
