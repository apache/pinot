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
import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.pinot.common.function.JsonPathCache;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.segment.spi.index.IndexService;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.reader.JsonIndexReader;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.CommonConstants.NullValuePlaceHolder;
import org.apache.pinot.spi.utils.JsonUtils;
import org.roaringbitmap.RoaringBitmap;


/**
 * The <code>JsonExtractIndexTransformFunction</code> provides the same behavior as JsonExtractScalar, with the
 * implementation changed to read values from the JSON index. For large JSON blobs this can be faster than parsing
 * GBs of JSON at query time. For small JSON blobs/highly filtered input this is generally slower than the *scalar
 * implementation. The inflection point is highly dependent on the number of docs remaining post filter.
 *
 * <p><b>Null handling:</b> when query-level null handling is enabled (e.g. {@code SET enableNullHandling = true})
 * and no default literal is supplied, an unresolved JSON path on a row surfaces as SQL {@code NULL} via the
 * {@link #getNullBitmap(ValueBlock)} bitmap rather than throwing. With null handling disabled, the legacy throw
 * is preserved. A user-supplied non-null default literal takes precedence and is written for unresolved rows
 * regardless of null-handling state.
 */
public class JsonExtractIndexTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "jsonExtractIndex";

  private TransformFunction _jsonFieldTransformFunction;
  private String _jsonPathString;
  private TransformResultMetadata _resultMetadata;
  private JsonIndexReader _jsonIndexReader;
  private Object _defaultValue;
  private Map<String, RoaringBitmap> _valueToMatchingDocsMap;
  private boolean _isSingleValue;
  private String _filterJsonPath;

  // Cache for the per-ValueBlock SV scan result. Both the SV transforms and getNullBitmap need
  // the same `String[]` for the same ValueBlock; without caching, we'd hit the JSON index twice
  // per block on the null-handling path. Identity comparison is safe — the broker constructs a
  // fresh ValueBlock per batch and never mutates a previously returned one.
  private ValueBlock _cachedValueBlock;
  private String[] _cachedValuesSV;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap,
      boolean nullHandlingEnabled) {
    super.init(arguments, columnContextMap, nullHandlingEnabled);
    // Check that there are exactly 3 or 4 or 5 arguments
    if (arguments.size() < 3 || arguments.size() > 5) {
      throw new IllegalArgumentException(
          "Expected 3/4/5 arguments for transform function: jsonExtractIndex(jsonFieldName, 'jsonPath', 'resultsType',"
              + " ['defaultValue'], ['jsonFilterExpression'])");
    }

    TransformFunction firstArgument = arguments.get(0);
    if (firstArgument instanceof IdentifierTransformFunction) {
      String columnName = ((IdentifierTransformFunction) firstArgument).getColumnName();
      _jsonIndexReader = columnContextMap.get(columnName).getDataSource().getJsonIndex();
      if (_jsonIndexReader == null) { //TODO: rework
        Optional<IndexType<?, ?, ?>> compositeIndex =
            IndexService.getInstance().getOptional("composite_json_index");
        if (compositeIndex.isPresent()) {
          _jsonIndexReader = (JsonIndexReader) columnContextMap.get(columnName)
              .getDataSource().getIndex(compositeIndex.get());
        }
      }
      if (_jsonIndexReader == null) {
        throw new IllegalStateException("jsonExtractIndex can only be applied on a column with JSON index");
      }
    } else {
      throw new IllegalArgumentException("jsonExtractIndex can only be applied to a raw column");
    }
    _jsonFieldTransformFunction = firstArgument;

    TransformFunction secondArgument = arguments.get(1);
    if (!(secondArgument instanceof LiteralTransformFunction)) {
      throw new IllegalArgumentException("JSON path argument must be a literal");
    }
    _jsonPathString = ((LiteralTransformFunction) secondArgument).getStringLiteral();
    try {
      JsonPathCache.INSTANCE.getOrCompute(_jsonPathString);
    } catch (Exception e) {
      throw new IllegalArgumentException("JSON path argument is not a valid JSON path");
    }

    TransformFunction thirdArgument = arguments.get(2);
    if (!(thirdArgument instanceof LiteralTransformFunction)) {
      throw new IllegalArgumentException("Result type argument must be a literal");
    }
    String resultsType = ((LiteralTransformFunction) thirdArgument).getStringLiteral().toUpperCase();
    _isSingleValue = !resultsType.endsWith("_ARRAY");
    if (_isSingleValue && _jsonPathString.contains("[*]")) {
      throw new IllegalArgumentException(
          "[*] syntax in json path is unsupported for singleValue field json_extract_index");
    }
    DataType dataType = _isSingleValue ? DataType.valueOf(resultsType)
        : DataType.valueOf(resultsType.substring(0, resultsType.length() - 6));

    if (arguments.size() >= 4) {
      TransformFunction fourthArgument = arguments.get(3);
      if (!(fourthArgument instanceof LiteralTransformFunction)) {
        throw new IllegalArgumentException("Default value must be a literal");
      }

      if (_isSingleValue) {
        _defaultValue = dataType.convert(((LiteralTransformFunction) fourthArgument).getStringLiteral());
      } else {
        try {
          JsonNode mvArray = JsonUtils.stringToJsonNode(((LiteralTransformFunction) fourthArgument).getStringLiteral());
          if (!mvArray.isArray()) {
            throw new IllegalArgumentException("Default value must be a valid JSON array");
          }
          Object[] defaultValues = new Object[mvArray.size()];
          for (int i = 0; i < mvArray.size(); i++) {
            defaultValues[i] = dataType.convert(mvArray.get(i).asText());
          }
          _defaultValue = defaultValues;
        } catch (IOException e) {
          throw new IllegalArgumentException("Default value must be a valid JSON array");
        }
      }
    }

    if (arguments.size() == 5) {
      TransformFunction fifthArgument = arguments.get(4);
      if (!(fifthArgument instanceof LiteralTransformFunction)) {
        throw new IllegalArgumentException("JSON path filter argument must be a literal");
      }
      _filterJsonPath = ((LiteralTransformFunction) fifthArgument).getStringLiteral();
    }

    _resultMetadata = new TransformResultMetadata(dataType, _isSingleValue, false);
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return _resultMetadata;
  }

  @Override
  public int[] transformToIntValuesSV(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    int[] inputDocIds = valueBlock.getDocIds();
    initIntValuesSV(numDocs);
    String[] valuesFromIndex = getCachedValuesFromIndex(valueBlock);
    for (int i = 0; i < numDocs; i++) {
      String value = valuesFromIndex[i];
      if (value == null) {
        if (_defaultValue != null) {
          _intValuesSV[i] = (int) _defaultValue;
          continue;
        }
        if (_nullHandlingEnabled) {
          _intValuesSV[i] = NullValuePlaceHolder.INT;
          continue;
        }
        throw new RuntimeException(
            String.format("Illegal Json Path: [%s], for docId [%s]", _jsonPathString, inputDocIds[i]));
      }
      _intValuesSV[i] = Integer.parseInt(value);
    }
    return _intValuesSV;
  }

  @Override
  public long[] transformToLongValuesSV(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    int[] inputDocIds = valueBlock.getDocIds();
    initLongValuesSV(numDocs);
    String[] valuesFromIndex = getCachedValuesFromIndex(valueBlock);
    for (int i = 0; i < numDocs; i++) {
      String value = valuesFromIndex[i];
      if (value == null) {
        if (_defaultValue != null) {
          _longValuesSV[i] = (long) _defaultValue;
          continue;
        }
        if (_nullHandlingEnabled) {
          _longValuesSV[i] = NullValuePlaceHolder.LONG;
          continue;
        }
        throw new RuntimeException(
            String.format("Illegal Json Path: [%s], for docId [%s]", _jsonPathString, inputDocIds[i]));
      }
      _longValuesSV[i] = Long.parseLong(value);
    }
    return _longValuesSV;
  }

  @Override
  public float[] transformToFloatValuesSV(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    int[] inputDocIds = valueBlock.getDocIds();
    initFloatValuesSV(numDocs);
    String[] valuesFromIndex = getCachedValuesFromIndex(valueBlock);
    for (int i = 0; i < numDocs; i++) {
      String value = valuesFromIndex[i];
      if (value == null) {
        if (_defaultValue != null) {
          _floatValuesSV[i] = (float) _defaultValue;
          continue;
        }
        if (_nullHandlingEnabled) {
          _floatValuesSV[i] = NullValuePlaceHolder.FLOAT;
          continue;
        }
        throw new RuntimeException(
            String.format("Illegal Json Path: [%s], for docId [%s]", _jsonPathString, inputDocIds[i]));
      }
      _floatValuesSV[i] = Float.parseFloat(value);
    }
    return _floatValuesSV;
  }

  @Override
  public double[] transformToDoubleValuesSV(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    int[] inputDocIds = valueBlock.getDocIds();
    initDoubleValuesSV(numDocs);
    String[] valuesFromIndex = getCachedValuesFromIndex(valueBlock);
    for (int i = 0; i < numDocs; i++) {
      String value = valuesFromIndex[i];
      if (value == null) {
        if (_defaultValue != null) {
          _doubleValuesSV[i] = (double) _defaultValue;
          continue;
        }
        if (_nullHandlingEnabled) {
          _doubleValuesSV[i] = NullValuePlaceHolder.DOUBLE;
          continue;
        }
        throw new RuntimeException(
            String.format("Illegal Json Path: [%s], for docId [%s]", _jsonPathString, inputDocIds[i]));
      }
      _doubleValuesSV[i] = Double.parseDouble(value);
    }
    return _doubleValuesSV;
  }

  @Override
  public BigDecimal[] transformToBigDecimalValuesSV(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    int[] inputDocIds = valueBlock.getDocIds();
    initBigDecimalValuesSV(numDocs);
    String[] valuesFromIndex = getCachedValuesFromIndex(valueBlock);
    for (int i = 0; i < numDocs; i++) {
      String value = valuesFromIndex[i];
      if (value == null) {
        if (_defaultValue != null) {
          _bigDecimalValuesSV[i] = (BigDecimal) _defaultValue;
          continue;
        }
        if (_nullHandlingEnabled) {
          _bigDecimalValuesSV[i] = NullValuePlaceHolder.BIG_DECIMAL;
          continue;
        }
        throw new RuntimeException(
            String.format("Illegal Json Path: [%s], for docId [%s]", _jsonPathString, inputDocIds[i]));
      }
      _bigDecimalValuesSV[i] = new BigDecimal(value);
    }
    return _bigDecimalValuesSV;
  }

  @Override
  public String[] transformToStringValuesSV(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    int[] inputDocIds = valueBlock.getDocIds();
    initStringValuesSV(numDocs);
    String[] valuesFromIndex = getCachedValuesFromIndex(valueBlock);
    for (int i = 0; i < numDocs; i++) {
      String value = valuesFromIndex[i];
      if (value == null) {
        if (_defaultValue != null) {
          _stringValuesSV[i] = (String) _defaultValue;
          continue;
        }
        if (_nullHandlingEnabled) {
          _stringValuesSV[i] = NullValuePlaceHolder.STRING;
          continue;
        }
        throw new RuntimeException(
            String.format("Illegal Json Path: [%s], for docId [%s]", _jsonPathString, inputDocIds[i]));
      }
      _stringValuesSV[i] = value;
    }
    return _stringValuesSV;
  }

  @Nullable
  @Override
  public RoaringBitmap getNullBitmap(ValueBlock valueBlock) {
    // Short-circuit to argument-bitmap propagation when this function isn't introducing nulls of
    // its own: non-SV output, null handling disabled, or any default literal supplied (the SV
    // transform writes it for unresolved rows). Unlike the scalar function, the parser converts a
    // SQL-NULL literal to the typed zero ("" for STRING, throws at init for numerics), so there's
    // no SQL-NULL-default path to fall through.
    if (!_isSingleValue || !_nullHandlingEnabled || _defaultValue != null) {
      return super.getNullBitmap(valueBlock);
    }
    String[] valuesFromIndex = getCachedValuesFromIndex(valueBlock);
    int numDocs = valueBlock.getNumDocs();
    RoaringBitmap nullBitmap = new RoaringBitmap();
    for (int i = 0; i < numDocs; i++) {
      if (valuesFromIndex[i] == null) {
        nullBitmap.add(i);
      }
    }
    return nullBitmap.isEmpty() ? null : nullBitmap;
  }

  @Override
  public int[][] transformToIntValuesMV(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    initIntValuesMV(numDocs);
    String[][] valuesFromIndex = _jsonIndexReader.getValuesMV(valueBlock.getDocIds(), valueBlock.getNumDocs(),
        getValueToMatchingDocsMap());

    for (int i = 0; i < numDocs; i++) {
      String[] value = valuesFromIndex[i];
      if (value.length == 0) {
        if (_defaultValue != null) {
          _intValuesMV[i] = new int[((Object[]) (_defaultValue)).length];
          for (int j = 0; j < _intValuesMV[i].length; j++) {
            _intValuesMV[i][j] = (int) ((Object[]) _defaultValue)[j];
          }
          continue;
        }
        throw new RuntimeException(
            String.format("Illegal Json Path: [%s], for docId [%s]", _jsonPathString, valueBlock.getDocIds()[i]));
      }
      _intValuesMV[i] = new int[value.length];
      for (int j = 0; j < value.length; j++) {
        _intValuesMV[i][j] = Integer.parseInt(value[j]);
      }
    }
    return _intValuesMV;
  }

  @Override
  public long[][] transformToLongValuesMV(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    initLongValuesMV(numDocs);
    String[][] valuesFromIndex = _jsonIndexReader.getValuesMV(valueBlock.getDocIds(), valueBlock.getNumDocs(),
        getValueToMatchingDocsMap());
    for (int i = 0; i < numDocs; i++) {
      String[] value = valuesFromIndex[i];
      if (value.length == 0) {
        if (_defaultValue != null) {
          _longValuesMV[i] = new long[((Object[]) (_defaultValue)).length];
          for (int j = 0; j < _longValuesMV[i].length; j++) {
            _longValuesMV[i][j] = (long) ((Object[]) _defaultValue)[j];
          }
          continue;
        }
        throw new RuntimeException(
            String.format("Illegal Json Path: [%s], for docId [%s]", _jsonPathString, valueBlock.getDocIds()[i]));
      }
      _longValuesMV[i] = new long[value.length];
      for (int j = 0; j < value.length; j++) {
        _longValuesMV[i][j] = Long.parseLong(value[j]);
      }
    }
    return _longValuesMV;
  }

  @Override
  public float[][] transformToFloatValuesMV(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    initFloatValuesMV(numDocs);
    String[][] valuesFromIndex = _jsonIndexReader.getValuesMV(valueBlock.getDocIds(), valueBlock.getNumDocs(),
        getValueToMatchingDocsMap());
    for (int i = 0; i < numDocs; i++) {
      String[] value = valuesFromIndex[i];
      if (value.length == 0) {
        if (_defaultValue != null) {
          _floatValuesMV[i] = new float[((Object[]) (_defaultValue)).length];
          for (int j = 0; j < _floatValuesMV[i].length; j++) {
            _floatValuesMV[i][j] = (float) ((Object[]) _defaultValue)[j];
          }
          continue;
        }
        throw new RuntimeException(
            String.format("Illegal Json Path: [%s], for docId [%s]", _jsonPathString, valueBlock.getDocIds()[i]));
      }
      _floatValuesMV[i] = new float[value.length];
      for (int j = 0; j < value.length; j++) {
        _floatValuesMV[i][j] = Float.parseFloat(value[j]);
      }
    }
    return _floatValuesMV;
  }

  @Override
  public double[][] transformToDoubleValuesMV(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    initDoubleValuesMV(numDocs);
    String[][] valuesFromIndex = _jsonIndexReader.getValuesMV(valueBlock.getDocIds(), valueBlock.getNumDocs(),
        getValueToMatchingDocsMap());
    for (int i = 0; i < numDocs; i++) {
      String[] value = valuesFromIndex[i];
      if (value.length == 0) {
        if (_defaultValue != null) {
          _doubleValuesMV[i] = new double[((Object[]) (_defaultValue)).length];
          for (int j = 0; j < _doubleValuesMV[i].length; j++) {
            _doubleValuesMV[i][j] = (double) ((Object[]) _defaultValue)[j];
          }
          continue;
        }
        throw new RuntimeException(
            String.format("Illegal Json Path: [%s], for docId [%s]", _jsonPathString, valueBlock.getDocIds()[i]));
      }
      _doubleValuesMV[i] = new double[value.length];
      for (int j = 0; j < value.length; j++) {
        _doubleValuesMV[i][j] = Double.parseDouble(value[j]);
      }
    }
    return _doubleValuesMV;
  }

  @Override
  public String[][] transformToStringValuesMV(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    initStringValuesMV(numDocs);
    String[][] valuesFromIndex = _jsonIndexReader.getValuesMV(valueBlock.getDocIds(), valueBlock.getNumDocs(),
        getValueToMatchingDocsMap());
    for (int i = 0; i < numDocs; i++) {
      String[] value = valuesFromIndex[i];
      if (value.length == 0) {
        if (_defaultValue != null) {
          _stringValuesMV[i] = new String[((Object[]) (_defaultValue)).length];
          for (int j = 0; j < _stringValuesMV[i].length; j++) {
            _stringValuesMV[i][j] = (String) ((Object[]) _defaultValue)[j];
          }
          continue;
        }
        throw new RuntimeException(
            String.format("Illegal Json Path: [%s], for docId [%s]", _jsonPathString, valueBlock.getDocIds()[i]));
      }
      _stringValuesMV[i] = new String[value.length];
      System.arraycopy(value, 0, _stringValuesMV[i], 0, value.length);
    }
    return _stringValuesMV;
  }

  /**
   * Lazily initialize _valueToMatchingDocsMap, so that map generation is skipped when filtering excludes all values
   */
  private Map<String, RoaringBitmap> getValueToMatchingDocsMap() {
    if (_valueToMatchingDocsMap == null) {
      _valueToMatchingDocsMap = _jsonIndexReader.getMatchingFlattenedDocsMap(_jsonPathString, _filterJsonPath);
      if (_isSingleValue) {
        // For single value result type, it's more efficient to use original docIDs map
        _jsonIndexReader.convertFlattenedDocIdsToDocIds(_valueToMatchingDocsMap);
      }
    }
    return _valueToMatchingDocsMap;
  }

  /**
   * Returns the JSON-index SV scan result for the given block, caching it so that the SV
   * transform and {@link #getNullBitmap} don't each pay the cost on the null-handling path.
   */
  private String[] getCachedValuesFromIndex(ValueBlock valueBlock) {
    if (valueBlock != _cachedValueBlock) {
      _cachedValuesSV = _jsonIndexReader.getValuesSV(valueBlock.getDocIds(), valueBlock.getNumDocs(),
          getValueToMatchingDocsMap(), false);
      _cachedValueBlock = valueBlock;
    }
    return _cachedValuesSV;
  }
}
