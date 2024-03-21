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

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.function.JsonPathCache;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.segment.spi.index.reader.JsonIndexReader;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.RoaringBitmap;


/**
 * The <code>JsonExtractIndexTransformFunction</code> provides the same behavior as JsonExtractScalar, with the
 * implementation changed to read values from the JSON index. For large JSON blobs this can be faster than parsing
 * GBs of JSON at query time. For small JSON blobs/highly filtered input this is generally slower than the *scalar
 * implementation. The inflection point is highly dependent on the number of docs remaining post filter.
 */
public class JsonExtractIndexTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "jsonExtractIndex";

  private TransformFunction _jsonFieldTransformFunction;
  private String _jsonPathString;
  private TransformResultMetadata _resultMetadata;
  private JsonIndexReader _jsonIndexReader;
  private Object _defaultValue;
  private Map<String, RoaringBitmap> _valueToMatchingFlattenedDocIdsMap;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
    // Check that there are exactly 3 or 4
    if (arguments.size() < 3 || arguments.size() > 4) {
      throw new IllegalArgumentException(
          "Expected 3/4 arguments for transform function: jsonExtractIndex(jsonFieldName, 'jsonPath', 'resultsType',"
              + " ['defaultValue'])");
    }

    TransformFunction firstArgument = arguments.get(0);
    if (firstArgument instanceof IdentifierTransformFunction) {
      String columnName = ((IdentifierTransformFunction) firstArgument).getColumnName();
      _jsonIndexReader = columnContextMap.get(columnName).getDataSource().getJsonIndex();
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
    String inputJsonPath = ((LiteralTransformFunction) secondArgument).getStringLiteral();
    try {
      JsonPathCache.INSTANCE.getOrCompute(inputJsonPath);
    } catch (Exception e) {
      throw new IllegalArgumentException("JSON path argument is not a valid JSON path");
    }
    _jsonPathString = inputJsonPath.substring(1); // remove $ prefix

    TransformFunction thirdArgument = arguments.get(2);
    if (!(thirdArgument instanceof LiteralTransformFunction)) {
      throw new IllegalArgumentException("Result type argument must be a literal");
    }
    String resultsType = ((LiteralTransformFunction) thirdArgument).getStringLiteral().toUpperCase();
    boolean isSingleValue = !resultsType.endsWith("_ARRAY");
    if (isSingleValue && inputJsonPath.contains("[*]")) {
      throw new IllegalArgumentException(
          "[*] syntax in json path is unsupported for singleValue field json_extract_index");
    }
    DataType dataType = isSingleValue ? DataType.valueOf(resultsType)
        : DataType.valueOf(resultsType.substring(0, resultsType.length() - 6));

    if (arguments.size() == 4) {
      TransformFunction fourthArgument = arguments.get(3);
      if (!(fourthArgument instanceof LiteralTransformFunction)) {
        throw new IllegalArgumentException("Default value must be a literal");
      }
      _defaultValue = dataType.convert(((LiteralTransformFunction) fourthArgument).getStringLiteral());
    }

    _resultMetadata = new TransformResultMetadata(dataType, isSingleValue, false);
    _valueToMatchingFlattenedDocIdsMap =
        _jsonIndexReader.getMatchingFlattenedDocsMap(inputJsonPath.substring(1));
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
    String[] valuesFromIndex = _jsonIndexReader.getValuesForSv(valueBlock.getDocIds(), valueBlock.getNumDocs(),
        _valueToMatchingFlattenedDocIdsMap);
    for (int i = 0; i < numDocs; i++) {
      String value = valuesFromIndex[inputDocIds[i]];
      if (value == null) {
        if (_defaultValue != null) {
          _intValuesSV[i] = (int) _defaultValue;
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
    String[] valuesFromIndex = _jsonIndexReader.getValuesForSv(valueBlock.getDocIds(), valueBlock.getNumDocs(),
        _valueToMatchingFlattenedDocIdsMap);
    for (int i = 0; i < numDocs; i++) {
      String value = valuesFromIndex[i];
      if (value == null) {
        if (_defaultValue != null) {
          _longValuesSV[i] = (long) _defaultValue;
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
    String[] valuesFromIndex = _jsonIndexReader.getValuesForSv(valueBlock.getDocIds(), valueBlock.getNumDocs(),
        _valueToMatchingFlattenedDocIdsMap);
    for (int i = 0; i < numDocs; i++) {
      String value = valuesFromIndex[i];
      if (value == null) {
        if (_defaultValue != null) {
          _floatValuesSV[i] = (float) _defaultValue;
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
    String[] valuesFromIndex = _jsonIndexReader.getValuesForSv(valueBlock.getDocIds(), valueBlock.getNumDocs(),
        _valueToMatchingFlattenedDocIdsMap);
    for (int i = 0; i < numDocs; i++) {
      String value = valuesFromIndex[i];
      if (value == null) {
        if (_defaultValue != null) {
          _doubleValuesSV[i] = (double) _defaultValue;
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
    String[] valuesFromIndex = _jsonIndexReader.getValuesForSv(valueBlock.getDocIds(), valueBlock.getNumDocs(),
        _valueToMatchingFlattenedDocIdsMap);
    for (int i = 0; i < numDocs; i++) {
      String value = valuesFromIndex[i];
      if (value == null) {
        if (_defaultValue != null) {
          _bigDecimalValuesSV[i] = (BigDecimal) _defaultValue;
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
    String[] valuesFromIndex = _jsonIndexReader.getValuesForSv(valueBlock.getDocIds(), valueBlock.getNumDocs(),
        _valueToMatchingFlattenedDocIdsMap);
    for (int i = 0; i < numDocs; i++) {
      String value = valuesFromIndex[i];
      if (value == null) {
        if (_defaultValue != null) {
          _stringValuesSV[i] = (String) _defaultValue;
          continue;
        }
        throw new RuntimeException(
            String.format("Illegal Json Path: [%s], for docId [%s]", _jsonPathString, inputDocIds[i]));
      }
      _stringValuesSV[i] = value;
    }
    return _stringValuesSV;
  }

  @Override
  public int[][] transformToIntValuesMV(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    initIntValuesMV(numDocs);
    String[][] valuesFromIndex = _jsonIndexReader.getValuesForMv(valueBlock.getDocIds(), valueBlock.getNumDocs(),
        _valueToMatchingFlattenedDocIdsMap);

    for (int i = 0; i < numDocs; i++) {
      String[] value = valuesFromIndex[i];
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
    String[][] valuesFromIndex = _jsonIndexReader.getValuesForMv(valueBlock.getDocIds(), valueBlock.getNumDocs(),
        _valueToMatchingFlattenedDocIdsMap);
    for (int i = 0; i < numDocs; i++) {
      String[] value = valuesFromIndex[i];
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
    String[][] valuesFromIndex = _jsonIndexReader.getValuesForMv(valueBlock.getDocIds(), valueBlock.getNumDocs(),
        _valueToMatchingFlattenedDocIdsMap);
    for (int i = 0; i < numDocs; i++) {
      String[] value = valuesFromIndex[i];
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
    String[][] valuesFromIndex = _jsonIndexReader.getValuesForMv(valueBlock.getDocIds(), valueBlock.getNumDocs(),
        _valueToMatchingFlattenedDocIdsMap);
    for (int i = 0; i < numDocs; i++) {
      String[] value = valuesFromIndex[i];
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
    String[][] valuesFromIndex = _jsonIndexReader.getValuesForMv(valueBlock.getDocIds(), valueBlock.getNumDocs(),
        _valueToMatchingFlattenedDocIdsMap);
    for (int i = 0; i < numDocs; i++) {
      String[] value = valuesFromIndex[i];
      _stringValuesMV[i] = new String[value.length];
      System.arraycopy(value, 0, _stringValuesMV[i], 0, value.length);
    }
    return _stringValuesMV;
  }
}
