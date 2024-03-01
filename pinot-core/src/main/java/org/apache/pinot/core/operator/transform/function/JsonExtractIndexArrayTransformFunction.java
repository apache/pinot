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

import java.util.List;
import java.util.Map;
import org.apache.pinot.common.function.JsonPathCache;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.segment.spi.index.reader.JsonIndexReader;
import org.apache.pinot.spi.data.FieldSpec;
import org.roaringbitmap.RoaringBitmap;


public class JsonExtractIndexArrayTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "jsonExtractIndexArray";

  private TransformFunction _jsonFieldTransformFunction;
  private String _jsonPathString;
  private String _filterJsonPathString;
  private TransformResultMetadata _resultMetadata;
  private JsonIndexReader _jsonIndexReader;
  private Object _defaultValue;
  private Map<String, RoaringBitmap> _matchingDocsMap;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
    // Check that there are exactly 3, 4 or 5 arguments
    if (arguments.size() < 3 || arguments.size() > 5) {
      throw new IllegalArgumentException(
          "Expected 3/4 arguments for transform function: jsonExtractIndex(jsonFieldName, 'jsonPath', 'resultsType',"
              + "['defaultValue'], [jsonFilterExpression])");
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
    FieldSpec.DataType dataType = FieldSpec.DataType.valueOf(isSingleValue ? resultsType : resultsType.substring(0, resultsType.length() - 6));

    if (arguments.size() >= 4) {
      TransformFunction fourthArgument = arguments.get(3);
      if (!(fourthArgument instanceof LiteralTransformFunction)) {
        throw new IllegalArgumentException("Default value must be a literal");
      }
      _defaultValue = dataType.convert(((LiteralTransformFunction) fourthArgument).getStringLiteral());
    }

    if (arguments.size() == 5) {
      TransformFunction fifthArgument = arguments.get(4);
      if (!(fifthArgument instanceof LiteralTransformFunction)) {
        throw new IllegalArgumentException("JSON path filter argument must be a literal");
      }
      String filterJsonPath = ((LiteralTransformFunction) fifthArgument).getStringLiteral();
      try {
        JsonPathCache.INSTANCE.getOrCompute(filterJsonPath);
      } catch (Exception e) {
        throw new IllegalArgumentException("JSON path argument is not a valid JSON path");
      }
      _filterJsonPathString = inputJsonPath.substring(1); // remove $ prefix
    }

    _resultMetadata = new TransformResultMetadata(dataType, isSingleValue, false);
    _matchingDocsMap = _jsonIndexReader.getMatchingFlattenedDocsMap(_jsonPathString, _filterJsonPathString);
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return _resultMetadata;
  }

  @Override
  public int[][] transformToIntValuesMV(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    int[] inputDocIds = valueBlock.getDocIds();
    initIntValuesMV(numDocs);
    String[][] valuesFromIndex =
        _jsonIndexReader.getValuesForKeyAndFlattenedDocs(valueBlock.getDocIds(), _matchingDocsMap);
    for (int i = 0; i < numDocs; i++) {
      String[] value = valuesFromIndex[i];

      if (value == null) {
        if (_defaultValue != null) {
          _intValuesMV[i] = new int[]{(int) _defaultValue};
          continue;
        }
        throw new RuntimeException(
            String.format("Illegal Json Path: [%s], for docId [%s]", _jsonPathString, inputDocIds[i]));
      } else {
        _intValuesMV[i] = new int[value.length];
        for (int j = 0; j < value.length; j++) {
          _intValuesMV[i][j] = Integer.parseInt(value[j]);
        }
      }
    }
    return _intValuesMV;
  }

  @Override
  public long[][] transformToLongValuesMV(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    int[] inputDocIds = valueBlock.getDocIds();
    initLongValuesMV(numDocs);
    String[][] valuesFromIndex =
        _jsonIndexReader.getValuesForKeyAndFlattenedDocs(valueBlock.getDocIds(), _matchingDocsMap);
    for (int i = 0; i < numDocs; i++) {
      String[] value = valuesFromIndex[i];

      if (value == null) {
        if (_defaultValue != null) {
          _longValuesMV[i] = new long[]{(long) _defaultValue};
          continue;
        }
        throw new RuntimeException(
            String.format("Illegal Json Path: [%s], for docId [%s]", _jsonPathString, inputDocIds[i]));
      } else {
        _longValuesMV[i] = new long[value.length];
        for (int j = 0; j < value.length; j++) {
          _longValuesMV[i][j] = Long.parseLong(value[j]);
        }
      }
    }
    return _longValuesMV;
  }

  @Override
  public float[][] transformToFloatValuesMV(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    int[] inputDocIds = valueBlock.getDocIds();
    initFloatValuesMV(numDocs);
    String[][] valuesFromIndex =
        _jsonIndexReader.getValuesForKeyAndFlattenedDocs(valueBlock.getDocIds(), _matchingDocsMap);
    for (int i = 0; i < numDocs; i++) {
      String[] value = valuesFromIndex[i];

      if (value == null) {
        if (_defaultValue != null) {
          _floatValuesMV[i] = new float[]{(float) _defaultValue};
          continue;
        }
        throw new RuntimeException(
            String.format("Illegal Json Path: [%s], for docId [%s]", _jsonPathString, inputDocIds[i]));
      } else {
        _floatValuesMV[i] = new float[value.length];
        for (int j = 0; j < value.length; j++) {
          _floatValuesMV[i][j] = Float.parseFloat(value[j]);
        }
      }
    }
    return _floatValuesMV;
  }

  @Override
  public double[][] transformToDoubleValuesMV(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    int[] inputDocIds = valueBlock.getDocIds();
    initDoubleValuesMV(numDocs);
    String[][] valuesFromIndex =
        _jsonIndexReader.getValuesForKeyAndFlattenedDocs(valueBlock.getDocIds(), _matchingDocsMap);
    for (int i = 0; i < numDocs; i++) {
      String[] value = valuesFromIndex[i];

      if (value == null) {
        if (_defaultValue != null) {
          _doubleValuesMV[i] = new double[]{(double) _defaultValue};
          continue;
        }
        throw new RuntimeException(
            String.format("Illegal Json Path: [%s], for docId [%s]", _jsonPathString, inputDocIds[i]));
      } else {
        _doubleValuesMV[i] = new double[value.length];
        for (int j = 0; j < value.length; j++) {
          _doubleValuesMV[i][j] = Double.parseDouble(value[j]);
        }
      }
    }
    return _doubleValuesMV;
  }

  @Override
  public String[][] transformToStringValuesMV(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    int[] inputDocIds = valueBlock.getDocIds();
    initStringValuesMV(numDocs);
    String[][] valuesFromIndex =
        _jsonIndexReader.getValuesForKeyAndFlattenedDocs(valueBlock.getDocIds(), _matchingDocsMap);
    for (int i = 0; i < numDocs; i++) {
      String[] value = valuesFromIndex[i];

      if (value == null) {
        if (_defaultValue != null) {
          _stringValuesMV[i] = new String[]{(String) _defaultValue};
          continue;
        }
        throw new RuntimeException(
            String.format("Illegal Json Path: [%s], for docId [%s]", _jsonPathString, inputDocIds[i]));
      } else {
        _stringValuesMV[i] = new String[value.length];
        for (int j = 0; j < value.length; j++) {
          _stringValuesMV[i][j] = value[j];
        }
      }
    }
    return _stringValuesMV;
  }
}
