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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
 * implementation.
 */
public class JsonExtractIndexTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "jsonExtractIndex";

  private TransformFunction _jsonFieldTransformFunction;
  private String _jsonPathString;
  private TransformResultMetadata _resultMetadata;
  private JsonIndexReader _jsonIndexReader;
  private Object _defaultValue;
  private Map<Object, RoaringBitmap> _postingListCache;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
    // Check that there are exactly 3 or 4 arguments
    if (arguments.size() < 3 || arguments.size() > 4) {
      throw new IllegalArgumentException(
          "Expected 3/4 arguments for transform function: jsonExtractIndex(jsonFieldName, 'jsonPath', 'resultsType',"
              + " ['defaultValue'])");
    }

    TransformFunction firstArgument = arguments.get(0);
    if (firstArgument instanceof LiteralTransformFunction || !firstArgument.getResultMetadata().isSingleValue()) {
      throw new IllegalArgumentException(
          "The first argument of jsonExtractIndex transform function must be a single-valued column or a transform "
              + "function");
    }
    if (firstArgument instanceof IdentifierTransformFunction) {
      String columnName = ((IdentifierTransformFunction) firstArgument).getColumnName();
      if (columnContextMap.containsKey(columnName)) {
        _jsonIndexReader = columnContextMap.get(columnName).getDataSource().getJsonIndex();
        if (_jsonIndexReader == null) {
          throw new UnsupportedOperationException("jsonExtractIndex can only be applied on a column with JSON index");
        }
      }
    } else {
      throw new IllegalArgumentException("jsonExtractIndex can only be applied to a raw column");
    }
    _jsonFieldTransformFunction = firstArgument;
    _jsonPathString = ((LiteralTransformFunction) arguments.get(1)).getStringLiteral().substring(1); // remove $ prefix
    String resultsType = ((LiteralTransformFunction) arguments.get(2)).getStringLiteral().toUpperCase();
    boolean isSingleValue = !resultsType.endsWith("_ARRAY");
    if (!isSingleValue) {
      throw new IllegalArgumentException("jsonExtractIndex only supports single value type");
    }
    DataType dataType = DataType.valueOf(resultsType);
    if (arguments.size() == 4) {
      _defaultValue = dataType.convert(((LiteralTransformFunction) arguments.get(3)).getStringLiteral());
    }

    _resultMetadata = new TransformResultMetadata(dataType, true, false);
    _postingListCache = new HashMap<>();
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
    String[] valuesFromIndex =
        _jsonIndexReader.getValuesForKeyAndDocs(_jsonPathString, valueBlock.getDocIds(), _postingListCache);
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
    String[] valuesFromIndex =
        _jsonIndexReader.getValuesForKeyAndDocs(_jsonPathString, valueBlock.getDocIds(), _postingListCache);
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
    String[] valuesFromIndex =
        _jsonIndexReader.getValuesForKeyAndDocs(_jsonPathString, valueBlock.getDocIds(), _postingListCache);
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
    String[] valuesFromIndex =
        _jsonIndexReader.getValuesForKeyAndDocs(_jsonPathString, valueBlock.getDocIds(), _postingListCache);
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
    String[] valuesFromIndex =
        _jsonIndexReader.getValuesForKeyAndDocs(_jsonPathString, valueBlock.getDocIds(), _postingListCache);
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
    String[] valuesFromIndex =
        _jsonIndexReader.getValuesForKeyAndDocs(_jsonPathString, valueBlock.getDocIds(), _postingListCache);
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
    throw new UnsupportedOperationException("jsonExtractIndex does not support transforming to multi-value columns");
  }

  @Override
  public long[][] transformToLongValuesMV(ValueBlock valueBlock) {
    throw new UnsupportedOperationException("jsonExtractIndex does not support transforming to multi-value columns");
  }

  @Override
  public float[][] transformToFloatValuesMV(ValueBlock valueBlock) {
    throw new UnsupportedOperationException("jsonExtractIndex does not support transforming to multi-value columns");
  }

  @Override
  public double[][] transformToDoubleValuesMV(ValueBlock valueBlock) {
    throw new UnsupportedOperationException("jsonExtractIndex does not support transforming to multi-value columns");
  }

  @Override
  public String[][] transformToStringValuesMV(ValueBlock valueBlock) {
    throw new UnsupportedOperationException("jsonExtractIndex does not support transforming to multi-value columns");
  }
}
