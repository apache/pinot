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

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.segment.spi.index.reader.Dictionary;


/**
 * The MAP_VALUE transform function takes 3 arguments:
 * <ul>
 *   <li>KeyColumn: dictionary-encoded multi-value column where the values are sorted inside each multi-value entry</li>
 *   <li>KeyValue: a literal (number or string)</li>
 *   <li>ValueColumn: dictionary-encoded multi-value column</li>
 * </ul>
 * For each docId, the multi-value entry for ValueColumn has the same number of values as the multi-value entry for
 * KeyColumn to store the key-value pairs.
 * <p>E.g. map {"k1": 9, "k2": 5} will be stored as: ["k1", "k2"] in KeyColumn and [9, 5] in ValueColumn.
 * <p>Except for the filter clause, in order to make MAP_VALUE transform function work, the keyValue provided must exist
 * in the keyColumn.
 * <p>To ensure that, the query can have a filter on the keyColumn.
 * <p>E.g. {@code SELECT mapValue(key, 'myKey', value) FROM myTable WHERE key = 'myKey'}
 */
public class MapValueTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "mapValue";

  private TransformFunction _keyColumnFunction;
  private int _keyDictId;
  private TransformFunction _valueColumnFunction;
  private Dictionary _valueColumnDictionary;
  private TransformResultMetadata _resultMetadata;

  private int[] _dictIds;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
    super.init(arguments, columnContextMap);
    Preconditions.checkArgument(arguments.size() == 3,
        "3 arguments are required for MAP_VALUE transform function: keyColumn, keyValue, valueColumn, e.g. MAP_VALUE"
            + "(key, 'myKey', value)");

    _keyColumnFunction = arguments.get(0);
    TransformResultMetadata keyColumnMetadata = _keyColumnFunction.getResultMetadata();
    Dictionary keyColumnDictionary = _keyColumnFunction.getDictionary();
    Preconditions.checkState(!keyColumnMetadata.isSingleValue() && keyColumnDictionary != null,
        "Key column must be dictionary-encoded multi-value column");

    TransformFunction keyValueFunction = arguments.get(1);
    Preconditions.checkState(keyValueFunction instanceof LiteralTransformFunction,
        "Key value must be a literal (number or string)");
    String keyValue = ((LiteralTransformFunction) keyValueFunction).getStringLiteral();
    _keyDictId = keyColumnDictionary.indexOf(keyValue);

    _valueColumnFunction = arguments.get(2);
    TransformResultMetadata valueColumnMetadata = _valueColumnFunction.getResultMetadata();
    _valueColumnDictionary = _valueColumnFunction.getDictionary();
    Preconditions.checkState(!valueColumnMetadata.isSingleValue() && _valueColumnDictionary != null,
        "Value column must be dictionary-encoded multi-value column");
    _resultMetadata = new TransformResultMetadata(valueColumnMetadata.getDataType(), true, true);
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return _resultMetadata;
  }

  @Override
  public Dictionary getDictionary() {
    return _valueColumnDictionary;
  }

  @Override
  public int[] transformToDictIdsSV(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    if (_dictIds == null || _dictIds.length < length) {
      _dictIds = new int[length];
    }
    int[][] keyDictIdsMV = _keyColumnFunction.transformToDictIdsMV(valueBlock);
    int[][] valueDictIdsMV = _valueColumnFunction.transformToDictIdsMV(valueBlock);
    for (int i = 0; i < length; i++) {
      int[] keyDictIds = keyDictIdsMV[i];

      // Allow NULL_VALUE_INDEX for filter
      int valueDictId = Dictionary.NULL_VALUE_INDEX;
      int numValues = keyDictIds.length;
      for (int j = 0; j < numValues; j++) {
        if (keyDictIds[j] == _keyDictId) {
          valueDictId = valueDictIdsMV[i][j];
          break;
        }
      }
      _dictIds[i] = valueDictId;
    }
    return _dictIds;
  }
}
