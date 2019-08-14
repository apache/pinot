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
import javax.annotation.Nonnull;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.core.segment.index.readers.Dictionary;


/**
 * map_value(keyColName, 'keyName', valColName)
 */
public class MapValueTransformFunction extends BaseTransformFunction {

  public static final String FUNCTION_NAME = "map_value";

  private TransformResultMetadata _resultMetadata;
  private TransformFunction _keyColumnFunction;
  private String _keyName;
  private TransformFunction _valueColumnFunction;
  private int[][] _keyDictIds;
  private int[][] _valueDictIds;
  private int _inputKeyDictId;
  private int[] _outputValueDictIds;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(@Nonnull List<TransformFunction> arguments, @Nonnull Map<String, DataSource> dataSourceMap) {
    int numArguments = arguments.size();
    if (numArguments != 3) {
      throw new IllegalArgumentException("3 arguments are required for MAP_VALUE transform function map_value(keyColName, 'keyName', valColName)");
    }
    _keyColumnFunction = arguments.get(0);
    _keyName = ((LiteralTransformFunction) arguments.get(1)).getLiteral();
    _valueColumnFunction = arguments.get(2);
    TransformResultMetadata valueColumnMetadata = _valueColumnFunction.getResultMetadata();
    _resultMetadata =
        new TransformResultMetadata(valueColumnMetadata.getDataType(), true, valueColumnMetadata.hasDictionary());
    _inputKeyDictId = _keyColumnFunction.getDictionary().indexOf(_keyName);
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return _resultMetadata;
  }

  @Override
  public int[] transformToDictIdsSV(@Nonnull ProjectionBlock projectionBlock) {
    _outputValueDictIds = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL];

    _keyDictIds = _keyColumnFunction.transformToDictIdsMV(projectionBlock);
    _valueDictIds = _valueColumnFunction.transformToDictIdsMV(projectionBlock);
    int length = projectionBlock.getNumDocs();
    for (int i = 0; i < length; i++) {
      int numKeys = _keyDictIds[i].length;
      for (int j = 0; j < numKeys; j++) {
        if (_keyDictIds[i][j] == _inputKeyDictId) {
          _outputValueDictIds[i] = _valueDictIds[i][j];
          break;
        }
      }
    }
    return _outputValueDictIds;
  }

  @Override
  public Dictionary getDictionary() {
    return _valueColumnFunction.getDictionary();
  }
}
