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
import org.apache.pinot.common.data.FieldSpec.DataType;
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

  private TransformFunction _keyColumnFunction;
  private TransformFunction _valueColumnFunction;
  private TransformResultMetadata _resultMetadata;
  private int _inputKeyDictId;

  private int[] _dictIds;
  private int[] _intValues;
  private long[] _longValues;
  private float[] _floatValues;
  private double[] _doubleValues;
  private String[] _stringValues;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(@Nonnull List<TransformFunction> arguments, @Nonnull Map<String, DataSource> dataSourceMap) {
    int numArguments = arguments.size();
    if (numArguments != 3) {
      throw new IllegalArgumentException(
          "3 arguments are required for MAP_VALUE transform function map_value(keyColName, 'keyName', valColName)");
    }
    _keyColumnFunction = arguments.get(0);
    String keyName = ((LiteralTransformFunction) arguments.get(1)).getLiteral();
    _valueColumnFunction = arguments.get(2);
    TransformResultMetadata valueColumnMetadata = _valueColumnFunction.getResultMetadata();
    _resultMetadata =
        new TransformResultMetadata(valueColumnMetadata.getDataType(), true, valueColumnMetadata.hasDictionary());
    _inputKeyDictId = _keyColumnFunction.getDictionary().indexOf(keyName);
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return _resultMetadata;
  }

  @Override
  public Dictionary getDictionary() {
    return _valueColumnFunction.getDictionary();
  }

  @Override
  public int[] transformToDictIdsSV(@Nonnull ProjectionBlock projectionBlock) {
    if (_dictIds == null) {
      _dictIds = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }
    int[][] keyDictIds = _keyColumnFunction.transformToDictIdsMV(projectionBlock);
    int[][] valueDictIds = _valueColumnFunction.transformToDictIdsMV(projectionBlock);
    int length = projectionBlock.getNumDocs();
    for (int i = 0; i < length; i++) {
      int numKeys = keyDictIds[i].length;
      for (int j = 0; j < numKeys; j++) {
        if (keyDictIds[i][j] == _inputKeyDictId) {
          _dictIds[i] = valueDictIds[i][j];
          break;
        }
      }
    }
    return _dictIds;
  }

  @Override
  public int[] transformToIntValuesSV(@Nonnull ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType() != DataType.INT) {
      return super.transformToIntValuesSV(projectionBlock);
    }
    if (_intValues == null) {
      _intValues = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }
    Dictionary dictionary = getDictionary();
    int[] dictIds = transformToDictIdsSV(projectionBlock);
    int length = projectionBlock.getNumDocs();
    for (int i = 0; i < length; i++) {
      _intValues[i] = dictionary.getIntValue(dictIds[i]);
    }
    return _intValues;
  }

  @Override
  public long[] transformToLongValuesSV(@Nonnull ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType() != DataType.LONG) {
      return super.transformToLongValuesSV(projectionBlock);
    }
    if (_longValues == null) {
      _longValues = new long[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }
    Dictionary dictionary = getDictionary();
    int[] dictIds = transformToDictIdsSV(projectionBlock);
    int length = projectionBlock.getNumDocs();
    for (int i = 0; i < length; i++) {
      _longValues[i] = dictionary.getLongValue(dictIds[i]);
    }
    return _longValues;
  }

  @Override
  public float[] transformToFloatValuesSV(@Nonnull ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType() != DataType.FLOAT) {
      return super.transformToFloatValuesSV(projectionBlock);
    }
    if (_floatValues == null) {
      _floatValues = new float[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }
    Dictionary dictionary = getDictionary();
    int[] dictIds = transformToDictIdsSV(projectionBlock);
    int length = projectionBlock.getNumDocs();
    for (int i = 0; i < length; i++) {
      _floatValues[i] = dictionary.getFloatValue(dictIds[i]);
    }
    return _floatValues;
  }

  @Override
  public double[] transformToDoubleValuesSV(@Nonnull ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType() != DataType.DOUBLE) {
      return super.transformToDoubleValuesSV(projectionBlock);
    }
    if (_doubleValues == null) {
      _doubleValues = new double[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }
    Dictionary dictionary = getDictionary();
    int[] dictIds = transformToDictIdsSV(projectionBlock);
    int length = projectionBlock.getNumDocs();
    for (int i = 0; i < length; i++) {
      _doubleValues[i] = dictionary.getDoubleValue(dictIds[i]);
    }
    return _doubleValues;
  }

  @Override
  public String[] transformToStringValuesSV(@Nonnull ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType() != DataType.STRING) {
      return super.transformToStringValuesSV(projectionBlock);
    }
    if (_stringValues == null) {
      _stringValues = new String[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }
    Dictionary dictionary = getDictionary();
    int[] dictIds = transformToDictIdsSV(projectionBlock);
    int length = projectionBlock.getNumDocs();
    for (int i = 0; i < length; i++) {
      _stringValues[i] = dictionary.getStringValue(dictIds[i]);
    }
    return _stringValues;
  }
}
