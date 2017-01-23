/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.operator.docvalsets;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.common.BaseBlockValSet;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.operator.transform.function.TransformFunction;
import java.util.HashMap;
import java.util.Map;


/**
 * This class represents the BlockValSet for a TransformBlock (for a specific transform).
 * There's one TransformBlockValSet for each node in the {@link com.linkedin.pinot.common.request.transform.TransformExpressionTree}.
 * These are hooked up together in the same way as the expression tree itself, with projection/literal block val sets
 * at the leaf nodes. The reason for this is to enable lazy evaluation of transform expressions, which gets triggered
 * when the 'get' methods are called.
 *
 */
public class TransformBlockValSet extends BaseBlockValSet {

  private final TransformFunction _function;
  private final BlockValSet[] _blockValSets;
  private final int _numDocs;
  private final FieldSpec.DataType _dataType;

  // Cache for result of different data types.
  private Map<FieldSpec.DataType, Object> _resultMap;

  /**
   * Constructor for the class.
   *
   * @param function Transform function
   * @param numDocs Number of docs in the input
   * @param blockValSets Input block value sets
   */
  public TransformBlockValSet(TransformFunction function, int numDocs, BlockValSet... blockValSets) {
    _function = function;
    _numDocs = numDocs;
    _blockValSets = blockValSets;
    _dataType = function.getOutputType();
    _resultMap = new HashMap<>();
  }

  @Override
  public int[] getIntValuesSV() {
    int[] result = (int[]) _resultMap.get(FieldSpec.DataType.INT);

    if (result != null) {
      return result;
    }

    result = _function.transform(_numDocs, _blockValSets);
    _resultMap.put(FieldSpec.DataType.INT, result);
    return result;
  }

  @Override
  public long[] getLongValuesSV() {
    long[] result = (long[]) _resultMap.get(FieldSpec.DataType.LONG);
    if (result != null) {
      return result;
    }

    result = _function.transform(_numDocs, _blockValSets);
    _resultMap.put(FieldSpec.DataType.LONG, result);
    return result;
  }

  @Override
  public float[] getFloatValuesSV() {
    float[] result = (float[]) _resultMap.get(FieldSpec.DataType.FLOAT);
    if (result != null) {
      return result;
    }

    result = _function.transform(_numDocs, _blockValSets);
    _resultMap.put(FieldSpec.DataType.FLOAT, result);
    return result;
  }

  @Override
  public double[] getDoubleValuesSV() {
    double[] result = (double[]) _resultMap.get(FieldSpec.DataType.DOUBLE);
    if (result != null) {
      return result;
    }

    result = _function.transform(_numDocs, _blockValSets);
    _resultMap.put(FieldSpec.DataType.DOUBLE, result);
    return result;
  }

  @Override
  public String[] getStringValuesSV() {
    String[] result = (String[]) _resultMap.get(FieldSpec.DataType.STRING);
    if (result != null) {
      return result;
    }

    result = _function.transform(_numDocs, _blockValSets);
    _resultMap.put(FieldSpec.DataType.STRING, result);
    return result;
  }

  @Override
  public int getNumDocs() {
    return _numDocs;
  }

  @Override
  public FieldSpec.DataType getValueType() {
    return _dataType;
  }
}
