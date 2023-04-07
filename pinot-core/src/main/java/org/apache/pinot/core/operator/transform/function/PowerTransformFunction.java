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


public class PowerTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "power";
  private TransformFunction _leftTransformFunction;
  private TransformFunction _rightTransformFunction;
  private double _exponent;
  private boolean _fixedExponent;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
    super.init(arguments, columnContextMap);
    // Check that there are more than 1 arguments
    if (arguments.size() != 2) {
      throw new IllegalArgumentException("Exactly 2 arguments are required for power transform function");
    }

    _fixedExponent = false;
    _leftTransformFunction = arguments.get(0);
    _rightTransformFunction = arguments.get(1);
    if (_rightTransformFunction instanceof LiteralTransformFunction) {
      _exponent = ((LiteralTransformFunction) _rightTransformFunction).getDoubleLiteral();
      _fixedExponent = true;
    }
    Preconditions.checkArgument(
        _leftTransformFunction.getResultMetadata().isSingleValue() || _rightTransformFunction.getResultMetadata()
            .isSingleValue(), "Argument must be single-valued for transform function: %s", getName());
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return DOUBLE_SV_NO_DICTIONARY_METADATA;
  }

  @Override
  public double[] transformToDoubleValuesSV(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    initDoubleValuesSV(length);
    double[] leftValues = _leftTransformFunction.transformToDoubleValuesSV(valueBlock);
    if (_fixedExponent) {
      for (int i = 0; i < length; i++) {
        _doubleValuesSV[i] = Math.pow(leftValues[i], _exponent);
      }
    } else {
      double[] rightValues = _rightTransformFunction.transformToDoubleValuesSV(valueBlock);
      for (int i = 0; i < length; i++) {
        _doubleValuesSV[i] = Math.pow(leftValues[i], rightValues[i]);
      }
    }
    return _doubleValuesSV;
  }
}
