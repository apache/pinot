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
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.spi.data.FieldSpec;


//TODO: The function should ideally be named 'round'
// but it is not possible because of existing DateTimeFunction with same name.
public class RoundDecimalTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "roundDecimal";
  private TransformFunction _leftTransformFunction;
  private TransformFunction _rightTransformFunction;
  private int _scale;
  private boolean _fixedScale;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
    super.init(arguments, columnContextMap);
    int numArguments = arguments.size();
    // Check that there are more than 2 arguments or no arguments
    if (numArguments < 1 || numArguments > 2) {
      throw new IllegalArgumentException(
          "roundDecimal transform function supports either 1 or 2 arguments. Num arguments provided: " + numArguments);
    }

    _fixedScale = false;
    _leftTransformFunction = arguments.get(0);
    if (numArguments > 1) {
      _rightTransformFunction = arguments.get(1);
      if (_rightTransformFunction instanceof LiteralTransformFunction) {
        _scale = ((LiteralTransformFunction) _rightTransformFunction).getIntLiteral();
        _fixedScale = true;
      }
      Preconditions.checkArgument(
          _rightTransformFunction.getResultMetadata().isSingleValue() && isIntegralResultDatatype(
              _rightTransformFunction),
          "Argument must be single-valued with type INT or LONG for transform function: %s", getName());
    } else {
      _rightTransformFunction = null;
    }

    Preconditions.checkArgument(_leftTransformFunction.getResultMetadata().isSingleValue(),
        "Argument must be single-valued for transform function: %s", getName());
  }

  private boolean isIntegralResultDatatype(TransformFunction transformFunction) {
    return transformFunction.getResultMetadata().getDataType().getStoredType() == FieldSpec.DataType.INT
        || transformFunction.getResultMetadata().getDataType().getStoredType() == FieldSpec.DataType.LONG;
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
    if (_fixedScale) {
      for (int i = 0; i < length; i++) {
        _doubleValuesSV[i] = BigDecimal.valueOf(leftValues[i])
            .setScale(_scale, RoundingMode.HALF_UP).doubleValue();
      }
    } else if (_rightTransformFunction != null) {
      int[] rightValues = _rightTransformFunction.transformToIntValuesSV(valueBlock);
      for (int i = 0; i < length; i++) {
        _doubleValuesSV[i] = BigDecimal.valueOf(leftValues[i])
            .setScale(rightValues[i], RoundingMode.HALF_UP).doubleValue();
      }
    } else {
      for (int i = 0; i < length; i++) {
        _doubleValuesSV[i] = (double) Math.round(leftValues[i]);
      }
    }
    return _doubleValuesSV;
  }
}
