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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ArrayCopyUtils;


public class MultiplicationTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "mult";

  private final List<TransformFunction> _transformFunctions = new ArrayList<>();
  private DataType _resultDataType;
  private double _literalDoubleProduct = 1.0;
  private BigDecimal _literalBigDecimalProduct = BigDecimal.ONE;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
    super.init(arguments, columnContextMap);
    // Check that there are more than 1 arguments
    if (arguments.size() < 2) {
      throw new IllegalArgumentException("At least 2 arguments are required for MULT transform function");
    }

    _resultDataType = DataType.DOUBLE;
    for (TransformFunction argument : arguments) {
      if (argument instanceof LiteralTransformFunction) {
        LiteralTransformFunction literalTransformFunction = (LiteralTransformFunction) argument;
        DataType dataType = literalTransformFunction.getResultMetadata().getDataType();
        if (dataType == DataType.BIG_DECIMAL) {
          _literalBigDecimalProduct =
              _literalBigDecimalProduct.multiply(literalTransformFunction.getBigDecimalLiteral());
          _resultDataType = DataType.BIG_DECIMAL;
        } else {
          _literalDoubleProduct *= ((LiteralTransformFunction) argument).getDoubleLiteral();
        }
      } else {
        if (!argument.getResultMetadata().isSingleValue()) {
          throw new IllegalArgumentException("All the arguments of MULT transform function must be single-valued");
        }
        if (argument.getResultMetadata().getDataType() == DataType.BIG_DECIMAL) {
          _resultDataType = DataType.BIG_DECIMAL;
        }
        _transformFunctions.add(argument);
      }
    }
    if (_resultDataType == DataType.BIG_DECIMAL) {
      _literalBigDecimalProduct = _literalBigDecimalProduct.multiply(BigDecimal.valueOf(_literalDoubleProduct));
    }
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    if (_resultDataType == DataType.BIG_DECIMAL) {
      return BIG_DECIMAL_SV_NO_DICTIONARY_METADATA;
    }
    return DOUBLE_SV_NO_DICTIONARY_METADATA;
  }

  @Override
  public double[] transformToDoubleValuesSV(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    initDoubleValuesSV(length);
    if (_resultDataType == DataType.BIG_DECIMAL) {
      BigDecimal[] values = transformToBigDecimalValuesSV(valueBlock);
      ArrayCopyUtils.copy(values, _doubleValuesSV, length);
    } else {
      Arrays.fill(_doubleValuesSV, 0, length, _literalDoubleProduct);
      for (TransformFunction transformFunction : _transformFunctions) {
        double[] values = transformFunction.transformToDoubleValuesSV(valueBlock);
        for (int i = 0; i < length; i++) {
          _doubleValuesSV[i] *= values[i];
        }
      }
    }
    return _doubleValuesSV;
  }

  @Override
  public BigDecimal[] transformToBigDecimalValuesSV(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    initBigDecimalValuesSV(length);
    if (_resultDataType == DataType.DOUBLE) {
      double[] values = transformToDoubleValuesSV(valueBlock);
      ArrayCopyUtils.copy(values, _bigDecimalValuesSV, length);
    } else {
      Arrays.fill(_bigDecimalValuesSV, 0, length, _literalBigDecimalProduct);
      for (TransformFunction transformFunction : _transformFunctions) {
        BigDecimal[] values = transformFunction.transformToBigDecimalValuesSV(valueBlock);
        for (int i = 0; i < length; i++) {
          _bigDecimalValuesSV[i] = _bigDecimalValuesSV[i].multiply(values[i]);
        }
      }
    }
    return _bigDecimalValuesSV;
  }
}
