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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ArrayCopyUtils;


public class SubtractionTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "sub";

  private DataType _resultDataType;
  private double[] _doubleLiterals;
  private BigDecimal[] _bigDecimalLiterals;
  private TransformFunction _firstTransformFunction;
  private TransformFunction _secondTransformFunction;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
    super.init(arguments, columnContextMap);
    // Check that there are exactly 2 arguments
    if (arguments.size() != 2) {
      throw new IllegalArgumentException("Exactly 2 arguments are required for SUB transform function");
    }

    _resultDataType = DataType.DOUBLE;
    for (TransformFunction argument : arguments) {
      if (argument.getResultMetadata().getDataType() == DataType.BIG_DECIMAL) {
        _resultDataType = DataType.BIG_DECIMAL;
        break;
      }
    }
    if (_resultDataType == DataType.BIG_DECIMAL) {
      _bigDecimalLiterals = new BigDecimal[2];
    } else {
      _doubleLiterals = new double[2];
    }

    for (int i = 0; i < arguments.size(); i++) {
      TransformFunction argument = arguments.get(i);
      if (argument instanceof LiteralTransformFunction) {
        LiteralTransformFunction literalTransformFunction = (LiteralTransformFunction) argument;
        if (_resultDataType == DataType.BIG_DECIMAL) {
          _bigDecimalLiterals[i] = literalTransformFunction.getBigDecimalLiteral();
        } else {
          _doubleLiterals[i] = ((LiteralTransformFunction) argument).getDoubleLiteral();
        }
      } else {
        if (!argument.getResultMetadata().isSingleValue()) {
          throw new IllegalArgumentException("every argument of SUB transform function must be single-valued");
        }
        if (i == 0) {
          _firstTransformFunction = argument;
        } else {
          _secondTransformFunction = argument;
        }
      }
    }
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    if (_resultDataType == DataType.BIG_DECIMAL) {
      return BIG_DECIMAL_SV_NO_DICTIONARY_METADATA;
    }
    return DOUBLE_SV_NO_DICTIONARY_METADATA;
  }

  @SuppressWarnings("Duplicates")
  @Override
  public double[] transformToDoubleValuesSV(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    initDoubleValuesSV(length);
    if (_resultDataType == DataType.BIG_DECIMAL) {
      BigDecimal[] values = transformToBigDecimalValuesSV(valueBlock);
      ArrayCopyUtils.copy(values, _doubleValuesSV, length);
    } else {
      if (_firstTransformFunction == null) {
        Arrays.fill(_doubleValuesSV, 0, length, _doubleLiterals[0]);
      } else {
        double[] values = _firstTransformFunction.transformToDoubleValuesSV(valueBlock);
        System.arraycopy(values, 0, _doubleValuesSV, 0, length);
      }
      if (_secondTransformFunction == null) {
        for (int i = 0; i < length; i++) {
          _doubleValuesSV[i] -= _doubleLiterals[1];
        }
      } else {
        double[] values = _secondTransformFunction.transformToDoubleValuesSV(valueBlock);
        for (int i = 0; i < length; i++) {
          _doubleValuesSV[i] -= values[i];
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
      if (_firstTransformFunction == null) {
        Arrays.fill(_bigDecimalValuesSV, 0, length, _bigDecimalLiterals[0]);
      } else {
        BigDecimal[] values = _firstTransformFunction.transformToBigDecimalValuesSV(valueBlock);
        System.arraycopy(values, 0, _bigDecimalValuesSV, 0, length);
      }
      if (_secondTransformFunction == null) {
        for (int i = 0; i < length; i++) {
          _bigDecimalValuesSV[i] = _bigDecimalValuesSV[i].subtract(_bigDecimalLiterals[1]);
        }
      } else {
        BigDecimal[] values = _secondTransformFunction.transformToBigDecimalValuesSV(valueBlock);
        for (int i = 0; i < length; i++) {
          _bigDecimalValuesSV[i] = _bigDecimalValuesSV[i].subtract(values[i]);
        }
      }
    }
    return _bigDecimalValuesSV;
  }
}
