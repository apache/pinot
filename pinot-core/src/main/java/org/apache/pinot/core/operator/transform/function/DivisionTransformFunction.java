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
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ArrayCopyUtils;


public class DivisionTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "div";

  private DataType _resultDataType;
  private double[] _doubleLiterals;
  private BigDecimal[] _bigDecimalLiterals;
  private TransformFunction _firstTransformFunction;
  private TransformFunction _secondTransformFunction;
  private double[] _doubleQuotients;
  private BigDecimal[] _bigDecimalQuotients;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
    // Check that there are exactly 2 arguments
    if (arguments.size() != 2) {
      throw new IllegalArgumentException("Exactly 2 arguments are required for DIV transform function");
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
          _bigDecimalLiterals[i] = new BigDecimal(literalTransformFunction.getLiteral());
        } else {
          _doubleLiterals[i] = Double.parseDouble(((LiteralTransformFunction) argument).getLiteral());
        }
      } else {
        if (!argument.getResultMetadata().isSingleValue()) {
          throw new IllegalArgumentException("every argument of DIV transform function must be single-valued");
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
  public double[] transformToDoubleValuesSV(ProjectionBlock projectionBlock) {
    int length = projectionBlock.getNumDocs();

    if (_doubleQuotients == null || _doubleQuotients.length < length) {
      _doubleQuotients = new double[length];
    }

    if (_resultDataType == DataType.BIG_DECIMAL) {
      BigDecimal[] values = transformToBigDecimalValuesSV(projectionBlock);
      ArrayCopyUtils.copy(values, _doubleQuotients, length);
    } else {
      if (_firstTransformFunction == null) {
        Arrays.fill(_doubleQuotients, 0, length, _doubleLiterals[0]);
      } else {
        double[] values = _firstTransformFunction.transformToDoubleValuesSV(projectionBlock);
        System.arraycopy(values, 0, _doubleQuotients, 0, length);
      }
      if (_secondTransformFunction == null) {
        for (int i = 0; i < length; i++) {
          _doubleQuotients[i] /= _doubleLiterals[1];
        }
      } else {
        double[] values = _secondTransformFunction.transformToDoubleValuesSV(projectionBlock);
        for (int i = 0; i < length; i++) {
          _doubleQuotients[i] /= values[i];
        }
      }
    }
    return _doubleQuotients;
  }

  @Override
  public BigDecimal[] transformToBigDecimalValuesSV(ProjectionBlock projectionBlock) {
    int length = projectionBlock.getNumDocs();
    if (_bigDecimalQuotients == null || _bigDecimalQuotients.length < length) {
      _bigDecimalQuotients = new BigDecimal[length];
    }

    if (_resultDataType == DataType.DOUBLE) {
      double[] values = transformToDoubleValuesSV(projectionBlock);
      ArrayCopyUtils.copy(values, _bigDecimalQuotients, length);
    } else {
      if (_firstTransformFunction == null) {
        Arrays.fill(_bigDecimalQuotients, 0, length, _bigDecimalLiterals[0]);
      } else {
        BigDecimal[] values = _firstTransformFunction.transformToBigDecimalValuesSV(projectionBlock);
        System.arraycopy(values, 0, _bigDecimalQuotients, 0, length);
      }
      if (_secondTransformFunction == null) {
        for (int i = 0; i < length; i++) {
          // todo: expose roundingMode/mathContext as parameter in DivisionTransformFunction.
          _bigDecimalQuotients[i] = _bigDecimalQuotients[i].divide(_bigDecimalLiterals[1], RoundingMode.HALF_EVEN);
        }
      } else {
        BigDecimal[] values = _secondTransformFunction.transformToBigDecimalValuesSV(projectionBlock);
        for (int i = 0; i < length; i++) {
          // todo: expose roundingMode/mathContext as parameter in DivisionTransformFunction.
          _bigDecimalQuotients[i] = _bigDecimalQuotients[i].divide(values[i], RoundingMode.HALF_EVEN);
        }
      }
    }
    return _bigDecimalQuotients;
  }
}
