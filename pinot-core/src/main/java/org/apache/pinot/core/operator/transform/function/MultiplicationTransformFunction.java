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
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ArrayCopyUtils;


public class MultiplicationTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "mult";

  private DataType _resultDataType;
  private double _literalDoubleProduct = 1.0;
  private BigDecimal _literalBigDecimalProduct = BigDecimal.ONE;
  private List<TransformFunction> _transformFunctions = new ArrayList<>();
  private double[] _doubleProducts;
  private BigDecimal[] _bigDecimalProducts;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
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
          _literalBigDecimalProduct = _literalBigDecimalProduct.multiply(
              new BigDecimal(literalTransformFunction.getLiteral()));
          _resultDataType = DataType.BIG_DECIMAL;
        } else {
          _literalDoubleProduct *= Double.parseDouble(((LiteralTransformFunction) argument).getLiteral());
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
  public double[] transformToDoubleValuesSV(ProjectionBlock projectionBlock) {
    int length = projectionBlock.getNumDocs();

    if (_doubleProducts == null || _doubleProducts.length < length) {
      _doubleProducts = new double[length];
    }

    if (_resultDataType == DataType.BIG_DECIMAL) {
      BigDecimal[] values = transformToBigDecimalValuesSV(projectionBlock);
      ArrayCopyUtils.copy(values, _doubleProducts, length);
    } else {
      Arrays.fill(_doubleProducts, 0, length, _literalDoubleProduct);
      for (TransformFunction transformFunction : _transformFunctions) {
        double[] values = transformFunction.transformToDoubleValuesSV(projectionBlock);
        for (int i = 0; i < length; i++) {
          _doubleProducts[i] *= values[i];
        }
      }
    }
    return _doubleProducts;
  }

  @Override
  public BigDecimal[] transformToBigDecimalValuesSV(ProjectionBlock projectionBlock) {
    int length = projectionBlock.getNumDocs();
    if (_bigDecimalProducts == null || _bigDecimalProducts.length < length) {
      _bigDecimalProducts = new BigDecimal[length];
    }

    if (_resultDataType == DataType.DOUBLE) {
      double[] values = transformToDoubleValuesSV(projectionBlock);
      ArrayCopyUtils.copy(values, _bigDecimalProducts, length);
    } else {
      Arrays.fill(_bigDecimalProducts, 0, length, _literalBigDecimalProduct);
      for (TransformFunction transformFunction : _transformFunctions) {
        BigDecimal[] values = transformFunction.transformToBigDecimalValuesSV(projectionBlock);
        for (int i = 0; i < length; i++) {
          _bigDecimalProducts[i] = _bigDecimalProducts[i].multiply(values[i]);
        }
      }
    }
    return _bigDecimalProducts;
  }
}
