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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.plan.DocIdSetPlanNode;


public class MultiplicationTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "mult";

  private double _literalProduct = 1.0;
  private List<TransformFunction> _transformFunctions = new ArrayList<>();
  private double[] _products;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(@Nonnull List<TransformFunction> arguments, @Nonnull Map<String, DataSource> dataSourceMap) {
    // Check that there are more than 1 arguments
    if (arguments.size() < 2) {
      throw new IllegalArgumentException("At least 2 arguments are required for MULT transform function");
    }

    checkOperands(arguments);
  }

  private void checkOperands(List<TransformFunction> operands) {
    for (TransformFunction operand : operands) {
      if (operand instanceof LiteralTransformFunction) {
        String literal = ((LiteralTransformFunction) operand).getLiteral();
        try {
          _literalProduct *= Double.parseDouble(literal);
        } catch (NumberFormatException ne) {
          throw new IllegalArgumentException("MUL transform function not supported on non-numeric literal: " + literal);
        }
      } else {
        TransformResultMetadata resultMetadata = operand.getResultMetadata();

        if (resultMetadata.getDataType() == FieldSpec.DataType.STRING) {
          throw new IllegalArgumentException("MUL transform function not supported on non-numeric types");
        }

        if (!resultMetadata.isSingleValue()) {
          throw new IllegalArgumentException("MUL transform function must have single-valued arguments");
        }

        _transformFunctions.add(operand);
      }
    }
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return DOUBLE_SV_NO_DICTIONARY_METADATA;
  }

  @Override
  public double[] transformToDoubleValuesSV(@Nonnull ProjectionBlock projectionBlock) {
    if (_products == null) {
      _products = new double[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }

    int length = projectionBlock.getNumDocs();
    Arrays.fill(_products, 0, length, _literalProduct);
    for (TransformFunction transformFunction : _transformFunctions) {
      switch (transformFunction.getResultMetadata().getDataType()) {
        case INT:
          int[] intValues = transformFunction.transformToIntValuesSV(projectionBlock);
          for (int i = 0; i < length; i++) {
            _products[i] *= intValues[i];
          }
          break;
        case LONG:
          long[] longValues = transformFunction.transformToLongValuesSV(projectionBlock);
          for (int i = 0; i < length; i++) {
            _products[i] *= longValues[i];
          }
          break;
        case FLOAT:
          float[] floatValues = transformFunction.transformToFloatValuesSV(projectionBlock);
          for (int i = 0; i < length; i++) {
            _products[i] *= floatValues[i];
          }
          break;
        case DOUBLE:
          double[] doubleValues = transformFunction.transformToDoubleValuesSV(projectionBlock);
          for (int i = 0; i < length; i++) {
            _products[i] *= doubleValues[i];
          }
          break;
        case STRING:
          String[] stringValues = transformFunction.transformToStringValuesSV(projectionBlock);
          for (int i = 0; i < length; i++) {
            _products[i] *= Double.parseDouble(stringValues[i]);
          }
          break;
        default:
          throw new UnsupportedOperationException();
      }
    }
    return _products;
  }
}
