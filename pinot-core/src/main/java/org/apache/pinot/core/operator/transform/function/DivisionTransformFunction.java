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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.core.util.ArrayCopyUtils;


public class DivisionTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "div";

  private double _firstLiteral;
  private TransformFunction _firstTransformFunction;
  private double _secondLiteral;
  private TransformFunction _secondTransformFunction;
  private double[] _quotients;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(@Nonnull List<TransformFunction> arguments, @Nonnull Map<String, DataSource> dataSourceMap) {
    // Check that there are exactly 2 arguments
    if (arguments.size() != 2) {
      throw new IllegalArgumentException("Exactly 2 arguments are required for DIV transform function");
    }

    checkOperands(arguments.get(0), arguments.get(1));
  }

  private void checkOperands(TransformFunction operand1, TransformFunction operand2) {
    if (operand1 instanceof LiteralTransformFunction) {
      String literal = ((LiteralTransformFunction) operand1).getLiteral();
      try {
        _firstLiteral = Double.parseDouble(literal);
      } catch (NumberFormatException ne) {
        throw new IllegalArgumentException("DIV transform function not supported on non-numeric literal: " + literal);
      }
    } else {
      TransformResultMetadata resultMetadata = operand1.getResultMetadata();

      if (resultMetadata.getDataType() == FieldSpec.DataType.STRING) {
        throw new IllegalArgumentException("DIV transform function not supported on non-numeric types");
      }

      if (!resultMetadata.isSingleValue()) {
        throw new IllegalArgumentException("DIV transform function must have single-valued arguments");
      }

      _firstTransformFunction = operand1;
    }

    if (operand2 instanceof LiteralTransformFunction) {
      String literal = ((LiteralTransformFunction) operand2).getLiteral();
      try {
        _secondLiteral = Double.parseDouble(literal);
      } catch (NumberFormatException ne) {
        throw new RuntimeException("DIV transform function not supported on non-numeric literal: " + literal);
      }
    } else {
      TransformResultMetadata resultMetadata = operand2.getResultMetadata();

      if (resultMetadata.getDataType() == FieldSpec.DataType.STRING) {
        throw new IllegalArgumentException("SUB transform function not supported on non-numeric types");
      }

      if (!resultMetadata.isSingleValue()) {
        throw new IllegalArgumentException("SUB transform function must have single-valued arguments");
      }

      _secondTransformFunction = operand2;
    }
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return DOUBLE_SV_NO_DICTIONARY_METADATA;
  }

  @SuppressWarnings("Duplicates")
  @Override
  public double[] transformToDoubleValuesSV(@Nonnull ProjectionBlock projectionBlock) {
    if (_quotients == null) {
      _quotients = new double[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }

    int length = projectionBlock.getNumDocs();

    if (_firstTransformFunction == null) {
      Arrays.fill(_quotients, 0, length, _firstLiteral);
    } else {
      switch (_firstTransformFunction.getResultMetadata().getDataType()) {
        case INT:
          int[] intValues = _firstTransformFunction.transformToIntValuesSV(projectionBlock);
          ArrayCopyUtils.copy(intValues, _quotients, length);
          break;
        case LONG:
          long[] longValues = _firstTransformFunction.transformToLongValuesSV(projectionBlock);
          ArrayCopyUtils.copy(longValues, _quotients, length);
          break;
        case FLOAT:
          float[] floatValues = _firstTransformFunction.transformToFloatValuesSV(projectionBlock);
          ArrayCopyUtils.copy(floatValues, _quotients, length);
          break;
        case DOUBLE:
          double[] doubleValues = _firstTransformFunction.transformToDoubleValuesSV(projectionBlock);
          System.arraycopy(doubleValues, 0, _quotients, 0, length);
          break;
        case STRING:
          String[] stringValues = _firstTransformFunction.transformToStringValuesSV(projectionBlock);
          ArrayCopyUtils.copy(stringValues, _quotients, length);
          break;
        default:
          throw new UnsupportedOperationException();
      }
    }

    if (_secondTransformFunction == null) {
      for (int i = 0; i < length; i++) {
        _quotients[i] /= _secondLiteral;
      }
    } else {
      switch (_secondTransformFunction.getResultMetadata().getDataType()) {
        case INT:
          int[] intValues = _secondTransformFunction.transformToIntValuesSV(projectionBlock);
          for (int i = 0; i < length; i++) {
            _quotients[i] /= intValues[i];
          }
          break;
        case LONG:
          long[] longValues = _secondTransformFunction.transformToLongValuesSV(projectionBlock);
          for (int i = 0; i < length; i++) {
            _quotients[i] /= longValues[i];
          }
          break;
        case FLOAT:
          float[] floatValues = _secondTransformFunction.transformToFloatValuesSV(projectionBlock);
          for (int i = 0; i < length; i++) {
            _quotients[i] /= floatValues[i];
          }
          break;
        case DOUBLE:
          double[] doubleValues = _secondTransformFunction.transformToDoubleValuesSV(projectionBlock);
          for (int i = 0; i < length; i++) {
            _quotients[i] /= doubleValues[i];
          }
          break;
        case STRING:
          String[] stringValues = _secondTransformFunction.transformToStringValuesSV(projectionBlock);
          for (int i = 0; i < length; i++) {
            _quotients[i] /= Double.parseDouble(stringValues[i]);
          }
          break;
        default:
          throw new UnsupportedOperationException();
      }
    }

    return _quotients;
  }
}

