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


public class AdditionTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "add";

  private double _literalSum = 0.0;
  private List<TransformFunction> _transformFunctions = new ArrayList<>();
  private double[] _sums;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(@Nonnull List<TransformFunction> arguments, @Nonnull Map<String, DataSource> dataSourceMap) {
    // Check that there are more than 1 arguments
    if (arguments.size() < 2) {
      throw new IllegalArgumentException("At least 2 arguments are required for ADD transform function");
    }

    checkOperands(arguments);
  }

  private void checkOperands(List<TransformFunction> operands) {
    for (TransformFunction operand : operands) {
      if (operand instanceof MapValueTransformFunction) {
        throw new IllegalArgumentException("ADD transform function not supported to work with MAP as inner transform function");
      }

      if (operand instanceof LiteralTransformFunction) {
        try {
          _literalSum += Double.parseDouble(((LiteralTransformFunction) operand).getLiteral());
        } catch (NumberFormatException ne) {
          throw new IllegalArgumentException("ADD transform function not supported on non-numeric literals");
        }
      } else {
        final TransformResultMetadata resultMetadata = operand.getResultMetadata();

        if (resultMetadata.getDataType() == FieldSpec.DataType.STRING) {
          throw new IllegalArgumentException("ADD transform function not supported on non-numeric types");
        }

        if (!resultMetadata.isSingleValue()) {
          throw new IllegalArgumentException("ADD transform function must have single-valued arguments");
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
    if (_sums == null) {
      _sums = new double[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }

    int length = projectionBlock.getNumDocs();
    Arrays.fill(_sums, 0, length, _literalSum);
    for (TransformFunction transformFunction : _transformFunctions) {
      switch (transformFunction.getResultMetadata().getDataType()) {
        case INT:
          int[] intValues = transformFunction.transformToIntValuesSV(projectionBlock);
          for (int i = 0; i < length; i++) {
            _sums[i] += intValues[i];
          }
          break;
        case LONG:
          long[] longValues = transformFunction.transformToLongValuesSV(projectionBlock);
          for (int i = 0; i < length; i++) {
            _sums[i] += longValues[i];
          }
          break;
        case FLOAT:
          float[] floatValues = transformFunction.transformToFloatValuesSV(projectionBlock);
          for (int i = 0; i < length; i++) {
            _sums[i] += floatValues[i];
          }
          break;
        case DOUBLE:
          double[] doubleValues = transformFunction.transformToDoubleValuesSV(projectionBlock);
          for (int i = 0; i < length; i++) {
            _sums[i] += doubleValues[i];
          }
          break;
        case STRING:
          String[] stringValues = transformFunction.transformToStringValuesSV(projectionBlock);
          for (int i = 0; i < length; i++) {
            _sums[i] += Double.parseDouble(stringValues[i]);
          }
          break;
        default:
          throw new UnsupportedOperationException();
      }
    }
    return _sums;
  }
}
