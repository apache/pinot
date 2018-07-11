/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.operator.transform.function;

import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.operator.blocks.ProjectionBlock;
import com.linkedin.pinot.core.operator.transform.TransformResultMetadata;
import com.linkedin.pinot.core.plan.DocIdSetPlanNode;
import com.linkedin.pinot.core.util.ArrayCopyUtils;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;


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

    TransformFunction firstArgument = arguments.get(0);
    if (firstArgument instanceof LiteralTransformFunction) {
      _firstLiteral = Double.parseDouble(((LiteralTransformFunction) firstArgument).getLiteral());
    } else {
      if (!firstArgument.getResultMetadata().isSingleValue()) {
        throw new IllegalArgumentException("First argument of DIV transform function must be single-valued");
      }
      _firstTransformFunction = firstArgument;
    }

    TransformFunction secondArgument = arguments.get(1);
    if (secondArgument instanceof LiteralTransformFunction) {
      _secondLiteral = Double.parseDouble(((LiteralTransformFunction) secondArgument).getLiteral());
    } else {
      if (!secondArgument.getResultMetadata().isSingleValue()) {
        throw new IllegalArgumentException("Second argument of DIV transform function must be single-valued");
      }
      _secondTransformFunction = secondArgument;
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

