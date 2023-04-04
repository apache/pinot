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
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;


public class ModuloTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "mod";

  private double _firstLiteral;
  private TransformFunction _firstTransformFunction;
  private double _secondLiteral;
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
      throw new IllegalArgumentException("Exactly 2 arguments are required for MOD transform function");
    }

    TransformFunction firstArgument = arguments.get(0);
    if (firstArgument instanceof LiteralTransformFunction) {
      _firstLiteral = ((LiteralTransformFunction) firstArgument).getDoubleLiteral();
    } else {
      if (!firstArgument.getResultMetadata().isSingleValue()) {
        throw new IllegalArgumentException("First argument of MOD transform function must be single-valued");
      }
      _firstTransformFunction = firstArgument;
    }

    TransformFunction secondArgument = arguments.get(1);
    if (secondArgument instanceof LiteralTransformFunction) {
      _secondLiteral = ((LiteralTransformFunction) secondArgument).getDoubleLiteral();
    } else {
      if (!secondArgument.getResultMetadata().isSingleValue()) {
        throw new IllegalArgumentException("Second argument of MOD transform function must be single-valued");
      }
      _secondTransformFunction = secondArgument;
    }
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return DOUBLE_SV_NO_DICTIONARY_METADATA;
  }

  @Override
  public double[] transformToDoubleValuesSV(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    initDoubleValuesSV(length);
    if (_firstTransformFunction == null) {
      Arrays.fill(_doubleValuesSV, 0, length, _firstLiteral);
    } else {
      double[] values = _firstTransformFunction.transformToDoubleValuesSV(valueBlock);
      System.arraycopy(values, 0, _doubleValuesSV, 0, length);
    }
    if (_secondTransformFunction == null) {
      for (int i = 0; i < length; i++) {
        _doubleValuesSV[i] %= _secondLiteral;
      }
    } else {
      double[] values = _secondTransformFunction.transformToDoubleValuesSV(valueBlock);
      for (int i = 0; i < length; i++) {
        _doubleValuesSV[i] %= values[i];
      }
    }
    return _doubleValuesSV;
  }
}
