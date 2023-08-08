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

import java.util.List;
import java.util.Map;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;


/**
 * The ArraySumTransformFunction class implements arraySum function for multi-valued columns
 *
 * Sample queries:
 * SELECT COUNT(*) FROM table WHERE arraySum(mvColumn) > 2
 * SELECT COUNT(*) FROM table GROUP BY arraySum(mvColumn)
 * SELECT SUM(arraySum(mvColumn)) FROM table
 */
public class ArraySumTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "arraySum";

  private TransformFunction _argument;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
    super.init(arguments, columnContextMap);
    // Check that there is only 1 argument
    if (arguments.size() != 1) {
      throw new IllegalArgumentException("Exactly 1 argument is required for ArraySum transform function");
    }

    // Check that the argument is a multi-valued column or transform function
    TransformFunction firstArgument = arguments.get(0);
    if (firstArgument instanceof LiteralTransformFunction || firstArgument.getResultMetadata().isSingleValue()) {
      throw new IllegalArgumentException(
          "The argument of ArraySum transform function must be a multi-valued column or a transform function");
    }
    if (!firstArgument.getResultMetadata().getDataType().getStoredType().isNumeric()) {
      throw new IllegalArgumentException("The argument of ArraySum transform function must be numeric");
    }
    _argument = firstArgument;
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return DOUBLE_SV_NO_DICTIONARY_METADATA;
  }

  @Override
  public double[] transformToDoubleValuesSV(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    initDoubleValuesSV(length);
    double[][] doubleValuesMV = _argument.transformToDoubleValuesMV(valueBlock);
    for (int i = 0; i < length; i++) {
      double sumRes = 0;
      for (double value : doubleValuesMV[i]) {
        sumRes += value;
      }
      _doubleValuesSV[i] = sumRes;
    }
    return _doubleValuesSV;
  }
}
