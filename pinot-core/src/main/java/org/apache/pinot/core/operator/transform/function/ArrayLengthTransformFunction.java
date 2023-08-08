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
 * The ArrayLengthTransformFunction class implements arrayLength function for multi-valued columns
 *
 * Sample queries:
 * SELECT COUNT(*) FROM table WHERE arrayLength(mvColumn) > 2
 * SELECT COUNT(*) FROM table GROUP BY arrayLength(mvColumn)
 * SELECT MAX(arrayLength(mvColumn)) FROM table
 */
public class ArrayLengthTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "arrayLength";

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
      throw new IllegalArgumentException("Exactly 1 argument is required for ARRAYLENGTH transform function");
    }

    // Check that the argument is a multi-valued column or transform function
    TransformFunction firstArgument = arguments.get(0);
    if (firstArgument instanceof LiteralTransformFunction || firstArgument.getResultMetadata().isSingleValue()) {
      throw new IllegalArgumentException(
          "The argument of ARRAYLENGTH transform function must be a multi-valued column or a transform function");
    }
    _argument = firstArgument;
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return INT_SV_NO_DICTIONARY_METADATA;
  }

  @Override
  public int[] transformToIntValuesSV(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    initIntValuesSV(length);
    switch (_argument.getResultMetadata().getDataType().getStoredType()) {
      case INT:
        int[][] intValuesMV = _argument.transformToIntValuesMV(valueBlock);
        for (int i = 0; i < length; i++) {
          _intValuesSV[i] = intValuesMV[i].length;
        }
        break;
      case LONG:
        long[][] longValuesMV = _argument.transformToLongValuesMV(valueBlock);
        for (int i = 0; i < length; i++) {
          _intValuesSV[i] = longValuesMV[i].length;
        }
        break;
      case FLOAT:
        float[][] floatValuesMV = _argument.transformToFloatValuesMV(valueBlock);
        for (int i = 0; i < length; i++) {
          _intValuesSV[i] = floatValuesMV[i].length;
        }
        break;
      case DOUBLE:
        double[][] doubleValuesMV = _argument.transformToDoubleValuesMV(valueBlock);
        for (int i = 0; i < length; i++) {
          _intValuesSV[i] = doubleValuesMV[i].length;
        }
        break;
      case STRING:
        String[][] stringValuesMV = _argument.transformToStringValuesMV(valueBlock);
        for (int i = 0; i < length; i++) {
          _intValuesSV[i] = stringValuesMV[i].length;
        }
        break;
      default:
        throw new IllegalStateException();
    }
    return _intValuesSV;
  }
}
