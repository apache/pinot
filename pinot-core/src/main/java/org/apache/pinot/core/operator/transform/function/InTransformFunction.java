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

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.ByteArray;


/**
 * The IN transform function takes more than 1 arguments:
 * <ul>
 *   <li>Expression: a single-value expression</li>
 *   <li>values: a set of literal strings</li>
 * </ul>
 * <p>For each docId, the function returns {@code 1} if the set of values contains the value of the expression, {code 0} if not.
 * <p>E.g. {@code SELECT col IN ('a','b','c') FROM myTable)}
 */
public class InTransformFunction extends BaseTransformFunction {
  private TransformFunction _transformFunction;
  private TransformFunction[] _valueTransformFunctions;
  private int[] _results;

  @Override
  public String getName() {
    return TransformFunctionType.IN.getName();
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
    Preconditions.checkArgument(arguments.size() >= 2,
        "at least 2 arguments are required for IN transform function: expression, values");
    Preconditions.checkArgument(arguments.get(0).getResultMetadata().isSingleValue(),
        "First argument for IN transform function must be a single-value expression");
    _transformFunction = arguments.get(0);
    _valueTransformFunctions = new TransformFunction[arguments.size() - 1];
    for (int i = 1; i < arguments.size(); i++) {
      Preconditions.checkArgument(arguments.get(i).getResultMetadata().isSingleValue(),
          "The values argument for IN transform function must be single value");
      _valueTransformFunctions[i - 1] = arguments.get(i);
    }
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return BOOLEAN_SV_NO_DICTIONARY_METADATA;
  }

  @Override
  public int[] transformToIntValuesSV(ProjectionBlock projectionBlock) {
    if (_results == null) {
      _results = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }

    int length = projectionBlock.getNumDocs();
    FieldSpec.DataType storedType = _transformFunction.getResultMetadata().getDataType().getStoredType();
    switch (storedType) {
      case INT:
        int[] intValues = _transformFunction.transformToIntValuesSV(projectionBlock);
        int[][] inIntValues = new int[_valueTransformFunctions.length][];
        for (int i = 0; i < _valueTransformFunctions.length; i++) {
          inIntValues[i] = _valueTransformFunctions[i].transformToIntValuesSV(projectionBlock);
        }
        for (int i = 0; i < length; i++) {
          for (int j = 0; j < inIntValues.length; j++) {
            _results[i] = inIntValues[j][i] == intValues[i] ? 1 : _results[i];
          }
        }
        break;
      case LONG:
        long[] longValues = _transformFunction.transformToLongValuesSV(projectionBlock);
        long[][] inLongValues = new long[_valueTransformFunctions.length][];
        for (int i = 0; i < _valueTransformFunctions.length; i++) {
          inLongValues[i] = _valueTransformFunctions[i].transformToLongValuesSV(projectionBlock);
        }
        for (int i = 0; i < length; i++) {
          for (int j = 0; j < inLongValues.length; j++) {
            _results[i] = inLongValues[j][i] == longValues[i] ? 1 : _results[i];
          }
        }
        break;
      case FLOAT:
        float[] floatValues = _transformFunction.transformToFloatValuesSV(projectionBlock);
        float[][] inFloatValues = new float[_valueTransformFunctions.length][];
        for (int i = 0; i < _valueTransformFunctions.length; i++) {
          inFloatValues[i] = _valueTransformFunctions[i].transformToFloatValuesSV(projectionBlock);
        }
        for (int i = 0; i < length; i++) {
          for (int j = 0; j < inFloatValues.length; j++) {
            _results[i] = Float.compare(inFloatValues[j][i], floatValues[i]) == 0 ? 1 : _results[i];
          }
        }
        break;
      case DOUBLE:
        double[] doubleValues = _transformFunction.transformToDoubleValuesSV(projectionBlock);
        double[][] inDoubleValues = new double[_valueTransformFunctions.length][];
        for (int i = 0; i < _valueTransformFunctions.length; i++) {
          inDoubleValues[i] = _valueTransformFunctions[i].transformToDoubleValuesSV(projectionBlock);
        }
        for (int i = 0; i < length; i++) {
          for (int j = 0; j < inDoubleValues.length; j++) {
            _results[i] = Double.compare(inDoubleValues[j][i], doubleValues[i]) == 0 ? 1 : _results[i];
          }
        }
        break;
      case STRING:
        String[] stringValues = _transformFunction.transformToStringValuesSV(projectionBlock);
        String[][] inStringValues = new String[_valueTransformFunctions.length][];
        for (int i = 0; i < _valueTransformFunctions.length; i++) {
          inStringValues[i] = _valueTransformFunctions[i].transformToStringValuesSV(projectionBlock);
        }
        for (int i = 0; i < length; i++) {
          for (int j = 0; j < inStringValues.length; j++) {
            _results[i] = inStringValues[j][i].equals(stringValues[i]) ? 1 : _results[i];
          }
        }
        break;
      case BYTES:
        byte[][] bytesValues = _transformFunction.transformToBytesValuesSV(projectionBlock);
        byte[][][] inBytesValues = new byte[_valueTransformFunctions.length][][];
        for (int i = 0; i < _valueTransformFunctions.length; i++) {
          inBytesValues[i] = _valueTransformFunctions[i].transformToBytesValuesSV(projectionBlock);
        }
        for (int i = 0; i < length; i++) {
          for (int j = 0; j < inBytesValues.length; j++) {
            _results[i] =
                new ByteArray(inBytesValues[j][i]).compareTo(new ByteArray(bytesValues[i])) == 0 ? 1 : _results[i];
          }
        }
        break;
      default:
        throw new IllegalStateException();
    }

    return _results;
  }
}
