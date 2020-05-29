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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.FunctionInvoker;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.spi.data.FieldSpec;


public class ScalarTransformFunctionWrapper extends BaseTransformFunction {

  FunctionInvoker _functionInvoker;
  String _name;
  Object[] _args;
  List<Integer> _nonLiteralArgIndices;
  List<FieldSpec.DataType> _nonLiteralArgType;
  List<TransformFunction> _nonLiteralTransformFunction;
  TransformResultMetadata _transformResultMetadata;
  String[] _stringResult;
  int[] _integerResult;
  float[] _floatResult;
  double[] _doubleResult;
  long[] _longResult;

  public ScalarTransformFunctionWrapper() {
    _nonLiteralArgIndices = new ArrayList<>();
    _nonLiteralArgType = new ArrayList<>();
    _nonLiteralTransformFunction = new ArrayList<>();
  }

  @Override
  public String getName() {
    return _name;
  }

  public void setFunction(String functionName, FunctionInfo info)
      throws Exception {
    _name = functionName;
    _functionInvoker = new FunctionInvoker(info);
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
    Preconditions.checkArgument(arguments.size() == _functionInvoker.getParameterTypes().length,
        "The number of arguments are not same for scalar function and transform function: %s", getName());

    _args = new Object[arguments.size()];
    for (int i = 0; i < arguments.size(); i++) {
      TransformFunction function = arguments.get(i);
      if (function instanceof LiteralTransformFunction) {
        String literal = ((LiteralTransformFunction) function).getLiteral();
        Class paramType = _functionInvoker.getParameterTypes()[i];
        switch (paramType.getTypeName()) {
          case "java.lang.Integer":
            _args[i] = Integer.parseInt(literal);
            break;
          case "java.lang.String":
            _args[i] = literal;
            break;
          case "java.lang.Double":
            _args[i] = Double.valueOf(literal);
            break;
          case "java.lang.Float":
            _args[i] = Float.valueOf(literal);
            break;
          case "java.lang.Long":
            _args[i] = Long.valueOf(literal);
            break;
          default:
            throw new RuntimeException(
                "Unsupported data type " + paramType.getTypeName() + "for transform function " + getName());
        }
      } else {
        _nonLiteralArgIndices.add(i);
        _nonLiteralTransformFunction.add(function);
        Class paramType = _functionInvoker.getParameterTypes()[i];

        switch (paramType.getTypeName()) {
          case "java.lang.Integer":
            _nonLiteralArgType.add(FieldSpec.DataType.INT);
            break;
          case "java.lang.String":
            _nonLiteralArgType.add(FieldSpec.DataType.STRING);
            break;
          case "java.lang.Double":
            _nonLiteralArgType.add(FieldSpec.DataType.DOUBLE);
            break;
          case "java.lang.Float":
            _nonLiteralArgType.add(FieldSpec.DataType.FLOAT);
            break;
          case "java.lang.Long":
            _nonLiteralArgType.add(FieldSpec.DataType.LONG);
            break;
          default:
            throw new RuntimeException(
                "Unsupported data type " + paramType.getTypeName() + "for transform function " + getName());
        }
      }
    }

    Class returnType = _functionInvoker.getReturnType();
    switch(returnType.getTypeName()) {
      case "java.lang.Integer":
        _transformResultMetadata =  INT_SV_NO_DICTIONARY_METADATA;
        break;
      case "java.lang.Long":
        _transformResultMetadata =  LONG_SV_NO_DICTIONARY_METADATA;
        break;
      case "java.lang.Float":
      case "java.lang.Double":
        _transformResultMetadata =  DOUBLE_SV_NO_DICTIONARY_METADATA;
        break;
      case "java.lang.Boolean":
      case "java.lang.String":
        _transformResultMetadata =  STRING_SV_NO_DICTIONARY_METADATA;
        break;
      default:
        throw new RuntimeException("Unsupported data type " + returnType.getTypeName() + "for transform function " + getName());
    }
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return _transformResultMetadata;
  }

  @SuppressWarnings("Duplicates")
  @Override
  public int[] transformToIntValuesSV(ProjectionBlock projectionBlock) {
    if (_integerResult == null) {
      _integerResult = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }
    int length = projectionBlock.getNumDocs();
    int numNonLiteralArgs = _nonLiteralArgIndices.size();
    Object[][] nonLiteralBlockValues = new Object[numNonLiteralArgs][];

    transformNonLiteralArgsToValues(projectionBlock, numNonLiteralArgs, nonLiteralBlockValues);

    //now invoke the actual function
    for (int i = 0; i < length; i++) {
      for (int k = 0; k < numNonLiteralArgs; k++) {
        _args[_nonLiteralArgIndices.get(k)] = nonLiteralBlockValues[k][i];
      }
      _integerResult[i] = (Integer) _functionInvoker.process(_args);
    }
    return _integerResult;
  }

  @SuppressWarnings("Duplicates")
  @Override
  public long[] transformToLongValuesSV(ProjectionBlock projectionBlock) {
    if (_longResult == null) {
      _longResult = new long[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }
    int length = projectionBlock.getNumDocs();
    int numNonLiteralArgs = _nonLiteralArgIndices.size();
    Object[][] nonLiteralBlockValues = new Object[numNonLiteralArgs][];

    transformNonLiteralArgsToValues(projectionBlock, numNonLiteralArgs, nonLiteralBlockValues);

    //now invoke the actual function
    for (int i = 0; i < length; i++) {
      for (int k = 0; k < numNonLiteralArgs; k++) {
        _args[_nonLiteralArgIndices.get(k)] = nonLiteralBlockValues[k][i];
      }
      _longResult[i] = (Long) _functionInvoker.process(_args);
    }
    return _longResult;
  }

  @SuppressWarnings("Duplicates")
  @Override
  public float[] transformToFloatValuesSV(ProjectionBlock projectionBlock) {
    if (_floatResult == null) {
      _floatResult = new float[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }
    int length = projectionBlock.getNumDocs();
    int numNonLiteralArgs = _nonLiteralArgIndices.size();
    Object[][] nonLiteralBlockValues = new Object[numNonLiteralArgs][];

    transformNonLiteralArgsToValues(projectionBlock, numNonLiteralArgs, nonLiteralBlockValues);

    //now invoke the actual function
    for (int i = 0; i < length; i++) {
      for (int k = 0; k < numNonLiteralArgs; k++) {
        _args[_nonLiteralArgIndices.get(k)] = nonLiteralBlockValues[k][i];
      }
      _floatResult[i] = (Float) _functionInvoker.process(_args);
    }
    return _floatResult;
  }

  @SuppressWarnings("Duplicates")
  @Override
  public double[] transformToDoubleValuesSV(ProjectionBlock projectionBlock) {
    if (_doubleResult == null) {
      _doubleResult = new double[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }
    int length = projectionBlock.getNumDocs();
    int numNonLiteralArgs = _nonLiteralArgIndices.size();
    Object[][] nonLiteralBlockValues = new Object[numNonLiteralArgs][];

    transformNonLiteralArgsToValues(projectionBlock, numNonLiteralArgs, nonLiteralBlockValues);

    //now invoke the actual function
    for (int i = 0; i < length; i++) {
      for (int k = 0; k < numNonLiteralArgs; k++) {
        _args[_nonLiteralArgIndices.get(k)] = nonLiteralBlockValues[k][i];
      }
      _doubleResult[i] = (Double) _functionInvoker.process(_args);
    }
    return _doubleResult;
  }

  @SuppressWarnings("Duplicates")
  @Override
  public String[] transformToStringValuesSV(ProjectionBlock projectionBlock) {
    if (_stringResult == null) {
      _stringResult = new String[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }

    int length = projectionBlock.getNumDocs();
    int numNonLiteralArgs = _nonLiteralArgIndices.size();
    Object[][] nonLiteralBlockValues = new Object[numNonLiteralArgs][];

    transformNonLiteralArgsToValues(projectionBlock, numNonLiteralArgs, nonLiteralBlockValues);

    //now invoke the actual function
    for (int i = 0; i < length; i++) {
      for (int k = 0; k < numNonLiteralArgs; k++) {
        _args[_nonLiteralArgIndices.get(k)] = nonLiteralBlockValues[k][i];
      }
      _stringResult[i] = (String) _functionInvoker.process(_args);
    }

    return _stringResult;
  }

  private void transformNonLiteralArgsToValues(ProjectionBlock projectionBlock, int numNonLiteralArgs,
      Object[][] nonLiteralBlockValues) {
    for (int i = 0; i < numNonLiteralArgs; i++) {
      TransformFunction transformFunc = _nonLiteralTransformFunction.get(i);
      FieldSpec.DataType returnType = _nonLiteralArgType.get(i);
      switch (returnType) {
        case STRING:
          nonLiteralBlockValues[i] = transformFunc.transformToStringValuesSV(projectionBlock);
          break;
        case INT:
          int[] values = transformFunc.transformToIntValuesSV(projectionBlock);
          nonLiteralBlockValues[i] = Arrays.stream(values).boxed().toArray(Integer[]::new);
          break;
        case DOUBLE:
          double[] doubleValues = transformFunc.transformToDoubleValuesSV(projectionBlock);
          nonLiteralBlockValues[i] = Arrays.stream(doubleValues).boxed().toArray(Double[]::new);
          break;
        case FLOAT:
          float[] floatValues = transformFunc.transformToFloatValuesSV(projectionBlock);
          Float[] floatObjectValues = new Float[floatValues.length];
          for (int j = 0; j < floatValues.length; j++) {
            floatObjectValues[j] = floatValues[j];
          }
          nonLiteralBlockValues[i] = floatObjectValues;
          break;
        case LONG:
          long[] longValues = transformFunc.transformToLongValuesSV(projectionBlock);
          nonLiteralBlockValues[i] = Arrays.stream(longValues).boxed().toArray(Long[]::new);
          break;
        default:
          throw new RuntimeException(
              "Unsupported return data type " + returnType + "for transform function " + getName());
      }
    }
  }
}
