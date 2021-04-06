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
import org.apache.commons.lang3.ArrayUtils;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.FunctionInvoker;
import org.apache.pinot.common.function.FunctionUtils;
import org.apache.pinot.common.utils.PinotDataType;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * Wrapper transform function on the annotated scalar function.
 */
public class ScalarTransformFunctionWrapper extends BaseTransformFunction {
  private final String _name;
  private final FunctionInvoker _functionInvoker;
  private final PinotDataType _resultType;
  private final TransformResultMetadata _resultMetadata;

  private Object[] _arguments;
  private int _numNonLiteralArguments;
  private int[] _nonLiteralIndices;
  private TransformFunction[] _nonLiteralFunctions;
  private Object[][] _nonLiteralValues;

  private int[] _intResults;
  private float[] _floatResults;
  private double[] _doubleResults;
  private long[] _longResults;
  private String[] _stringResults;
  private byte[][] _bytesResults;

  private int[][] _intMVResults;
  private long[][] _longMVResults;
  private float[][] _floatMVResults;
  private double[][] _doubleMVResults;
  private String[][] _stringMVResults;

  public ScalarTransformFunctionWrapper(FunctionInfo functionInfo) {
    _name = functionInfo.getMethod().getName();
    _functionInvoker = new FunctionInvoker(functionInfo);
    Class<?>[] parameterClasses = _functionInvoker.getParameterClasses();
    PinotDataType[] parameterTypes = _functionInvoker.getParameterTypes();
    int numParameters = parameterClasses.length;
    for (int i = 0; i < numParameters; i++) {
      Preconditions.checkArgument(parameterTypes[i] != null, "Unsupported parameter class: %s for method: %s",
          parameterClasses[i], functionInfo.getMethod());
    }
    Class<?> resultClass = _functionInvoker.getResultClass();
    PinotDataType resultType = FunctionUtils.getParameterType(resultClass);
    if (resultType != null) {
      _resultType = resultType;
      _resultMetadata =
          new TransformResultMetadata(FunctionUtils.getDataType(resultClass), _resultType.isSingleValue(), false);
    } else {
      // Handle unrecognized result class with STRING
      _resultType = PinotDataType.STRING;
      _resultMetadata = new TransformResultMetadata(DataType.STRING, true, false);
    }
  }

  @Override
  public String getName() {
    return _name;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
    int numArguments = arguments.size();
    PinotDataType[] parameterTypes = _functionInvoker.getParameterTypes();
    Preconditions.checkArgument(numArguments == parameterTypes.length,
        "Wrong number of arguments for method: %s, expected: %s, actual: %s", _functionInvoker.getMethod(),
        parameterTypes.length, numArguments);

    _arguments = new Object[numArguments];
    _nonLiteralIndices = new int[numArguments];
    _nonLiteralFunctions = new TransformFunction[numArguments];
    for (int i = 0; i < numArguments; i++) {
      TransformFunction transformFunction = arguments.get(i);
      if (transformFunction instanceof LiteralTransformFunction) {
        String literal = ((LiteralTransformFunction) transformFunction).getLiteral();
        _arguments[i] = parameterTypes[i].convert(literal, PinotDataType.STRING);
      } else {
        _nonLiteralIndices[_numNonLiteralArguments] = i;
        _nonLiteralFunctions[_numNonLiteralArguments] = transformFunction;
        _numNonLiteralArguments++;
      }
    }
    _nonLiteralValues = new Object[_numNonLiteralArguments][];
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return _resultMetadata;
  }

  @Override
  public int[] transformToIntValuesSV(ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType() != DataType.INT) {
      return super.transformToIntValuesSV(projectionBlock);
    }
    if (_intResults == null) {
      _intResults = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }
    getNonLiteralValues(projectionBlock);
    int length = projectionBlock.getNumDocs();
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _arguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      _intResults[i] = (int) _functionInvoker.invoke(_arguments);
    }
    return _intResults;
  }

  @Override
  public long[] transformToLongValuesSV(ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType() != DataType.LONG) {
      return super.transformToLongValuesSV(projectionBlock);
    }
    if (_longResults == null) {
      _longResults = new long[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }
    getNonLiteralValues(projectionBlock);
    int length = projectionBlock.getNumDocs();
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _arguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      _longResults[i] = (long) _functionInvoker.invoke(_arguments);
    }
    return _longResults;
  }

  @Override
  public float[] transformToFloatValuesSV(ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType() != DataType.FLOAT) {
      return super.transformToFloatValuesSV(projectionBlock);
    }
    if (_floatResults == null) {
      _floatResults = new float[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }
    getNonLiteralValues(projectionBlock);
    int length = projectionBlock.getNumDocs();
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _arguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      _floatResults[i] = (float) _functionInvoker.invoke(_arguments);
    }
    return _floatResults;
  }

  @Override
  public double[] transformToDoubleValuesSV(ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType() != DataType.DOUBLE) {
      return super.transformToDoubleValuesSV(projectionBlock);
    }
    if (_doubleResults == null) {
      _doubleResults = new double[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }
    getNonLiteralValues(projectionBlock);
    int length = projectionBlock.getNumDocs();
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _arguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      _doubleResults[i] = (double) _functionInvoker.invoke(_arguments);
    }
    return _doubleResults;
  }

  @Override
  public String[] transformToStringValuesSV(ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType() != DataType.STRING) {
      return super.transformToStringValuesSV(projectionBlock);
    }
    if (_stringResults == null) {
      _stringResults = new String[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }
    getNonLiteralValues(projectionBlock);
    int length = projectionBlock.getNumDocs();
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _arguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      _stringResults[i] = _functionInvoker.invoke(_arguments).toString();
    }
    return _stringResults;
  }

  @Override
  public byte[][] transformToBytesValuesSV(ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType() != DataType.BYTES) {
      return super.transformToBytesValuesSV(projectionBlock);
    }
    if (_bytesResults == null) {
      _bytesResults = new byte[DocIdSetPlanNode.MAX_DOC_PER_CALL][];
    }
    getNonLiteralValues(projectionBlock);
    int length = projectionBlock.getNumDocs();
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _arguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      _bytesResults[i] = (byte[]) _functionInvoker.invoke(_arguments);
    }
    return _bytesResults;
  }

  @Override
  public int[][] transformToIntValuesMV(ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType() != DataType.INT) {
      return super.transformToIntValuesMV(projectionBlock);
    }
    if (_intMVResults == null) {
      _intMVResults = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL][];
    }
    getNonLiteralValues(projectionBlock);
    int length = projectionBlock.getNumDocs();
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _arguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      _intMVResults[i] = (int[]) _functionInvoker.invoke(_arguments);
    }
    return _intMVResults;
  }

  @Override
  public long[][] transformToLongValuesMV(ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType() != DataType.LONG) {
      return super.transformToLongValuesMV(projectionBlock);
    }
    if (_longMVResults == null) {
      _longMVResults = new long[DocIdSetPlanNode.MAX_DOC_PER_CALL][];
    }
    getNonLiteralValues(projectionBlock);
    int length = projectionBlock.getNumDocs();
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _arguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      _longMVResults[i] = (long[]) _functionInvoker.invoke(_arguments);
    }
    return _longMVResults;
  }

  @Override
  public float[][] transformToFloatValuesMV(ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType() != DataType.FLOAT) {
      return super.transformToFloatValuesMV(projectionBlock);
    }
    if (_floatMVResults == null) {
      _floatMVResults = new float[DocIdSetPlanNode.MAX_DOC_PER_CALL][];
    }
    getNonLiteralValues(projectionBlock);
    int length = projectionBlock.getNumDocs();
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _arguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      _floatMVResults[i] = (float[]) _functionInvoker.invoke(_arguments);
    }
    return _floatMVResults;
  }

  @Override
  public double[][] transformToDoubleValuesMV(ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType() != DataType.DOUBLE) {
      return super.transformToDoubleValuesMV(projectionBlock);
    }
    if (_doubleMVResults == null) {
      _doubleMVResults = new double[DocIdSetPlanNode.MAX_DOC_PER_CALL][];
    }
    getNonLiteralValues(projectionBlock);
    int length = projectionBlock.getNumDocs();
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _arguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      _doubleMVResults[i] = (double[]) _functionInvoker.invoke(_arguments);
    }
    return _doubleMVResults;
  }

  @Override
  public String[][] transformToStringValuesMV(ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType() != DataType.STRING) {
      return super.transformToStringValuesMV(projectionBlock);
    }
    if (_stringMVResults == null) {
      _stringMVResults = new String[DocIdSetPlanNode.MAX_DOC_PER_CALL][];
    }
    getNonLiteralValues(projectionBlock);
    int length = projectionBlock.getNumDocs();
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _arguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      _stringMVResults[i] = (String[]) _functionInvoker.invoke(_arguments);
    }
    return _stringMVResults;
  }

  /**
   * Helper method to fetch values for the non-literal transform functions based on the parameter types.
   */
  private void getNonLiteralValues(ProjectionBlock projectionBlock) {
    PinotDataType[] parameterTypes = _functionInvoker.getParameterTypes();
    for (int i = 0; i < _numNonLiteralArguments; i++) {
      PinotDataType parameterType = parameterTypes[_nonLiteralIndices[i]];
      TransformFunction transformFunction = _nonLiteralFunctions[i];
      switch (parameterType) {
        case INTEGER:
          _nonLiteralValues[i] = ArrayUtils.toObject(transformFunction.transformToIntValuesSV(projectionBlock));
          break;
        case LONG:
          _nonLiteralValues[i] = ArrayUtils.toObject(transformFunction.transformToLongValuesSV(projectionBlock));
          break;
        case FLOAT:
          _nonLiteralValues[i] = ArrayUtils.toObject(transformFunction.transformToFloatValuesSV(projectionBlock));
          break;
        case DOUBLE:
          _nonLiteralValues[i] = ArrayUtils.toObject(transformFunction.transformToDoubleValuesSV(projectionBlock));
          break;
        case STRING:
          _nonLiteralValues[i] = transformFunction.transformToStringValuesSV(projectionBlock);
          break;
        case BYTES:
          _nonLiteralValues[i] = transformFunction.transformToBytesValuesSV(projectionBlock);
          break;
        case PRIMITIVE_INT_ARRAY:
          _nonLiteralValues[i] = transformFunction.transformToIntValuesMV(projectionBlock);
          break;
        case PRIMITIVE_LONG_ARRAY:
          _nonLiteralValues[i] = transformFunction.transformToLongValuesMV(projectionBlock);
          break;
        case PRIMITIVE_FLOAT_ARRAY:
          _nonLiteralValues[i] = transformFunction.transformToFloatValuesMV(projectionBlock);
          break;
        case PRIMITIVE_DOUBLE_ARRAY:
          _nonLiteralValues[i] = transformFunction.transformToDoubleValuesMV(projectionBlock);
          break;
        case STRING_ARRAY:
          _nonLiteralValues[i] = transformFunction.transformToStringValuesMV(projectionBlock);
          break;
        default:
          throw new IllegalStateException("Unsupported parameter type: " + parameterType);
      }
    }
  }
}
