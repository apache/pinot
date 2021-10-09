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
import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;
import it.unimi.dsi.fastutil.floats.FloatOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.BytesUtils;


/**
 * The IN transform function takes one main expression (lhs) and multiple value expressions.
 * <p>For each docId, the function returns {@code true} if the set of values contains the value of the expression,
 * {@code false} otherwise.
 * <p>E.g. {@code SELECT col IN ('a','b','c') FROM myTable)}
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class InTransformFunction extends BaseTransformFunction {
  private TransformFunction _mainFunction;
  private TransformFunction[] _valueFunctions;
  private Set _valueSet;

  @Override
  public String getName() {
    return TransformFunctionType.IN.getName();
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
    int numArguments = arguments.size();
    Preconditions.checkArgument(numArguments >= 2,
        "At least 2 arguments are required for IN transform function: expression, values");
    _mainFunction = arguments.get(0);

    boolean allLiteralValues = true;
    ObjectOpenHashSet<String> stringValues = new ObjectOpenHashSet<>(numArguments - 1);
    for (int i = 1; i < numArguments; i++) {
      TransformFunction valueFunction = arguments.get(i);
      if (valueFunction instanceof LiteralTransformFunction) {
        stringValues.add(((LiteralTransformFunction) valueFunction).getLiteral());
      } else {
        allLiteralValues = false;
        break;
      }
    }

    if (allLiteralValues) {
      int numValues = stringValues.size();
      DataType storedType = _mainFunction.getResultMetadata().getDataType().getStoredType();
      switch (storedType) {
        case INT:
          IntOpenHashSet intValues = new IntOpenHashSet(numValues);
          for (String stringValue : stringValues) {
            intValues.add(Integer.parseInt(stringValue));
          }
          _valueSet = intValues;
          break;
        case LONG:
          LongOpenHashSet longValues = new LongOpenHashSet(numValues);
          for (String stringValue : stringValues) {
            longValues.add(Long.parseLong(stringValue));
          }
          _valueSet = longValues;
          break;
        case FLOAT:
          FloatOpenHashSet floatValues = new FloatOpenHashSet(numValues);
          for (String stringValue : stringValues) {
            floatValues.add(Float.parseFloat(stringValue));
          }
          _valueSet = floatValues;
          break;
        case DOUBLE:
          DoubleOpenHashSet doubleValues = new DoubleOpenHashSet(numValues);
          for (String stringValue : stringValues) {
            doubleValues.add(Double.parseDouble(stringValue));
          }
          _valueSet = doubleValues;
          break;
        case STRING:
          _valueSet = stringValues;
          break;
        case BYTES:
          ObjectOpenHashSet<ByteArray> bytesValues = new ObjectOpenHashSet<>(numValues);
          for (String stringValue : stringValues) {
            bytesValues.add(BytesUtils.toByteArray(stringValue));
          }
          _valueSet = bytesValues;
          break;
        default:
          throw new IllegalStateException();
      }
    } else {
      Preconditions.checkArgument(_mainFunction.getResultMetadata().isSingleValue(),
          "The first argument for IN transform function must be single-valued when there are non-literal values");
      _valueFunctions = new TransformFunction[numArguments - 1];
      for (int i = 1; i < numArguments; i++) {
        TransformFunction valueFunction = arguments.get(i);
        Preconditions.checkArgument(valueFunction.getResultMetadata().isSingleValue(),
            "The values for IN transform function must be single-valued");
        _valueFunctions[i - 1] = valueFunction;
      }
    }
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return BOOLEAN_SV_NO_DICTIONARY_METADATA;
  }

  @Override
  public int[] transformToIntValuesSV(ProjectionBlock projectionBlock) {
    if (_intValuesSV == null) {
      _intValuesSV = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    } else {
      Arrays.fill(_intValuesSV, 0);
    }

    int length = projectionBlock.getNumDocs();
    TransformResultMetadata mainFunctionMetadata = _mainFunction.getResultMetadata();
    DataType storedType = mainFunctionMetadata.getDataType().getStoredType();
    if (_valueSet != null) {
      if (_mainFunction.getResultMetadata().isSingleValue()) {
        switch (storedType) {
          case INT:
            IntOpenHashSet inIntValues = (IntOpenHashSet) _valueSet;
            int[] intValues = _mainFunction.transformToIntValuesSV(projectionBlock);
            for (int i = 0; i < length; i++) {
              if (inIntValues.contains(intValues[i])) {
                _intValuesSV[i] = 1;
              }
            }
            break;
          case LONG:
            LongOpenHashSet inLongValues = (LongOpenHashSet) _valueSet;
            long[] longValues = _mainFunction.transformToLongValuesSV(projectionBlock);
            for (int i = 0; i < length; i++) {
              if (inLongValues.contains(longValues[i])) {
                _intValuesSV[i] = 1;
              }
            }
            break;
          case FLOAT:
            FloatOpenHashSet inFloatValues = (FloatOpenHashSet) _valueSet;
            float[] floatValues = _mainFunction.transformToFloatValuesSV(projectionBlock);
            for (int i = 0; i < length; i++) {
              if (inFloatValues.contains(floatValues[i])) {
                _intValuesSV[i] = 1;
              }
            }
            break;
          case DOUBLE:
            DoubleOpenHashSet inDoubleValues = (DoubleOpenHashSet) _valueSet;
            double[] doubleValues = _mainFunction.transformToDoubleValuesSV(projectionBlock);
            for (int i = 0; i < length; i++) {
              if (inDoubleValues.contains(doubleValues[i])) {
                _intValuesSV[i] = 1;
              }
            }
            break;
          case STRING:
            ObjectOpenHashSet<String> inStringValues = (ObjectOpenHashSet<String>) _valueSet;
            String[] stringValues = _mainFunction.transformToStringValuesSV(projectionBlock);
            for (int i = 0; i < length; i++) {
              if (inStringValues.contains(stringValues[i])) {
                _intValuesSV[i] = 1;
              }
            }
            break;
          case BYTES:
            ObjectOpenHashSet<ByteArray> inBytesValues = (ObjectOpenHashSet<ByteArray>) _valueSet;
            byte[][] bytesValues = _mainFunction.transformToBytesValuesSV(projectionBlock);
            for (int i = 0; i < length; i++) {
              if (inBytesValues.contains(new ByteArray(bytesValues[i]))) {
                _intValuesSV[i] = 1;
              }
            }
            break;
          default:
            throw new IllegalStateException();
        }
      } else {
        switch (storedType) {
          case INT:
            IntOpenHashSet inIntValues = (IntOpenHashSet) _valueSet;
            int[][] intValues = _mainFunction.transformToIntValuesMV(projectionBlock);
            for (int i = 0; i < length; i++) {
              for (int intValue : intValues[i]) {
                if (inIntValues.contains(intValue)) {
                  _intValuesSV[i] = 1;
                  break;
                }
              }
            }
            break;
          case LONG:
            LongOpenHashSet inLongValues = (LongOpenHashSet) _valueSet;
            long[][] longValues = _mainFunction.transformToLongValuesMV(projectionBlock);
            for (int i = 0; i < length; i++) {
              for (long longValue : longValues[i]) {
                if (inLongValues.contains(longValue)) {
                  _intValuesSV[i] = 1;
                  break;
                }
              }
            }
            break;
          case FLOAT:
            FloatOpenHashSet inFloatValues = (FloatOpenHashSet) _valueSet;
            float[][] floatValues = _mainFunction.transformToFloatValuesMV(projectionBlock);
            for (int i = 0; i < length; i++) {
              for (float floatValue : floatValues[i]) {
                if (inFloatValues.contains(floatValue)) {
                  _intValuesSV[i] = 1;
                  break;
                }
              }
            }
            break;
          case DOUBLE:
            DoubleOpenHashSet inDoubleValues = (DoubleOpenHashSet) _valueSet;
            double[][] doubleValues = _mainFunction.transformToDoubleValuesMV(projectionBlock);
            for (int i = 0; i < length; i++) {
              for (double doubleValue : doubleValues[i]) {
                if (inDoubleValues.contains(doubleValue)) {
                  _intValuesSV[i] = 1;
                  break;
                }
              }
            }
            break;
          case STRING:
            ObjectOpenHashSet<String> inStringValues = (ObjectOpenHashSet<String>) _valueSet;
            String[][] stringValues = _mainFunction.transformToStringValuesMV(projectionBlock);
            for (int i = 0; i < length; i++) {
              for (String stringValue : stringValues[i]) {
                if (inStringValues.contains(stringValue)) {
                  _intValuesSV[i] = 1;
                  break;
                }
              }
            }
            break;
          default:
            throw new IllegalStateException();
        }
      }
    } else {
      int numValues = _valueFunctions.length;
      switch (storedType) {
        case INT:
          int[] intValues = _mainFunction.transformToIntValuesSV(projectionBlock);
          int[][] inIntValues = new int[numValues][];
          for (int i = 0; i < numValues; i++) {
            inIntValues[i] = _valueFunctions[i].transformToIntValuesSV(projectionBlock);
          }
          for (int i = 0; i < length; i++) {
            for (int[] inIntValue : inIntValues) {
              if (intValues[i] == inIntValue[i]) {
                _intValuesSV[i] = 1;
                break;
              }
            }
          }
          break;
        case LONG:
          long[] longValues = _mainFunction.transformToLongValuesSV(projectionBlock);
          long[][] inLongValues = new long[numValues][];
          for (int i = 0; i < numValues; i++) {
            inLongValues[i] = _valueFunctions[i].transformToLongValuesSV(projectionBlock);
          }
          for (int i = 0; i < length; i++) {
            for (long[] inLongValue : inLongValues) {
              if (longValues[i] == inLongValue[i]) {
                _intValuesSV[i] = 1;
                break;
              }
            }
          }
          break;
        case FLOAT:
          float[] floatValues = _mainFunction.transformToFloatValuesSV(projectionBlock);
          float[][] inFloatValues = new float[numValues][];
          for (int i = 0; i < numValues; i++) {
            inFloatValues[i] = _valueFunctions[i].transformToFloatValuesSV(projectionBlock);
          }
          for (int i = 0; i < length; i++) {
            for (float[] inFloatValue : inFloatValues) {
              if (floatValues[i] == inFloatValue[i]) {
                _intValuesSV[i] = 1;
                break;
              }
            }
          }
          break;
        case DOUBLE:
          double[] doubleValues = _mainFunction.transformToDoubleValuesSV(projectionBlock);
          double[][] inDoubleValues = new double[numValues][];
          for (int i = 0; i < numValues; i++) {
            inDoubleValues[i] = _valueFunctions[i].transformToDoubleValuesSV(projectionBlock);
          }
          for (int i = 0; i < length; i++) {
            for (double[] inDoubleValue : inDoubleValues) {
              if (doubleValues[i] == inDoubleValue[i]) {
                _intValuesSV[i] = 1;
                break;
              }
            }
          }
          break;
        case STRING:
          String[] stringValues = _mainFunction.transformToStringValuesSV(projectionBlock);
          String[][] inStringValues = new String[numValues][];
          for (int i = 0; i < numValues; i++) {
            inStringValues[i] = _valueFunctions[i].transformToStringValuesSV(projectionBlock);
          }
          for (int i = 0; i < length; i++) {
            for (String[] inStringValue : inStringValues) {
              if (stringValues[i].equals(inStringValue[i])) {
                _intValuesSV[i] = 1;
                break;
              }
            }
          }
          break;
        case BYTES:
          byte[][] bytesValues = _mainFunction.transformToBytesValuesSV(projectionBlock);
          byte[][][] inBytesValues = new byte[numValues][][];
          for (int i = 0; i < numValues; i++) {
            inBytesValues[i] = _valueFunctions[i].transformToBytesValuesSV(projectionBlock);
          }
          for (int i = 0; i < length; i++) {
            for (byte[][] inBytesValue : inBytesValues) {
              if (Arrays.equals(bytesValues[i], inBytesValue[i])) {
                _intValuesSV[i] = 1;
                break;
              }
            }
          }
          break;
        default:
          throw new IllegalStateException();
      }
    }

    return _intValuesSV;
  }
}
