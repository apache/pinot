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

import java.lang.reflect.Method;
import java.util.Set;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.scalar.ArithmeticFunctions;
import org.apache.pinot.common.function.scalar.bitwise.BitAndScalarFunction;
import org.apache.pinot.common.function.scalar.bitwise.BitExtractScalarFunction;
import org.apache.pinot.common.function.scalar.bitwise.BitMaskScalarFunction;
import org.apache.pinot.common.function.scalar.bitwise.BitNotScalarFunction;
import org.apache.pinot.common.function.scalar.bitwise.BitOrScalarFunction;
import org.apache.pinot.common.function.scalar.bitwise.BitShiftLeftScalarFunction;
import org.apache.pinot.common.function.scalar.bitwise.BitShiftRightScalarFunction;
import org.apache.pinot.common.function.scalar.bitwise.BitShiftRightUnsignedScalarFunction;
import org.apache.pinot.common.function.scalar.bitwise.BitXorScalarFunction;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;


/// Evaluates built-in bitwise scalar methods on primitive blocks, dispatching once per block.
/// Instances reuse result buffers and are confined to a segment operator.
final class BitwiseTransformFunction extends BaseTransformFunction {
  private static final Set<Method> METHODS;

  static {
    try {
      METHODS = Set.of(
          BitAndScalarFunction.class.getMethod("intBitAnd", int.class, int.class),
          BitAndScalarFunction.class.getMethod("longBitAnd", long.class, long.class),
          BitOrScalarFunction.class.getMethod("intBitOr", int.class, int.class),
          BitOrScalarFunction.class.getMethod("longBitOr", long.class, long.class),
          BitXorScalarFunction.class.getMethod("intBitXor", int.class, int.class),
          BitXorScalarFunction.class.getMethod("longBitXor", long.class, long.class),
          BitNotScalarFunction.class.getMethod("intBitNot", int.class),
          BitNotScalarFunction.class.getMethod("longBitNot", long.class),
          BitShiftLeftScalarFunction.class.getMethod("intBitShiftLeft", int.class, int.class),
          BitShiftLeftScalarFunction.class.getMethod("longBitShiftLeft", long.class, int.class),
          BitShiftRightScalarFunction.class.getMethod("intBitShiftRight", int.class, int.class),
          BitShiftRightScalarFunction.class.getMethod("longBitShiftRight", long.class, int.class),
          BitShiftRightUnsignedScalarFunction.class.getMethod("intBitShiftRightUnsigned", int.class, int.class),
          BitShiftRightUnsignedScalarFunction.class.getMethod("longBitShiftRightUnsigned", long.class, int.class),
          BitExtractScalarFunction.class.getMethod("intBitExtract", int.class, long.class),
          BitExtractScalarFunction.class.getMethod("longBitExtract", long.class, long.class),
          BitMaskScalarFunction.class.getMethod("intBitMask", int.class),
          BitMaskScalarFunction.class.getMethod("longBitMask", long.class),
          ArithmeticFunctions.class.getMethod("bitCount", long.class));
    } catch (NoSuchMethodException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private final String _name;
  private final TransformResultMetadata _resultMetadata;

  BitwiseTransformFunction(Method method) {
    _name = method.getName();
    _resultMetadata = method.getReturnType() == int.class
        ? INT_SV_NO_DICTIONARY_METADATA : LONG_SV_NO_DICTIONARY_METADATA;
  }

  static boolean supports(FunctionInfo functionInfo, ColumnDataType[] argumentTypes) {
    Method method = functionInfo.getMethod();
    if (method.getDeclaringClass() != functionInfo.getClazz() || !METHODS.contains(method)) {
      return false;
    }
    for (ColumnDataType type : argumentTypes) {
      if (type != ColumnDataType.BOOLEAN && type != ColumnDataType.INT && type != ColumnDataType.LONG) {
        return false;
      }
    }
    return true;
  }

  @Override
  public String getName() {
    return _name;
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return _resultMetadata;
  }

  @Override
  public int[] transformToIntValuesSV(ValueBlock valueBlock) {
    if (_resultMetadata != INT_SV_NO_DICTIONARY_METADATA) {
      return super.transformToIntValuesSV(valueBlock);
    }
    int length = valueBlock.getNumDocs();
    initIntValuesSV(length);
    // These overloads consume LONG values even though their result is INT.
    if (_name.equals("bitCount")) {
      long[] values = _arguments.get(0).transformToLongValuesSV(valueBlock);
      for (int i = 0; i < length; i++) {
        _intValuesSV[i] = ArithmeticFunctions.bitCount(values[i]);
      }
      return _intValuesSV;
    }
    if (_name.equals("longBitExtract")) {
      long[] values = _arguments.get(0).transformToLongValuesSV(valueBlock);
      long[] bits = _arguments.get(1).transformToLongValuesSV(valueBlock);
      for (int i = 0; i < length; i++) {
        _intValuesSV[i] = BitExtractScalarFunction.longBitExtract(values[i], bits[i]);
      }
      return _intValuesSV;
    }
    int[] values = _arguments.get(0).transformToIntValuesSV(valueBlock);
    switch (_name) {
      case "intBitAnd" -> {
        int[] right = _arguments.get(1).transformToIntValuesSV(valueBlock);
        for (int i = 0; i < length; i++) {
          _intValuesSV[i] = BitAndScalarFunction.intBitAnd(values[i], right[i]);
        }
      }
      case "intBitOr" -> {
        int[] right = _arguments.get(1).transformToIntValuesSV(valueBlock);
        for (int i = 0; i < length; i++) {
          _intValuesSV[i] = BitOrScalarFunction.intBitOr(values[i], right[i]);
        }
      }
      case "intBitXor" -> {
        int[] right = _arguments.get(1).transformToIntValuesSV(valueBlock);
        for (int i = 0; i < length; i++) {
          _intValuesSV[i] = BitXorScalarFunction.intBitXor(values[i], right[i]);
        }
      }
      case "intBitNot" -> {
        for (int i = 0; i < length; i++) {
          _intValuesSV[i] = BitNotScalarFunction.intBitNot(values[i]);
        }
      }
      case "intBitShiftLeft" -> {
        int[] shifts = _arguments.get(1).transformToIntValuesSV(valueBlock);
        for (int i = 0; i < length; i++) {
          _intValuesSV[i] = BitShiftLeftScalarFunction.intBitShiftLeft(values[i], shifts[i]);
        }
      }
      case "intBitShiftRight" -> {
        int[] shifts = _arguments.get(1).transformToIntValuesSV(valueBlock);
        for (int i = 0; i < length; i++) {
          _intValuesSV[i] = BitShiftRightScalarFunction.intBitShiftRight(values[i], shifts[i]);
        }
      }
      case "intBitShiftRightUnsigned" -> {
        int[] shifts = _arguments.get(1).transformToIntValuesSV(valueBlock);
        for (int i = 0; i < length; i++) {
          _intValuesSV[i] = BitShiftRightUnsignedScalarFunction.intBitShiftRightUnsigned(values[i], shifts[i]);
        }
      }
      case "intBitExtract" -> {
        long[] bits = _arguments.get(1).transformToLongValuesSV(valueBlock);
        for (int i = 0; i < length; i++) {
          _intValuesSV[i] = BitExtractScalarFunction.intBitExtract(values[i], bits[i]);
        }
      }
      default -> throw new IllegalStateException("Unsupported bitwise method: " + _name);
    }
    return _intValuesSV;
  }

  @Override
  public long[] transformToLongValuesSV(ValueBlock valueBlock) {
    if (_resultMetadata != LONG_SV_NO_DICTIONARY_METADATA) {
      return super.transformToLongValuesSV(valueBlock);
    }
    int length = valueBlock.getNumDocs();
    initLongValuesSV(length);
    if (_name.equals("intBitMask")) {
      int[] bits = _arguments.get(0).transformToIntValuesSV(valueBlock);
      for (int i = 0; i < length; i++) {
        _longValuesSV[i] = BitMaskScalarFunction.intBitMask(bits[i]);
      }
      return _longValuesSV;
    }
    long[] values = _arguments.get(0).transformToLongValuesSV(valueBlock);
    switch (_name) {
      case "longBitAnd" -> {
        long[] right = _arguments.get(1).transformToLongValuesSV(valueBlock);
        for (int i = 0; i < length; i++) {
          _longValuesSV[i] = BitAndScalarFunction.longBitAnd(values[i], right[i]);
        }
      }
      case "longBitOr" -> {
        long[] right = _arguments.get(1).transformToLongValuesSV(valueBlock);
        for (int i = 0; i < length; i++) {
          _longValuesSV[i] = BitOrScalarFunction.longBitOr(values[i], right[i]);
        }
      }
      case "longBitXor" -> {
        long[] right = _arguments.get(1).transformToLongValuesSV(valueBlock);
        for (int i = 0; i < length; i++) {
          _longValuesSV[i] = BitXorScalarFunction.longBitXor(values[i], right[i]);
        }
      }
      case "longBitNot" -> {
        for (int i = 0; i < length; i++) {
          _longValuesSV[i] = BitNotScalarFunction.longBitNot(values[i]);
        }
      }
      case "longBitShiftLeft" -> {
        int[] shifts = _arguments.get(1).transformToIntValuesSV(valueBlock);
        for (int i = 0; i < length; i++) {
          _longValuesSV[i] = BitShiftLeftScalarFunction.longBitShiftLeft(values[i], shifts[i]);
        }
      }
      case "longBitShiftRight" -> {
        int[] shifts = _arguments.get(1).transformToIntValuesSV(valueBlock);
        for (int i = 0; i < length; i++) {
          _longValuesSV[i] = BitShiftRightScalarFunction.longBitShiftRight(values[i], shifts[i]);
        }
      }
      case "longBitShiftRightUnsigned" -> {
        int[] shifts = _arguments.get(1).transformToIntValuesSV(valueBlock);
        for (int i = 0; i < length; i++) {
          _longValuesSV[i] = BitShiftRightUnsignedScalarFunction.longBitShiftRightUnsigned(values[i], shifts[i]);
        }
      }
      case "longBitMask" -> {
        for (int i = 0; i < length; i++) {
          _longValuesSV[i] = BitMaskScalarFunction.longBitMask(values[i]);
        }
      }
      default -> throw new IllegalStateException("Unsupported bitwise method: " + _name);
    }
    return _longValuesSV;
  }
}
