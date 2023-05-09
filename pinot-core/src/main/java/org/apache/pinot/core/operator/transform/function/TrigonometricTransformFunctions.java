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
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;


public class TrigonometricTransformFunctions {
  public static class Atan2TransformFunction extends BaseTransformFunction {
    public static final String FUNCTION_NAME = "atan2";
    private TransformFunction _leftTransformFunction;
    private TransformFunction _rightTransformFunction;

    @Override
    public String getName() {
      return FUNCTION_NAME;
    }

    @Override
    public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
      super.init(arguments, columnContextMap);
      // Check that there are more than 1 arguments
      if (arguments.size() != 2) {
        throw new IllegalArgumentException("Exactly 2 arguments are required for Atan2 transform function");
      }

      _leftTransformFunction = arguments.get(0);
      _rightTransformFunction = arguments.get(1);
      Preconditions.checkArgument(
          _leftTransformFunction.getResultMetadata().isSingleValue() || _rightTransformFunction.getResultMetadata()
              .isSingleValue() || _leftTransformFunction.getResultMetadata().getDataType().isUnknown()
              || _rightTransformFunction.getResultMetadata().getDataType().isUnknown(),
          "Argument must be single-valued for transform function: %s", getName());
    }

    @Override
    public TransformResultMetadata getResultMetadata() {
      return DOUBLE_SV_NO_DICTIONARY_METADATA;
    }

    @Override
    public double[] transformToDoubleValuesSV(ValueBlock valueBlock) {
      int length = valueBlock.getNumDocs();
      initDoubleValuesSV(length);
      double[] leftValues = _leftTransformFunction.transformToDoubleValuesSV(valueBlock);
      double[] rightValues = _rightTransformFunction.transformToDoubleValuesSV(valueBlock);
      for (int i = 0; i < length; i++) {
        _doubleValuesSV[i] = Math.atan2(leftValues[i], rightValues[i]);
      }
      return _doubleValuesSV;
    }
  }

  public static class DegreesTransformFunction extends SingleParamMathTransformFunction {
    public static final String FUNCTION_NAME = "degrees";

    @Override
    public String getName() {
      return FUNCTION_NAME;
    }

    @Override
    protected void applyMathOperator(double[] values, int length) {
      for (int i = 0; i < length; i++) {
        _doubleValuesSV[i] = Math.toDegrees(values[i]);
      }
    }
  }

  public static class AcosTransformFunction extends SingleParamMathTransformFunction {
    public static final String FUNCTION_NAME = "acos";

    @Override
    public String getName() {
      return FUNCTION_NAME;
    }

    @Override
    protected void applyMathOperator(double[] values, int length) {
      for (int i = 0; i < length; i++) {
        _doubleValuesSV[i] = Math.acos(values[i]);
      }
    }
  }

  public static class TanTransformFunction extends SingleParamMathTransformFunction {
    public static final String FUNCTION_NAME = "tan";

    @Override
    public String getName() {
      return FUNCTION_NAME;
    }

    @Override
    protected void applyMathOperator(double[] values, int length) {
      for (int i = 0; i < length; i++) {
        _doubleValuesSV[i] = Math.tan(values[i]);
      }
    }
  }

  public static class SinhTransformFunction extends SingleParamMathTransformFunction {
    public static final String FUNCTION_NAME = "sinh";

    @Override
    public String getName() {
      return FUNCTION_NAME;
    }

    @Override
    protected void applyMathOperator(double[] values, int length) {
      for (int i = 0; i < length; i++) {
        _doubleValuesSV[i] = Math.sinh(values[i]);
      }
    }
  }

  public static class CotTransformFunction extends SingleParamMathTransformFunction {
    public static final String FUNCTION_NAME = "cot";

    @Override
    public String getName() {
      return FUNCTION_NAME;
    }

    @Override
    protected void applyMathOperator(double[] values, int length) {
      for (int i = 0; i < length; i++) {
        _doubleValuesSV[i] = 1.0 / Math.tan(values[i]);
      }
    }
  }

  public static class AtanTransformFunction extends SingleParamMathTransformFunction {
    public static final String FUNCTION_NAME = "atan";

    @Override
    public String getName() {
      return FUNCTION_NAME;
    }

    @Override
    protected void applyMathOperator(double[] values, int length) {
      for (int i = 0; i < length; i++) {
        _doubleValuesSV[i] = Math.atan(values[i]);
      }
    }
  }

  public static class CosTransformFunction extends SingleParamMathTransformFunction {
    public static final String FUNCTION_NAME = "cos";

    @Override
    public String getName() {
      return FUNCTION_NAME;
    }

    @Override
    protected void applyMathOperator(double[] values, int length) {
      for (int i = 0; i < length; i++) {
        _doubleValuesSV[i] = Math.cos(values[i]);
      }
    }
  }

  public static class AsinTransformFunction extends SingleParamMathTransformFunction {
    public static final String FUNCTION_NAME = "asin";

    @Override
    public String getName() {
      return FUNCTION_NAME;
    }

    @Override
    protected void applyMathOperator(double[] values, int length) {
      for (int i = 0; i < length; i++) {
        _doubleValuesSV[i] = Math.asin(values[i]);
      }
    }
  }

  public static class CoshTransformFunction extends SingleParamMathTransformFunction {
    public static final String FUNCTION_NAME = "cosh";

    @Override
    public String getName() {
      return FUNCTION_NAME;
    }

    @Override
    protected void applyMathOperator(double[] values, int length) {
      for (int i = 0; i < length; i++) {
        _doubleValuesSV[i] = Math.cosh(values[i]);
      }
    }
  }

  public static class SinTransformFunction extends SingleParamMathTransformFunction {
    public static final String FUNCTION_NAME = "sin";

    @Override
    public String getName() {
      return FUNCTION_NAME;
    }

    @Override
    protected void applyMathOperator(double[] values, int length) {
      for (int i = 0; i < length; i++) {
        _doubleValuesSV[i] = Math.sin(values[i]);
      }
    }
  }

  public static class TanhTransformFunction extends SingleParamMathTransformFunction {
    public static final String FUNCTION_NAME = "tanh";

    @Override
    public String getName() {
      return FUNCTION_NAME;
    }

    @Override
    protected void applyMathOperator(double[] values, int length) {
      for (int i = 0; i < length; i++) {
        _doubleValuesSV[i] = Math.tanh(values[i]);
      }
    }
  }

  public static class RadiansTransformFunction extends SingleParamMathTransformFunction {
    public static final String FUNCTION_NAME = "radians";

    @Override
    public String getName() {
      return FUNCTION_NAME;
    }

    @Override
    protected void applyMathOperator(double[] values, int length) {
      for (int i = 0; i < length; i++) {
        _doubleValuesSV[i] = Math.toRadians(values[i]);
      }
    }
  }
}
