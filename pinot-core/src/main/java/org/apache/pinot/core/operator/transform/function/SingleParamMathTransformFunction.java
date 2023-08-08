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
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ArrayCopyUtils;


/**
 * A group of commonly used math transformation which has only one single parameter,
 * including abs, ceil, exp, floor, ln, sqrt.
 * Note:
 * abs(x) -> output data type is either double or BigDecimal.
 * ceil(x) -> output data type is either double or BigDecimal.
 * exp(x) -> output data type is always double.
 * floor(x) -> output data type is either double or BigDecimal.
 * ln(x) -> output data type is always double.
 * sqrt(x) -> output data type is always double.
 */
public abstract class SingleParamMathTransformFunction extends BaseTransformFunction {
  private TransformFunction _transformFunction;
  private DataType _resultDataType;

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
    super.init(arguments, columnContextMap);
    Preconditions.checkArgument(arguments.size() == 1, "Exactly 1 argument is required for transform function: %s",
        getName());
    TransformFunction transformFunction = arguments.get(0);
    Preconditions.checkArgument(!(transformFunction instanceof LiteralTransformFunction),
        "Argument cannot be literal for transform function: %s", getName());
    Preconditions.checkArgument(transformFunction.getResultMetadata().isSingleValue(),
        "Argument must be single-valued for transform function: %s", getName());

    _transformFunction = transformFunction;
    _resultDataType =
        _transformFunction.getResultMetadata().getDataType() == DataType.BIG_DECIMAL ? DataType.BIG_DECIMAL
            : DataType.DOUBLE;
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    if (_resultDataType == DataType.BIG_DECIMAL) {
      return BIG_DECIMAL_SV_NO_DICTIONARY_METADATA;
    }
    return DOUBLE_SV_NO_DICTIONARY_METADATA;
  }

  @Override
  public double[] transformToDoubleValuesSV(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    initDoubleValuesSV(length);
    if (_resultDataType == DataType.BIG_DECIMAL) {
      BigDecimal[] values = transformToBigDecimalValuesSV(valueBlock);
      ArrayCopyUtils.copy(values, _doubleValuesSV, length);
    } else {
      double[] values = _transformFunction.transformToDoubleValuesSV(valueBlock);
      applyMathOperator(values, valueBlock.getNumDocs());
    }
    return _doubleValuesSV;
  }

  @Override
  public BigDecimal[] transformToBigDecimalValuesSV(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    initBigDecimalValuesSV(length);
    if (_resultDataType == DataType.DOUBLE) {
      double[] values = transformToDoubleValuesSV(valueBlock);
      ArrayCopyUtils.copy(values, _bigDecimalValuesSV, length);
    } else {
      BigDecimal[] values = _transformFunction.transformToBigDecimalValuesSV(valueBlock);
      applyMathOperator(values, valueBlock.getNumDocs());
    }
    return _bigDecimalValuesSV;
  }

  abstract protected void applyMathOperator(double[] values, int length);

  protected void applyMathOperator(BigDecimal[] values, int length) {
    throw new UnsupportedOperationException("Math operator does not support BIG_DECIMAL data type");
  }

  public static class AbsTransformFunction extends SingleParamMathTransformFunction {
    public static final String FUNCTION_NAME = "abs";

    @Override
    public String getName() {
      return FUNCTION_NAME;
    }

    @Override
    protected void applyMathOperator(double[] values, int length) {
      for (int i = 0; i < length; i++) {
        _doubleValuesSV[i] = Math.abs(values[i]);
      }
    }

    @Override
    protected void applyMathOperator(BigDecimal[] values, int length) {
      for (int i = 0; i < length; i++) {
        _bigDecimalValuesSV[i] = values[i].abs();
      }
    }
  }

  public static class CeilTransformFunction extends SingleParamMathTransformFunction {
    public static final String FUNCTION_NAME = "ceil";

    @Override
    public String getName() {
      return FUNCTION_NAME;
    }

    @Override
    protected void applyMathOperator(double[] values, int length) {
      for (int i = 0; i < length; i++) {
        _doubleValuesSV[i] = Math.ceil(values[i]);
      }
    }

    @Override
    protected void applyMathOperator(BigDecimal[] values, int length) {
      for (int i = 0; i < length; i++) {
        _bigDecimalValuesSV[i] = values[i].setScale(0, RoundingMode.CEILING);
      }
    }
  }

  public static class ExpTransformFunction extends SingleParamMathTransformFunction {
    public static final String FUNCTION_NAME = "exp";

    @Override
    public String getName() {
      return FUNCTION_NAME;
    }

    @Override
    protected void applyMathOperator(double[] values, int length) {
      for (int i = 0; i < length; i++) {
        _doubleValuesSV[i] = Math.exp(values[i]);
      }
    }
  }

  public static class FloorTransformFunction extends SingleParamMathTransformFunction {
    public static final String FUNCTION_NAME = "floor";

    @Override
    public String getName() {
      return FUNCTION_NAME;
    }

    @Override
    protected void applyMathOperator(double[] values, int length) {
      for (int i = 0; i < length; i++) {
        _doubleValuesSV[i] = Math.floor(values[i]);
      }
    }

    @Override
    protected void applyMathOperator(BigDecimal[] values, int length) {
      for (int i = 0; i < length; i++) {
        _bigDecimalValuesSV[i] = values[i].setScale(0, RoundingMode.FLOOR);
      }
    }
  }

  public static class LnTransformFunction extends SingleParamMathTransformFunction {
    public static final String FUNCTION_NAME = "ln";

    @Override
    public String getName() {
      return FUNCTION_NAME;
    }

    @Override
    protected void applyMathOperator(double[] values, int length) {
      for (int i = 0; i < length; i++) {
        _doubleValuesSV[i] = Math.log(values[i]);
      }
    }
  }

  public static class Log2TransformFunction extends SingleParamMathTransformFunction {
    public static final String FUNCTION_NAME = "log2";
    public static final double LOG_BASE = Math.log(2.0);

    @Override
    public String getName() {
      return FUNCTION_NAME;
    }

    @Override
    protected void applyMathOperator(double[] values, int length) {
      for (int i = 0; i < length; i++) {
        _doubleValuesSV[i] = Math.log(values[i]) / LOG_BASE;
      }
    }
  }

  public static class Log10TransformFunction extends SingleParamMathTransformFunction {
    public static final String FUNCTION_NAME = "log10";

    @Override
    public String getName() {
      return FUNCTION_NAME;
    }

    @Override
    protected void applyMathOperator(double[] values, int length) {
      for (int i = 0; i < length; i++) {
        _doubleValuesSV[i] = Math.log10(values[i]);
      }
    }
  }

  public static class SqrtTransformFunction extends SingleParamMathTransformFunction {
    public static final String FUNCTION_NAME = "sqrt";

    @Override
    public String getName() {
      return FUNCTION_NAME;
    }

    @Override
    protected void applyMathOperator(double[] values, int length) {
      for (int i = 0; i < length; i++) {
        _doubleValuesSV[i] = Math.sqrt(values[i]);
      }
    }
  }

  public static class SignTransformFunction extends SingleParamMathTransformFunction {
    public static final String FUNCTION_NAME = "sign";

    @Override
    public String getName() {
      return FUNCTION_NAME;
    }

    @Override
    protected void applyMathOperator(double[] values, int length) {
      for (int i = 0; i < length; i++) {
        _doubleValuesSV[i] = Math.signum(values[i]);
      }
    }
  }
}
