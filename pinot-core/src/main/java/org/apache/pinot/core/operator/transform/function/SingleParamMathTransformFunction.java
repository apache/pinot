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

import javax.annotation.Nonnull;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.plan.DocIdSetPlanNode;

/**
 * A group of commonly used math transformation which has only one single parameter,
 * including abs, exp, ceil, floor, ln, log10, sqrt.
 *
 */
public abstract class SingleParamMathTransformFunction extends BaseTransformFunction {
  private double _firstLiteral;
  private TransformFunction _firstTransformFunction;
  private double[] _result;

  @Override
  public abstract String getName();

  @Override
  public void init(@Nonnull List<TransformFunction> arguments, @Nonnull Map<String, DataSource> dataSourceMap) {
    // Check that there are exactly 1 arguments
    if (arguments.size() != 1) {
      throw new IllegalArgumentException("Exactly 1 arguments are required for " + getName() + " transform function");
    }

    TransformFunction firstArgument = arguments.get(0);
    if (firstArgument instanceof LiteralTransformFunction) {
      _firstLiteral = Double.parseDouble(((LiteralTransformFunction) firstArgument).getLiteral());
    } else {
      if (!firstArgument.getResultMetadata().isSingleValue()) {
        throw new IllegalArgumentException("First argument of " + getName() + " transform function must be single-valued");
      }
      _firstTransformFunction = firstArgument;
    }
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return DOUBLE_SV_NO_DICTIONARY_METADATA;
  }

  @SuppressWarnings("Duplicates")
  @Override
  public double[] transformToDoubleValuesSV(@Nonnull ProjectionBlock projectionBlock) {
    if (_result == null) {
      _result = new double[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }

    int length = projectionBlock.getNumDocs();

    if (_firstTransformFunction == null) {
      Arrays.fill(_result, 0, length, _firstLiteral);
    } else {
      switch (_firstTransformFunction.getResultMetadata().getDataType()) {
        case INT:
          int[] intValues = _firstTransformFunction.transformToIntValuesSV(projectionBlock);
          transformInt(_result, intValues, length);
          break;
        case LONG:
          long[] longValues = _firstTransformFunction.transformToLongValuesSV(projectionBlock);
          transformLong(_result, longValues, length);
          break;
        case FLOAT:
          float[] floatValues = _firstTransformFunction.transformToFloatValuesSV(projectionBlock);
          transformFloat(_result, floatValues, length);
          break;
        case DOUBLE:
          double[] doubleValues = _firstTransformFunction.transformToDoubleValuesSV(projectionBlock);
          transformDouble(_result, doubleValues, length);
          break;
        case STRING:
          String[] stringValues = _firstTransformFunction.transformToStringValuesSV(projectionBlock);
          transformString(_result, stringValues, length);
          break;
        default:
          throw new UnsupportedOperationException();
      }
    }

    return _result;
  }

  abstract protected void transformInt(double[] result, int[] values, int length);

  abstract protected void transformLong(double[] result, long[] values, int length);

  abstract protected void transformFloat(double[] result, float[] values, int length);

  abstract protected void transformDouble(double[] result, double[] values, int length);

  abstract protected void transformString(double[] result, String[] values, int length);

  public static class AbsTransformFunction extends SingleParamMathTransformFunction {
    public static final String FUNCTION_NAME = "abs";

    @Override
    public String getName() {
      return FUNCTION_NAME;
    }

    @Override
    protected void transformInt(double[] result, int[] values, int length) {
      for (int i = 0; i < length; i++) {
         result[i] = Math.abs(values[i]);
      }
    }

    @Override
    protected void transformLong(double[] result, long[] values, int length) {
      for (int i = 0; i < length; i++) {
        result[i] = Math.abs(values[i]);
      }
    }

    @Override
    protected void transformFloat(double[] result, float[] values, int length) {
      for (int i = 0; i < length; i++) {
        result[i] = Math.abs(values[i]);
      }
    }

    @Override
    protected void transformDouble(double[] result, double[] values, int length) {
      for (int i = 0; i < length; i++) {
        result[i] = Math.abs(values[i]);
      }
    }

    @Override
    protected void transformString(double[] result, String[] values, int length) {
      for (int i = 0; i < length; i++) {
        double doubleValue = Double.parseDouble(values[i]);
        result[i] = Math.abs(doubleValue);
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
    protected void transformInt(double[] result, int[] values, int length) {
      for (int i = 0; i < length; i++) {
         result[i] = Math.ceil(values[i]);
      }
    }

    @Override
    protected void transformLong(double[] result, long[] values, int length) {
      for (int i = 0; i < length; i++) {
        result[i] = Math.ceil(values[i]);
      }
    }

    @Override
    protected void transformFloat(double[] result, float[] values, int length) {
      for (int i = 0; i < length; i++) {
        result[i] = Math.ceil(values[i]);
      }
    }

    @Override
    protected void transformDouble(double[] result, double[] values, int length) {
      for (int i = 0; i < length; i++) {
        result[i] = Math.ceil(values[i]);
      }
    }

    @Override
    protected void transformString(double[] result, String[] values, int length) {
      for (int i = 0; i < length; i++) {
        double doubleValue = Double.parseDouble(values[i]);
        result[i] = Math.ceil(doubleValue);
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
    protected void transformInt(double[] result, int[] values, int length) {
      for (int i = 0; i < length; i++) {
         result[i] = Math.exp(values[i]);
      }
    }

    @Override
    protected void transformLong(double[] result, long[] values, int length) {
      for (int i = 0; i < length; i++) {
        result[i] = Math.exp(values[i]);
      }
    }

    @Override
    protected void transformFloat(double[] result, float[] values, int length) {
      for (int i = 0; i < length; i++) {
        result[i] = Math.exp(values[i]);
      }
    }

    @Override
    protected void transformDouble(double[] result, double[] values, int length) {
      for (int i = 0; i < length; i++) {
        result[i] = Math.exp(values[i]);
      }
    }

    @Override
    protected void transformString(double[] result, String[] values, int length) {
      for (int i = 0; i < length; i++) {
        double doubleValue = Double.parseDouble(values[i]);
        result[i] = Math.exp(doubleValue);
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
    protected void transformInt(double[] result, int[] values, int length) {
      for (int i = 0; i < length; i++) {
         result[i] = Math.floor(values[i]);
      }
    }

    @Override
    protected void transformLong(double[] result, long[] values, int length) {
      for (int i = 0; i < length; i++) {
        result[i] = Math.floor(values[i]);
      }
    }

    @Override
    protected void transformFloat(double[] result, float[] values, int length) {
      for (int i = 0; i < length; i++) {
        result[i] = Math.floor(values[i]);
      }
    }

    @Override
    protected void transformDouble(double[] result, double[] values, int length) {
      for (int i = 0; i < length; i++) {
        result[i] = Math.floor(values[i]);
      }
    }

    @Override
    protected void transformString(double[] result, String[] values, int length) {
      for (int i = 0; i < length; i++) {
        double doubleValue = Double.parseDouble(values[i]);
        result[i] = Math.floor(doubleValue);
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
    protected void transformInt(double[] result, int[] values, int length) {
      for (int i = 0; i < length; i++) {
         result[i] = Math.log(values[i]);
      }
    }

    @Override
    protected void transformLong(double[] result, long[] values, int length) {
      for (int i = 0; i < length; i++) {
        result[i] = Math.log(values[i]);
      }
    }

    @Override
    protected void transformFloat(double[] result, float[] values, int length) {
      for (int i = 0; i < length; i++) {
        result[i] = Math.log(values[i]);
      }
    }

    @Override
    protected void transformDouble(double[] result, double[] values, int length) {
      for (int i = 0; i < length; i++) {
        result[i] = Math.log(values[i]);
      }
    }

    @Override
    protected void transformString(double[] result, String[] values, int length) {
      for (int i = 0; i < length; i++) {
        double doubleValue = Double.parseDouble(values[i]);
        result[i] = Math.log(doubleValue);
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
    protected void transformInt(double[] result, int[] values, int length) {
      for (int i = 0; i < length; i++) {
         result[i] = Math.log10(values[i]);
      }
    }

    @Override
    protected void transformLong(double[] result, long[] values, int length) {
      for (int i = 0; i < length; i++) {
        result[i] = Math.log10(values[i]);
      }
    }

    @Override
    protected void transformFloat(double[] result, float[] values, int length) {
      for (int i = 0; i < length; i++) {
        result[i] = Math.log10(values[i]);
      }
    }

    @Override
    protected void transformDouble(double[] result, double[] values, int length) {
      for (int i = 0; i < length; i++) {
        result[i] = Math.log10(values[i]);
      }
    }

    @Override
    protected void transformString(double[] result, String[] values, int length) {
      for (int i = 0; i < length; i++) {
        double doubleValue = Double.parseDouble(values[i]);
        result[i] = Math.log10(doubleValue);
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
    protected void transformInt(double[] result, int[] values, int length) {
      for (int i = 0; i < length; i++) {
         result[i] = Math.sqrt(values[i]);
      }
    }

    @Override
    protected void transformLong(double[] result, long[] values, int length) {
      for (int i = 0; i < length; i++) {
        result[i] = Math.sqrt(values[i]);
      }
    }

    @Override
    protected void transformFloat(double[] result, float[] values, int length) {
      for (int i = 0; i < length; i++) {
        result[i] = Math.sqrt(values[i]);
      }
    }

    @Override
    protected void transformDouble(double[] result, double[] values, int length) {
      for (int i = 0; i < length; i++) {
        result[i] = Math.sqrt(values[i]);
      }
    }

    @Override
    protected void transformString(double[] result, String[] values, int length) {
      for (int i = 0; i < length; i++) {
        double doubleValue = Double.parseDouble(values[i]);
        result[i] = Math.sqrt(doubleValue);
      }
    }
  }

}
