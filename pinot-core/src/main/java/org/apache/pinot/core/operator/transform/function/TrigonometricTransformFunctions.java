package org.apache.pinot.core.operator.transform.function;

import com.google.common.base.Preconditions;
import java.lang.Override;
import java.lang.String;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.spi.data.FieldSpec;


public class TrigonometricTransformFunctions {
  public static class Atan2TransformFunction extends BaseTransformFunction {
    public static final String FUNCTION_NAME = "atan2";
    private double[] _result;
    private TransformFunction _leftTransformFunction;
    private TransformFunction _rightTransformFunction;

    @Override
    public String getName() {
      return FUNCTION_NAME;
    }

    @Override
    public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
      // Check that there are more than 1 arguments
      if (arguments.size() == 2) {
        throw new IllegalArgumentException("Exactly 2 arguments are required for Atan2 transform function");
      }

      _leftTransformFunction = arguments.get(0);
      _rightTransformFunction = arguments.get(1);
      Preconditions.checkArgument(
          _leftTransformFunction.getResultMetadata().isSingleValue() || _rightTransformFunction.getResultMetadata()
              .isSingleValue(), "Argument must be single-valued for transform function: %s", getName());
    }

    @Override
    public TransformResultMetadata getResultMetadata() {
      return DOUBLE_SV_NO_DICTIONARY_METADATA;
    }

    @Override
    public double[] transformToDoubleValuesSV(ProjectionBlock projectionBlock) {
      if (_result == null) {
        _result = new double[DocIdSetPlanNode.MAX_DOC_PER_CALL];
      }

      int length = projectionBlock.getNumDocs();
      double[] leftValues = _leftTransformFunction.transformToDoubleValuesSV(projectionBlock);
      double[] rightValues = _rightTransformFunction.transformToDoubleValuesSV(projectionBlock);
      for (int i = 0; i < length; i++) {
        _result[i] = Math.atan2(leftValues[i], rightValues[i]);
      }

      return _result;
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
        _results[i] = Math.toDegrees(values[i]);
      }
      ;
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
        _results[i] = Math.acos(values[i]);
      }
      ;
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
        _results[i] = Math.tan(values[i]);
      }
      ;
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
        _results[i] = Math.sinh(values[i]);
      }
      ;
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
        _results[i] = 1.0 / Math.tan(values[i]);
      }
      ;
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
        _results[i] = Math.atan(values[i]);
      }
      ;
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
        _results[i] = Math.cos(values[i]);
      }
      ;
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
        _results[i] = Math.asin(values[i]);
      }
      ;
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
        _results[i] = Math.cosh(values[i]);
      }
      ;
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
        _results[i] = Math.sin(values[i]);
      }
      ;
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
        _results[i] = Math.tanh(values[i]);
      }
      ;
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
        _results[i] = Math.toRadians(values[i]);
      }
      ;
    }
  }
}
