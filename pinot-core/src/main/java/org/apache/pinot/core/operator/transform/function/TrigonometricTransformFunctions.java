package org.apache.pinot.core.operator.transform.function;

import java.lang.Override;
import java.lang.String;

public class TrigonometricTransformFunctions {

  public static class DegreesTransformFunction extends SingleParamMathTransformFunction {
    @Override
    public String getName() {
      return "degrees";
    }

    @Override
    protected void applyMathOperator(double[] values, int length) {
      for(int i = 0; i < length; i++) { _results[i] =  Math.toDegrees(values[i]); };
    }
  }

  public static class AcosTransformFunction extends SingleParamMathTransformFunction {
    @Override
    public String getName() {
      return "acos";
    }

    @Override
    protected void applyMathOperator(double[] values, int length) {
      for(int i = 0; i < length; i++) { _results[i] =  Math.acos(values[i]); };
    }
  }

  public static class TanTransformFunction extends SingleParamMathTransformFunction {
    @Override
    public String getName() {
      return "tan";
    }

    @Override
    protected void applyMathOperator(double[] values, int length) {
      for(int i = 0; i < length; i++) { _results[i] =  Math.tan(values[i]); };
    }
  }

  public static class SinhTransformFunction extends SingleParamMathTransformFunction {
    @Override
    public String getName() {
      return "sinh";
    }

    @Override
    protected void applyMathOperator(double[] values, int length) {
      for(int i = 0; i < length; i++) { _results[i] =  Math.sinh(values[i]); };
    }
  }

  public static class CotTransformFunction extends SingleParamMathTransformFunction {
    @Override
    public String getName() {
      return "cot";
    }

    @Override
    protected void applyMathOperator(double[] values, int length) {
      for(int i = 0; i < length; i++) { _results[i] =  1.0/Math.tan(values[i]); };
    }
  }

  public static class AtanTransformFunction extends SingleParamMathTransformFunction {
    @Override
    public String getName() {
      return "atan";
    }

    @Override
    protected void applyMathOperator(double[] values, int length) {
      for(int i = 0; i < length; i++) { _results[i] =  Math.atan(values[i]); };
    }
  }

  public static class CosTransformFunction extends SingleParamMathTransformFunction {
    @Override
    public String getName() {
      return "cos";
    }

    @Override
    protected void applyMathOperator(double[] values, int length) {
      for(int i = 0; i < length; i++) { _results[i] =  Math.cos(values[i]); };
    }
  }

  public static class AsinTransformFunction extends SingleParamMathTransformFunction {
    @Override
    public String getName() {
      return "asin";
    }

    @Override
    protected void applyMathOperator(double[] values, int length) {
      for(int i = 0; i < length; i++) { _results[i] =  Math.asin(values[i]); };
    }
  }

  public static class CoshTransformFunction extends SingleParamMathTransformFunction {
    @Override
    public String getName() {
      return "cosh";
    }

    @Override
    protected void applyMathOperator(double[] values, int length) {
      for(int i = 0; i < length; i++) { _results[i] =  Math.cosh(values[i]); };
    }
  }

  public static class SinTransformFunction extends SingleParamMathTransformFunction {
    @Override
    public String getName() {
      return "sin";
    }

    @Override
    protected void applyMathOperator(double[] values, int length) {
      for(int i = 0; i < length; i++) { _results[i] =  Math.sin(values[i]); };
    }
  }

  public static class TanhTransformFunction extends SingleParamMathTransformFunction {
    @Override
    public String getName() {
      return "tanh";
    }

    @Override
    protected void applyMathOperator(double[] values, int length) {
      for(int i = 0; i < length; i++) { _results[i] =  Math.tanh(values[i]); };
    }
  }

  public static class RadiansTransformFunction extends SingleParamMathTransformFunction {
    @Override
    public String getName() {
      return "radians";
    }

    @Override
    protected void applyMathOperator(double[] values, int length) {
      for(int i = 0; i < length; i++) { _results[i] =  Math.toRadians(values[i]); };
    }
  }
}
