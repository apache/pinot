package org.apache.pinot.core.operator.transform.function.string;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.operator.transform.function.BaseTransformFunction;
import org.apache.pinot.core.operator.transform.function.LiteralTransformFunction;
import org.apache.pinot.core.operator.transform.function.TransformFunction;
import org.apache.pinot.core.plan.DocIdSetPlanNode;


public abstract class SingleParamStringTransformFunction extends BaseTransformFunction {
  private TransformFunction _transformFunction;
  protected List<String> _arguments = new ArrayList<>();
  protected String[] _results;

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
    Preconditions
        .checkArgument(arguments.size() == 1, "Exactly 1 argument is required for transform function: %s", getName());
    TransformFunction transformFunction = arguments.get(0);
    Preconditions.checkArgument(!(transformFunction instanceof LiteralTransformFunction),
        "Argument cannot be literal for transform function: %s", getName());
    Preconditions.checkArgument(transformFunction.getResultMetadata().isSingleValue(),
        "Argument must be single-valued for transform function: %s", getName());

    _transformFunction = transformFunction;
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return STRING_SV_NO_DICTIONARY_METADATA;
  }

  @Override
  public String[] transformToStringValuesSV(ProjectionBlock projectionBlock) {
    if (_results == null) {
      _results = new String[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }

    String[] values = _transformFunction.transformToStringValuesSV(projectionBlock);
    applyStringOperator(values, projectionBlock.getNumDocs());

    return _results;
  }

  abstract protected void applyStringOperator(String[] values, int length);

  public static class TrimTransformFunction extends SingleParamStringTransformFunction {
    public static final String FUNCTION_NAME = "trim";

    @Override
    public String getName() {
      return FUNCTION_NAME;
    }

    @Override
    protected void applyStringOperator(String[] values, int length) {
      for (int i = 0; i < length; i++) {
        _results[i] = values[i].trim();
      }
    }
  }

  public static class UpperTransformFunction extends SingleParamStringTransformFunction {
    public static final String FUNCTION_NAME = "upper";

    @Override
    public String getName() {
      return FUNCTION_NAME;
    }

    @Override
    protected void applyStringOperator(String[] values, int length) {
      for (int i = 0; i < length; i++) {
        _results[i] = values[i].toUpperCase();
      }
    }
  }

  public static class LowerTransformFunction extends SingleParamStringTransformFunction {
    public static final String FUNCTION_NAME = "lower";

    @Override
    public String getName() {
      return FUNCTION_NAME;
    }

    @Override
    protected void applyStringOperator(String[] values, int length) {
      for (int i = 0; i < length; i++) {
        _results[i] = values[i].toLowerCase();
      }
    }
  }

  public static class ReverseTransformFunction extends SingleParamStringTransformFunction {
    public static final String FUNCTION_NAME = "reverse";

    @Override
    public String getName() {
      return FUNCTION_NAME;
    }

    @Override
    protected void applyStringOperator(String[] values, int length) {
      for (int i = 0; i < length; i++) {
        _results[i] = new StringBuilder(values[i]).reverse().toString();
      }
    }
  }

//  public static class SubstringTransformFunction extends StringTransformFunction {
//    public static final String FUNCTION_NAME = "substr";
//
//    @Override
//    public String getName() {
//      return FUNCTION_NAME;
//    }
//
//    @Override
//    protected void applyStringOperator(String[] values, int length) {
//      if (_arguments.size() < 1 || _arguments.size() > 2) {
//        throw new IllegalArgumentException("SUBSTR transform function must have either 1 or 2 arguments");
//      }
//
//      Integer beginIndex = Integer.parseInt(_arguments.get(0));
//      Integer endIndex = null;
//      if (_arguments.size() == 2) {
//        endIndex = Integer.parseInt(_arguments.get(1));
//      }
//      for (int i = 0; i < length; i++) {
//        _results[i] = (endIndex == null) ? values[i].substring(beginIndex) : values[i].substring(beginIndex, endIndex);
//      }
//    }
//  }
}
