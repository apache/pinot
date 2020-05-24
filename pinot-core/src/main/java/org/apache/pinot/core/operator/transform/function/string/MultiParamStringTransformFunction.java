package org.apache.pinot.core.operator.transform.function.string;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.operator.transform.function.BaseTransformFunction;
import org.apache.pinot.core.operator.transform.function.IdentifierTransformFunction;
import org.apache.pinot.core.operator.transform.function.LiteralTransformFunction;
import org.apache.pinot.core.operator.transform.function.TransformFunction;
import org.apache.pinot.core.plan.DocIdSetPlanNode;


public abstract class MultiParamStringTransformFunction extends BaseTransformFunction {
  private List<TransformFunction> _transformFunctions = new ArrayList<>();
  protected List<String> _arguments = new ArrayList<>();
  protected List<TransformFunction> _input = new ArrayList<>();

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
    for (TransformFunction argument : arguments) {
      if (argument instanceof LiteralTransformFunction) {
        _arguments.add(((LiteralTransformFunction) argument).getLiteral());
      } else if (argument instanceof IdentifierTransformFunction) {
        if (!argument.getResultMetadata().isSingleValue()) {
          throw new IllegalArgumentException("All the arguments of MULT transform function must be single-valued");
        }
        _input.add(argument);
      } else {
        if (!argument.getResultMetadata().isSingleValue()) {
          throw new IllegalArgumentException("All the arguments of MULT transform function must be single-valued");
        }
        _transformFunctions.add(argument);
      }
    }
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return null;
  }

  @Override
  public String[] transformToStringValuesSV(ProjectionBlock projectionBlock) {
    String[] results = new String[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    transformToStringValuesWithArgument(projectionBlock, results);
    return results;
  }

  @Override
  public int[] transformToIntValuesSV(ProjectionBlock projectionBlock) {
    Integer[] results = new Integer[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    transformToStringValuesWithArgument(projectionBlock, results);
    return Arrays.stream(results).mapToInt(Integer::intValue).toArray();
  }

  private void transformToStringValuesWithArgument(ProjectionBlock projectionBlock, Object[] results) {
    String[][] values = new String[_input.size()][];

    int i = 0;
    for (TransformFunction inputTransformFunction : _input) {
      values[i] = inputTransformFunction.transformToStringValuesSV(projectionBlock);
      i++;
    }

    String[][] transformedArguments = new String[_transformFunctions.size()][];
    int j = 0;
    for (TransformFunction transformFunction : _transformFunctions) {
      transformedArguments[j] = transformFunction.transformToStringValuesSV(projectionBlock);
      j++;
    }

    applyStringOperator(values, transformedArguments, results, projectionBlock.getNumDocs());
  }
  abstract void applyStringOperator(String[][] values, String[][] transformedArguments, Object[] results, int length);

  public static class SubstringTransformFunction extends MultiParamStringTransformFunction {
    public static final String FUNCTION_NAME = "substr";

    @Override
    public String getName() {
      return FUNCTION_NAME;
    }

    @Override
    protected void applyStringOperator(String[][] values, String[][] transformedArguments, Object[] result, int length) {
      if (_arguments.size() < 1 || _arguments.size() > 2) {
        throw new IllegalArgumentException("SUBSTR transform function must have either 1 or 2 arguments");
      }

      if (values.length > 1) {
        throw new IllegalArgumentException("SUBSTR transform function cannot have more than 1 input");
      }

      Integer beginIndex = Integer.parseInt(_arguments.get(0));
      Integer endIndex = null;
      if (_arguments.size() == 2) {
        endIndex = Integer.parseInt(_arguments.get(1));
      }

      String[] input = values[0];
      for (int i = 0; i < length; i++) {
        result[i] = (endIndex == null) ? input[i].substring(beginIndex) : input[i].substring(beginIndex, endIndex);
      }
    }
  }

  public static class StringPositionTransformFunction extends MultiParamStringTransformFunction {
    public static final String FUNCTION_NAME = "strrpos";

    @Override
    public String getName() {
      return FUNCTION_NAME;
    }

    @Override
    protected void applyStringOperator(String[][] values, String[][] transformedArguments, Object[] result, int length) {
      int totalArguments = transformedArguments.length + _arguments.size();
      if (totalArguments != 1) {
        throw new IllegalArgumentException("STRPOS transform function must have exactly 1 argument");
      }

      if (values.length > 1) {
        throw new IllegalArgumentException("STRPOS transform function must have exactly 1 input");
      }

      if(transformedArguments.length > 0) {

        String[] input = values[0];
        String[] argument = transformedArguments[0];


        for (int i = 0; i < length; i++) {
          result[i] = input[i].indexOf(argument[i]);
        }
      }else if(_arguments.size() > 0){
        String[] input = values[0];
        String argument = _arguments.get(0);

        for (int i = 0; i < length; i++) {
          result[i] = input[i].indexOf(argument);
        }
      }
    }
  }
}
