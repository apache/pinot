package org.apache.pinot.core.operator.transform.function;

import com.google.common.base.Preconditions;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.segment.spi.datasource.DataSource;


//TODO: The function should ideally be named 'round'
// but it is not possible because of existing DateTimeFunction with same name.
public class RoundDecimalTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "roundDecimal";
  private double[] _result;
  private TransformFunction _leftTransformFunction;
  private TransformFunction _rightTransformFunction;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
    int numArguments = arguments.size();
    // Check that there are more than 2 arguments or no arguments
    if (numArguments < 1 || numArguments > 2) {
      throw new IllegalArgumentException(
          "roundDecimal transform function supports either 1 or 2 arguments. Num arguments provided: " + numArguments);
    }

    _leftTransformFunction = arguments.get(0);
    if (numArguments > 1) {
      _rightTransformFunction = arguments.get(1);
    } else {
      _rightTransformFunction = new LiteralTransformFunction("0");
    }

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
    int[] rightValues = _rightTransformFunction.transformToIntValuesSV(projectionBlock);
    for (int i = 0; i < length; i++) {
      _result[i] = BigDecimal.valueOf(leftValues[i]).setScale(rightValues[i], RoundingMode.HALF_UP).doubleValue();
    }

    return _result;
  }
}
