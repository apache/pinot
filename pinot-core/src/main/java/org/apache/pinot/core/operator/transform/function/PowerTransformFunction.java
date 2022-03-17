package org.apache.pinot.core.operator.transform.function;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.segment.spi.datasource.DataSource;


public class PowerTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "power";
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
    if (arguments.size() != 2) {
      throw new IllegalArgumentException("Exactly 2 arguments are required for power transform function");
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
      _result[i] = Math.pow(leftValues[i], rightValues[i]);
    }
    return _result;
  }
}
