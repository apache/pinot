package org.apache.pinot.core.operator.transform;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.operator.blocks.CombinedTransformBlock;
import org.apache.pinot.core.operator.blocks.TransformBlock;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;


public class CombinedTransformOperator extends TransformOperator {
  private static final String OPERATOR_NAME = "CombinedTransformOperator";

  private final BaseFilterOperator _mainFilterOperator;
  protected final List<TransformOperator> _transformOperatorList;

  /**
   * Constructor for the class
   */
  public CombinedTransformOperator(List<TransformOperator> transformOperatorList,
      BaseFilterOperator filterOperator, Collection<ExpressionContext> expressions) {
    super(null, transformOperatorList.get(0)._projectionOperator, expressions);

    _transformOperatorList = transformOperatorList;
    _mainFilterOperator = filterOperator;
  }

  @Override
  protected TransformBlock getNextBlock() {
    List<TransformBlock> transformBlockList = new ArrayList<>();

    // Get next block from all underlying transform operators
    for (TransformOperator transformOperator : _transformOperatorList) {
      transformBlockList.add(transformOperator.nextBlock());
    }

    return new CombinedTransformBlock(transformBlockList);
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }
}
