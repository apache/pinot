package org.apache.pinot.core.operator.transform;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.operator.blocks.CombinedTransformBlock;
import org.apache.pinot.core.operator.blocks.TransformBlock;


public class CombinedTransformOperator extends TransformOperator {
  private static final String OPERATOR_NAME = "CombinedTransformOperator";

  protected final List<TransformOperator> _transformOperatorList;
  protected final TransformOperator _nonFilteredAggTransformOperator;

  /**
   * Constructor for the class
   */
  public CombinedTransformOperator(List<TransformOperator> transformOperatorList,
      TransformOperator nonFilteredAggTransformOperator,
      Collection<ExpressionContext> expressions) {
    super(null, transformOperatorList.get(0)._projectionOperator, expressions);

    _nonFilteredAggTransformOperator = nonFilteredAggTransformOperator;
    _transformOperatorList = transformOperatorList;
  }

  @Override
  protected TransformBlock getNextBlock() {
    List<TransformBlock> transformBlockList = new ArrayList<>();
    boolean hasTransformBlock = false;
    TransformBlock nonFilteredAggTransformBlock = _nonFilteredAggTransformOperator.getNextBlock();

    // Get next block from all underlying transform operators
    for (TransformOperator transformOperator : _transformOperatorList) {

      if (nonFilteredAggTransformBlock != null) {
        transformOperator.accept(nonFilteredAggTransformBlock);
      }

      TransformBlock transformBlock = transformOperator.getNextBlock();

      if (transformBlock != null) {
        hasTransformBlock = true;
      }

      transformBlockList.add(transformBlock);
    }


    if (!hasTransformBlock && nonFilteredAggTransformBlock == null) {
      return null;
    }

    return new CombinedTransformBlock(transformBlockList,
        nonFilteredAggTransformBlock);
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }
}
