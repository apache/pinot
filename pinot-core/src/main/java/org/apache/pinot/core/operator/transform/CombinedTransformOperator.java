package org.apache.pinot.core.operator.transform;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.blocks.CombinedTransformBlock;
import org.apache.pinot.core.operator.blocks.FilterBlock;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.blocks.TransformBlock;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.operator.query.AggregationOperator;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;


public class CombinedTransformOperator extends TransformOperator {
  private static final String OPERATOR_NAME = "CombinedTransformOperator";

  private final BaseFilterOperator _mainFilterOperator;
  protected final List<TransformOperator> _transformOperatorList;
  protected final TransformOperator _nonFilteredAggTransformOperator;

  /**
   * Constructor for the class
   */
  public CombinedTransformOperator(List<TransformOperator> transformOperatorList,
      TransformOperator nonFilteredAggTransformOperator,
      BaseFilterOperator filterOperator, Collection<ExpressionContext> expressions) {
    super(null, transformOperatorList.get(0)._projectionOperator, expressions);

    _nonFilteredAggTransformOperator = nonFilteredAggTransformOperator;
    _transformOperatorList = transformOperatorList;
    _mainFilterOperator = filterOperator;
  }

  @Override
  protected TransformBlock getNextBlock() {
    List<TransformBlock> transformBlockList = new ArrayList<>();
    FilterBlock filterBlock = _mainFilterOperator.nextBlock();
    boolean hasTransformBlock = false;

    // Get next block from all underlying transform operators
    for (TransformOperator transformOperator : _transformOperatorList) {
      transformOperator.accept(filterBlock);

      TransformBlock transformBlock = transformOperator.getNextBlock();

      if (transformBlock != null) {
        hasTransformBlock = true;
      }

      transformBlockList.add(transformBlock);
    }


    TransformBlock nonFilterAggTransformBlock = _nonFilteredAggTransformOperator != null ?
        _nonFilteredAggTransformOperator.getNextBlock() : null;

    if (!hasTransformBlock && nonFilterAggTransformBlock == null) {
      return null;
    }

    return new CombinedTransformBlock(transformBlockList,
        nonFilterAggTransformBlock);
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }
}
