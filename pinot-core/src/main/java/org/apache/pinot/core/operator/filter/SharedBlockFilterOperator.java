package org.apache.pinot.core.operator.filter;

import java.util.List;
import org.apache.pinot.core.operator.blocks.FilterBlock;


public class SharedBlockFilterOperator extends BaseFilterOperator {
  private static final String OPERATOR_NAME = "SharedBlockFilterOperator";

  private final BlockDrivenAndFilterOperator _baseFilterOperator;
  private final List<FilterBlock> _singleSharedTransformBlock;

  public SharedBlockFilterOperator(BlockDrivenAndFilterOperator baseFilterOperator,
      List<FilterBlock> singleSharedTransformBlock) {
    _baseFilterOperator = baseFilterOperator;
    _singleSharedTransformBlock = singleSharedTransformBlock;
  }

  @Override
  protected FilterBlock getNextBlock() {
    return _baseFilterOperator.getNextBlock(_singleSharedTransformBlock.get(0));
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }
}
