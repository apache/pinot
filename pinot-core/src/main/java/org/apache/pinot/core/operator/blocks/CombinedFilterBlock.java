package org.apache.pinot.core.operator.blocks;

import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;


public class CombinedFilterBlock extends FilterBlock {
  private final Map<ExpressionContext,  FilterBlock> _filterBlockMap;
  private final FilterBlock _mainFilterBlock;

  public CombinedFilterBlock(Map<ExpressionContext, FilterBlock> filterBlockMap,
      FilterBlock mainFilterBlock) {
    super(mainFilterBlock.getBlockDocIdSet());

    _filterBlockMap = filterBlockMap;
    _mainFilterBlock = mainFilterBlock;
  }

  public FilterBlock getFilterBlock(ExpressionContext expressionContext) {
    if (expressionContext == null) {
      throw new IllegalStateException("ExpressionContext is null");
    }

    return _filterBlockMap.get(expressionContext);
  }

  public FilterBlock getMainFilterBlock() {
    return _mainFilterBlock;
  }
}
