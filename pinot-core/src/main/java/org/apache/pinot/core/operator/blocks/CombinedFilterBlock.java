package org.apache.pinot.core.operator.blocks;

import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;


/**
 * Contains a mapping between expressions and filter blocks
 */
public class CombinedFilterBlock extends FilterBlock {
  private final Map<ExpressionContext,  FilterBlock> _filterBlockMap;

  public CombinedFilterBlock(Map<ExpressionContext, FilterBlock> filterBlockMap,
      FilterBlock mainFilterBlock) {
    // Initialize with main predicate's filter block to allow main predicate
    // chain to operate normally
    super(mainFilterBlock.getBlockDocIdSet());

    _filterBlockMap = filterBlockMap;
  }

  public FilterBlock getFilterBlock(ExpressionContext expressionContext) {
    if (expressionContext == null) {
      throw new IllegalStateException("ExpressionContext is null");
    }

    return _filterBlockMap.get(expressionContext);
  }
}
