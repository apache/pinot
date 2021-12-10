package org.apache.pinot.core.operator.filter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.CombinedFilterBlock;
import org.apache.pinot.core.operator.blocks.FilterBlock;
import org.apache.pinot.core.operator.docidsets.AndDocIdSet;


public class CombinedFilterOperator extends BaseFilterOperator {
  private static final String OPERATOR_NAME = "CombinedFilterOperator";

  protected Map<ExpressionContext, BaseFilterOperator> _filterOperators;
  protected Map<ExpressionContext, FilterBlock> _filterBlockMap;
  protected FilterBlock _mainFilterBlock;
  protected BaseFilterOperator _mainFilterOperator;

  public CombinedFilterOperator(Map<ExpressionContext, BaseFilterOperator> filterOperators,
      BaseFilterOperator mainFilterOperator) {
    _filterOperators = filterOperators;
    _mainFilterOperator = mainFilterOperator;

    _filterBlockMap = new HashMap<>();
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }

  @Override
  public List<Operator> getChildOperators() {
    return new ArrayList<>(_filterOperators.values());
  }

  @Nullable
  @Override
  public String toExplainString() {
    return null;
  }

  @Override
  protected FilterBlock getNextBlock() {
    _mainFilterBlock = _mainFilterOperator.nextBlock();

    Iterator<Map.Entry<ExpressionContext, BaseFilterOperator>> iterator = _filterOperators.entrySet().iterator();

    while (iterator.hasNext()) {
      Map.Entry<ExpressionContext, BaseFilterOperator> entry = iterator.next();
      FilterBlock subFilterBlock = entry.getValue().nextBlock();

      _filterBlockMap.put(entry.getKey(),
          new FilterBlock(new AndDocIdSet(Arrays.asList(subFilterBlock.getBlockDocIdSet(),
          _mainFilterBlock.getBlockDocIdSet()))));
    }

    return new CombinedFilterBlock(_filterBlockMap, _mainFilterBlock);
  }
}
