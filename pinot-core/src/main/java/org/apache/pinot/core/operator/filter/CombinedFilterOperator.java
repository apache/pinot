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
import org.apache.pinot.core.operator.VisitableOperator;
import org.apache.pinot.core.operator.blocks.CombinedFilterBlock;
import org.apache.pinot.core.operator.blocks.FilterBlock;
import org.apache.pinot.core.operator.docidsets.AndDocIdSet;


public class CombinedFilterOperator extends BaseFilterOperator implements VisitableOperator {
  private static final String OPERATOR_NAME = "CombinedFilterOperator";

  protected Map<ExpressionContext, BaseFilterOperator> _filterOperators;
  protected BaseFilterOperator _mainFilterOperator;
  protected CombinedFilterBlock _resultBlock;

  public CombinedFilterOperator(Map<ExpressionContext, BaseFilterOperator> filterOperators,
      BaseFilterOperator mainFilterOperator) {
    _filterOperators = filterOperators;
    _mainFilterOperator = mainFilterOperator;
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
    if (_resultBlock != null) {
      return _resultBlock;
    }

    FilterBlock mainFilterBlock = _mainFilterOperator.nextBlock();

    Map<ExpressionContext, FilterBlock> filterBlockMap = new HashMap<>();
    Iterator<Map.Entry<ExpressionContext, BaseFilterOperator>> iterator = _filterOperators.entrySet().iterator();

    while (iterator.hasNext()) {
      Map.Entry<ExpressionContext, BaseFilterOperator> entry = iterator.next();
      FilterBlock subFilterBlock = entry.getValue().nextBlock();

      filterBlockMap.put(entry.getKey(),
          new FilterBlock(new AndDocIdSet(Arrays.asList(subFilterBlock.getBlockDocIdSet(),
          mainFilterBlock.getBlockDocIdSet()))));
    }

    _resultBlock = new CombinedFilterBlock(filterBlockMap, mainFilterBlock);

    return _resultBlock;
  }

  @Override
  public <T> void accept(T v) {
    if (v instanceof BaseFilterOperator) {
      assert _mainFilterOperator == null;

      _mainFilterOperator = (BaseFilterOperator) v;
    } else if (v instanceof Map) {
      assert _filterOperators == null;

      _filterOperators = (Map<ExpressionContext, BaseFilterOperator>) v;
    }
  }
}
