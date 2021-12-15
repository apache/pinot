package org.apache.pinot.core.query.aggregation.function;

import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;


public class FilterableAggregationFunction implements
                                           AggregationFunction<Object, Comparable> {
  private AggregationFunction<Object, Comparable> _innerAggregationFunction;
  private ExpressionContext _associatedExpressionContext;
  private FilterContext _filterContext;

  public FilterableAggregationFunction(AggregationFunction aggregationFunction,
      ExpressionContext associatedExpressionContext, FilterContext filterContext) {
    _innerAggregationFunction = aggregationFunction;
    _associatedExpressionContext = associatedExpressionContext;
    _filterContext = filterContext;
  }

  @Override
  public AggregationFunctionType getType() {
    return _innerAggregationFunction.getType();
  }

  @Override
  public String getColumnName() {
    return _innerAggregationFunction.getColumnName();
  }

  @Override
  public String getResultColumnName() {
    return _innerAggregationFunction.getResultColumnName();
  }

  @Override
  public List<ExpressionContext> getInputExpressions() {
    return _innerAggregationFunction.getInputExpressions();
  }

  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    return _innerAggregationFunction.createAggregationResultHolder();
  }

  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity) {
    return _innerAggregationFunction.createGroupByResultHolder(initialCapacity, maxCapacity);
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    _innerAggregationFunction.aggregate(length, aggregationResultHolder, blockValSetMap);
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    _innerAggregationFunction.aggregateGroupBySV(length, groupKeyArray, groupByResultHolder,
        blockValSetMap);
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    _innerAggregationFunction.aggregateGroupByMV(length, groupKeysArray, groupByResultHolder,
        blockValSetMap);
  }

  @Override
  public Object extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    return _innerAggregationFunction.extractAggregationResult(aggregationResultHolder);
  }

  @Override
  public Object extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    return _innerAggregationFunction.extractGroupByResult(groupByResultHolder, groupKey);
  }

  @Override
  public Object merge(Object intermediateResult1, Object intermediateResult2) {
    return _innerAggregationFunction.merge(intermediateResult1, intermediateResult2);
  }

  @Override
  public DataSchema.ColumnDataType getIntermediateResultColumnType() {
    return _innerAggregationFunction.getIntermediateResultColumnType();
  }

  @Override
  public DataSchema.ColumnDataType getFinalResultColumnType() {
    return _innerAggregationFunction.getFinalResultColumnType();
  }

  @Override
  public Comparable extractFinalResult(Object o) {
    return _innerAggregationFunction.extractFinalResult(o);
  }

  @Override
  public String toExplainString() {
    return null;
  }

  public ExpressionContext getAssociatedExpressionContext() {
    return _associatedExpressionContext;
  }

  public FilterContext getFilterContext() {
    return _filterContext;
  }
}
