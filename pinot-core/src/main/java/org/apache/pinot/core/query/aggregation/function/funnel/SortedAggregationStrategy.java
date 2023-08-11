package org.apache.pinot.core.query.aggregation.function.funnel;

import java.util.List;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.segment.spi.index.reader.Dictionary;


/**
 * Aggregation strategy for segments partitioned and sorted by the main correlation column.
 */
class SortedAggregationStrategy extends AggregationStrategy<SortedAggregationResult> {
  public SortedAggregationStrategy(List<ExpressionContext> stepExpressions,
      List<ExpressionContext> correlateByExpressions) {
    super(stepExpressions, correlateByExpressions);
  }

  @Override
  public SortedAggregationResult createAggregationResult(Dictionary dictionary) {
    return new SortedAggregationResult(_numSteps);
  }

  @Override
  void add(Dictionary dictionary, SortedAggregationResult aggResult, int step, int correlationId) {
    aggResult.add(step, correlationId);
  }
}
