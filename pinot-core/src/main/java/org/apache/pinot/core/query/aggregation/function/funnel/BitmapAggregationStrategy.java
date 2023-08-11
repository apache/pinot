package org.apache.pinot.core.query.aggregation.function.funnel;

import java.util.List;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.segment.spi.index.reader.Dictionary;


/**
 * Aggregation strategy leveraging roaring bitmap algebra (unions/intersections).
 */
class BitmapAggregationStrategy extends AggregationStrategy<DictIdsWrapper> {
  public BitmapAggregationStrategy(List<ExpressionContext> stepExpressions,
      List<ExpressionContext> correlateByExpressions) {
    super(stepExpressions, correlateByExpressions);
  }

  @Override
  public DictIdsWrapper createAggregationResult(Dictionary dictionary) {
    return new DictIdsWrapper(_numSteps, dictionary);
  }

  @Override
  protected void add(Dictionary dictionary, DictIdsWrapper dictIdsWrapper, int step, int correlationId) {
    dictIdsWrapper._stepsBitmaps[step].add(correlationId);
  }
}
