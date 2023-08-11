package org.apache.pinot.core.query.aggregation.function.funnel;

import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;


/**
 * Interface for segment aggregation result extraction strategy.
 *
 * <p>The implementation should be stateless, and can be shared among multiple segments in multiple threads.
 *
 * @param <A> Aggregation result accumulated across blocks within segment, kept by result holder.
 * @param <I> Intermediate result at segment level (extracted from aforementioned aggregation result).
 */
@ThreadSafe
interface ResultExtractionStrategy<A, I> {

  /**
   * Extracts the intermediate result from the aggregation result holder (aggregation only).
   */
  default I extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    return extractIntermediateResult(aggregationResultHolder.getResult());
  }

  /**
   * Extracts the intermediate result from the group-by result holder for the given group key (aggregation group-by).
   */
  default I extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    return extractIntermediateResult(groupByResultHolder.getResult(groupKey));
  }

  I extractIntermediateResult(A aggregationResult);
}
