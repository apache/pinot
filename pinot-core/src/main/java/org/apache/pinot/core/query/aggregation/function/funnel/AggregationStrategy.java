package org.apache.pinot.core.query.aggregation.function.funnel;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.segment.spi.index.reader.Dictionary;


/**
 * Interface for within segment aggregation strategy.
 *
 * <p>The implementation should be stateless, and can be shared among multiple segments in multiple threads. The
 * result for each segment should be stored and passed in via the result holder.
 * There should be no assumptions beyond segment boundaries, different aggregation strategies may be utilized
 * across different segments for a given query.
 *
 * @param <A> Aggregation result accumulated across blocks within segment, kept by result holder.
 */
@ThreadSafe
public abstract class AggregationStrategy<A> {

  protected final int _numSteps;
  private final List<ExpressionContext> _stepExpressions;
  private final List<ExpressionContext> _correlateByExpressions;
  private final ExpressionContext _primaryCorrelationCol;

  public AggregationStrategy(List<ExpressionContext> stepExpressions, List<ExpressionContext> correlateByExpressions) {
    _stepExpressions = stepExpressions;
    _correlateByExpressions = correlateByExpressions;
    _primaryCorrelationCol = _correlateByExpressions.get(0);
    _numSteps = _stepExpressions.size();
  }

  /**
   * Returns an aggregation result for this aggregation strategy to be kept in a result holder (aggregation only).
   */
  abstract A createAggregationResult(Dictionary dictionary);

  public A getAggregationResultGroupBy(Dictionary dictionary, GroupByResultHolder groupByResultHolder, int groupKey) {
    A aggResult = groupByResultHolder.getResult(groupKey);
    if (aggResult == null) {
      aggResult = createAggregationResult(dictionary);
      groupByResultHolder.setValueForKey(groupKey, aggResult);
    }
    return aggResult;
  }

  public A getAggregationResult(Dictionary dictionary, AggregationResultHolder aggregationResultHolder) {
    A aggResult = aggregationResultHolder.getResult();
    if (aggResult == null) {
      aggResult = createAggregationResult(dictionary);
      aggregationResultHolder.setValue(aggResult);
    }
    return aggResult;
  }

  /**
   * Performs aggregation on the given block value sets (aggregation only).
   */
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    final Dictionary dictionary = getDictionary(blockValSetMap);
    final int[] correlationIds = getCorrelationIds(blockValSetMap);
    final int[][] steps = getSteps(blockValSetMap);

    final A aggResult = getAggregationResult(dictionary, aggregationResultHolder);
    for (int i = 0; i < length; i++) {
      for (int n = 0; n < _numSteps; n++) {
        if (steps[n][i] > 0) {
          add(dictionary, aggResult, n, correlationIds[i]);
        }
      }
    }
  }

  /**
   * Performs aggregation on the given group key array and block value sets (aggregation group-by on single-value
   * columns).
   */
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    final Dictionary dictionary = getDictionary(blockValSetMap);
    final int[] correlationIds = getCorrelationIds(blockValSetMap);
    final int[][] steps = getSteps(blockValSetMap);

    for (int i = 0; i < length; i++) {
      for (int n = 0; n < _numSteps; n++) {
        final int groupKey = groupKeyArray[i];
        final A aggResult = getAggregationResultGroupBy(dictionary, groupByResultHolder, groupKey);
        if (steps[n][i] > 0) {
          add(dictionary, aggResult, n, correlationIds[i]);
        }
      }
    }
  }

  /**
   * Performs aggregation on the given group keys array and block value sets (aggregation group-by on multi-value
   * columns).
   */
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    final Dictionary dictionary = getDictionary(blockValSetMap);
    final int[] correlationIds = getCorrelationIds(blockValSetMap);
    final int[][] steps = getSteps(blockValSetMap);

    for (int i = 0; i < length; i++) {
      for (int n = 0; n < _numSteps; n++) {
        for (int groupKey : groupKeysArray[i]) {
          final A aggResult = getAggregationResultGroupBy(dictionary, groupByResultHolder, groupKey);
          if (steps[n][i] > 0) {
            add(dictionary, aggResult, n, correlationIds[i]);
          }
        }
      }
    }
  }

  /**
   * Adds a correlation id to the aggregation counter for a given step in the funnel.
   */
  abstract void add(Dictionary dictionary, A aggResult, int step, int correlationId);

  private Dictionary getDictionary(Map<ExpressionContext, BlockValSet> blockValSetMap) {
    final Dictionary primaryCorrelationDictionary = blockValSetMap.get(_primaryCorrelationCol).getDictionary();
    Preconditions.checkArgument(primaryCorrelationDictionary != null,
        "CORRELATE_BY column in FUNNELCOUNT aggregation function not supported, please use a dictionary encoded "
            + "column.");
    return primaryCorrelationDictionary;
  }

  private int[] getCorrelationIds(Map<ExpressionContext, BlockValSet> blockValSetMap) {
    return blockValSetMap.get(_primaryCorrelationCol).getDictionaryIdsSV();
  }

  private int[][] getSteps(Map<ExpressionContext, BlockValSet> blockValSetMap) {
    final int[][] steps = new int[_numSteps][];
    for (int n = 0; n < _numSteps; n++) {
      final BlockValSet stepBlockValSet = blockValSetMap.get(_stepExpressions.get(n));
      steps[n] = stepBlockValSet.getIntValuesSV();
    }
    return steps;
  }
}
