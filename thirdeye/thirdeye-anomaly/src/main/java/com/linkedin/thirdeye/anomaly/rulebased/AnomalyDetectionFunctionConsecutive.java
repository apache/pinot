package com.linkedin.thirdeye.anomaly.rulebased;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.linkedin.thirdeye.anomaly.api.FunctionProperties;
import com.linkedin.thirdeye.anomaly.api.external.AnomalyDetectionFunction;
import com.linkedin.thirdeye.anomaly.api.external.AnomalyResult;
import com.linkedin.thirdeye.anomaly.exception.IllegalFunctionException;
import com.linkedin.thirdeye.anomaly.util.TimeGranularityUtils;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeRange;

/**
 *
 */
public class AnomalyDetectionFunctionConsecutive extends AnomalyDetectionFunctionAbstractWrapper {

  private int length;

  public AnomalyDetectionFunctionConsecutive(AnomalyDetectionFunction childFunc, int length)
      throws IllegalFunctionException {
    super(childFunc);
    this.length = length;
  }


  @Override
  public void init(StarTreeConfig starTreeConfig, FunctionProperties functionConfig) throws IllegalFunctionException {
    super.init(starTreeConfig, functionConfig);
  }

  /**
   * Adds length times aggregate granularity time to the child's minimum window time granularity.
   *
   * {@inheritDoc}
   * @see com.linkedin.thirdeye.anomaly.api.external.AnomalyDetectionFunction#getTrainingWindowTimeGranularity()
   */
  @Override
  public TimeGranularity getTrainingWindowTimeGranularity() {
    TimeGranularity aggregateGranularity = childFunc.getAggregationTimeGranularity();
    TimeGranularity childGranularity = childFunc.getTrainingWindowTimeGranularity();
    long newGranularity = TimeGranularityUtils.toMillis(childGranularity)
        + length * TimeGranularityUtils.toMillis(aggregateGranularity);

    int size = (int) aggregateGranularity.getUnit().convert(newGranularity, TimeUnit.MILLISECONDS);
    return new TimeGranularity(size, aggregateGranularity.getUnit());
  }

  /**
   * Returns a list of anomaly results, with anomaly score being the average. Properties are inherited from the last
   * anomaly result in the window.
   *
   * {@inheritDoc}
   * @see com.linkedin.thirdeye.anomaly.api.external.AnomalyDetectionFunction#analyze(com.linkedin.thirdeye.api.DimensionKey, com.linkedin.thirdeye.api.MetricTimeSeries)
   */
  @Override
  public List<AnomalyResult> analyze(DimensionKey dimensionKey, MetricTimeSeries series, TimeRange timeInterval,
      List<AnomalyResult> anomalyHistory) {
    // consecutive requires the nested function to produce some extra results
    long newTimeIntervalStart = timeInterval.getStart() - (
        length * TimeGranularityUtils.toMillis(childFunc.getAggregationTimeGranularity()));
    TimeRange timeIntervalExtended = new TimeRange(newTimeIntervalStart, timeInterval.getEnd());

    List<AnomalyResult> consecutiveResults = new ArrayList<AnomalyResult>(series.getTimeWindowSet().size());
    List<AnomalyResult> intermediateResults = childFunc.analyze(dimensionKey, series, timeIntervalExtended,
        anomalyHistory);
    HashMap<Long, AnomalyResult> mappedResults = new HashMap<>();
    for (AnomalyResult ar : intermediateResults) {
      mappedResults.put(ar.getTimeWindow(), ar);
    }

    long windowGranularity = TimeGranularityUtils.toMillis(childFunc.getAggregationTimeGranularity());
    for (Long timeWindow : mappedResults.keySet()) {
      int anomalyCount = 0;
      double scoreAvg = 0;
      double volumeAvg = 0;

      for (int i = 0; i < length; i++) {
        long currentTimeWindow = timeWindow - (i * windowGranularity);
        if (mappedResults.containsKey(currentTimeWindow)) {
          AnomalyResult currResult = mappedResults.get(currentTimeWindow);
          anomalyCount += (currResult.isAnomaly()) ? 1 : 0;
          scoreAvg += currResult.getAnomalyScore() / length;
          volumeAvg += currResult.getAnomalyVolume() / length;
        } else {
          break;
        }
      }

      // inherit properties from latest in consecutive series
      consecutiveResults.add(new AnomalyResult(anomalyCount == length, timeWindow, scoreAvg, volumeAvg,
          mappedResults.get(timeWindow).getProperties()));
    }
    return consecutiveResults;
  }

  public String toString() {
    return String.format("%s for %d consecutive buckets", childFunc.toString(), length);
  }
}
