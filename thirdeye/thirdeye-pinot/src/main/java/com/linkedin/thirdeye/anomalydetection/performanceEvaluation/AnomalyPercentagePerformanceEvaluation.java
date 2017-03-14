package com.linkedin.thirdeye.anomalydetection.performanceEvaluation;

import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.List;
import org.joda.time.Interval;


public class AnomalyPercentagePerformanceEvaluation implements PerformanceEvaluate{
  private Interval windowInterval;
  private List<MergedAnomalyResultDTO> detectedResults;

  public AnomalyPercentagePerformanceEvaluation(Interval windowInterval, List<MergedAnomalyResultDTO> detectedResults) {
    this.windowInterval = windowInterval;
    this.detectedResults = detectedResults;
  }

  @Override
  public double evaluate(){
    long anomalyLength = 0;
    long totalLength = windowInterval.toDurationMillis();

    List<Interval> anomalyIntervals = IntervalUtils.mergedAnomalyResultsToIntervals(detectedResults);
    anomalyIntervals = IntervalUtils.mergeIntervals(anomalyIntervals);

    for(Interval interval : anomalyIntervals) {
      anomalyLength += interval.toDurationMillis();
    }

    double ratio = (double) anomalyLength / totalLength;
    return ratio;
  }
}
