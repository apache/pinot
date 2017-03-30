package com.linkedin.thirdeye.anomalydetection.performanceEvaluation;

import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.joda.time.Interval;


public abstract class BasePerformanceEvaluate implements PerformanceEvaluate {
  /**
   * convert merge anomalies to a dimension-intervals map
   * @param mergedAnomalyResultDTOList
   * @return
   */
  public Map<DimensionMap, List<Interval>> mergedAnomalyResultsToIntervalMap (List<MergedAnomalyResultDTO> mergedAnomalyResultDTOList) {
    Map<DimensionMap, List<Interval>> anomalyIntervals = new HashMap<>();
    for(MergedAnomalyResultDTO mergedAnomaly : mergedAnomalyResultDTOList) {
      if(!anomalyIntervals.containsKey(mergedAnomaly.getDimensions())) {
        anomalyIntervals.put(mergedAnomaly.getDimensions(), new ArrayList<Interval>());
      }
      anomalyIntervals.get(mergedAnomaly.getDimensions()).add(
          new Interval(mergedAnomaly.getStartTime(), mergedAnomaly.getEndTime()));
    }
    return anomalyIntervals;
  }

  /**
   * convert merge anomalies to interval list without considering the dimension
   * @param mergedAnomalyResultDTOList
   * @return
   */
  public List<Interval> mergedAnomalyResultsToIntervals (List<MergedAnomalyResultDTO> mergedAnomalyResultDTOList) {
    List<Interval> anomalyIntervals = new ArrayList<>();
    for(MergedAnomalyResultDTO mergedAnomaly : mergedAnomalyResultDTOList) {
      anomalyIntervals.add(new Interval(mergedAnomaly.getStartTime(), mergedAnomaly.getEndTime()));
    }
    return anomalyIntervals;
  }
}
