package com.linkedin.thirdeye.anomalydetection.performanceEvaluation;

import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import org.joda.time.Interval;


public class IntervalUtils {
  public static List<Interval> mergeIntervals (List<Interval> intervals) {
    if(intervals.size() == 0) {
      return intervals;
    }
    // Sort Intervals
    Collections.sort(intervals, new Comparator<Interval>() {
      @Override
      public int compare(Interval o1, Interval o2) {
        return o1.getStart().compareTo(o2.getStart());
      }
    });

    // Merge intervals
    Stack<Interval> intervalStack = new Stack<>();
    intervalStack.push(intervals.get(0));

    for(int i = 1; i < intervals.size(); i++) {
      Interval top = intervalStack.peek();
      Interval target = intervals.get(i);

      if(top.overlap(target) == null) {
        intervalStack.push(target);
      }
      else if(top.equals(target)) {
        continue;
      }
      else {
        Interval newTop = new Interval(Math.min(top.getStart().getMillis(), target.getStart().getMillis()),
            Math.max(top.getEnd().getMillis(), target.getEnd().getMillis()));
        intervalStack.pop();
        intervalStack.push(newTop);
      }
    }

    return intervalStack;
  }

  public static void mergeIntervals(Map<DimensionMap, List<Interval>> anomalyIntervals) {
    for(DimensionMap dimension : anomalyIntervals.keySet()) {
      anomalyIntervals.put(dimension ,mergeIntervals(anomalyIntervals.get(dimension)));
    }
  }

  public static Map<DimensionMap, List<Interval>> mergedAnomalyResultsToIntervalMap (List<MergedAnomalyResultDTO> mergedAnomalyResultDTOList) {
    Map<DimensionMap, List<Interval>> anomalyIntervals = new HashMap<>();
    for(MergedAnomalyResultDTO mergedAnomaly : mergedAnomalyResultDTOList) {
      if(!anomalyIntervals.containsKey(mergedAnomaly.getDimensions())) {
        anomalyIntervals.put(mergedAnomaly.getDimensions(), new ArrayList<>());
      }
      anomalyIntervals.get(mergedAnomaly.getDimensions()).add(
          new Interval(mergedAnomaly.getStartTime(), mergedAnomaly.getEndTime()));
    }
    return anomalyIntervals;
  }

  public static List<Interval> mergedAnomalyResultsToIntervals (List<MergedAnomalyResultDTO> mergedAnomalyResultDTOList) {
    List<Interval> anomalyIntervals = new ArrayList<>();
    for(MergedAnomalyResultDTO mergedAnomaly : mergedAnomalyResultDTOList) {
      anomalyIntervals.add(new Interval(mergedAnomaly.getStartTime(), mergedAnomaly.getEndTime()));
    }
    return anomalyIntervals;
  }
}
