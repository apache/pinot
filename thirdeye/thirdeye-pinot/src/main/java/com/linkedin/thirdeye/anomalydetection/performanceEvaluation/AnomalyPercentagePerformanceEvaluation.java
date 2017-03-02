package com.linkedin.thirdeye.anomalydetection.performanceEvaluation;

import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.List;
import org.joda.time.Interval;


/**
 * Created by ychung on 2/28/17.
 */
public class AnomalyPercentagePerformanceEvaluation {
  public static Comparable evaluate(Interval windowInterval, List<MergedAnomalyResultDTO> detectedResults){
    long anomalyLength = 0;
    long totalLength = windowInterval.toDurationMillis();
    for(MergedAnomalyResultDTO mergedAnomaly : detectedResults){
      anomalyLength += mergedAnomaly.getEndTime() - mergedAnomaly.getStartTime();
    }
    double ratio = (double) anomalyLength / totalLength;
    return ratio;
  }
}
