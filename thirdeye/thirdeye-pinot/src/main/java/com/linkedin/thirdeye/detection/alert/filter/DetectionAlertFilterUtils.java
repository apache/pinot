package com.linkedin.thirdeye.detection.alert.filter;

import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


public class DetectionAlertFilterUtils {
  public static List<Long> extractLongs(Collection<Number> numbers) {
    List<Long> output = new ArrayList<>();
    for (Number n : numbers) {
      if (n == null) {
        continue;
      }
      output.add(n.longValue());
    }
    return output;
  }

  public static Long getLastTimeStamp(Collection<MergedAnomalyResultDTO> anomalies, long startTime) {
    long lastTimeStamp = startTime;
    for (MergedAnomalyResultDTO anomaly : anomalies) {
      lastTimeStamp = Math.max(anomaly.getEndTime(), lastTimeStamp);
    }
    return lastTimeStamp;
  }

}
