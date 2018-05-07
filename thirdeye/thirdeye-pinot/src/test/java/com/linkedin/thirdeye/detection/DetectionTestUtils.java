package com.linkedin.thirdeye.detection;

import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.Map;


public class DetectionTestUtils {
  public static MergedAnomalyResultDTO makeAnomaly(Long configId, long start, long end, String metric, String dataset, Map<String, String> dimensions) {
    MergedAnomalyResultDTO anomaly = new MergedAnomalyResultDTO();
    anomaly.setDetectionConfigId(configId);
    anomaly.setStartTime(start);
    anomaly.setEndTime(end);
    anomaly.setMetric(metric);
    anomaly.setCollection(dataset);

    DimensionMap dimMap = new DimensionMap();
    dimMap.putAll(dimensions);
    anomaly.setDimensions(dimMap);

    return anomaly;
  }
}
