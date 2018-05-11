package com.linkedin.thirdeye.detection;

import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.Collections;
import java.util.Map;


public class DetectionTestUtils {
  private static final Long PROP_ID_VALUE = 1000L;

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

  public static MergedAnomalyResultDTO makeAnomaly(long start, long end) {
    return DetectionTestUtils.makeAnomaly(PROP_ID_VALUE, start, end, null, null, Collections.<String, String>emptyMap());
  }

  public static MergedAnomalyResultDTO makeAnomaly(Long configId, long start, long end) {
    return DetectionTestUtils.makeAnomaly(configId, start, end, null, null, Collections.<String, String>emptyMap());
  }

  public static MergedAnomalyResultDTO makeAnomaly(long start, long end, Map<String, String> dimensions) {
    return DetectionTestUtils.makeAnomaly(PROP_ID_VALUE, start, end, null, null, dimensions);
  }

}
