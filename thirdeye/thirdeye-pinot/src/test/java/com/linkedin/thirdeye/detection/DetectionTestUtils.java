/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.detection;

import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.Collections;
import java.util.Map;
import java.util.Set;


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

  public static MergedAnomalyResultDTO makeAnomaly(Long configId, long start, long end, Map<String, String> dimensions) {
    return DetectionTestUtils.makeAnomaly(configId, start, end, null, null, dimensions);
  }

  public static MergedAnomalyResultDTO makeAnomaly(long start, long end, Set<MergedAnomalyResultDTO> children) {
    MergedAnomalyResultDTO result = makeAnomaly(start, end);
    result.setChildren(children);
    return result;
  }

  public static MergedAnomalyResultDTO makeAnomaly(long start, long end, Map<String, String> dimensions, Set<MergedAnomalyResultDTO> children) {
    MergedAnomalyResultDTO result = makeAnomaly(start, end, dimensions);
    result.setChildren(children);
    return result;
  }

  public static MergedAnomalyResultDTO makeAnomaly(long start, long end, String metricUrn, long currentValue, long baselineValue) {
    MergedAnomalyResultDTO result = makeAnomaly(start, end);
    result.setMetricUrn(metricUrn);
    result.setAvgCurrentVal(currentValue);
    result.setAvgBaselineVal(baselineValue);
    return result;
  }
}
