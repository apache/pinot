/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pinot.thirdeye.detection;

import java.util.HashMap;
import org.apache.pinot.thirdeye.common.dimension.DimensionMap;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.Collections;
import java.util.Map;
import java.util.Set;


public class DetectionTestUtils {
  private static final Long PROP_ID_VALUE = 1000L;

  public static MergedAnomalyResultDTO makeAnomaly(Long configId, Long legacyFunctionId, long start, long end, String metric, String dataset, Map<String, String> dimensions) {
    MergedAnomalyResultDTO anomaly = new MergedAnomalyResultDTO();
    anomaly.setDetectionConfigId(configId);
    anomaly.setStartTime(start);
    anomaly.setEndTime(end);
    anomaly.setMetric(metric);
    anomaly.setCollection(dataset);
    anomaly.setFunctionId(legacyFunctionId);

    DimensionMap dimMap = new DimensionMap();
    dimMap.putAll(dimensions);
    anomaly.setDimensions(dimMap);

    return anomaly;
  }

  public static MergedAnomalyResultDTO makeAnomaly(Long configId, long start, long end, String metric, String dataset, Map<String, String> dimensions) {
    return DetectionTestUtils.makeAnomaly(configId, null, start, end, metric, dataset, dimensions);
  }

  public static MergedAnomalyResultDTO setAnomalyId(MergedAnomalyResultDTO anomaly, long id) {
    anomaly.setId(id);
    return anomaly;
  }

  public static MergedAnomalyResultDTO makeAnomaly(long start, long end) {
    return DetectionTestUtils.makeAnomaly(PROP_ID_VALUE, start, end, null, null, Collections.<String, String>emptyMap());
  }

  public static MergedAnomalyResultDTO makeAnomaly(Long configId, Long legacyFuncId, long start, long end) {
    return DetectionTestUtils.makeAnomaly(configId, legacyFuncId, start, end, null, null, Collections.<String, String>emptyMap());
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

  public static MergedAnomalyResultDTO makeAnomaly(long start, long end, long configId, String metricUrn,
      double currentVal) {
    MergedAnomalyResultDTO anomaly = makeAnomaly(configId, start, end, new HashMap<>());
    anomaly.setMetricUrn(metricUrn);
    anomaly.setAvgCurrentVal(currentVal);
    return anomaly;
  }
}
