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

package org.apache.pinot.thirdeye.anomaly.detection;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import org.apache.pinot.thirdeye.common.dimension.DimensionMap;
import org.apache.pinot.thirdeye.common.metric.MetricTimeSeries;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.detector.metric.transfer.ScalingFactor;
import java.util.Collections;
import java.util.List;
import java.util.Map;


public class AnomalyDetectionInputContext {
  private Map<DimensionMap, MetricTimeSeries> dimensionMapMetricTimeSeriesMap = Collections.emptyMap();
  private MetricTimeSeries globalMetric;
  private ListMultimap<DimensionMap, MergedAnomalyResultDTO> knownMergedAnomalies = ArrayListMultimap.create();;
  private List<ScalingFactor> scalingFactors = Collections.emptyList();

  public Map<DimensionMap, MetricTimeSeries> getDimensionMapMetricTimeSeriesMap() {
    return dimensionMapMetricTimeSeriesMap;
  }

  public void setDimensionMapMetricTimeSeriesMap(
      Map<DimensionMap, MetricTimeSeries> dimensionMapMetricTimeSeriesMap) {
    this.dimensionMapMetricTimeSeriesMap = dimensionMapMetricTimeSeriesMap;
  }

  public ListMultimap<DimensionMap, MergedAnomalyResultDTO> getKnownMergedAnomalies() {
    return knownMergedAnomalies;
  }

  public void setKnownMergedAnomalies(ListMultimap<DimensionMap, MergedAnomalyResultDTO> knownMergedAnomalies) {
    this.knownMergedAnomalies = knownMergedAnomalies;
  }

  public List<ScalingFactor> getScalingFactors() {
    return scalingFactors;
  }

  public void setScalingFactors(List<ScalingFactor> scalingFactors) {
    this.scalingFactors = scalingFactors;
  }

  public MetricTimeSeries getGlobalMetric() {
    return globalMetric;
  }

  public void setGlobalMetric(MetricTimeSeries globalMetric) {
    this.globalMetric = globalMetric;
  }
}
