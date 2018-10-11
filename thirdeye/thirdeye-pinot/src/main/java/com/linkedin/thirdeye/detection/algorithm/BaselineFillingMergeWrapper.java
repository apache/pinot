/*
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

package com.linkedin.thirdeye.detection.algorithm;

import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.baseline.BaselineProvider;
import com.linkedin.thirdeye.detection.baseline.BaselineProviderLoader;
import com.linkedin.thirdeye.detection.baseline.RuleBaselineProvider;
import com.linkedin.thirdeye.rootcause.impl.MetricEntity;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BaselineFillingMergeWrapper extends MergeWrapper {
  private static final Logger LOG = LoggerFactory.getLogger(MergeWrapper.class);

  private static final String PROP_BASELINE_PROVIDER = "baselineValueProvider";
  private static final String PROP_CURRENT_PROVIDER = "currentValueProvider";
  private static final String PROP_METRIC_URN = "metricUrn";

  private BaselineProvider baselineValueProvider; // optionally configure a baseline value loader
  private BaselineProvider currentValueProvider;

  public BaselineFillingMergeWrapper(DataProvider provider, DetectionConfigDTO config, long startTime, long endTime)
      throws Exception {
    super(provider, config, startTime, endTime);
    if (config.getProperties().containsKey(PROP_BASELINE_PROVIDER)) {
      this.baselineValueProvider = BaselineProviderLoader.from(
          MapUtils.getMap(config.getProperties(), PROP_BASELINE_PROVIDER));
    }
    if (config.getProperties().containsKey(PROP_CURRENT_PROVIDER)) {
      this.currentValueProvider = BaselineProviderLoader.from(MapUtils.getMap(config.getProperties(), PROP_CURRENT_PROVIDER));
    } else {
      // default current provider
      this.currentValueProvider = new RuleBaselineProvider();
      this.currentValueProvider.init(Collections.<String, Object>singletonMap("offset", "current"));
    }
    if (config.getProperties().containsKey(PROP_METRIC_URN)){
      for (Map<String, Object> properties : this.nestedProperties){
        properties.put(PROP_METRIC_URN, MapUtils.getString(config.getProperties(), PROP_METRIC_URN));
      }
    }
  }

  @Override
  protected List<MergedAnomalyResultDTO> merge(Collection<MergedAnomalyResultDTO> anomalies) {
    return this.fillCurrentAndBaselineValue(super.merge(anomalies));
  }

  /**
   * Fill in current and baseline value for the anomalies
   * @param mergedAnomalies anomalies
   * @return anomalies with current and baseline value filled
   */
  List<MergedAnomalyResultDTO> fillCurrentAndBaselineValue(List<MergedAnomalyResultDTO> mergedAnomalies) {
    Map<MetricSlice, MergedAnomalyResultDTO> metricSlicesToAnomaly = new HashMap<>();

    for (MergedAnomalyResultDTO anomaly : mergedAnomalies) {
      try {
        String metricUrn = anomaly.getMetricUrn();
        final MetricSlice slice = MetricSlice.from(MetricEntity.fromURN(metricUrn).getId(), anomaly.getStartTime(), anomaly.getEndTime(),
            MetricEntity.fromURN(metricUrn).getFilters());
        metricSlicesToAnomaly.put(slice, anomaly);
      } catch (Exception e) {
        // ignore
        LOG.warn("cannot get metric slice for anomaly {}", anomaly);
      }
    }

    Map<MetricSlice, Double> currentValues = this.currentValueProvider.computeBaselineAggregates(metricSlicesToAnomaly.keySet(), this.provider);
    Map<MetricSlice, Double> baselineValues = new HashMap<>();
    if (this.baselineValueProvider != null) {
      baselineValues = this.baselineValueProvider.computeBaselineAggregates(metricSlicesToAnomaly.keySet(), this.provider);
    }
    for (Map.Entry<MetricSlice, MergedAnomalyResultDTO> entry : metricSlicesToAnomaly.entrySet()) {
      MergedAnomalyResultDTO anomaly = entry.getValue();
      MetricSlice slice = entry.getKey();
      if (currentValues.containsKey(slice)){
        anomaly.setAvgCurrentVal(currentValues.get(slice));
      }
      if (baselineValues.containsKey(slice)){
        anomaly.setAvgBaselineVal(baselineValues.get(slice));
      }
    }
    return mergedAnomalies;
  }

}
