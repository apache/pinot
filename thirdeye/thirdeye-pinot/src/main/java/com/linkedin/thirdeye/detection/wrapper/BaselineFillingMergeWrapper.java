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

package com.linkedin.thirdeye.detection.wrapper;

import com.google.common.base.Preconditions;
import com.linkedin.thirdeye.dataframe.Series;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.DetectionUtils;
import com.linkedin.thirdeye.detection.DefaultInputDataFetcher;
import com.linkedin.thirdeye.detection.InputDataFetcher;
import com.linkedin.thirdeye.detection.algorithm.MergeWrapper;
import com.linkedin.thirdeye.detection.components.RuleBaselineProvider;
import com.linkedin.thirdeye.detection.spec.RuleBaselineProviderSpec;
import com.linkedin.thirdeye.detection.spi.components.BaselineProvider;
import com.linkedin.thirdeye.rootcause.impl.MetricEntity;
import com.linkedin.thirdeye.rootcause.timeseries.BaselineAggregateType;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Baseline filling merger. This merger's merging behavior is the same as MergeWrapper. But add the capability
 * of filling baseline & current values.
 */
public class BaselineFillingMergeWrapper extends MergeWrapper {
  private static final Logger LOG = LoggerFactory.getLogger(MergeWrapper.class);

  private static final String PROP_BASELINE_PROVIDER = "baselineValueProvider";
  private static final String PROP_CURRENT_PROVIDER = "currentValueProvider";
  private static final String PROP_METRIC_URN = "metricUrn";

  private BaselineProvider baselineValueProvider; // optionally configure a baseline value loader
  private BaselineProvider currentValueProvider;
  private Series.DoubleFunction aggregationFunction;

  public BaselineFillingMergeWrapper(DataProvider provider, DetectionConfigDTO config, long startTime, long endTime)
  {
    super(provider, config, startTime, endTime);

    if (config.getProperties().containsKey(PROP_BASELINE_PROVIDER)) {
      String referenceKey = DetectionUtils.getComponentName(MapUtils.getString(config.getProperties(), PROP_BASELINE_PROVIDER));
      Preconditions.checkArgument(this.config.getComponents().containsKey(referenceKey));
      this.baselineValueProvider = (BaselineProvider) this.config.getComponents().get(referenceKey);
    }
    if (config.getProperties().containsKey(PROP_CURRENT_PROVIDER)) {
      String detectorReferenceKey = DetectionUtils.getComponentName(MapUtils.getString(config.getProperties(), currentValueProvider));
      Preconditions.checkArgument(this.config.getComponents().containsKey(detectorReferenceKey));
      this.currentValueProvider = (BaselineProvider) this.config.getComponents().get(detectorReferenceKey);
    } else {
      // default current provider
      this.currentValueProvider = new RuleBaselineProvider();
      RuleBaselineProviderSpec spec = new RuleBaselineProviderSpec();
      spec.setOffset("current");
      InputDataFetcher dataFetcher = new DefaultInputDataFetcher(this.provider, this.config.getId());
      this.currentValueProvider.init(spec, dataFetcher);
    }
    String nestedUrn = MapUtils.getString(config.getProperties(), PROP_METRIC_URN);
    if (nestedUrn != null){
      for (Map<String, Object> properties : this.nestedProperties){
        properties.put(PROP_METRIC_URN, nestedUrn);
      }
    }

    this.aggregationFunction = BaselineAggregateType.valueOf(MapUtils.getString(config.getProperties(), "metricFunction", "MEAN")).getFunction();
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
    for (MergedAnomalyResultDTO anomaly : mergedAnomalies) {
      try {
        String metricUrn = anomaly.getMetricUrn();
        final MetricSlice slice = MetricSlice.from(MetricEntity.fromURN(metricUrn).getId(), anomaly.getStartTime(), anomaly.getEndTime(),
            MetricEntity.fromURN(metricUrn).getFilters());
        anomaly.setAvgCurrentVal(this.currentValueProvider.computePredictedAggregates(slice, aggregationFunction));
        if (this.baselineValueProvider != null) {
          anomaly.setAvgBaselineVal(this.baselineValueProvider.computePredictedAggregates(slice, aggregationFunction));
        }
      } catch (Exception e) {
        // ignore
        LOG.warn("cannot get metric slice for anomaly {}", anomaly, e);
      }
    }
    return mergedAnomalies;
  }

}
