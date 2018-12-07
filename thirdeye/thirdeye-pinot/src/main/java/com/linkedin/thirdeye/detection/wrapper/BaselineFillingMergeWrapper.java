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
import com.google.common.collect.Collections2;
import com.linkedin.thirdeye.dataframe.DoubleSeries;
import com.linkedin.thirdeye.dataframe.Series;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.DetectionUtils;
import com.linkedin.thirdeye.detection.DefaultInputDataFetcher;
import com.linkedin.thirdeye.detection.InputDataFetcher;
import com.linkedin.thirdeye.detection.algorithm.MergeWrapper;
import com.linkedin.thirdeye.detection.components.RuleBaselineProvider;
import com.linkedin.thirdeye.detection.spec.RuleBaselineProviderSpec;
import com.linkedin.thirdeye.detection.spi.components.BaselineProvider;
import com.linkedin.thirdeye.detection.spi.model.AnomalySlice;
import com.linkedin.thirdeye.rootcause.impl.MetricEntity;
import com.linkedin.thirdeye.rootcause.timeseries.BaselineAggregateType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Baseline filling merger. This merger's merging behavior is the same as MergeWrapper. But add the capability
 * of filling baseline & current values and inject detector, metric urn. Each detector has a separate baseline filling merge wrapper
 */
public class BaselineFillingMergeWrapper extends MergeWrapper {
  private static final Logger LOG = LoggerFactory.getLogger(BaselineFillingMergeWrapper.class);

  private static final String PROP_BASELINE_PROVIDER = "baselineValueProvider";
  private static final String PROP_CURRENT_PROVIDER = "currentValueProvider";
  private static final String PROP_METRIC_URN = "metricUrn";
  private static final String PROP_BASELINE_PROVIDER_COMPONENT_NAME = "baselineProviderComponentName";
  private static final String PROP_DETECTOR = "detector";
  private static final String PROP_DETECTOR_COMPONENT_NAME = "detectorComponentName";

  private BaselineProvider baselineValueProvider; // optionally configure a baseline value loader
  private BaselineProvider currentValueProvider;
  private String baselineProviderComponentKey;
  private String detectorComponentName;
  private String metricUrn;

  public BaselineFillingMergeWrapper(DataProvider provider, DetectionConfigDTO config, long startTime, long endTime)
  {
    super(provider, config, startTime, endTime);

    if (config.getProperties().containsKey(PROP_BASELINE_PROVIDER)) {
      this.baselineProviderComponentKey = DetectionUtils.getComponentName(MapUtils.getString(config.getProperties(), PROP_BASELINE_PROVIDER));
      Preconditions.checkArgument(this.config.getComponents().containsKey(this.baselineProviderComponentKey));
      this.baselineValueProvider = (BaselineProvider) this.config.getComponents().get(this.baselineProviderComponentKey);
    } else {
      // default baseline provider, use wo1w
      this.baselineValueProvider = new RuleBaselineProvider();
      RuleBaselineProviderSpec spec = new RuleBaselineProviderSpec();
      spec.setOffset("wo1w");
      InputDataFetcher dataFetcher = new DefaultInputDataFetcher(this.provider, this.config.getId());
      this.baselineValueProvider.init(spec, dataFetcher);
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

    // inject detector to nested property if possible
    String detectorComponentKey = MapUtils.getString(config.getProperties(), PROP_DETECTOR);
    if (detectorComponentKey != null){
      this.detectorComponentName = DetectionUtils.getComponentName(detectorComponentKey);
      for (Map<String, Object> properties : this.nestedProperties){
        properties.put(PROP_DETECTOR, detectorComponentKey);
      }
    }

    // inject metricUrn to nested property if possible
    String nestedUrn = MapUtils.getString(config.getProperties(), PROP_METRIC_URN);
    if (nestedUrn != null){
      this.metricUrn = nestedUrn;
      for (Map<String, Object> properties : this.nestedProperties){
        properties.put(PROP_METRIC_URN, nestedUrn);
      }
    }
  }

  @Override
  protected List<MergedAnomalyResultDTO> merge(Collection<MergedAnomalyResultDTO> anomalies) {
    return this.fillCurrentAndBaselineValue(super.merge(anomalies));
  }

  @Override
  protected List<MergedAnomalyResultDTO> retrieveAnomaliesFromDatabase(List<MergedAnomalyResultDTO> generated) {
    AnomalySlice effectiveSlice = this.slice
        .withStart(this.getStartTime(generated) - this.maxGap - 1)
        .withEnd(this.getEndTime(generated) + this.maxGap + 1);

    Collection<MergedAnomalyResultDTO> retrieved = this.provider.fetchAnomalies(Collections.singleton(effectiveSlice), this.config.getId()).get(effectiveSlice);

    Collection<MergedAnomalyResultDTO> anomalies =
        Collections2.filter(retrieved,
                mergedAnomaly -> mergedAnomaly != null &&
                !mergedAnomaly.isChild() &&
                    // merge if only the anomaly generated by the same detector
                this.detectorComponentName.equals(mergedAnomaly.getProperties().getOrDefault(PROP_DETECTOR_COMPONENT_NAME, "")) &&
                    // merge if only the anomaly is in the same dimension
                this.metricUrn.equals(mergedAnomaly.getMetricUrn())
        );
    return new ArrayList<>(anomalies);
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
        MetricEntity me = MetricEntity.fromURN(metricUrn);
        long metricId = me.getId();
        MetricConfigDTO metricConfigDTO = this.provider.fetchMetrics(Collections.singletonList(metricId)).get(metricId);
        // aggregation function
        Series.DoubleFunction aggregationFunction = DoubleSeries.MEAN;

        try {
          aggregationFunction =
              BaselineAggregateType.valueOf(metricConfigDTO.getDefaultAggFunction().name()).getFunction();
        } catch (Exception e) {
          LOG.warn("cannot get aggregation function for metric, using average", metricId);
        }

        final MetricSlice slice = MetricSlice.from(metricId, anomaly.getStartTime(), anomaly.getEndTime(), me.getFilters());
        anomaly.setAvgCurrentVal(this.currentValueProvider.computePredictedAggregates(slice, aggregationFunction));
        if (this.baselineValueProvider != null) {
          anomaly.setAvgBaselineVal(this.baselineValueProvider.computePredictedAggregates(slice, aggregationFunction));
          anomaly.getProperties().put(PROP_BASELINE_PROVIDER_COMPONENT_NAME, this.baselineProviderComponentKey);
          anomaly.setWeight(calculateWeight(anomaly));
        }
      } catch (Exception e) {
        // ignore
        LOG.warn("cannot get current or baseline value for anomaly {}. ", anomaly, e);
      }
    }
    return mergedAnomalies;
  }

  private double calculateWeight(MergedAnomalyResultDTO anomaly) {
    return (anomaly.getAvgCurrentVal() - anomaly.getAvgBaselineVal()) / anomaly.getAvgBaselineVal();
  }
}
