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

package org.apache.pinot.thirdeye.detection.wrapper;

import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;
import java.util.HashMap;
import java.util.stream.Collectors;
import org.apache.pinot.thirdeye.dataframe.DoubleSeries;
import org.apache.pinot.thirdeye.dataframe.Series;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.DetectionUtils;
import org.apache.pinot.thirdeye.detection.DefaultInputDataFetcher;
import org.apache.pinot.thirdeye.detection.InputDataFetcher;
import org.apache.pinot.thirdeye.detection.algorithm.MergeWrapper;
import org.apache.pinot.thirdeye.detection.components.RuleBaselineProvider;
import org.apache.pinot.thirdeye.detection.spec.RuleBaselineProviderSpec;
import org.apache.pinot.thirdeye.detection.spi.components.BaselineProvider;
import org.apache.pinot.thirdeye.detection.spi.model.AnomalySlice;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;
import org.apache.pinot.thirdeye.rootcause.timeseries.BaselineAggregateType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
  private static final String DEFAULT_WOW_BASELINE_PROVIDER_NAME = "DEFAULT_WOW";

  private final BaselineProvider baselineValueProvider; // optionally configure a baseline value loader
  private final BaselineProvider currentValueProvider;
  private final String baselineProviderComponentName;
  private String detectorComponentName;
  private String metricUrn;
  private final Map<Long, MergedAnomalyResultDTO> existingAnomalies; // from id to anomalies

  public BaselineFillingMergeWrapper(DataProvider provider, DetectionConfigDTO config, long startTime, long endTime)
  {
    super(provider, config, startTime, endTime);

    if (config.getProperties().containsKey(PROP_BASELINE_PROVIDER)) {
      this.baselineProviderComponentName = DetectionUtils.getComponentName(MapUtils.getString(config.getProperties(), PROP_BASELINE_PROVIDER));
      Preconditions.checkArgument(this.config.getComponents().containsKey(this.baselineProviderComponentName));
      this.baselineValueProvider = (BaselineProvider) this.config.getComponents().get(this.baselineProviderComponentName);
    } else {
      // default baseline provider, use wo1w
      this.baselineValueProvider = new RuleBaselineProvider();
      this.baselineProviderComponentName = DEFAULT_WOW_BASELINE_PROVIDER_NAME;
      RuleBaselineProviderSpec spec = new RuleBaselineProviderSpec();
      spec.setOffset("wo1w");
      InputDataFetcher dataFetcher = new DefaultInputDataFetcher(this.provider, this.config.getId());
      this.baselineValueProvider.init(spec, dataFetcher);
    }

    if (config.getProperties().containsKey(PROP_CURRENT_PROVIDER)) {
      String detectorReferenceKey = DetectionUtils.getComponentName(MapUtils.getString(config.getProperties(), PROP_CURRENT_PROVIDER));
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
    this.existingAnomalies = new HashMap<>();
  }

  @Override
  protected List<MergedAnomalyResultDTO> merge(Collection<MergedAnomalyResultDTO> anomalies) {
    List<MergedAnomalyResultDTO> mergedAnomalies = super.merge(anomalies);

    // skip current & baseline filling if the anomaly is not merged
    this.fillCurrentAndBaselineValue(mergedAnomalies.stream()
        .filter(mergedAnomaly -> !isExistingAnomaly(this.existingAnomalies, mergedAnomaly)).collect(Collectors.toList()));

    return mergedAnomalies;
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
    for (MergedAnomalyResultDTO anomaly : anomalies) {
      if(anomaly.getId() != null) this.existingAnomalies.put(anomaly.getId(), copyAnomalyInfo(anomaly, new MergedAnomalyResultDTO()));
    }
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
          anomaly.getProperties().put(PROP_BASELINE_PROVIDER_COMPONENT_NAME, this.baselineProviderComponentName);
          anomaly.setWeight(calculateWeight(anomaly));
        }
      } catch (Exception e) {
        // ignore
        LOG.warn("cannot get current or baseline value for anomaly {}. ", anomaly, e);
      }
    }
    return mergedAnomalies;
  }

  // check if an anomaly is a existing anomaly and the duration is not modified
  protected boolean isExistingAnomaly(Map<Long, MergedAnomalyResultDTO> existingAnomalies, MergedAnomalyResultDTO anomaly) {
    if (!existingAnomalies.containsKey(anomaly.getId())) {
      return false;
    }
    MergedAnomalyResultDTO previousAnomaly = existingAnomalies.get(anomaly.getId());
    return anomaly.getStartTime() == previousAnomaly.getStartTime() && anomaly.getEndTime() == previousAnomaly.getEndTime();
  }

  private double calculateWeight(MergedAnomalyResultDTO anomaly) {
    return (anomaly.getAvgCurrentVal() - anomaly.getAvgBaselineVal()) / anomaly.getAvgBaselineVal();
  }
}
