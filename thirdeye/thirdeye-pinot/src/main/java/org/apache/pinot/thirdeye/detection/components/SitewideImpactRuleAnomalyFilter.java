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

package org.apache.pinot.thirdeye.detection.components;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.apache.pinot.thirdeye.dashboard.resources.v2.BaselineParsingUtils;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.detection.InputDataFetcher;
import org.apache.pinot.thirdeye.detection.Pattern;
import org.apache.pinot.thirdeye.detection.annotation.Components;
import org.apache.pinot.thirdeye.detection.annotation.DetectionTag;
import org.apache.pinot.thirdeye.detection.spec.SitewideImpactRuleAnomalyFilterSpec;
import org.apache.pinot.thirdeye.detection.spi.components.AnomalyFilter;
import org.apache.pinot.thirdeye.detection.spi.model.InputData;
import org.apache.pinot.thirdeye.detection.spi.model.InputDataSpec;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;
import org.apache.pinot.thirdeye.rootcause.timeseries.Baseline;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

/**
 * Site-wide impact anomaly filter
 */
@Components(type = "SITEWIDE_IMPACT_FILTER", tags = {DetectionTag.RULE_FILTER})
public class SitewideImpactRuleAnomalyFilter implements AnomalyFilter<SitewideImpactRuleAnomalyFilterSpec> {
  private double threshold;
  private InputDataFetcher dataFetcher;
  private Baseline baseline;
  private String siteWideMetricUrn;
  private Pattern pattern;

  @Override
  public boolean isQualified(MergedAnomalyResultDTO anomaly) {
    MetricEntity me = MetricEntity.fromURN(anomaly.getMetricUrn());
    List<MetricSlice> slices = new ArrayList<>();
    MetricSlice currentSlice = MetricSlice.from(me.getId(), anomaly.getStartTime(), anomaly.getEndTime(), me.getFilters());

    // customize baseline offset
    MetricSlice baselineSlice = null;
    if (baseline != null) {
      baselineSlice = this.baseline.scatter(currentSlice).get(0);
      slices.add(baselineSlice);
    }

    MetricSlice siteWideSlice;
    if (Strings.isNullOrEmpty(this.siteWideMetricUrn)) {
      // if global metric is not set
      MetricEntity siteWideEntity = MetricEntity.fromURN(anomaly.getMetricUrn());
      siteWideSlice = MetricSlice.from(siteWideEntity.getId(), anomaly.getStartTime(), anomaly.getEndTime());
    } else {
      MetricEntity siteWideEntity = MetricEntity.fromURN(this.siteWideMetricUrn);
      siteWideSlice = MetricSlice.from(siteWideEntity.getId(), anomaly.getStartTime(), anomaly.getEndTime(),
          siteWideEntity.getFilters());
    }
    slices.add(siteWideSlice);

    Map<MetricSlice, DataFrame> aggregates = this.dataFetcher.fetchData(
        new InputDataSpec().withAggregateSlices(slices))
        .getAggregates();

    double currentValue = anomaly.getAvgCurrentVal();
    double baselineValue = baseline == null ? anomaly.getAvgBaselineVal() :  this.baseline.gather(currentSlice, aggregates).getDouble(
        DataFrame.COL_VALUE, 0);
    double siteWideValue = getValueFromAggregates(siteWideSlice, aggregates);

    // if inconsistent with up/down, filter the anomaly
    if (!pattern.equals(Pattern.UP_OR_DOWN) && (currentValue < baselineValue && pattern.equals(Pattern.UP)) || (currentValue > baselineValue && pattern.equals(Pattern.DOWN))) {
      return false;
    }
    // if doesn't pass the threshold, filter the anomaly
    if (siteWideValue != 0
        && (Math.abs(currentValue - baselineValue) / siteWideValue) < this.threshold) {
      return false;
    }

    return true;
  }

  @Override
  public void init(SitewideImpactRuleAnomalyFilterSpec spec, InputDataFetcher dataFetcher) {
    this.dataFetcher = dataFetcher;
    this.threshold = spec.getThreshold();
    Preconditions.checkArgument(Math.abs(this.threshold) <= 1, "Site wide impact threshold should be less or equal than 1");

    this.pattern = Pattern.valueOf(spec.getPattern().toUpperCase());

    // customize baseline offset
    if (StringUtils.isNotBlank(spec.getOffset())){
      this.baseline = BaselineParsingUtils.parseOffset(spec.getOffset(), spec.getTimezone());
    }

    if (!Strings.isNullOrEmpty(spec.getSitewideCollection()) && !Strings.isNullOrEmpty(spec.getSitewideMetricName())) {
      // build filters
      Map<String, Collection<String>> filterMaps = spec.getFilters();
      Multimap<String, String> filters = ArrayListMultimap.create();
      if (filterMaps != null) {
        for (Map.Entry<String, Collection<String>> entry : filterMaps.entrySet()) {
          filters.putAll(entry.getKey(), entry.getValue());
        }
      }

      // build site wide metric Urn
      InputDataSpec.MetricAndDatasetName metricAndDatasetName =
          new InputDataSpec.MetricAndDatasetName(spec.getSitewideMetricName(), spec.getSitewideCollection());
      InputData data = this.dataFetcher.fetchData(
          new InputDataSpec().withMetricNamesAndDatasetNames(Collections.singletonList(metricAndDatasetName)));
      MetricConfigDTO metricConfigDTO = data.getMetricForMetricAndDatasetNames().get(metricAndDatasetName);
      MetricEntity me = MetricEntity.fromMetric(1.0, metricConfigDTO.getId(), filters);
      this.siteWideMetricUrn = me.getUrn();
    }
  }

  private double getValueFromAggregates(MetricSlice slice, Map<MetricSlice, DataFrame> aggregates) {
    return aggregates.get(slice).getDouble(DataFrame.COL_VALUE, 0);
  }
}
