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

import org.apache.pinot.thirdeye.dashboard.resources.v2.BaselineParsingUtils;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.detection.InputDataFetcher;
import org.apache.pinot.thirdeye.detection.Pattern;
import org.apache.pinot.thirdeye.detection.annotation.Components;
import org.apache.pinot.thirdeye.detection.annotation.DetectionTag;
import org.apache.pinot.thirdeye.detection.spec.PercentageChangeRuleAnomalyFilterSpec;
import org.apache.pinot.thirdeye.detection.spi.components.AnomalyFilter;
import org.apache.pinot.thirdeye.detection.spi.model.InputDataSpec;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;
import org.apache.pinot.thirdeye.rootcause.timeseries.Baseline;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Percentage change anomaly filter. Check if the anomaly's percentage change compared to baseline is above the threshold.
 * If not, filters the anomaly.
 */
@Components(type = "PERCENTAGE_CHANGE_FILTER", tags = {DetectionTag.RULE_FILTER})
public class PercentageChangeRuleAnomalyFilter implements AnomalyFilter<PercentageChangeRuleAnomalyFilterSpec> {
  private static final Logger LOG = LoggerFactory.getLogger(PercentageChangeRuleAnomalyFilter.class);
  private double threshold;
  private double upThreshold;
  private double downThreshold;
  private InputDataFetcher dataFetcher;
  private Baseline baseline;
  private Pattern pattern;

  @Override
  public boolean isQualified(MergedAnomalyResultDTO anomaly) {
    MetricEntity me = MetricEntity.fromURN(anomaly.getMetricUrn());
    List<MetricSlice> slices = new ArrayList<>();
    MetricSlice currentSlice = MetricSlice.from(me.getId(), anomaly.getStartTime(), anomaly.getEndTime(), me.getFilters());
    // customize baseline offset
    if (baseline != null) {
      slices.addAll(this.baseline.scatter(currentSlice));
    }

    Map<MetricSlice, DataFrame> aggregates =
        this.dataFetcher.fetchData(new InputDataSpec().withAggregateSlices(slices)).getAggregates();

    double currentValue = anomaly.getAvgCurrentVal();
    double baselineValue;
    if (baseline == null) {
      baselineValue = anomaly.getAvgBaselineVal();
    } else {
      try {
        baselineValue = this.baseline.gather(currentSlice, aggregates).getDouble(DataFrame.COL_VALUE, 0);
      } catch (Exception e) {
        baselineValue = anomaly.getAvgBaselineVal();
        LOG.warn("Unable to fetch baseline for anomaly {}. start = {} end = {} filters = {}. Using anomaly"
            + " baseline ", anomaly.getId(), anomaly.getStartTime(), anomaly.getEndTime(), me.getFilters(), e);
      }
    }

    // if inconsistent with up/down, filter the anomaly
    if (!pattern.equals(Pattern.UP_OR_DOWN) && (currentValue < baselineValue && pattern.equals(Pattern.UP)) || (
        currentValue > baselineValue && pattern.equals(Pattern.DOWN))) {
      return false;
    }

    double percentageChange = Math.abs(currentValue / baselineValue - 1);
    if (currentValue < baselineValue) {
      double downThreshold = Double.isNaN(this.downThreshold) ? this.threshold : this.downThreshold;
      return Double.compare(downThreshold, percentageChange) <= 0;
    } else {
      double upThreshold = Double.isNaN(this.upThreshold) ? this.threshold : this.upThreshold;
      return Double.compare(upThreshold, percentageChange) <= 0;
    }
  }

  @Override
  public void init(PercentageChangeRuleAnomalyFilterSpec spec, InputDataFetcher dataFetcher) {
    this.dataFetcher = dataFetcher;
    this.pattern = Pattern.valueOf(spec.getPattern().toUpperCase());
    // customize baseline offset
    if (StringUtils.isNotBlank(spec.getOffset())) {
      this.baseline = BaselineParsingUtils.parseOffset(spec.getOffset(), spec.getTimezone());
    }
    this.threshold = spec.getThreshold();
    this.upThreshold = spec.getUpThreshold();
    this.downThreshold = spec.getDownThreshold();
  }
}
