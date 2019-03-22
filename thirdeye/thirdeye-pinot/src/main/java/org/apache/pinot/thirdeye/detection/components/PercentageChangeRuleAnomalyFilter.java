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
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.thirdeye.dataframe.util.DataFrameUtils.*;

/**
 * Percentage change anomaly filter. Check if the anomaly's percentage change compared to baseline is above the threshold.
 * If not, filters the anomaly.
 */
@Components(type = "PERCENTAGE_CHANGE_FILTER", tags = {DetectionTag.RULE_FILTER})
public class PercentageChangeRuleAnomalyFilter implements AnomalyFilter<PercentageChangeRuleAnomalyFilterSpec> {
  private static final Logger LOG = LoggerFactory.getLogger(PercentageChangeRuleAnomalyFilter.class);
  private double threshold;
  private InputDataFetcher dataFetcher;
  private Baseline baseline;
  private Pattern pattern;

  @Override
  public boolean isQualified(MergedAnomalyResultDTO anomaly) {
    MetricEntity me = MetricEntity.fromURN(anomaly.getMetricUrn());
    List<MetricSlice> slices = new ArrayList<>();
    MetricSlice currentSlice =
        MetricSlice.from(me.getId(), anomaly.getStartTime(), anomaly.getEndTime(), me.getFilters());
    slices.add(currentSlice);

    // customize baseline offset
    MetricSlice baselineSlice = null;
    if (baseline != null) {
      baselineSlice = this.baseline.scatter(currentSlice).get(0);
      slices.add(baselineSlice);
    }

    Map<MetricSlice, DataFrame> aggregates =
        this.dataFetcher.fetchData(new InputDataSpec().withAggregateSlices(slices)).getAggregates();

    double currentValue;
    if (aggregates.get(currentSlice).isEmpty()) {
      currentValue = anomaly.getAvgCurrentVal();
    } else {
      currentValue = getValueFromAggregates(currentSlice, aggregates);
    }

    double baselineValue;
    if (baselineSlice == null) {
      baselineValue = anomaly.getAvgBaselineVal();
    } else if (aggregates.get(baselineSlice).isEmpty()) {
      baselineValue = anomaly.getAvgBaselineVal();
      LOG.warn("Unable to fetch data for baseline slice for anomaly {}. start = {} end = {} filters = {}. Using anomaly"
              + " baseline ", anomaly.getId(), anomaly.getStartTime(), anomaly.getEndTime(), me.getFilters());
    } else {
      baselineValue = getValueFromAggregates(baselineSlice, aggregates);
    }

    // if inconsistent with up/down, filter the anomaly
    if (!pattern.equals(Pattern.UP_OR_DOWN) && (currentValue < baselineValue && pattern.equals(Pattern.UP)) || (
        currentValue > baselineValue && pattern.equals(Pattern.DOWN))) {
      return false;
    }
    if (baselineValue != 0 && Math.abs(currentValue / baselineValue - 1) < this.threshold) {
      return false;
    }
    return true;
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
  }

  private double getValueFromAggregates(MetricSlice slice, Map<MetricSlice, DataFrame> aggregates) {
    return aggregates.get(slice).getDouble(COL_VALUE, 0);
  }
}
