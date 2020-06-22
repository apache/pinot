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

import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.thirdeye.common.utils.MetricUtils;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.detection.InputDataFetcher;
import org.apache.pinot.thirdeye.detection.annotation.Components;
import org.apache.pinot.thirdeye.detection.annotation.DetectionTag;
import org.apache.pinot.thirdeye.detection.spec.ThresholdRuleFilterSpec;
import org.apache.pinot.thirdeye.detection.spi.components.AnomalyFilter;
import org.apache.pinot.thirdeye.detection.spi.model.InputDataSpec;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;
import org.joda.time.Interval;


/**
 * This threshold rule filter stage filters the anomalies if either the min or max thresholds do not pass.
 */
@Components(title = "Aggregate Threshold Filter", type = "THRESHOLD_RULE_FILTER", tags = {
    DetectionTag.RULE_FILTER}, description = "Threshold rule filter. filters the anomalies if either the min or max thresholds do not satisfied.")
public class ThresholdRuleAnomalyFilter implements AnomalyFilter<ThresholdRuleFilterSpec> {
  private double minValueHourly;
  private double maxValueHourly;
  private double minValueDaily;
  private double maxValueDaily;
  private double maxValue;
  private double minValue;
  private InputDataFetcher dataFetcher;

  @Override
  public boolean isQualified(MergedAnomalyResultDTO anomaly) {
    MetricEntity me = MetricEntity.fromURN(anomaly.getMetricUrn());
    MetricConfigDTO metric = dataFetcher.fetchData(new InputDataSpec().withMetricIds(Collections.singleton(me.getId())))
        .getMetrics()
        .get(me.getId());
    double currentValue = anomaly.getAvgCurrentVal();

    Interval anomalyInterval = new Interval(anomaly.getStartTime(), anomaly.getEndTime());

    // apply multiplier if the metric is aggregated by SUM or COUNT
    double hourlyMultiplier = MetricUtils.isAggCumulative(metric) ?
        (TimeUnit.HOURS.toMillis(1) / (double) anomalyInterval.toDurationMillis()) : 1.0;
    double dailyMultiplier = MetricUtils.isAggCumulative(metric) ?
        (TimeUnit.DAYS.toMillis(1) / (double) anomalyInterval.toDurationMillis()) : 1.0;

    if (!Double.isNaN(this.minValue) && currentValue < this.minValue
        || !Double.isNaN(this.maxValue) && currentValue > this.maxValue) {
      return false;
    }
    if (!Double.isNaN(this.minValueHourly) && currentValue * hourlyMultiplier < this.minValueHourly) {
      return false;
    }
    if (!Double.isNaN(this.maxValueHourly) && currentValue * hourlyMultiplier > this.maxValueHourly) {
      return false;
    }
    if (!Double.isNaN(this.minValueDaily) && currentValue * dailyMultiplier < this.minValueDaily) {
      return false;
    }
    if (!Double.isNaN(this.maxValueDaily) && currentValue * dailyMultiplier > this.maxValueDaily) {
      return false;
    }
    return true;
  }

  @Override
  public void init(ThresholdRuleFilterSpec spec, InputDataFetcher dataFetcher) {
    this.minValueHourly = spec.getMinValueHourly();
    this.maxValueHourly = spec.getMaxValueHourly();
    this.minValueDaily = spec.getMinValueDaily();
    this.maxValueDaily = spec.getMaxValueDaily();
    this.maxValue = spec.getMaxValue();
    this.minValue = spec.getMinValue();
    this.dataFetcher = dataFetcher;
  }
}
