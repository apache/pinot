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

package com.linkedin.thirdeye.detection.algorithm.stage;

import com.google.common.base.Strings;
import com.google.common.collect.ArrayListMultimap;
import com.linkedin.thirdeye.dashboard.resources.v2.BaselineParsingUtils;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.annotation.Components;
import com.linkedin.thirdeye.detection.annotation.DetectionTag;
import com.linkedin.thirdeye.rootcause.impl.MetricEntity;
import com.linkedin.thirdeye.rootcause.timeseries.Baseline;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.apache.commons.collections.MapUtils;

import static com.linkedin.thirdeye.dataframe.util.DataFrameUtils.*;


/**
 * This filter stage filters the anomalies if either the absolute changeThreshold, percentage changeThreshold or site wide impact does not pass the threshold.
 */
@Components(title = "Baseline Filter",
    type = "BUSINESS_RULE_FILTER",
    tags = {DetectionTag.RULE_FILTER},
    description = "Baseline rule filter. Filters the anomalies if percentage change, absolute difference or site wide impact is below certain threshold.")
public class BaselineRuleFilterStage implements AnomalyFilterStage {
  private static final String PROP_CHANGE = "changeThreshold";
  private static final double PROP_CHANGE_DEFAULT = Double.NaN;

  private static final String PROP_OFFSET = "offset";
  private static final String PROP_OFFSET_DEFAULT = "median1w";

  private static final String PROP_DIFFERENCE = "differenceThreshold";
  private static final double PROP_DIFFERENCE_DEFAULT = Double.NaN;

  private static final String PROP_TIMEZONE_DEFAULT = "UTC";
  private static final String PROP_TIMEZONE = "timezone";

  private static final String PROP_SITEWIDE_METRIC = "siteWideMetricUrn";
  private static final String PROP_SITEWIDE_THRESHOLD = "siteWideImpactThreshold";
  private static final double PROP_SITEWIDE_THRESHOLD_DEFAULT = Double.NaN;

  private Baseline baseline;
  private double changeThreshold;
  private double differenceThreshold;
  private double siteWideImpactThreshold;
  private String siteWideMetricUrn;

  public boolean isQualified(MergedAnomalyResultDTO anomaly, DataProvider provider) {
    MetricEntity me = MetricEntity.fromURN(anomaly.getMetricUrn());
    MetricSlice currentSlice =
        MetricSlice.from(me.getId(), anomaly.getStartTime(), anomaly.getEndTime(), me.getFilters());
    MetricSlice baselineSlice = this.baseline.scatter(currentSlice).get(0);

    Map<MetricSlice, DataFrame> aggregates = provider.fetchAggregates(Arrays.asList(currentSlice, baselineSlice), Collections.<String>emptyList());
    double currentValue = getValueFromAggregates(currentSlice, aggregates);
    double baselineValue = getValueFromAggregates(baselineSlice, aggregates);
    if (!Double.isNaN(this.differenceThreshold) && Math.abs(currentValue - baselineValue) < this.differenceThreshold) {
      return false;
    }
    if (!Double.isNaN(this.changeThreshold) && baselineValue != 0 && Math.abs(currentValue / baselineValue - 1) < this.changeThreshold) {
      return false;
    }
    if (!Double.isNaN(this.siteWideImpactThreshold)) {
      String siteWideImpactMetricUrn = Strings.isNullOrEmpty(this.siteWideMetricUrn) ? anomaly.getMetricUrn() : this.siteWideMetricUrn;
      MetricEntity siteWideEntity = MetricEntity.fromURN(siteWideImpactMetricUrn).withFilters(ArrayListMultimap.<String, String>create());
      MetricSlice siteWideSlice = this.baseline.scatter(
          MetricSlice.from(siteWideEntity.getId(), anomaly.getStartTime(), anomaly.getEndTime())).get(0);
      double siteWideBaselineValue = getValueFromAggregates(siteWideSlice,
          provider.fetchAggregates(Collections.singleton(siteWideSlice), Collections.<String>emptyList()));

      if (siteWideBaselineValue != 0 && (Math.abs(currentValue - baselineValue) / siteWideBaselineValue) < this.siteWideImpactThreshold) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void init(Map<String, Object> properties, Long configId, long startTime, long endTime) {
    String offset = MapUtils.getString(properties, PROP_OFFSET, PROP_OFFSET_DEFAULT);
    String timezone = MapUtils.getString(properties, PROP_TIMEZONE, PROP_TIMEZONE_DEFAULT);
    this.baseline = BaselineParsingUtils.parseOffset(offset, timezone);
    // percentage changeThreshold
    this.changeThreshold = MapUtils.getDoubleValue(properties, PROP_CHANGE, PROP_CHANGE_DEFAULT);
    // absolute changeThreshold
    this.differenceThreshold = MapUtils.getDoubleValue(properties, PROP_DIFFERENCE, PROP_DIFFERENCE_DEFAULT);
    // site wide impact
    this.siteWideImpactThreshold = MapUtils.getDoubleValue(properties, PROP_SITEWIDE_THRESHOLD, PROP_SITEWIDE_THRESHOLD_DEFAULT);
    this.siteWideMetricUrn = MapUtils.getString(properties, PROP_SITEWIDE_METRIC);
  }


  double getValueFromAggregates(MetricSlice slice, Map<MetricSlice, DataFrame> aggregates) {
    return aggregates.get(slice).getDouble(COL_VALUE, 0);
  }
}
