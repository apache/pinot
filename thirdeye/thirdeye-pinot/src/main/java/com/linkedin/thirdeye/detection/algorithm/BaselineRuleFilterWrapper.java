/**
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

import com.google.common.base.Strings;
import com.google.common.collect.ArrayListMultimap;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.rootcause.impl.MetricEntity;
import com.linkedin.thirdeye.rootcause.timeseries.Baseline;
import com.linkedin.thirdeye.rootcause.timeseries.BaselineAggregate;
import com.linkedin.thirdeye.rootcause.timeseries.BaselineAggregateType;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.apache.commons.collections.MapUtils;
import org.joda.time.DateTimeZone;

import static com.linkedin.thirdeye.dataframe.util.DataFrameUtils.*;


/**
 * This filter wrapper filters the anomalies if either the absolute change, percentage change or site wide impact does not pass the threshold.
 */
public class BaselineRuleFilterWrapper extends RuleBasedFilterWrapper {
  private static final String PROP_WEEKS = "weeks";
  private static final int PROP_WEEKS_DEFAULT = 1;

  private static final String PROP_CHANGE = "change";
  private static final double PROP_CHANGE_DEFAULT = Double.NaN;

  private static final String PROP_DIFFERENCE = "difference";
  private static final double PROP_DIFFERENCE_DEFAULT = Double.NaN;

  private static final String PROP_TIMEZONE = "timezone";
  private static final String PROP_TIMEZONE_DEFAULT = "UTC";

  private static final String PROP_SITEWIDE_METRIC = "siteWideMetricUrn";
  private static final String PROP_SITEWIDE_THRESHOLD = "siteWideImpactThreshold";
  private static final double PROP_SITEWIDE_THRESHOLD_DEFAULT = Double.NaN;

  private Baseline baseline;
  private double change;
  private double difference;
  private double siteWideImpactThreshold;
  private String siteWideMetricUrn;

  public BaselineRuleFilterWrapper(DataProvider provider, DetectionConfigDTO config, long startTime, long endTime) {
    super(provider, config, startTime, endTime);
    int weeks = MapUtils.getIntValue(config.getProperties(), PROP_WEEKS, PROP_WEEKS_DEFAULT);
    DateTimeZone timezone =
        DateTimeZone.forID(MapUtils.getString(this.config.getProperties(), PROP_TIMEZONE, PROP_TIMEZONE_DEFAULT));
    this.baseline = BaselineAggregate.fromWeekOverWeek(BaselineAggregateType.MEDIAN, weeks, 1, timezone);
    // percentage change
    this.change = MapUtils.getDoubleValue(config.getProperties(), PROP_CHANGE, PROP_CHANGE_DEFAULT);
    // absolute change
    this.difference = MapUtils.getDoubleValue(config.getProperties(), PROP_DIFFERENCE, PROP_DIFFERENCE_DEFAULT);
    // site wide impact
    this.siteWideImpactThreshold = MapUtils.getDoubleValue(config.getProperties(), PROP_SITEWIDE_THRESHOLD, PROP_SITEWIDE_THRESHOLD_DEFAULT);
    this.siteWideMetricUrn = MapUtils.getString(config.getProperties(), PROP_SITEWIDE_METRIC);
  }

  @Override
  boolean isQualified(MergedAnomalyResultDTO anomaly) {
    MetricEntity me = MetricEntity.fromURN(anomaly.getMetricUrn());
    MetricSlice currentSlice =
        MetricSlice.from(me.getId(), anomaly.getStartTime(), anomaly.getEndTime(), me.getFilters());
    MetricSlice baselineSlice = this.baseline.scatter(currentSlice).get(0);

    Map<MetricSlice, DataFrame> aggregates = this.provider.fetchAggregates(Arrays.asList(currentSlice, baselineSlice), Collections.<String>emptyList());
    double currentValue = getValueFromAggregates(currentSlice, aggregates);
    double baselineValue = getValueFromAggregates(baselineSlice, aggregates);
    if (!Double.isNaN(this.difference) && Math.abs(currentValue - baselineValue) < this.difference) {
      return false;
    }
    if (!Double.isNaN(this.change) && baselineValue != 0 && Math.abs(currentValue / baselineValue - 1) < this.change) {
      return false;
    }
    if (!Double.isNaN(this.siteWideImpactThreshold)) {
      String siteWideImpactMetricUrn = Strings.isNullOrEmpty(this.siteWideMetricUrn) ? anomaly.getMetricUrn() : this.siteWideMetricUrn;
      MetricEntity siteWideEntity = MetricEntity.fromURN(siteWideImpactMetricUrn).withFilters(ArrayListMultimap.<String, String>create());
      MetricSlice siteWideSlice = this.baseline.scatter(
          MetricSlice.from(siteWideEntity.getId(), anomaly.getStartTime(), anomaly.getEndTime(), me.getFilters())).get(0);
      double siteWideBaselineValue = getValueFromAggregates(siteWideSlice,
          this.provider.fetchAggregates(Collections.singleton(siteWideSlice), Collections.<String>emptyList()));

      if (siteWideBaselineValue != 0 && (Math.abs(currentValue - baselineValue) / siteWideBaselineValue) < this.siteWideImpactThreshold) {
        return false;
      }
    }
    return true;
  }

  double getValueFromAggregates(MetricSlice slice, Map<MetricSlice, DataFrame> aggregates) {
    return aggregates.get(slice).getDouble(COL_VALUE, 0);
  }
}
