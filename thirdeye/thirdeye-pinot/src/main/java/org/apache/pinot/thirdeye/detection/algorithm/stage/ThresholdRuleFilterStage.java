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

package org.apache.pinot.thirdeye.detection.algorithm.stage;

import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.annotation.Components;
import org.apache.pinot.thirdeye.detection.annotation.Param;
import org.apache.pinot.thirdeye.detection.annotation.DetectionTag;
import org.apache.pinot.thirdeye.detection.annotation.PresentationOption;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;
import java.util.Collections;
import java.util.Map;
import org.apache.commons.collections4.MapUtils;

import static org.apache.pinot.thirdeye.dataframe.util.DataFrameUtils.*;


/**
 * This threshold rule filter stage filters the anomalies if either the min or max thresholds do not pass.
 */
@Components(title = "Aggregate Threshold Filter",
    type = "THRESHOLD_RULE_FILTER",
    tags = {DetectionTag.RULE_FILTER},
    description = "Threshold rule filter. filters the anomalies if either the min or max thresholds do not satisfied.",
    presentation = {
        @PresentationOption(
            name = "absolute value",
            description = "aggregated absolute value within a time period",
            template = "is between ${min} and ${max}"
        )},
    params = {@Param(name = "min", placeholder = "value"), @Param(name = "max", placeholder = "value")}
)
public class ThresholdRuleFilterStage implements AnomalyFilterStage {
    private static final String PROP_MIN = "min";
    private static final double PROP_MIN_DEFAULT = Double.NaN;

    private static final String PROP_MAX = "max";
    private static final double PROP_MAX_DEFAULT = Double.NaN;

    private double min;
    private double max;

    @Override
    public boolean isQualified(MergedAnomalyResultDTO anomaly, DataProvider provider) {
      MetricEntity me = MetricEntity.fromURN(anomaly.getMetricUrn());
      MetricSlice currentSlice =
          MetricSlice.from(me.getId(), anomaly.getStartTime(), anomaly.getEndTime(), me.getFilters());

      Map<MetricSlice, DataFrame> aggregates =
          provider.fetchAggregates(Collections.singleton(currentSlice), Collections.<String>emptyList());
      double currentValue = getValueFromAggregates(currentSlice, aggregates);
      if (!Double.isNaN(this.min) && currentValue < this.min) {
        return false;
      }
      if (!Double.isNaN(this.max) && currentValue > this.max) {
        return false;
      }
      return true;
    }

    @Override
    public void init(Map<String, Object> properties, Long configId, long startTime, long endTime) {
      this.min = MapUtils.getDoubleValue(properties, PROP_MIN, PROP_MIN_DEFAULT);
      this.max = MapUtils.getDoubleValue(properties, PROP_MAX, PROP_MAX_DEFAULT);
    }

    double getValueFromAggregates(MetricSlice slice, Map<MetricSlice, DataFrame> aggregates) {
      return aggregates.get(slice).getDouble(COL_VALUE, 0);
    }
}
