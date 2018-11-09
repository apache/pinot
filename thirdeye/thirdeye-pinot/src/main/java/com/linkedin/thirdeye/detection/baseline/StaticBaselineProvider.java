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

package com.linkedin.thirdeye.detection.baseline;

import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.DoubleSeries;
import com.linkedin.thirdeye.dataframe.Series;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.wrapper.DetectionUtils;
import com.linkedin.thirdeye.detection.spi.model.InputData;
import com.linkedin.thirdeye.detection.spi.model.InputDataSpec;
import com.linkedin.thirdeye.rootcause.timeseries.BaselineAggregateType;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.collections.MapUtils;

import static com.linkedin.thirdeye.dataframe.util.DataFrameUtils.*;


/**
 * The static baseline provider. Higher level interfaces.
 */
public abstract class StaticBaselineProvider implements BaselineProvider {
  Series.DoubleFunction aggregationFunction = DoubleSeries.MEAN;

  @Override
  public void init(Map<String, Object> properties) {
    this.aggregationFunction = BaselineAggregateType.valueOf(MapUtils.getString(properties, "metricFunction", "MEAN")).getFunction();
  }

  /**
   * compute baseline times eries for the metric slices.
   * The DataFrame should includes one Long series called COL_TIME = "timestamp" and a double series called COL_VALUE = "value"
   * @param slices slices to compute the baseline time series for.
   * @param data input data as defined by getInputDataSpec method.
   * @return the mapping of the metric slice to its time series data frame.
   */
  public abstract Map<MetricSlice, DataFrame> computeBaselineTimeSeries(Collection<MetricSlice> slices, InputData data);

  /**
   * The input data spec to describe what data fetch. The data will be feed in to the
   * @see StaticBaselineProvider#computeBaselineTimeSeries method
   * @param slices slices to compute the baseline time series for.
   * @return the input data spec to describe what time series, anomalies, etc. to fetch.
   */
  abstract InputDataSpec getInputDataSpec(Collection<MetricSlice> slices);

  /**
   * To compute the baseline aggregate values for each metric slice. Optionally override this method.
   * By default calls compute baseline time series then aggregate to one value based on the aggregation function.
   * The input data are defined by
   * @see StaticBaselineProvider#getAggregateInputDataSpec(Collection<MetricSlice>) method.
   *
   * @param slices the metric slices
   * @param data input data as defined by getAggregateInputDataSpec method.
   * @return the mapping of metric slice to its aggregate values
   */
  public Map<MetricSlice, Double> computeBaselineAggregates(Collection<MetricSlice> slices, InputData data) {
    Map<MetricSlice, DataFrame> baselineTimeSeries = this.computeBaselineTimeSeries(slices, data);
    Map<MetricSlice, Double> baselineAggregates = new HashMap<>();
    for (MetricSlice slice : slices) {
      DataFrame ts = baselineTimeSeries.get(slice);
      baselineAggregates.put(slice, ts.getDoubles(COL_VALUE).aggregate(this.aggregationFunction).getDouble(0));
    }
    return baselineAggregates;
  }

  /**
   * The input data spec to describe what data fetch. The data will be feed in to the
   * @see StaticBaselineProvider#computeBaselineAggregates method.
   * Optionally override this method. By default calls the
   * @see StaticBaselineProvider#getInputDataSpec method to get a data spec.
   * @param slices slices to compute the baseline time series for.
   * @return the input data spec to describe what time series, anomalies, etc. for the framework to fetch.
   */
  InputDataSpec getAggregateInputDataSpec(Collection<MetricSlice> slices) {
    return this.getInputDataSpec(slices);
  }

  @Override
  public final Map<MetricSlice, DataFrame> computeBaselineTimeSeries(Collection<MetricSlice> slices, DataProvider provider) {
    return this.computeBaselineTimeSeries(slices, DetectionUtils.getDataForSpec(provider, this.getInputDataSpec(slices), -1));
  }

  @Override
  public final Map<MetricSlice, Double> computeBaselineAggregates(Collection<MetricSlice> slices, DataProvider provider) {
    return this.computeBaselineAggregates(slices, DetectionUtils.getDataForSpec(provider, this.getAggregateInputDataSpec(slices), -1));
  }
}
